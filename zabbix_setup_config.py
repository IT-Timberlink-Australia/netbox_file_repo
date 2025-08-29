from dcim.models import Device, Site, Platform
from virtualization.models import VirtualMachine
from extras.models import CustomField
from extras.scripts import Script, BooleanVar, ObjectVar
from django.apps import apps
from django.db import transaction
from django.db.models import ManyToManyField

# ---------------- Config you provided ----------------
# Your COT is "zabbix-template-list" (27 rows) and each row has:
# - template_name (str)
# - template_id (str/number)
# - template_interface_id (str/number/None)
# - platform (M2M -> dcim.Platform)
COT_TYPE_HUMAN_NAME = "zabbix-template-list"   # used only for logs

# Keys we expect on the catalog model (real columns)
COT_NAME_COL   = "template_name"
COT_ID_COL     = "template_id"
COT_IFACE_COL  = "template_interface_id"
COT_PLATFORM_M2M = "platform"  # M2M to dcim.Platform


class ZabbixConfigFromCOT(Script):
    """
    Zabbix readiness + mappings from Custom Object "zabbix-template-list" (no groups, no proxy)

    Step 1 (gate): require cf.mon_req is True AND status == 'active'
       - If FALSE: set cf.mon_req=False, cf.monitoring_status="Missing Required Fields", SKIP

    Step 2 (gate): require platform, primary IPv4, site, environment, name, description
       - If missing: cf.monitoring_status="Missing Required Fields", SKIP

    Step 3 (only if passed Step 1 & 2):
       3.0) TEMPLATE SYNC (primary by PLATFORM, with name fallback)
            - cf.zabbix_template_name <- COT.template_name
            - cf.zabbix_template_int_id <- COT.template_interface_id
            - Build cf.zabbix_template_id as CSV: [primary_id] + extras from cf.zabbix_extra_templates
              (dedup, preserve order). Respect overwrite toggle (default: only fill blanks).
       3.1) SLA from role.custom_fields.sla_report_code (respect overwrite toggle)

    Final: Evaluate readiness and set
       - cf.zabbix_status = True when ready else False
       - cf.monitoring_status = "In-progress" when ready, or leaves the earlier "Missing Required Fields"
    """

    class Meta:
        name = "Zabbix: COT Template + SLA + Readiness (platform primary)"
        description = "Loads Custom Objects entries for 'zabbix-template-list', maps primary template by Platform, extras to IDs, SLA, readiness."
        commit_default = True

    # Scope
    include_devices = BooleanVar(default=True, description="Process Devices")
    include_vms     = BooleanVar(default=True, description="Process Virtual Machines")
    limit_site      = ObjectVar(model=Site, required=False, description="Limit by Site (Devices DB-filter; VMs soft-filter)")

    # Behavior
    overwrite_all_mapped_fields = BooleanVar(
        default=False,
        description="Overwrite template/SLA fields even if already set (default only fills blanks)."
    )
    dry_run = BooleanVar(default=False, description="Force dry run (ignore Meta.commit)")

    # ---------------- helpers (generic) ----------------
    def _norm_str(self, v):
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            return s if s else None
        return str(v).strip() or None

    def _is_true(self, v) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return v == 1
        if isinstance(v, str):
            return v.strip().lower() in ("true", "1", "yes", "y", "on")
        return False

    def _cf_dict(self, obj):
        return dict(getattr(obj, "custom_field_data", {}) or {})

    def _has_primary_v4(self, obj):
        return getattr(obj, "primary_ip4_id", None) is not None

    def _desc(self, obj):
        return self._norm_str(getattr(obj, "description", None)) or self._norm_str(getattr(obj, "comments", None))

    def _get_role(self, obj):
        return getattr(obj, "device_role", None) or getattr(obj, "role", None)

    # ---------------- catalog model discovery ----------------
    def _select_catalog_model(self):
        """
        Choose the netbox_custom_objects entries model that looks like the REST payload:
        - app_label == 'netbox_custom_objects'
        - has real columns 'template_name', 'template_id', 'template_interface_id'
        - has an M2M field named 'platform' to dcim.Platform
        - prefer row count close to 27 (for sanity with your current data)
        """
        best = None
        best_score = -1
        best_rows = 0

        for Model in apps.get_models():
            if Model._meta.app_label != "netbox_custom_objects":
                continue
            name = Model.__name__.lower()
            if "customobjecttype" in name:
                # skip the type-definition table
                continue

            # confirm required columns exist
            fields = {f.name: f for f in Model._meta.get_fields() if hasattr(f, "name")}
            has_cols = all(col in fields for col in (COT_NAME_COL, COT_ID_COL, COT_IFACE_COL))
            if not has_cols:
                continue

            # confirm 'platform' is an M2M to dcim.Platform
            plat_field = fields.get(COT_PLATFORM_M2M)
            is_m2m_to_platform = False
            if isinstance(plat_field, ManyToManyField):
                rel_model = plat_field.remote_field.model
                is_m2m_to_platform = (
                    getattr(rel_model, "_meta", None)
                    and rel_model._meta.app_label == "dcim"
                    and rel_model._meta.model_name == "platform"
                )

            if not is_m2m_to_platform:
                continue

            # looks promising; score it
            try:
                rows = Model.objects.count()
            except Exception:
                rows = 0
            # score: strong bonus for exact shape + how close to 27 rows
            score = 100 + (30 - abs(rows - 27))
            if score > best_score:
                best, best_score, best_rows = Model, score, rows

        if not best:
            raise RuntimeError(
                "Could not locate the Custom Objects entries model for "
                f"'{COT_TYPE_HUMAN_NAME}'. Ensure the model has "
                f"columns '{COT_NAME_COL}', '{COT_ID_COL}', '{COT_IFACE_COL}' "
                f"and an M2M '{COT_PLATFORM_M2M}' to dcim.Platform."
            )

        self.log_info(
            f"Catalog model selected: {best._meta.label} (rows={best_rows}) "
            f"for '{COT_TYPE_HUMAN_NAME}'."
        )
        return best

    def _load_catalog(self):
        """
        Build:
          - name_to_id:    lower(name) -> template_id (int where possible)
          - name_to_iface: lower(name) -> template_interface_id (int where possible)
          - by_platform:   platform_pk -> (template_name, template_id, template_interface_id)
        """
        Entry = self._select_catalog_model()

        name_to_id = {}
        name_to_iface = {}
        by_platform = {}

        has_iface_numeric = False
        scanned = 0

        for row in Entry.objects.all():
            scanned += 1
            nm = self._norm_str(getattr(row, COT_NAME_COL, None))
            if not nm:
                continue
            key = nm.lower()

            tid = getattr(row, COT_ID_COL, None)
            try:
                tid = int(tid)
            except Exception:
                pass
            name_to_id[key] = tid

            iid = getattr(row, COT_IFACE_COL, None)
            try:
                iid = int(iid)
                has_iface_numeric = True
            except Exception:
                # allow None or a non-numeric label if present
                pass
            name_to_iface[key] = iid

            # map each linked Platform to this (name,id,iface)
            try:
                for p in getattr(row, COT_PLATFORM_M2M).all():
                    pk = getattr(p, "pk", None)
                    if pk is not None and pk not in by_platform:
                        by_platform[pk] = (nm, tid, iid)
            except Exception:
                # if the M2M isn't populated, we just won't have platform-based matches for those rows
                pass

        iface_map = name_to_iface if has_iface_numeric or any(v not in (None, "") for v in name_to_iface.values()) else None

        self.log_info(
            f"COT catalog loaded from {Entry._meta.label} "
            f"(rows_scanned={scanned}, mapped_names={len(name_to_id)}, "
            f"iface={'yes' if iface_map else 'no'}, platforms={len(by_platform)})"
        )
        return name_to_id, iface_map, by_platform

    # ---------------- SLA / readiness ----------------
    def _ensure_sla_code(self, obj, cf, counters, overwrite=False):
        cur = self._norm_str(cf.get("sla_report_code"))
        if cur and not overwrite:
            return cf, False, None
        role = self._get_role(obj)
        if not role:
            counters["sla_no_role"] += 1
            return cf, False, "no_role"
        role_code = self._norm_str((getattr(role, "custom_field_data", {}) or {}).get("sla_report_code"))
        if not role_code:
            counters["sla_role_no_code"] += 1
            return cf, False, "role_no_code"
        cf["sla_report_code"] = role_code
        counters["sla_added"] += 1
        return cf, True, None

    def _evaluate_ready(self, obj, cf_after):
        missing = []
        if getattr(obj, "status", None) != "active":
            missing.append('status="active"')
        if not self._has_primary_v4(obj):
            missing.append("primary IPv4")
        if getattr(obj, "platform_id", None) is None:
            missing.append("platform")
        if not self._is_true(cf_after.get("mon_req")):
            missing.append("mon_req=True")
        if not self._norm_str(cf_after.get("zabbix_template_name")):
            missing.append("zabbix_template set")
        if not self._norm_str(cf_after.get("environment")):
            missing.append("environment set")
        if not self._desc(obj):
            missing.append("description set")
        if not self._norm_str(cf_after.get("sla_report_code")):
            missing.append("SLA code set")

        meets = (len(missing) == 0)
        cf_after["zabbix_status"] = True if meets else False
        cf_after["monitoring_status"] = "In-progress" if meets else cf_after.get("monitoring_status") or "Missing Required Fields"
        return meets, missing, cf_after

    # ---------------- parsing helpers ----------------
    def _parse_extras(self, extra_value):
        if extra_value is None:
            return []
        if isinstance(extra_value, (list, tuple)):
            out = []
            for item in extra_value:
                if isinstance(item, str):
                    v = item.strip()
                elif isinstance(item, dict):
                    v = (item.get("value") or item.get("label") or item.get("name") or "").strip()
                else:
                    v = str(item).strip()
                if v:
                    out.append(v)
            return out
        if isinstance(extra_value, str):
            return [p.strip() for p in extra_value.split(",") if p.strip()]
        v = str(extra_value).strip()
        return [v] if v else []

    # ---------------- query streams ----------------
    def _device_qs(self, site):
        qs = Device.objects.all().select_related("platform", "primary_ip4", "site")
        if site:
            qs = qs.filter(site=site)
        return qs.iterator()

    def _vm_qs(self, _site):
        qs = VirtualMachine.objects.all().select_related("platform", "primary_ip4", "cluster", "tenant")
        return qs.iterator()

    # ---------------- main ----------------
    def run(self, data, commit):
        commit = (commit and not data.get("dry_run", False))
        overwrite = data.get("overwrite_all_mapped_fields", False)

        # Require zabbix_status CF (boolean)
        try:
            zcf = CustomField.objects.get(name="zabbix_status")
            if getattr(zcf, "type", None) != "boolean":
                self.log_failure(f"Custom field 'zabbix_status' exists with type '{zcf.type}', expected 'boolean'.")
                return
        except CustomField.DoesNotExist:
            self.log_failure("Custom field 'zabbix_status' (boolean) not found. Create it for Device & VM first.")
            return

        include_devices = data.get("include_devices", True)
        include_vms     = data.get("include_vms", True)
        limit_site_obj  = data.get("limit_site")
        limit_site_id   = int(limit_site_obj.id) if limit_site_obj else None

        # Load the Custom Objects catalog once
        try:
            name_to_id, name_to_iface, by_platform = self._load_catalog()
        except Exception as e:
            self.log_failure(str(e))
            return

        # Counters
        sla_counters = {"sla_added": 0, "sla_no_role": 0, "sla_role_no_code": 0}
        status_true = status_false = 0
        devices_checked = vms_checked = 0
        step1_skips = step2_skips = 0
        tmpl_primary_updates = tmpl_primary_skips = 0
        ids_updated = ids_skipped = 0

        with transaction.atomic():
            streams = []
            if include_devices:
                streams.append(("Device", self._device_qs(limit_site_obj)))
            if include_vms:
                streams.append(("VM", self._vm_qs(limit_site_obj)))

            for kind, stream in streams:
                for obj in stream:
                    # Soft site limit for VMs
                    if kind == "VM" and limit_site_id is not None:
                        site = getattr(obj, "site", None)
                        sid = getattr(site, "id", None) if site else None
                        if sid is None:
                            loc = getattr(obj, "location", None)
                            if loc:
                                sid = getattr(getattr(loc, "site", None), "id", None)
                        if sid is None:
                            cluster = getattr(obj, "cluster", None)
                            if cluster:
                                sid = getattr(getattr(cluster, "site", None), "id", None)
                        if sid != limit_site_id:
                            continue

                    if kind == "Device":
                        devices_checked += 1
                    else:
                        vms_checked += 1

                    cf = self._cf_dict(obj)

                    # -------- Step 1: mon_req & status ----------
                    if not (self._is_true(cf.get("mon_req")) and getattr(obj, "status", None) == "active"):
                        cf["mon_req"] = False
                        cf["monitoring_status"] = "Missing Required Fields"
                        if commit:
                            obj.custom_field_data = cf
                            obj.save()
                        step1_skips += 1
                        meets, missing, cf_after = self._evaluate_ready(obj, cf)
                        if commit:
                            obj.custom_field_data = cf_after
                            obj.save()
                        status_true += 1 if meets else 0
                        status_false += 0 if meets else 1
                        continue

                    # -------- Step 2: required fields present ----------
                    missing_fields = []
                    if getattr(obj, "platform_id", None) is None:
                        missing_fields.append("platform")
                    if not self._has_primary_v4(obj):
                        missing_fields.append("primary IPv4")
                    # site resolution (VMs: site or location.site or cluster.site)
                    site_id = getattr(obj, "site_id", None) if kind == "Device" else (
                        getattr(getattr(obj, "site", None), "id", None) or
                        getattr(getattr(getattr(obj, "location", None), "site", None), "id", None) or
                        getattr(getattr(getattr(obj, "cluster", None), "site", None), "id", None)
                    )
                    if site_id is None:
                        missing_fields.append("site")
                    if not self._norm_str(cf.get("environment")):
                        missing_fields.append("environment")
                    if not self._norm_str(getattr(obj, "name", None)):
                        missing_fields.append("name")
                    if not self._desc(obj):
                        missing_fields.append("description")

                    if missing_fields:
                        cf["monitoring_status"] = "Missing Required Fields"
                        if commit:
                            obj.custom_field_data = cf
                            obj.save()
                        step2_skips += 1
                        meets, missing, cf_after = self._evaluate_ready(obj, cf)
                        if commit:
                            obj.custom_field_data = cf_after
                            obj.save()
                        status_true += 1 if meets else 0
                        status_false += 0 if meets else 1
                        continue

                    # -------- Step 3.0: TEMPLATE SYNC (primary by platform) ----------
                    plat_pk = getattr(getattr(obj, "platform", None), "pk", None)
                    cur_name = self._norm_str(cf.get("zabbix_template_name"))
                    cur_int  = cf.get("zabbix_template_int_id")

                    primary_name = None
                    primary_id   = None
                    primary_iface = None

                    if plat_pk in by_platform:
                        primary_name, primary_id, primary_iface = by_platform[plat_pk]
                    elif cur_name and cur_name.lower() in name_to_id:
                        # fallback to current name if present in catalog
                        primary_name = cur_name
                        primary_id   = name_to_id.get(cur_name.lower())
                        primary_iface = name_to_iface.get(cur_name.lower()) if name_to_iface else None

                    def needs_write(value, new):
                        if overwrite:
                            return True
                        return (value in (None, "", 0)) and (new not in (None, "", 0))

                    changed_primary = False
                    if primary_name is not None:
                        if needs_write(cur_name, primary_name):
                            cf["zabbix_template_name"] = primary_name
                            changed_primary = True
                        if name_to_iface is not None and needs_write(cur_int, primary_iface):
                            cf["zabbix_template_int_id"] = primary_iface
                            changed_primary = True
                        if changed_primary and commit:
                            obj.custom_field_data = cf
                            obj.save()
                        tmpl_primary_updates += 1 if changed_primary else 0
                        tmpl_primary_skips   += 0 if changed_primary else 1
                    else:
                        self.log_info(f"[{kind}] {obj.name}: no catalog match for platform/current name")

                    # Build zabbix_template_id CSV = [primary_id] + extras from cf.zabbix_extra_templates
                    names_ordered = []
                    seen = set()
                    if primary_name:
                        k = primary_name.lower()
                        if k not in seen:
                            seen.add(k); names_ordered.append(primary_name)
                    # extras field name per your change: zabbix_extra_templates
                    extras = self._parse_extras(cf.get("zabbix_extra_templates"))
                    for n in extras:
                        k = n.strip().lower()
                        if k and k not in seen:
                            seen.add(k); names_ordered.append(n)

                    matched_ids, misses = [], []
                    for n in names_ordered:
                        tid = name_to_id.get(n.strip().lower())
                        if tid is None:
                            misses.append(n)
                        else:
                            matched_ids.append(tid)
                    if misses:
                        self.log_info(f"[{kind}] {obj.name}: catalog missing template_id for: {', '.join(misses)}")

                    new_ids_csv = ",".join(str(t) for t in matched_ids) if matched_ids else ""
                    cur_ids_csv = self._norm_str(cf.get("zabbix_template_id")) or ""
                    if overwrite or (cur_ids_csv in ("", None) and new_ids_csv):
                        if cur_ids_csv != new_ids_csv:
                            cf["zabbix_template_id"] = new_ids_csv
                            if commit:
                                obj.custom_field_data = cf
                                obj.save()
                            ids_updated += 1
                        else:
                            ids_skipped += 1
                    else:
                        ids_skipped += 1

                    # -------- Step 3.1: SLA ----------
                    cf, sla_added, sla_err = self._ensure_sla_code(obj, cf, sla_counters, overwrite=overwrite)
                    if sla_added and commit:
                        obj.custom_field_data = cf
                        obj.save()

                    # -------- Final readiness ----------
                    meets, missing, cf_after = self._evaluate_ready(obj, cf)
                    if commit:
                        obj.custom_field_data = cf_after
                        obj.save()
                    if meets:
                        status_true += 1
                    else:
                        status_false += 1

            if not commit:
                self.log_info("Dry run: no changes committed.")
                transaction.set_rollback(True)

        # ---------------- summary ----------------
        self.log_info(f"SLA: added={sla_counters['sla_added']}, no_role={sla_counters['sla_no_role']}, role_no_code={sla_counters['sla_role_no_code']}")
        self.log_info(f"Template: primary updates={tmpl_primary_updates}, primary skips={tmpl_primary_skips}")
        self.log_info(f"Template IDs: updated={ids_updated}, skipped={ids_skipped}")
        self.log_info(f"Status: True={status_true}, False={status_false}; Checked: devices={devices_checked}, vms={vms_checked}; Skipped Step1={step1_skips}, Step2={step2_skips}")
