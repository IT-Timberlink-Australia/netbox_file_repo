from dcim.models import Device, Site, Platform
from virtualization.models import VirtualMachine
from extras.models import CustomField
from extras.scripts import Script, BooleanVar, ObjectVar
from django.apps import apps
from django.db import transaction
from django.db.models import ManyToManyField, ForeignKey

# ---- Config / expectations from your API (27 rows today) ----
COT_HUMAN = "zabbix-template-list"  # only for logs (matches your endpoint)  # :contentReference[oaicite:1]{index=1}

# Preferred keys for fields (column or JSON)
NAME_KEYS   = ("template_name", "name", "template")
ID_KEYS     = ("template_id", "id", "zabbix_template_id")
IFACE_KEYS  = ("template_interface_id", "iface_id", "interface_id")

# Likely JSON containers on entry rows
JSON_CONTAINERS = ("data", "payload", "attributes", "values")

# Ways platforms might show up
PLATFORM_FIELD_NAMES = ("platform", "platforms")
PLATFORM_JSON_KEYS   = (
    "platform", "platforms",
    "platform_id", "platform_ids", "platform_pk", "platform_pks",
    "platform_name", "platform_names", "platform_slug", "platform_slugs",
)

class ZabbixConfigFromCOT(Script):
    """
    Zabbix readiness & mappings from Custom Object 'zabbix-template-list'
    - Primary template by Platform (with name fallback)
    - zabbix_template_id = primary + extras (CSV)
    - SLA from role.sla_report_code
    - Readiness gates & final status 'In-progress' on success
    """

    class Meta:
        name = "Zabbix: COT Template + SLA + Readiness (platform primary)"
        description = "Resilient loader for Custom Objects entries; primary by Platform; extras to IDs; SLA & readiness."
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

    # Debug (optional)
    debug_catalog = BooleanVar(default=False, required=False, description="Verbose discovery logs for the catalog model")

    # ---------- basic utils ----------
    def _norm_str(self, v):
        if v is None: return None
        if isinstance(v, str):
            s = v.strip()
            return s if s else None
        return str(v).strip() or None

    def _is_true(self, v) -> bool:
        if isinstance(v, bool): return v
        if isinstance(v, int):  return v == 1
        if isinstance(v, str):  return v.strip().lower() in ("true", "1", "yes", "y", "on")
        return False

    def _cf(self, obj):
        return dict(getattr(obj, "custom_field_data", {}) or {})

    def _has_primary_v4(self, obj):
        return getattr(obj, "primary_ip4_id", None) is not None

    def _desc(self, obj):
        return self._norm_str(getattr(obj, "description", None)) or self._norm_str(getattr(obj, "comments", None))

    def _role(self, obj):
        return getattr(obj, "device_role", None) or getattr(obj, "role", None)

    # ---------- JSON helpers ----------
    def _json_container_name(self, Model):
        fields = {f.name for f in Model._meta.get_fields() if hasattr(f, "attname")}
        for cand in JSON_CONTAINERS:
            if cand in fields:
                return cand
        for cand in JSON_CONTAINERS:
            if hasattr(Model, cand):
                return cand
        return None

    def _get_attr_or_json(self, row, keys, json_name):
        # direct attribute
        for k in keys:
            if hasattr(row, k):
                val = getattr(row, k, None)
                if val not in (None, ""):
                    return val
        # JSON container
        if json_name and hasattr(row, json_name):
            blob = getattr(row, json_name)
            if isinstance(blob, dict):
                for k in keys:
                    if k in blob and blob[k] not in (None, ""):
                        return blob[k]
        return None

    # ---------- platform extraction ----------
    def _platform_pks_from_row(self, row, json_name):
        pks = set()

        # Relations (FK/M2M) named platform/platforms
        for fname in PLATFORM_FIELD_NAMES:
            if hasattr(row, fname):
                rel = getattr(row, fname)
                # FK-like
                if hasattr(rel, "pk"):
                    if rel.pk:
                        pks.add(rel.pk)
                # M2M manager
                elif hasattr(rel, "all"):
                    try:
                        for p in rel.all():
                            pk = getattr(p, "pk", None)
                            if pk:
                                pks.add(pk)
                    except Exception:
                        pass

        # JSON fallbacks: resolve id/name/slug → Platform pk
        if json_name and hasattr(row, json_name):
            blob = getattr(row, json_name)
            if isinstance(blob, dict):
                for key in PLATFORM_JSON_KEYS:
                    if key not in blob:
                        continue
                    val = blob.get(key)

                    def add_by_name_or_slug(s):
                        try:
                            plat = Platform.objects.filter(name__iexact=s).first() or Platform.objects.filter(slug__iexact=s).first()
                            if plat:
                                pks.add(plat.pk)
                        except Exception:
                            pass

                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, int):
                                pks.add(item)
                            elif isinstance(item, str):
                                try: pks.add(int(item))
                                except Exception: add_by_name_or_slug(item)
                            elif isinstance(item, dict):
                                pid = item.get("id") or item.get("pk")
                                if pid is not None:
                                    try: pks.add(int(pid))
                                    except Exception:
                                        for nk in ("name", "slug", "label"):
                                            if nk in item and item[nk]:
                                                add_by_name_or_slug(str(item[nk]))
                                                break
                    elif isinstance(val, int):
                        pks.add(val)
                    elif isinstance(val, str):
                        try: pks.add(int(val))
                        except Exception: add_by_name_or_slug(val)
                    elif isinstance(val, dict):
                        pid = val.get("id") or val.get("pk")
                        if pid is not None:
                            try: pks.add(int(pid))
                            except Exception:
                                for nk in ("name", "slug", "label"):
                                    if nk in val and val[nk]:
                                        add_by_name_or_slug(str(val[nk]))
                                        break

        return list(pks)

    # ---------- catalog model discovery ----------
    def _model_sample_score(self, Model, debug=False):
        """
        Return a tuple (score, stats_dict)
        Score prioritizes: having names+ids, and producing platform links.
        """
        json_name = self._json_container_name(Model)
        try:
            qs = Model.objects.all()[:200]
        except Exception:
            return (-1, {"model": Model, "json": json_name, "valid": 0, "plat_rows": 0, "plats": 0})

        valid = 0
        plat_rows = 0
        plat_total = 0
        for row in qs:
            nm = self._norm_str(self._get_attr_or_json(row, NAME_KEYS, json_name))
            tid = self._get_attr_or_json(row, ID_KEYS, json_name)
            if nm and tid not in (None, ""):
                valid += 1
            pks = self._platform_pks_from_row(row, json_name)
            if pks:
                plat_rows += 1
                plat_total += len(pks)

        # score: platform rows are king, then valid name+id rows
        score = (plat_rows * 1000) + (plat_total * 10) + valid
        if debug:
            self.log_info(f"[COT-scan] {Model._meta.label}: valid={valid}, plat_rows={plat_rows}, plat_total={plat_total}, json='{json_name}'")
        return (score, {"model": Model, "json": json_name, "valid": valid, "plat_rows": plat_rows, "plats": plat_total})

    def _pick_catalog_model(self, debug=False):
        best = None
        best_score = -1
        best_stats = None

        for Model in apps.get_models():
            if Model._meta.app_label.lower() != "netbox_custom_objects":
                continue
            name = Model.__name__.lower()
            if "customobjecttype" in name:
                continue  # not entries
            score, stats = self._model_sample_score(Model, debug=debug)
            if score > best_score:
                best, best_score, best_stats = stats["model"], score, stats

        if debug and best:
            self.log_success(f"[COT-pick] {best._meta.label} -> score={best_score}, valid={best_stats['valid']}, plat_rows={best_stats['plat_rows']}, json='{best_stats['json']}'")
        return best, (best_stats["json"] if best_stats else None)

    def _load_catalog(self, debug=False):
        """
        Build:
          - name_to_id: lower(name) -> template_id
          - name_to_iface: lower(name) -> template_interface_id (may be int/str/None)
          - by_platform: platform_pk -> (template_name, template_id, template_interface_id)
        Chooses the model that actually yields platform mappings; falls back to names only if needed.
        """
        Entry, json_name = self._pick_catalog_model(debug=debug)
        if not Entry:
            raise RuntimeError("Could not locate a Custom Objects entries model with usable template data.")

        name_to_id, name_to_iface, by_platform = {}, {}, {}
        has_iface_any = False
        scanned = 0
        plat_rows = 0

        for row in Entry.objects.all():
            scanned += 1
            nm = self._norm_str(self._get_attr_or_json(row, NAME_KEYS, json_name))
            if not nm:
                continue
            key = nm.lower()

            tid = self._get_attr_or_json(row, ID_KEYS, json_name)
            # keep as str if non-numeric; try cast for convenience
            try: tid = int(tid)
            except Exception: pass
            name_to_id[key] = tid

            iid = self._get_attr_or_json(row, IFACE_KEYS, json_name)
            if iid not in (None, ""):
                has_iface_any = True
                try: iid = int(iid)
                except Exception: pass
            name_to_iface[key] = iid

            pks = self._platform_pks_from_row(row, json_name)
            if pks:
                plat_rows += 1
                for pk in pks:
                    if pk not in by_platform:
                        by_platform[pk] = (nm, tid, iid)

        iface_map = name_to_iface if has_iface_any else None

        self.log_info(
            f"COT catalog loaded from {Entry._meta.label} "
            f"(rows_scanned={scanned}, mapped_names={len(name_to_id)}, "
            f"iface={'yes' if iface_map else 'no'}, platforms={len(by_platform)}, platform_rows={plat_rows})"
        )
        return name_to_id, iface_map, by_platform

    # ---------- SLA / readiness ----------
    def _ensure_sla(self, obj, cf, counters, overwrite=False):
        cur = self._norm_str(cf.get("sla_report_code"))
        if cur and not overwrite:
            return cf, False
        role = self._role(obj)
        if not role:
            counters["no_role"] += 1
            return cf, False
        code = self._norm_str((getattr(role, "custom_field_data", {}) or {}).get("sla_report_code"))
        if not code:
            counters["role_no_code"] += 1
            return cf, False
        cf["sla_report_code"] = code
        counters["added"] += 1
        return cf, True

    def _ready_eval(self, obj, cf_after):
        missing = []
        if getattr(obj, "status", None) != "active": missing.append('status="active"')
        if not self._has_primary_v4(obj): missing.append("primary IPv4")
        if getattr(obj, "platform_id", None) is None: missing.append("platform")
        if not self._is_true(cf_after.get("mon_req")): missing.append("mon_req=True")
        if not self._norm_str(cf_after.get("zabbix_template_name")): missing.append("zabbix_template set")
        if not self._norm_str(cf_after.get("environment")): missing.append("environment set")
        if not self._desc(obj): missing.append("description set")
        if not self._norm_str(cf_after.get("sla_report_code")): missing.append("SLA code set")

        meets = (len(missing) == 0)
        cf_after["zabbix_status"] = True if meets else False
        cf_after["monitoring_status"] = "In-progress" if meets else cf_after.get("monitoring_status") or "Missing Required Fields"
        return meets, missing, cf_after

    # ---------- parsing ----------
    def _parse_extras(self, v):
        if v is None: return []
        if isinstance(v, (list, tuple)):
            out=[]
            for item in v:
                if isinstance(item, str):
                    s=item.strip()
                elif isinstance(item, dict):
                    s=(item.get("value") or item.get("label") or item.get("name") or "").strip()
                else:
                    s=str(item).strip()
                if s: out.append(s)
            return out
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        s = str(v).strip()
        return [s] if s else []

    # ---------- query streams ----------
    def _devices(self, site):
        qs = Device.objects.all().select_related("platform", "primary_ip4", "site")
        if site: qs = qs.filter(site=site)
        return qs.iterator()

    def _vms(self):
        return VirtualMachine.objects.all().select_related("platform", "primary_ip4", "cluster", "tenant").iterator()

    # ---------- main ----------
    def run(self, data, commit):
        commit = (commit and not data.get("dry_run", False))
        overwrite = data.get("overwrite_all_mapped_fields", False)
        debug = data.get("debug_catalog", False)

        # Ensure zabbix_status CF exists & is boolean
        try:
            zcf = CustomField.objects.get(name="zabbix_status")
            if getattr(zcf, "type", None) != "boolean":
                self.log_failure(f"Custom field 'zabbix_status' exists with type '{zcf.type}', expected 'boolean'."); return
        except CustomField.DoesNotExist:
            self.log_failure("Custom field 'zabbix_status' (boolean) not found. Create it for Device & VM first."); return

        include_devices = data.get("include_devices", True)
        include_vms     = data.get("include_vms", True)
        limit_site_obj  = data.get("limit_site")
        limit_site_id   = int(limit_site_obj.id) if limit_site_obj else None

        # Load catalog (robust)
        try:
            name_to_id, name_to_iface, by_platform = self._load_catalog(debug=debug)
        except Exception as e:
            self.log_failure(str(e)); return

        sla_c = {"added": 0, "no_role": 0, "role_no_code": 0}
        status_true = status_false = 0
        devices_checked = vms_checked = 0
        step1_skips = step2_skips = 0
        tmpl_primary_updates = tmpl_primary_skips = 0
        ids_updated = ids_skipped = 0

        with transaction.atomic():
            streams = []
            if include_devices: streams.append(("Device", self._devices(limit_site_obj)))
            if include_vms:    streams.append(("VM", self._vms()))

            for kind, stream in streams:
                for obj in stream:
                    # VM soft site limit
                    if kind == "VM" and limit_site_id is not None:
                        sid = getattr(getattr(obj,"site",None),"id",None) or \
                              getattr(getattr(getattr(obj,"location",None),"site",None),"id",None) or \
                              getattr(getattr(getattr(obj,"cluster",None),"site",None),"id",None)
                        if sid != limit_site_id:
                            continue

                    if kind == "Device": devices_checked += 1
                    else: vms_checked += 1

                    cf = self._cf(obj)

                    # Step 1 gate
                    if not (self._is_true(cf.get("mon_req")) and getattr(obj, "status", None) == "active"):
                        cf["mon_req"] = False
                        cf["monitoring_status"] = "Missing Required Fields"
                        if commit: obj.custom_field_data = cf; obj.save()
                        step1_skips += 1
                        meets, missing, cf_after = self._ready_eval(obj, cf)
                        if commit: obj.custom_field_data = cf_after; obj.save()
                        status_true += 1 if meets else 0
                        status_false += 0 if meets else 1
                        continue

                    # Step 2 gate
                    missing_fields = []
                    if getattr(obj, "platform_id", None) is None: missing_fields.append("platform")
                    if not self._has_primary_v4(obj): missing_fields.append("primary IPv4")
                    site_id = getattr(obj, "site_id", None) if kind == "Device" else (
                        getattr(getattr(obj,"site",None),"id",None) or
                        getattr(getattr(getattr(obj,"location",None),"site",None),"id",None) or
                        getattr(getattr(getattr(obj,"cluster",None),"site",None),"id",None)
                    )
                    if site_id is None: missing_fields.append("site")
                    if not self._norm_str(cf.get("environment")): missing_fields.append("environment")
                    if not self._norm_str(getattr(obj,"name",None)): missing_fields.append("name")
                    if not self._desc(obj): missing_fields.append("description")

                    if missing_fields:
                        cf["monitoring_status"] = "Missing Required Fields"
                        if commit: obj.custom_field_data = cf; obj.save()
                        step2_skips += 1
                        meets, missing, cf_after = self._ready_eval(obj, cf)
                        if commit: obj.custom_field_data = cf_after; obj.save()
                        status_true += 1 if meets else 0
                        status_false += 0 if meets else 1
                        continue

                    # Step 3.0: Template sync
                    plat_pk = getattr(getattr(obj, "platform", None), "pk", None)
                    cur_name = self._norm_str(cf.get("zabbix_template_name"))
                    cur_int  = cf.get("zabbix_template_int_id")

                    primary_name = None
                    primary_id   = None
                    primary_iface = None

                    if plat_pk in by_platform:
                        primary_name, primary_id, primary_iface = by_platform[plat_pk]
                    elif cur_name and cur_name.lower() in name_to_id:
                        primary_name = cur_name
                        primary_id   = name_to_id.get(cur_name.lower())
                        primary_iface = name_to_iface.get(cur_name.lower()) if name_to_iface else None

                    def needs_write(old, new):
                        if overwrite: return True
                        return (old in (None, "", 0)) and (new not in (None, "", 0))

                    changed_primary = False
                    if primary_name is not None:
                        if needs_write(cur_name, primary_name):
                            cf["zabbix_template_name"] = primary_name
                            changed_primary = True
                        if name_to_iface is not None and needs_write(cur_int, primary_iface):
                            cf["zabbix_template_int_id"] = primary_iface
                            changed_primary = True
                        if changed_primary and commit:
                            obj.custom_field_data = cf; obj.save()
                        tmpl_primary_updates += 1 if changed_primary else 0
                        tmpl_primary_skips   += 0 if changed_primary else 1
                    else:
                        self.log_info(f"[{kind}] {obj.name}: no catalog match for platform/current name")

                    # Build zabbix_template_id CSV = [primary] + extras
                    names = []
                    seen = set()
                    if primary_name:
                        k = primary_name.lower()
                        if k not in seen: seen.add(k); names.append(primary_name)
                    for n in self._parse_extras(cf.get("zabbix_extra_templates")):
                        k = n.strip().lower()
                        if k and k not in seen: seen.add(k); names.append(n)

                    ids = []
                    misses = []
                    for n in names:
                        tid = name_to_id.get(n.strip().lower())
                        if tid is None: misses.append(n)
                        else: ids.append(tid)
                    if misses:
                        self.log_info(f"[{kind}] {obj.name}: catalog missing template_id for: {', '.join(misses)}")

                    new_csv = ",".join(str(t) for t in ids) if ids else ""
                    cur_csv = self._norm_str(cf.get("zabbix_template_id")) or ""
                    if overwrite or (cur_csv in ("", None) and new_csv):
                        if cur_csv != new_csv:
                            cf["zabbix_template_id"] = new_csv
                            if commit: obj.custom_field_data = cf; obj.save()
                            ids_updated += 1
                        else:
                            ids_skipped += 1
                    else:
                        ids_skipped += 1

                    # Step 3.1: SLA
                    cf, added = self._ensure_sla(obj, cf, sla_c, overwrite=overwrite)
                    if added and commit:
                        obj.custom_field_data = cf; obj.save()

                    # Final readiness
                    meets, missing, cf_after = self._ready_eval(obj, cf)
                    if commit: obj.custom_field_data = cf_after; obj.save()
                    if meets: status_true += 1
                    else:     status_false += 1

            if not commit:
                self.log_info("Dry run: no changes committed.")
                transaction.set_rollback(True)

        # Summary
        self.log_info(f"SLA: added={sla_c['added']}, no_role={sla_c['no_role']}, role_no_code={sla_c['role_no_code']}")
        self.log_info(f"Template: primary updates={tmpl_primary_updates}, primary skips={tmpl_primary_skips}")
        self.log_info(f"Template IDs: updated={ids_updated}, skipped={ids_skipped}")
        self.log_info(f"Status: True={status_true}, False={status_false}; Checked: devices={devices_checked}, vms={vms_checked}; Skipped Step1={step1_skips}, Step2={step2_skips}")
