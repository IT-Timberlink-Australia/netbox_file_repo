# NetBox Script: Prestage Zabbix fields from Custom Objects (zabbix-template-list)
# - Loads template catalog from netbox_custom_objects
# - Assigns primary template by Platform; resolves extras by name -> IDs
# - Copies Role.sla_report_code to the object
# - Evaluates "Ready" status for later sync (no Zabbix calls here)

from dcim.models import Device, Site, Platform
from virtualization.models import VirtualMachine
from extras.scripts import Script, BooleanVar, ObjectVar
from django.apps import apps
from django.db import transaction
from django.db.models import ManyToManyField, ForeignKey

COT_TYPE_SLUG = "zabbix-template-list"   # your Custom Object Type slug

# Field name preferences inside the catalog rows
NAME_KEYS   = ("template_name", "name", "template")
ID_KEYS     = ("template_id", "id", "zabbix_template_id")
IFACE_KEYS  = ("template_interface_id", "iface_id", "interface_id")
PLATFORM_JSON_KEYS = ("platforms", "platform_ids", "platform_slugs", "platform_names")

# Common JSON container names on Custom Object rows (we search these first)
JSON_CONTAINERS = ("data", "payload", "attributes", "values")

# Interface type IDs used by Zabbix (FYI): agent=1, snmp=2, ipmi=3, jmx=4


class PrestageZabbixFromCOT(Script):
    class Meta:
        name = "Zabbix: COT Template + SLA + Readiness (platform primary)"
        description = "Load template catalog from Custom Objects and pre-fill monitoring CFs on Devices/VMs"
        commit_default = False

    include_devices = BooleanVar(
        description="Include Devices",
        default=True,
    )
    include_vms = BooleanVar(
        description="Include Virtual Machines",
        default=False,
    )
    limit_site = ObjectVar(
        description="Soft limit by Site (Devices always; VMs via device/location/cluster.site)",
        required=False,
        model=Site,
    )
    overwrite = BooleanVar(
        description="Overwrite existing values (primary template/interface, SLA, IDs)",
        default=False,
    )
    debug_catalog = BooleanVar(
        description="Verbose catalog discovery logs",
        default=False,
    )

    # -------------------- tiny helpers --------------------
    def _norm_str(self, v):
        return (str(v).strip() if v is not None else "")

    def _is_true(self, v):
        return str(v).lower() in {"1", "true", "yes", "on"}

    def _cf(self, obj):
        return dict(getattr(obj, "custom_field_data", {}) or {})

    def _desc(self, obj):
        return self._norm_str(getattr(obj, "description", ""))

    def _role(self, obj):
        # Device.role or VM.role (VMs may use 'role' too)
        return getattr(obj, "device_role", None) or getattr(obj, "role", None)

    def _has_primary_ip(self, obj):
        # Prefer IPv4 but accept IPv6 if that's all there is
        return bool(getattr(obj, "primary_ip4", None) or getattr(obj, "primary_ip6", None))

    # -------------------- catalog discovery --------------------
    def _get_cot_models(self):
        """Return (CustomObjectType, CustomObject) models from the plugin."""
        app_label = "netbox_custom_objects"
        Type = Obj = None
        for Model in apps.get_models():
            if Model._meta.app_label.lower() != app_label:
                continue
            nm = Model.__name__.lower()
            if "customobjecttype" in nm:
                Type = Model
            elif "customobject" in nm:
                Obj = Model
        if not (Type and Obj):
            raise RuntimeError("Could not locate netbox_custom_objects models (CustomObjectType/CustomObject).")
        return Type, Obj

    def _load_catalog_rows(self, debug=False):
        """Fetch rows for COT_TYPE_SLUG and yield dict-like row wrappers for field access."""
        Type, Obj = self._get_cot_models()
        try:
            cot = Type.objects.get(slug=COT_TYPE_SLUG)
        except Exception as e:
            raise RuntimeError(f"CustomObjectType slug '{COT_TYPE_SLUG}' not found: {e}")

        # Rows/entries — different plugin versions store row fields differently
        qs = Obj.objects.filter(object_type=cot)
        if debug:
            self.log_info(f"[COT] Using {Obj._meta.label} for type='{COT_TYPE_SLUG}', rows={qs.count()}")

        for row in qs:
            yield row

    def _json_container_names(self, Model):
        fields = {f.name for f in Model._meta.get_fields() if hasattr(f, "attname")}
        return [name for name in JSON_CONTAINERS if name in fields]

    def _iter_possible_blobs(self, row, preferred_names):
        # 1) known containers first
        for name in preferred_names:
            if hasattr(row, name):
                blob = getattr(row, name)
                if isinstance(blob, dict):
                    yield blob
        # 2) any dict-like attributes
        for f in row._meta.get_fields():
            nm = getattr(f, "name", None)
            if not nm or nm in preferred_names:
                continue
            try:
                val = getattr(row, nm, None)
            except Exception:
                continue
            if isinstance(val, dict):
                yield val

    def _get_field_from_row(self, row, keys, preferred_names):
        """Try direct attributes, then dict blobs, return first non-empty string/int."""
        # direct attributes
        for k in keys:
            if hasattr(row, k):
                val = getattr(row, k)
                if val not in (None, ""):
                    return val

        # JSON blobs
        for blob in self._iter_possible_blobs(row, preferred_names):
            for k in keys:
                if k in blob and blob[k] not in (None, ""):
                    return blob[k]
        return None

    def _get_platform_pks_from_row(self, row, preferred_names):
        """Support either M2M to Platform (row.platforms) or JSON with pks/slugs/names."""
        # M2M?
        try:
            fld = row._meta.get_field("platforms")
            if isinstance(fld, ManyToManyField) and isinstance(fld.remote_field.model, type(Platform)):
                return list(getattr(row, "platforms").values_list("pk", flat=True))
        except Exception:
            pass

        # JSON-style
        pks = set()
        def add_by_name_or_slug(val):
            if not val: return
            val = str(val).strip()
            if not val: return
            hit = Platform.objects.filter(slug__iexact=val).first() or \
                  Platform.objects.filter(name__iexact=val).first()
            if hit: pks.add(hit.pk)

        # search known containers first; then any dict attr
        for blob in self._iter_possible_blobs(row, preferred_names):
            for k in PLATFORM_JSON_KEYS:
                if k not in blob:
                    continue
                val = blob[k]
                if isinstance(val, (list, tuple, set)):
                    for item in val:
                        if isinstance(item, int):
                            pks.add(item)
                        elif isinstance(item, str):
                            try:
                                pks.add(int(item))
                            except Exception:
                                add_by_name_or_slug(item)
                        elif isinstance(item, dict):
                            pid = item.get("id") or item.get("pk")
                            if pid is not None:
                                try:
                                    pks.add(int(pid))
                                except Exception:
                                    add_by_name_or_slug(item.get("slug") or item.get("name"))
                elif isinstance(val, int):
                    pks.add(val)
                elif isinstance(val, str):
                    try:
                        pks.add(int(val))
                    except Exception:
                        add_by_name_or_slug(val)
                elif isinstance(val, dict):
                    pid = val.get("id") or val.get("pk")
                    if pid is not None:
                        try:
                            pks.add(int(pid))
                        except Exception:
                            add_by_name_or_slug(val.get("slug") or val.get("name"))
        return list(pks)

    def _load_template_catalog(self, debug=False):
        """Return (name_to_id, name_to_iface, by_platform) from COT rows."""
        # discover preferred JSON containers for row model
        Type, RowModel = self._get_cot_models()
        preferred_names = self._json_container_names(RowModel)

        name_to_id = {}
        name_to_iface = {}
        by_platform = {}

        rows = list(self._load_catalog_rows(debug=debug))
        for row in rows:
            name = self._get_field_from_row(row, NAME_KEYS, preferred_names)
            tid  = self._get_field_from_row(row, ID_KEYS, preferred_names)
            ifid = self._get_field_from_row(row, IFACE_KEYS, preferred_names)

            name = self._norm_str(name)
            if not name:
                continue
            # normalize id/iface to int when possible
            try: tid = int(tid)
            except Exception: tid = None
            try: ifid = int(ifid)
            except Exception: ifid = None

            lname = name.lower()
            if tid is not None:
                name_to_id[lname] = tid
            if ifid is not None:
                name_to_iface[lname] = ifid

            plat_pks = self._get_platform_pks_from_row(row, preferred_names)
            for pk in plat_pks:
                # the *first* encountered per-platform becomes the primary mapping
                if pk not in by_platform and tid is not None:
                    by_platform[pk] = (name, tid, ifid)

        if debug:
            self.log_info(f"[COT] Catalog rows={len(rows)}; name_to_id={len(name_to_id)}; "
                          f"platform_mappings={len(by_platform)}")
        return name_to_id, name_to_iface or None, by_platform

    # -------------------- SLA and readiness --------------------
    def _ensure_sla(self, obj, cf, counters, overwrite=False):
        cur = self._norm_str(cf.get("sla_report_code"))
        if cur and not overwrite:
            return cf, False
        role = self._role(obj)
        if not role:
            counters["no_role"] += 1
            return cf, False
        role_cf = dict(getattr(role, "custom_field_data", {}) or {})
        code = self._norm_str(role_cf.get("sla_report_code"))
        if not code:
            counters["role_no_code"] += 1
            return cf, False
        cf["sla_report_code"] = code
        counters["added"] += 1
        return cf, True

    def _ready_eval(self, obj, cf_after):
        missing = []
        status = self._norm_str(getattr(obj, "status", ""))
        if status != "active": missing.append('status="active"')
        if not self._has_primary_ip(obj): missing.append("primary IP set")
        if getattr(obj, "platform_id", None) is None: missing.append("platform set")
        if not self._is_true(cf_after.get("mon_req")): missing.append("mon_req=True")
        if not self._norm_str(cf_after.get("zabbix_template_name")): missing.append("zabbix_template set")
        if not self._norm_str(cf_after.get("environment")): missing.append("environment set")
        if not self._desc(obj): missing.append("description set")
        if not self._norm_str(cf_after.get("sla_report_code")): missing.append("SLA code set")

        if missing:
            cf_after["monitoring_status"] = f"Missing Required Fields: {', '.join(missing)}"
            return False, missing, cf_after
        cf_after["monitoring_status"] = "Ready"
        return True, [], cf_after

    # -------------------- object streams --------------------
    def _devices(self, site):
        qs = Device.objects.all().select_related("site", "role", "platform")
        if site:
            qs = qs.filter(site=site)
        return qs

    def _vms(self):
        return VirtualMachine.objects.all().select_related("role", "platform", "cluster__site", "site", "location__site")

    # -------------------- main execution --------------------
    def run(self, data, commit):
        include_devices = data.get("include_devices")
        include_vms     = data.get("include_vms")
        limit_site_obj  = data.get("limit_site")
        overwrite       = data.get("overwrite")
        debug_catalog   = data.get("debug_catalog")

        name_to_id, name_to_iface, by_platform = self._load_template_catalog(debug=debug_catalog)

        status_true = status_false = 0
        devices_checked = vms_checked = 0
        tmpl_primary_updates = tmpl_primary_skips = 0
        ids_updated = ids_skipped = 0
        step1_skips = step2_skips = 0

        with transaction.atomic():
            streams = []
            if include_devices:
                streams.append(("Device", self._devices(limit_site_obj)))
            if include_vms:
                streams.append(("VM", self._vms()))

            for kind, qs in streams:
                for obj in qs:
                    # Soft site limit for VMs (via cluster/site or location.site)
                    if kind == "VM" and limit_site_obj is not None:
                        sid = getattr(getattr(obj, "site", None), "id", None) \
                              or getattr(getattr(getattr(obj, "location", None), "site", None), "id", None) \
                              or getattr(getattr(getattr(obj, "cluster", None), "site", None), "id", None)
                        if sid != getattr(limit_site_obj, "id", None):
                            continue

                    if kind == "Device":
                        devices_checked += 1
                    else:
                        vms_checked += 1

                    cf = self._cf(obj)

                    # Step 1: must be opted-in AND active for template/id work
                    if not (self._is_true(cf.get("mon_req")) and self._norm_str(getattr(obj, "status", "")) == "active"):
                        cf["mon_req"] = False
                        cf["monitoring_status"] = "Missing Required Fields"
                        step1_skips += 1
                        if commit:
                            obj.custom_field_data = cf
                            obj.save()
                        continue

                    # Step 2: assign primary template by platform (or keep a valid current one)
                    plat_pk = getattr(obj, "platform_id", None)
                    cur_name = self._norm_str(cf.get("zabbix_template_name"))
                    cur_int  = cf.get("zabbix_template_int_id", None)

                    primary_name = primary_id = primary_iface = None
                    if plat_pk in by_platform:
                        primary_name, primary_id, primary_iface = by_platform[plat_pk]
                    elif cur_name and cur_name.lower() in name_to_id:
                        # keep current only if resolvable (stops churn)
                        primary_name = cur_name
                        primary_id   = name_to_id.get(cur_name.lower())
                        primary_iface = name_to_iface.get(cur_name.lower()) if name_to_iface else None

                    def needs_write(old, new):
                        if overwrite:
                            return True
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
                            obj.custom_field_data = cf
                            obj.save()
                        tmpl_primary_updates += 1 if changed_primary else 0
                        tmpl_primary_skips   += 0 if changed_primary else 1
                    else:
                        self.log_info(f"[{kind}] {obj.name}: no catalog match for platform/current name")
                        step2_skips += 1

                    # Build zabbix_template_id CSV: [primary] + extras (by name)
                    names = []
                    seen = set()
                    if primary_name:
                        names.append(primary_name)
                        seen.add(primary_name.lower())

                    extra_csv = self._norm_str(cf.get("zabbix_extra_templates"))
                    if extra_csv:
                        for nm in [t.strip() for t in extra_csv.split(",") if t.strip()]:
                            lname = nm.lower()
                            if lname not in seen:
                                names.append(nm)
                                seen.add(lname)

                    id_list = []
                    for nm in names:
                        lid = name_to_id.get(nm.lower())
                        if lid is not None:
                            id_list.append(str(lid))

                    if id_list:
                        old_csv = self._norm_str(cf.get("zabbix_template_id"))
                        new_csv = ",".join(id_list)
                        if overwrite or old_csv != new_csv:
                            cf["zabbix_template_id"] = new_csv
                            if commit:
                                obj.custom_field_data = cf
                                obj.save()
                            ids_updated += 1
                        else:
                            ids_skipped += 1

                    # SLA code from Role
                    sla_counts = {"added": 0, "no_role": 0, "role_no_code": 0}
                    cf, _ = self._ensure_sla(obj, cf, sla_counts, overwrite=overwrite)
                    if commit:
                        obj.custom_field_data = cf
                        obj.save()

                    # Final readiness
                    meets, _, cf_after = self._ready_eval(obj, cf)
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

        # Summary
        self.log_info(f"Template: primary updates={tmpl_primary_updates}, primary skips={tmpl_primary_skips}")
        self.log_info(f"Template IDs: updated={ids_updated}, skipped={ids_skipped}")
        self.log_info(f"Status: Ready={status_true}, NotReady={status_false}; "
                      f"Checked Devices={devices_checked}, VMs={vms_checked}; "
                      f"Skipped Step1={step1_skips}, Step2={step2_skips}")
