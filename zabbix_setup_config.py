# NetBox Script: Prestage Zabbix fields from Custom Objects (zabbix-template-list)
# Adapts to plugin variants:
# - CustomObjectType may have slug or only name/label
# - Row FK may be "object_type" or "custom_object_type"

from dcim.models import Device, Site, Platform
from virtualization.models import VirtualMachine
from extras.scripts import Script, BooleanVar, ObjectVar
from django.apps import apps
from django.db import transaction
from django.db.models import ManyToManyField

# Accept this selector for slug/name/label (case-insensitive)
COT_TYPE_SELECTOR = "zabbix-template-list"

# Field name preferences inside catalog rows
NAME_KEYS   = ("template_name", "name", "template")
ID_KEYS     = ("template_id", "id", "zabbix_template_id")
IFACE_KEYS  = ("template_interface_id", "iface_id", "interface_id")
PLATFORM_JSON_KEYS = ("platforms", "platform_ids", "platform_slugs", "platform_names")

# Common JSON container names on Custom Object rows (we search these first)
JSON_CONTAINERS = ("data", "payload", "attributes", "values")


class PrestageZabbixFromCOT(Script):
    class Meta:
        name = "Zabbix: COT Template + SLA + Readiness (platform primary)"
        description = "Load template catalog from Custom Objects and pre-fill monitoring CFs on Devices/VMs"
        commit_default = False

    include_devices = BooleanVar(description="Include Devices", default=True)
    include_vms     = BooleanVar(description="Include Virtual Machines", default=False)
    limit_site      = ObjectVar(description="Soft limit by Site", required=False, model=Site)
    overwrite       = BooleanVar(description="Overwrite existing values", default=False)
    debug_catalog   = BooleanVar(description="Verbose catalog discovery logs", default=False)

    # -------- helpers --------
    def _norm_str(self, v): return (str(v).strip() if v is not None else "")
    def _is_true(self, v):  return str(v).lower() in {"1", "true", "yes", "on"}
    def _cf(self, obj):     return dict(getattr(obj, "custom_field_data", {}) or {})
    def _desc(self, obj):   return self._norm_str(getattr(obj, "description", ""))
    def _role(self, obj):   return getattr(obj, "device_role", None) or getattr(obj, "role", None)
    def _has_primary_ip(self, obj):
        return bool(getattr(obj, "primary_ip4", None) or getattr(obj, "primary_ip6", None))

    # -------- plugin model discovery --------
    def _get_cot_models(self):
        """Return (TypeModel, RowModel) from netbox_custom_objects plugin."""
        app_label = "netbox_custom_objects"
        Type = Row = None
        for Model in apps.get_models():
            if Model._meta.app_label.lower() != app_label:
                continue
            name = Model.__name__.lower()
            # Prefer exact names if present
            if name == "customobjecttype":
                Type = Model
            elif name == "customobject":
                Row = Model
        # Fallback: heuristic search
        if not Type or not Row:
            for Model in apps.get_models():
                if Model._meta.app_label.lower() != app_label:
                    continue
                nm = Model.__name__.lower()
                if (not Type) and "customobjecttype" in nm:
                    Type = Model
                if (not Row) and "customobject" in nm and "type" not in nm:
                    Row = Model
        if not (Type and Row):
            raise RuntimeError("Could not locate plugin models (CustomObjectType/CustomObject).")
        return Type, Row

    def _resolve_type_instance(self, Type, selector, debug=False):
        """Find the CustomObjectType by slug/name/label (case-insensitive)."""
        fields = {f.name for f in Type._meta.get_fields()}
        # Build one or more queries in priority order
        for key in ("slug__iexact", "name__iexact", "label__iexact"):
            base = key.split("__", 1)[0]
            if base in fields:
                try:
                    obj = Type.objects.filter(**{key: selector}).first()
                    if obj:
                        if debug:
                            self.log_info(f"[COT] Matched type via {base!r}: {getattr(obj, base, None)}")
                        return obj
                except Exception as e:
                    if debug:
                        self.log_warning(f"[COT] Lookup by {key} raised: {e}")
        # As a last resort, take the first type and warn
        any_type = Type.objects.first()
        if not any_type:
            raise RuntimeError("No CustomObjectType instances found.")
        self.log_warning(f"[COT] Could not match type by slug/name/label='{selector}'. "
                         f"Using the first available type: id={any_type.pk}, name={getattr(any_type,'name',None)}")
        return any_type

    def _row_queryset_for_type(self, RowModel, type_obj, debug=False):
        """Filter rows by FK field: object_type or custom_object_type (auto-detect)."""
        fields = {f.name for f in RowModel._meta.get_fields()}
        fk_name = None
        if "object_type" in fields:
            fk_name = "object_type"
        elif "custom_object_type" in fields:
            fk_name = "custom_object_type"
        else:
            raise RuntimeError(f"Row model {RowModel._meta.label} has no FK 'object_type' or 'custom_object_type'.")
        qs = RowModel.objects.filter(**{fk_name: type_obj})
        if debug:
            self.log_info(f"[COT] Rows model={RowModel._meta.label}, fk='{fk_name}', count={qs.count()}")
        return qs

    def _json_container_names(self, Model):
        fields = {f.name for f in Model._meta.get_fields() if hasattr(f, "attname")}
        return [n for n in JSON_CONTAINERS if n in fields]

    def _iter_possible_blobs(self, row, preferred):
        # 1) known containers first
        for name in preferred:
            if hasattr(row, name):
                blob = getattr(row, name)
                if isinstance(blob, dict):
                    yield blob
        # 2) any dict-like attributes
        for f in row._meta.get_fields():
            nm = getattr(f, "name", None)
            if not nm or nm in preferred:
                continue
            try:
                val = getattr(row, nm, None)
            except Exception:
                continue
            if isinstance(val, dict):
                yield val

    def _get_field_from_row(self, row, keys, preferred):
        # direct attributes
        for k in keys:
            if hasattr(row, k):
                val = getattr(row, k)
                if val not in (None, ""):
                    return val
        # JSON blobs
        for blob in self._iter_possible_blobs(row, preferred):
            for k in keys:
                if k in blob and blob[k] not in (None, ""):
                    return blob[k]
        return None

    def _get_platform_pks_from_row(self, row, preferred):
        """Support either M2M to Platform (row.platforms) or JSON with pks/slugs/names."""
        # M2M?
        try:
            fld = row._meta.get_field("platforms")
            if isinstance(fld, ManyToManyField) and fld.remote_field.model is Platform:
                return list(getattr(row, "platforms").values_list("pk", flat=True))
        except Exception:
            pass

        # JSON-style
        pks = set()

        def add_by_name_or_slug(val):
            if not val:
                return
            s = str(val).strip()
            if not s:
                return
            hit = Platform.objects.filter(slug__iexact=s).first() or \
                  Platform.objects.filter(name__iexact=s).first()
            if hit:
                pks.add(hit.pk)

        for blob in self._iter_possible_blobs(row, preferred):
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
        Type, RowModel = self._get_cot_models()
        preferred = self._json_container_names(RowModel)

        type_obj = self._resolve_type_instance(Type, COT_TYPE_SELECTOR, debug=debug)
        rows_qs = self._row_queryset_for_type(RowModel, type_obj, debug=debug)

        name_to_id = {}
        name_to_iface = {}
        by_platform = {}

        rows = list(rows_qs)
        for row in rows:
            name = self._get_field_from_row(row, NAME_KEYS, preferred)
            tid  = self._get_field_from_row(row, ID_KEYS, preferred)
            ifid = self._get_field_from_row(row, IFACE_KEYS, preferred)

            name = self._norm_str(name)
            if not name:
                continue
            try: tid = int(tid)
            except Exception: tid = None
            try: ifid = int(ifid)
            except Exception: ifid = None

            lname = name.lower()
            if tid is not None:
                name_to_id[lname] = tid
            if ifid is not None:
                name_to_iface[lname] = ifid

            plat_pks = self._get_platform_pks_from_row(row, preferred)
            for pk in plat_pks:
                if pk not in by_platform and tid is not None:
                    by_platform[pk] = (name, tid, ifid)

        if debug:
            self.log_info(f"[COT] Catalog rows={len(rows)}; name_to_id={len(name_to_id)}; "
                          f"platform_mappings={len(by_platform)}")
        return name_to_id, (name_to_iface or None), by_platform

    # -------- SLA + readiness --------
    def _ensure_sla(self, obj, cf, counters, overwrite=False):
        cur = self._norm_str(cf.get("sla_report_code"))
        if cur and not overwrite:
            return cf, False
        role = self._role(obj)
        if not role:
            counters["no_role"] = counters.get("no_role", 0) + 1
            return cf, False
        role_cf = dict(getattr(role, "custom_field_data", {}) or {})
        code = self._norm_str(role_cf.get("sla_report_code"))
        if not code:
            counters["role_no_code"] = counters.get("role_no_code", 0) + 1
            return cf, False
        cf["sla_report_code"] = code
        counters["added"] = counters.get("added", 0) + 1
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

    # -------- streams --------
    def _devices(self, site):
        qs = Device.objects.all().select_related("site", "role", "platform")
        if site:
            qs = qs.filter(site=site)
        return qs

    def _vms(self):
        return VirtualMachine.objects.all().select_related("role", "platform", "cluster__site", "site", "location__site")

    # -------- main --------
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
                        if name_to_iface is not None and needs_write(cur_int, primary_iface_
