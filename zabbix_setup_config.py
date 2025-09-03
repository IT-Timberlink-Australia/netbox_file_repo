# NetBox Script: Prestage Zabbix fields from Custom Objects (zabbix-template-list)
# Reads Custom Object *field values* (not just row attributes/JSON), so it works
# with the netbox-custom-objects plugin even when fields are defined dynamically.

from dcim.models import Device, Site, Platform
from virtualization.models import VirtualMachine
from extras.scripts import Script, BooleanVar, ObjectVar
from django.apps import apps
from django.db import transaction
from django.db.models import ForeignKey, ManyToManyField
import re

COT_TYPE_SELECTOR = "zabbix-template-list"

# Normalize strings to slugs for matching field names robustly
def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

# Candidate keys we care about (slugged)
NAME_KEYS   = {_slug(x) for x in ("template_name", "name", "template")}
ID_KEYS     = {_slug(x) for x in ("template_id", "zabbix_template_id")}
IFACE_KEYS  = {_slug(x) for x in ("template_interface_id", "iface_id", "interface_id", "interface")}
PLATFORM_KEYS = {_slug(x) for x in ("platform", "platforms")}

IFACE_MAP = {"agent": 1, "snmp": 2, "ipmi": 3, "jmx": 4}


class PrestageZabbixFromCOT(Script):
    class Meta:
        name = "Zabbix: COT Template + SLA + Readiness (platform primary)"
        description = "Load template catalog from Custom Objects and pre-fill monitoring CFs on Devices/VMs"
        commit_default = False

    include_devices = BooleanVar(description="Include Devices", default=True)
    include_vms     = BooleanVar(description="Include Virtual Machines", default=False)
    limit_site      = ObjectVar(description="Soft limit by Site", required=False, model=Site)
    overwrite       = BooleanVar(description="Overwrite existing values", default=False)
    debug_catalog   = BooleanVar(description="Verbose catalog discovery logs", default=True)

    # -------- tiny helpers --------
    def _norm(self, v): return (str(v).strip() if v is not None else "")
    def _is_true(self, v): return str(v).lower() in {"1","true","yes","on"}
    def _cf(self, obj): return dict(getattr(obj, "custom_field_data", {}) or {})
    def _has_primary_ip(self, obj):
        return bool(getattr(obj, "primary_ip4", None) or getattr(obj, "primary_ip6", None))
    def _role(self, obj): return getattr(obj, "device_role", None) or getattr(obj, "role", None)

    # -------- plugin model discovery --------
    def _get_models(self):
        app_label = "netbox_custom_objects"
        Type = Row = Field = Value = None
        for M in apps.get_models():
            if M._meta.app_label.lower() != app_label:
                continue
            n = M.__name__.lower()
            if "customobjecttype" in n and Type is None:
                Type = M
            elif n == "customobject" and Row is None:
                Row = M
            elif "customobjectfield" in n and "value" not in n and Field is None:
                Field = M
            elif "customobjectfield" in n and "value" in n and Value is None:
                Value = M
            elif "customobjectvalue" in n and Value is None:
                Value = M
        if not (Type and Row):
            raise RuntimeError("Could not locate CustomObjectType/CustomObject models.")
        return Type, Row, Field, Value  # Field/Value may be None; we’ll handle that

    def _resolve_type(self, Type, selector, debug=False):
        fields = {f.name for f in Type._meta.get_fields()}
        for key in ("slug__iexact", "name__iexact", "label__iexact"):
            base = key.split("__",1)[0]
            if base in fields:
                obj = Type.objects.filter(**{key: selector}).first()
                if obj:
                    if debug: self.log_info(f"[COT] Matched type via '{base}': {getattr(obj, base, None)}")
                    return obj
        any_type = Type.objects.first()
        if not any_type:
            raise RuntimeError("No CustomObjectType instances found.")
        self.log_warning(f"[COT] Could not match by slug/name/label='{selector}'. Using first: id={any_type.pk}")
        return any_type

    def _rows_for_type(self, Row, TypeObj, selector, debug=False):
        # Try typical FKs or *_id fields; else string markers; else all
        fields = {f.name: f for f in Row._meta.get_fields()}
        # FK
        for name, f in fields.items():
            if isinstance(f, ForeignKey) and f.remote_field and f.remote_field.model == TypeObj.__class__:
                qs = Row.objects.filter(**{name: TypeObj})
                if debug: self.log_info(f"[COT] Rows via FK '{name}': count={qs.count()}")
                return qs
        # *_id
        for idf in ("custom_object_type_id", "object_type_id", "type_id"):
            if idf in fields or hasattr(Row, idf):
                try:
                    qs = Row.objects.filter(**{idf: TypeObj.pk})
                    if qs.exists():
                        if debug: self.log_info(f"[COT] Rows via int '{idf}': count={qs.count()}")
                        return qs
                except Exception:
                    pass
        # string marker
        for sfield in ("type","group_name","label","name"):
            if sfield in fields or hasattr(Row, sfield):
                try:
                    qs = Row.objects.filter(**{f"{sfield}__iexact": selector})
                    if qs.exists():
                        if debug: self.log_info(f"[COT] Rows via marker '{sfield}': count={qs.count()}")
                        return qs
                except Exception:
                    pass
        qs = Row.objects.all()
        if debug: self.log_warning(f"[COT] Scanning ALL rows (count={qs.count()}).")
        return qs

    # -------- value extraction (key bit) --------
    def _field_defs_for_type(self, Field, TypeObj):
        # Find definitions for this type; collect {slug -> FieldDef obj}
        defs = {}
        if not Field:
            return defs
        # Candidate FK names pointing Field -> Type
        for rel_name in ("custom_object_type", "object_type", "type"):
            if hasattr(Field, "_meta") and rel_name in {f.name for f in Field._meta.get_fields()}:
                for fd in Field.objects.filter(**{rel_name: TypeObj}):
                    slug = _slug(getattr(fd, "name", None) or getattr(fd, "label", None))
                    if slug:
                        defs[slug] = fd
                break
        return defs

    def _values_for_row(self, row, defs, ValueModel, debug=False):
        """
        Build a dict of slug->value for a row by walking reverse relations to 'value' objects.
        Supports multi-select: returns lists. Related Platform objects resolved to PKs.
        """
        out = {}
        # Try common reverse relation names first
        candidates = []
        for rel in row._meta.get_fields():
            # look for reverse FK managers (one-to-many)
            try:
                acc = getattr(rel, "get_accessor_name", None)
                if not acc:
                    continue
                acc = acc()
                mgr = getattr(row, acc, None)
                if hasattr(mgr, "all"):
                    candidates.append((rel, mgr))
            except Exception:
                continue

        for rel, mgr in candidates:
            try:
                for val in mgr.all():
                    # Identify link to field def
                    fd = getattr(val, "custom_object_field", None) or getattr(val, "field", None)
                    if not fd:
                        continue
                    key = _slug(getattr(fd, "name", None) or getattr(fd, "label", None))
                    if not key:
                        continue

                    # Extract a value in a very forgiving way
                    v = None
                    # direct 'value' attr
                    for attr in ("value", "raw_value", "serialized_value", "string", "text", "number", "boolean", "json", "data"):
                        if hasattr(val, attr):
                            v = getattr(val, attr)
                            if v not in (None, ""):
                                break
                    # related single object
                    if v in (None, ""):
                        for attr in ("related_object", "object", "content_object"):
                            if hasattr(val, attr):
                                v = getattr(val, attr)
                                if v is not None:
                                    break
                    # related many objects
                    if v in (None, ""):
                        for attr in ("related_objects",):
                            if hasattr(val, attr):
                                relmgr = getattr(val, attr)
                                if hasattr(relmgr, "all"):
                                    v = list(relmgr.all())
                                    break

                    # Normalize Platforms to PKs; lists to list of PKs/strings
                    def norm_one(x):
                        if x is None:
                            return None
                        if isinstance(x, Platform):
                            return x.pk
                        return x

                    if isinstance(v, list):
                        v = [norm_one(x) for x in v if x is not None]
                    else:
                        v = norm_one(v)

                    # Collect (merge lists if repeated)
                    if key in out and isinstance(out[key], list):
                        out[key].append(v)
                    elif key in out and isinstance(v, list):
                        out[key] = out[key] + v
                    elif key in out:
                        out[key] = [out[key], v]
                    else:
                        out[key] = v
            except Exception:
                continue

        if debug:
            self.log_info(f"[COT] Row discovered fields: {sorted(out.keys())}")
        return out

    # -------- catalog load --------
    def _load_catalog(self, debug=False):
        Type, Row, Field, Value = self._get_models()
        type_obj = self._resolve_type(Type, COT_TYPE_SELECTOR, debug=debug)
        rows_qs = self._rows_for_type(Row, type_obj, COT_TYPE_SELECTOR, debug=debug)
        defs = self._field_defs_for_type(Field, type_obj)

        name_to_id = {}
        name_to_iface = {}
        by_platform = {}

        rows = list(rows_qs)
        if debug:
            self.log_info(f"[COT] Scanning rows: {len(rows)}; field defs={sorted(defs.keys())}")

        for row in rows:
            vals = self._values_for_row(row, defs, Value, debug=debug)
            # Resolve template name
            tname = None
            for k in NAME_KEYS:
                if k in vals and self._norm(vals[k]):
                    tname = self._norm(vals[k])
                    break
            if not tname:
                continue

            # Resolve template id
            tid = None
            for k in ID_KEYS:
                if k in vals and self._norm(vals[k]):
                    try:
                        tid = int(self._norm(vals[k]))
                    except Exception:
                        pass
                    break
            if tid is None:
                continue

            # Resolve interface id (string or int)
            tif = None
            for k in IFACE_KEYS:
                if k in vals and self._norm(vals[k]):
                    raw = self._norm(vals[k]).lower()
                    if raw.isdigit():
                        tif = int(raw)
                    else:
                        tif = IFACE_MAP.get(raw)
                    break

            # Resolve platforms → PK list
            plat_pks = []
            for k in PLATFORM_KEYS:
                if k in vals and vals[k] not in (None, "", []):
                    v = vals[k]
                    if not isinstance(v, list):
                        v = [v]
                    for item in v:
                        if isinstance(item, int):
                            plat_pks.append(item)
                        elif isinstance(item, Platform):
                            plat_pks.append(item.pk)
                        elif isinstance(item, str) and item.strip().isdigit():
                            plat_pks.append(int(item.strip()))          # <-- handle "9" as 9
                        else:
                            s = self._norm(item)
                            if s:
                                hit = Platform.objects.filter(slug__iexact=s).first() \
                                or Platform.objects.filter(name__iexact=s).first()
                                if hit:
                                    plat_pks.append(hit.pk)

            lname = tname.lower()
            name_to_id[lname] = tid
            if tif is not None:
                name_to_iface[lname] = tif
            for pk in plat_pks:
                if pk not in by_platform:
                    by_platform[pk] = (tname, tid, tif)

        if debug:
            self.log_info(f"[COT] Catalog built: names={len(name_to_id)}, platform_mappings={len(by_platform)}")
        return name_to_id, (name_to_iface or None), by_platform

    # -------- SLA / readiness --------
    def _ensure_sla(self, obj, cf, overwrite=False):
        cur = self._norm(cf.get("sla_report_code"))
        if cur and not overwrite:
            return cf, False
        role = self._role(obj)
        if not role:
            return cf, False
        rcf = dict(getattr(role, "custom_field_data", {}) or {})
        code = self._norm(rcf.get("sla_report_code"))
        if not code:
            return cf, False
        cf["sla_report_code"] = code
        return cf, True

    def _ready_eval(self, obj, cf_after):
        missing = []
        if self._norm(getattr(obj, "status", "")) != "active": missing.append('status="active"')
        if not self._has_primary_ip(obj): missing.append("primary IP set")
        if getattr(obj, "platform_id", None) is None: missing.append("platform set")
        if not self._is_true(cf_after.get("mon_req")): missing.append("mon_req=True")
        if not self._norm(cf_after.get("zabbix_template_name")): missing.append("zabbix_template set")
        if not self._norm(cf_after.get("environment")): missing.append("environment set")
        if not self._norm(cf_after.get("sla_report_code")): missing.append("SLA code set")
        if missing:
            cf_after["monitoring_status"] = f"Missing Required Fields: {', '.join(missing)}"
            return False, cf_after
        cf_after["monitoring_status"] = "Ready"
        return True, cf_after

    # -------- object streams --------
    def _devices(self, site):
        qs = Device.objects.all().select_related("site","role","platform")
        if site: qs = qs.filter(site=site)
        return qs

    def _vms(self):
        return VirtualMachine.objects.all().select_related("role","platform","cluster__site","site","location__site")

    # -------- main --------
    def run(self, data, commit):
        include_devices = data.get("include_devices")
        include_vms     = data.get("include_vms")
        limit_site_obj  = data.get("limit_site")
        overwrite       = data.get("overwrite")
        debug_catalog   = data.get("debug_catalog")

        name_to_id, name_to_iface, by_platform = self._load_catalog(debug=debug_catalog)

        tmpl_primary_updates = tmpl_primary_skips = 0
        ids_updated = ids_skipped = 0
        status_true = status_false = 0
        step1_skips = step2_skips = 0
        devices_checked = vms_checked = 0

        with transaction.atomic():
            streams = []
            if include_devices: streams.append(("Device", self._devices(limit_site_obj)))
            if include_vms:     streams.append(("VM", self._vms()))

            for kind, qs in streams:
                for obj in qs:
                    if kind == "VM" and limit_site_obj is not None:
                        sid = getattr(getattr(obj,"site",None),"id",None) \
                           or getattr(getattr(getattr(obj,"location",None),"site",None),"id",None) \
                           or getattr(getattr(getattr(obj,"cluster",None),"site",None),"id",None)
                        if sid != getattr(limit_site_obj,"id",None):
                            continue

                    if kind == "Device": devices_checked += 1
                    else:                 vms_checked += 1

                    cf = self._cf(obj)

                    if not (self._is_true(cf.get("mon_req")) and self._norm(getattr(obj,"status","")) == "active"):
                        cf["mon_req"] = False
                        cf["monitoring_status"] = "Missing Required Fields"
                        step1_skips += 1
                        if commit:
                            obj.custom_field_data = cf; obj.save()
                        continue

                    plat_pk = getattr(obj, "platform_id", None)
                    cur_name = self._norm(cf.get("zabbix_template_name"))
                    cur_int  = cf.get("zabbix_template_int_id", None)

                    primary_name = primary_id = primary_iface = None
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
                            cf["zabbix_template_name"] = primary_name; changed_primary = True
                        if name_to_iface is not None and needs_write(cur_int, primary_iface):
                            cf["zabbix_template_int_id"] = primary_iface; changed_primary = True
                        if changed_primary and commit:
                            obj.custom_field_data = cf; obj.save()
                        tmpl_primary_updates += 1 if changed_primary else 0
                        tmpl_primary_skips   += 0 if changed_primary else 1
                    else:
                        self.log_info(f"[{kind}] {obj.name}: no catalog match for platform/current name")
                        step2_skips += 1

                    # Build zabbix_template_id CSV: [primary] + extras(by name)
                    names = []
                    seen = set()
                    if primary_name:
                        names.append(primary_name); seen.add(primary_name.lower())
                    extra_csv = self._norm(cf.get("zabbix_extra_templates"))
                    if extra_csv:
                        for nm in [t.strip() for t in extra_csv.split(",") if t.strip()]:
                            if nm.lower() not in seen:
                                names.append(nm); seen.add(nm.lower())

                    ids_list = []
                    for nm in names:
                        lid = name_to_id.get(nm.lower())
                        if lid is not None:
                            ids_list.append(str(lid))

                    if ids_list:
                        old_csv = self._norm(cf.get("zabbix_template_id"))
                        new_csv = ",".join(ids_list)
                        if overwrite or old_csv != new_csv:
                            cf["zabbix_template_id"] = new_csv
                            if commit:
                                obj.custom_field_data = cf; obj.save()
                            ids_updated += 1
                        else:
                            ids_skipped += 1

                    # SLA from Role
                    cf, _ = self._ensure_sla(obj, cf, overwrite=overwrite)
                    if commit:
                        obj.custom_field_data = cf; obj.save()

                    ok, cf_final = self._ready_eval(obj, cf)
                    if commit:
                        obj.custom_field_data = cf_final; obj.save()
                    if ok: status_true += 1
                    else:  status_false += 1

            if not commit:
                self.log_info("Dry run: no changes committed."); transaction.set_rollback(True)

        self.log_info(f"Template: primary updates={tmpl_primary_updates}, primary skips={tmpl_primary_skips}")
        self.log_info(f"Template IDs: updated={ids_updated}, skipped={ids_skipped}")
        self.log_info(f"Status: Ready={status_true}, NotReady={status_false}; "
                      f"Checked Devices={devices_checked}, VMs={vms_checked}; "
                      f"Skipped Step1={step1_skips}, Step2={step2_skips}")
