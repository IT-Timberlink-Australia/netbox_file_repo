# NetBox Script: Zabbix catalog from Custom Objects (dynamic table-aware)
# Finds the per-type table model (e.g., "table{ID}model") created by the
# netbox-custom-objects plugin and reads real columns + Platform relation.
#
# Safe to run dry (Commit OFF). Turn Commit ON after you see platform_mappings > 0.

from dcim.models import Device, Site, Platform
from virtualization.models import VirtualMachine
from extras.scripts import Script, BooleanVar, ObjectVar
from django.apps import apps
from django.db import transaction
from django.db.models import ForeignKey, ManyToManyField

import re

COT_TYPE_SELECTOR = "zabbix-template-list"

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

# We’ll match fields by these slugged names
WANTED = {
    "name": {"template_name", "name", "template"},
    "id": {"template_id", "zabbix_template_id", "id"},
    "iface": {"template_interface_id", "iface_id", "interface_id", "interface"},
    "platform": {"platform", "platforms"},
}
IFACE_MAP = {"agent": 1, "snmp": 2, "ipmi": 3, "jmx": 4}


class ZabbixCatalogFromCustomObjects(Script):
    class Meta:
        name = "Zabbix: Build catalog from Custom Objects (dynamic-table aware)"
        description = "Reads the per-type table model and populates CFs for Devices/VMs"
        commit_default = False

    include_devices = BooleanVar(description="Include Devices", default=True)
    include_vms     = BooleanVar(description="Include Virtual Machines", default=False)
    limit_site      = ObjectVar(description="Soft limit by Site", required=False, model=Site)
    overwrite       = BooleanVar(description="Overwrite existing values", default=False)
    debug_catalog   = BooleanVar(description="Verbose discovery logs", default=True)

    # ---- small helpers
    def _norm(self, v): return (str(v).strip() if v is not None else "")
    def _is_true(self, v): return str(v).lower() in {"1","true","yes","on"}
    def _cf(self, obj): return dict(getattr(obj, "custom_field_data", {}) or {})
    def _role(self, obj): return getattr(obj, "device_role", None) or getattr(obj, "role", None)
    def _has_primary_ip(self, obj): return bool(getattr(obj, "primary_ip4", None) or getattr(obj, "primary_ip6", None))

    # ---- find plugin models
    def _get_type(self):
        # Find the CustomObjectType model and match our type by slug/name/label
        Type = None
        for M in apps.get_models():
            if M._meta.app_label.lower() == "netbox_custom_objects" and "type" in M.__name__.lower() and "field" not in M.__name__.lower():
                Type = M; break
        if not Type:
            raise RuntimeError("CustomObjectType model not found in plugin.")
        fields = {f.name for f in Type._meta.get_fields()}
        for key in ("slug__iexact", "name__iexact", "label__iexact"):
            base = key.split("__",1)[0]
            if base in fields:
                obj = Type.objects.filter(**{key: COT_TYPE_SELECTOR}).first()
                if obj:
                    return obj
        any_type = Type.objects.first()
        if not any_type:
            raise RuntimeError("No CustomObjectType instances exist.")
        self.log_warning(f"[COT] Could not match '{COT_TYPE_SELECTOR}'. Using first type id={any_type.pk}.")
        return any_type

    def _choose_dynamic_row_model(self, type_obj, debug=False):
        """
        Heuristic: among models in app 'netbox_custom_objects' that are NOT
        Type/Field/Value, choose the one whose field names best match our expected
        columns (template_name/id/interface/platform).
        """
        best = None
        best_score = -1
        best_fields = None

        for M in apps.get_models():
            if M._meta.app_label.lower() != "netbox_custom_objects":
                continue
            nm = M.__name__.lower()
            if any(k in nm for k in ("type", "field", "value", "through", "m2m")):
                continue  # skip meta/through models

            field_names = {f.name for f in M._meta.get_fields() if hasattr(f, "name")}
            slugs = {_slug(n) for n in field_names}

            # score overlap with targets
            score = 0
            score += 2 if slugs & WANTED["name"] else 0
            score += 2 if slugs & WANTED["id"] else 0
            score += 1 if slugs & WANTED["iface"] else 0
            score += 1 if slugs & WANTED["platform"] else 0

            # bonus if it has a relation to dcim.Platform
            for f in M._meta.get_fields():
                if isinstance(f, (ManyToManyField, ForeignKey)) and getattr(f.remote_field, "model", None) is Platform:
                    score += 2
                    break

            if score > best_score:
                best, best_score, best_fields = M, score, sorted(field_names)

        if not best or best_score <= 0:
            raise RuntimeError("Could not locate the dynamic table model for this type.")

        if debug:
            self.log_info(f"[COT] Chosen dynamic model: {best._meta.label} (score={best_score})")
            self.log_info(f"[COT] Dynamic model fields: {best_fields}")
        return best

    # ---- extractors from dynamic rows
    def _fieldmap(self, Model):
        names = {f.name for f in Model._meta.get_fields() if hasattr(f, "name")}
        slug_map = { _slug(n): n for n in names }
        pick = lambda wanted: next((slug_map[s] for s in wanted if s in slug_map), None)
        return {
            "name": pick(WANTED["name"]),
            "id": pick(WANTED["id"]),
            "iface": pick(WANTED["iface"]),
            "platform": pick(WANTED["platform"]),
        }

    def _platform_pks_from_row(self, row):
        # M2M to Platform?
        for f in row._meta.get_fields():
            if isinstance(f, ManyToManyField) and getattr(f.remote_field, "model", None) is Platform:
                return list(getattr(row, f.name).values_list("pk", flat=True))
        # FK to Platform?
        for f in row._meta.get_fields():
            if isinstance(f, ForeignKey) and getattr(f.remote_field, "model", None) is Platform:
                obj = getattr(row, f.name, None)
                return [obj.pk] if obj else []
        # Fallback: integer/digit-string field called platform/platforms
        for fname in ("platforms", "platform"):
            if hasattr(row, fname):
                val = getattr(row, fname)
                vals = val if isinstance(val, (list, tuple)) else [val]
                out = []
                for v in vals:
                    if isinstance(v, int):
                        out.append(v)
                    elif isinstance(v, str) and v.strip().isdigit():
                        out.append(int(v.strip()))
                if out:
                    return out
        return []

    def _load_catalog(self, debug=False):
        type_obj = self._get_type()
        RowModel = self._choose_dynamic_row_model(type_obj, debug=debug)
        fmap = self._fieldmap(RowModel)
        if debug:
            self.log_info(f"[COT] Field mapping used: {fmap}")

        name_to_id = {}
        name_to_iface = {}
        by_platform = {}

        rows = list(RowModel.objects.all())
        for row in rows:
            # Template name
            fname = fmap["name"]
            tname = self._norm(getattr(row, fname, None)) if fname else ""
            if not tname:
                continue

            # Template ID
            fid = fmap["id"]
            tid = None
            if fid:
                raw = self._norm(getattr(row, fid, None))
                if raw:
                    try: tid = int(raw)
                    except Exception: tid = None
            if tid is None:
                continue

            # Interface ID
            fif = fmap["iface"]
            tif = None
            if fif:
                raw = self._norm(getattr(row, fif, None)).lower()
                if raw:
                    if raw.isdigit():
                        tif = int(raw)
                    else:
                        tif = IFACE_MAP.get(raw)

            # Platforms
            plat_pks = self._platform_pks_from_row(row)

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

    # ---- SLA + readiness
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

    # ---- object streams
    def _devices(self, site):
        qs = Device.objects.all().select_related("site","role","platform")
        if site: qs = qs.filter(site=site)
        return qs

    def _vms(self):
        return VirtualMachine.objects.all().select_related("role","platform","cluster__site","site","location__site")

    # ---- main
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

                    # Step 1: mon_req + active
                    if not (self._is_true(cf.get("mon_req")) and self._norm(getattr(obj,"status","")) == "active"):
                        cf["mon_req"] = False
                        cf["monitoring_status"] = "Missing Required Fields"
                        step1_skips += 1
                        if commit:
                            obj.custom_field_data = cf; obj.save()
                        continue

                    # Step 2: choose primary by platform
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
                    names, seen = [], set()
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

                    # SLA from Role → device CF
                    cur = self._norm(cf.get("sla_report_code"))
                    if not cur or overwrite:
                        role = self._role(obj)
                        if role:
                            rcf = dict(getattr(role, "custom_field_data", {}) or {})
                            code = self._norm(rcf.get("sla_report_code"))
                            if code:
                                cf["sla_report_code"] = code
                                if commit:
                                    obj.custom_field_data = cf; obj.save()

                    # Final readiness
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
