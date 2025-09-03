from dcim.models import Device

updated = skipped = 0
for d in Device.objects.all().select_related("role"):
    cf = dict(d.custom_field_data or {})
    mon_req = bool(cf.get("mon_req", False))
    if not mon_req:
        # keep it simple: only touch devices you've marked for monitoring
        continue
    role = getattr(d, "role", None)
    role_cf = dict(getattr(role, "custom_field_data", {}) or {})
    code = (role_cf.get("sla_report_code") or "").strip()
    if cf.get("sla_code") == code:
        skipped += 1
        continue
    cf["sla_code"] = code
    d.custom_field_data = cf
    d.save()
    updated += 1

print(f"Devices updated: {updated}, skipped (already correct): {skipped}")
