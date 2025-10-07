from decimal import Decimal, InvalidOperation
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from celery import shared_task
from django.db import transaction
from google.ads.googleads.errors import GoogleAdsException
from django.conf import settings

from .models import CallRecord, ShopmonkeyOrder, OfflineConversion
from .services.shopmonkey import fetch_orders_by_phone, ShopmonkeyWAFBlocked
from ads.services.google_ads import upload_gclid_conversion, format_ads_datetime


@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def process_call_record(self, record_id: int):
    """
    Processes a CallRecord:
    1. Fetches related Shopmonkey orders.
    2. Saves them in DB.
    3. Uploads conversion to Google Ads if criteria met.
    """

    # Step 1: Get the CallRecord
    base = CallRecord.objects.only("id", "phone", "gclid", "processed").get(id=record_id)
    phone = base.phone
    gclid = base.gclid

    print(f"📞 Processing CallRecord ID={record_id}, phone={phone}, gclid={gclid}")

    # Step 2: Fetch orders from Shopmonkey
    try:
        orders = fetch_orders_by_phone(phone)
        if not orders:
            print("⚠️ No orders found for phone:", phone)
            return "no_orders_found"
    except ShopmonkeyWAFBlocked:
        print("🚫 Shopmonkey WAF blocked the request.")
        return "waf_blocked"
    except Exception as e:
        print("❌ Error fetching orders:", str(e))
        raise self.retry(exc=e, countdown=60)

    created_any = False

    # Step 3: Process orders atomically
    with transaction.atomic():
        record = CallRecord.objects.select_for_update().get(id=record_id)
        if record.processed:
            print("ℹ️ Already processed.")
            return "already_processed"

        now_dt = timezone.now()

        for o in orders:
            archived = bool(o.get("archived"))
            paid = bool(o.get("paid"))
            invoiced = bool(o.get("invoiced"))

            try:
                total_cents = int(o.get("totalCostCents") or 0)
            except (TypeError, ValueError):
                total_cents = 0

            print(f"🧾 Order: archived={archived}, paid={paid}, invoiced={invoiced}, total_cents={total_cents}")

            # ✅ Allow any of archived, paid, or invoiced orders
            if not ((archived or paid or invoiced) and total_cents > 0):
                print("⏭️ Skipping non-qualifying order")
                continue

            order_id = str(o.get("id") or "")

            # Save order
            order, _ = ShopmonkeyOrder.objects.update_or_create(
                order_id=order_id,
                defaults={
                    "phone": record.phone,
                    "total_cents": total_cents,
                    "archived": archived,
                    "raw": o,
                },
            )

            if not gclid:
                print("⚠️ Skipping Google Ads upload — no GCLID found.")
                continue

            try:
                value = Decimal(total_cents) / Decimal(100)
            except (InvalidOperation, ZeroDivisionError):
                value = Decimal("0")

            conv, created = OfflineConversion.objects.get_or_create(
                gclid=gclid,
                order=order,
                defaults={"value": value, "uploaded": False},
            )

            if conv.uploaded:
                print("🟡 Conversion already uploaded for this order.")
                continue

            completed_iso = o.get("completedAt") or o.get("completed_at")
            completed_dt = parse_datetime(completed_iso) if isinstance(completed_iso, str) else None
            conv_time = format_ads_datetime(completed_dt or now_dt)

            print(f"📤 Uploading conversion for GCLID={gclid} Value=${value} OrderID={order_id}")

            # Upload to Google Ads
            try:
                resp = upload_gclid_conversion(
                    customer_id=str(settings.GOOGLE_CUSTOMER_ID).replace("-", ""),
                    conversion_action_resource=settings.GOOGLE_CONVERSION_ACTION_RESOURCE,
                    gclid=gclid,
                    conversion_date_time=conv_time,
                    value=float(value),
                    currency=getattr(settings, "GOOGLE_CURRENCY_CODE", "USD"),
                    order_id=order_id or None,
                )
            except GoogleAdsException as ex:
                print("⚠️ Google Ads upload failed:", str(ex))
                raise self.retry(exc=ex, countdown=90)

            # Save upload result
            results = getattr(resp, "results", []) or []
            try:
                serialized_results = [
                    getattr(r, "conversion_action", None) or getattr(r, "gclid", None) or str(r)
                    for r in results
                ]
            except Exception:
                serialized_results = [str(results)]

            conv.upload_response = serialized_results
            conv.uploaded = True
            conv.save(update_fields=["upload_response", "uploaded"])
            created_any = True

        # Mark CallRecord as processed
        record.processed = True
        record.save(update_fields=["processed"])

    print(f"✅ Finished processing CallRecord {record_id} — created_any={created_any}")
    return "ok" if created_any else "no_matching_orders"
