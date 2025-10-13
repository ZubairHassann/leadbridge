from decimal import Decimal, InvalidOperation
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from celery import shared_task
from django.db import transaction
from google.ads.googleads.errors import GoogleAdsException
from django.conf import settings
import hashlib

from .models import CallRecord, ShopmonkeyOrder, OfflineConversion
from .services.shopmonkey import fetch_orders_by_phone, ShopmonkeyWAFBlocked
from ads.services.google_ads import (
    upload_gclid_conversion,
    format_ads_datetime,
    upload_enhanced_conversion,  # <-- you'll add this below
)


# ==========================
# Helper: Hash identifiers
# ==========================
def hash_identifier(value: str):
    """Normalize and hash a phone or email for Enhanced Conversions."""
    if not value:
        return None
    normalized = value.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# ======================================
# Celery Task: Process Call Record
# ======================================
@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def process_call_record(self, record_id: int):
    """
    Processes a CallRecord:
    1. Fetches related Shopmonkey orders.
    2. Saves them in DB.
    3. Uploads conversion to Google Ads if criteria met.
    """

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

            if not ((archived or paid or invoiced) and total_cents > 0):
                print("⏭️ Skipping non-qualifying order")
                continue

            order_id = str(o.get("id") or "")

            order, _ = ShopmonkeyOrder.objects.update_or_create(
                order_id=order_id,
                defaults={
                    "phone": record.phone,
                    "total_cents": total_cents,
                    "archived": archived,
                    "raw": o,
                },
            )

            value = Decimal(total_cents) / Decimal(100) if total_cents else Decimal("0")

            # ======================================
            # Upload logic — GCLID or Enhanced
            # ======================================
            if gclid:
                print(f"📤 Uploading conversion via GCLID={gclid}")

                # 🛠 Safely parse completedAt or fallback to now
                completed_iso = o.get("completedAt") or o.get("completed_at")
                if isinstance(completed_iso, str):
                    completed_dt = parse_datetime(completed_iso)
                else:
                    completed_dt = None

                conv_time = format_ads_datetime(completed_dt or now_dt)

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

            else:
                # ⚙️ Fallback to Enhanced Conversions (hashed phone/email)
                phone_hash = hash_identifier(record.phone)
                email_hash = hash_identifier(record.payload.get("customer_email"))
                if not (phone_hash or email_hash):
                    print("⚠️ Skipping upload — no GCLID or user identifiers available.")
                    continue

                print("📤 Uploading via Enhanced Conversion (hashed phone/email)")
                resp = upload_enhanced_conversion(
                    phone_hash=phone_hash,
                    email_hash=email_hash,
                    value=float(value),
                    conversion_time=format_ads_datetime(now_dt),
                    order_id=order_id,
                )

            # ✅ Save conversion record
            conv, created = OfflineConversion.objects.get_or_create(
                gclid=gclid or phone_hash or email_hash,
                order=order,
                defaults={"value": value, "uploaded": False},
            )

            conv.upload_response = getattr(resp, "results", []) or []
            conv.uploaded = True
            conv.save(update_fields=["upload_response", "uploaded"])
            created_any = True

            # ✅ Mark CallRecord processed after all orders
            record.processed = True
            record.save(update_fields=["processed"])

            print(f"✅ Finished processing CallRecord {record_id} — created_any={created_any}")
            return "ok" if created_any else "no_matching_orders"

