from decimal import Decimal
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from celery import shared_task
from django.db import transaction
from google.ads.googleads.errors import GoogleAdsException
from django.conf import settings
import hashlib, logging, re

from .models import CallRecord, ShopmonkeyOrder, OfflineConversion
from .services.shopmonkey import fetch_orders_by_phone, ShopmonkeyWAFBlocked
from ads.services.google_ads import (
    upload_gclid_conversion,
    format_ads_datetime,
    upload_enhanced_conversion,
)

logger = logging.getLogger(__name__)

# ==============================
# Helpers for Enhanced Conversion
# ==============================
def normalize_phone(phone: str):
    """Strip non-digits and leading country code."""
    if not phone:
        return None
    digits = re.sub(r"[^0-9]", "", phone)
    return digits.lstrip("1")


def hash_identifier(value: str):
    """Lowercase + SHA256 hash for phone/email."""
    if not value:
        return None
    normalized = value.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# ======================================
# Celery Task: Process Call Record
# ======================================
@shared_task(bind=True, max_retries=3, default_retry_delay=86400)  # retry every 24h
def process_call_record(self, record_id: int):
    """
    Processes a CallRecord:
    1️⃣ Fetches related Shopmonkey orders.
    2️⃣ Saves them in DB.
    3️⃣ Uploads conversions to Google Ads (GCLID or Enhanced).
    Retries automatically if no qualifying orders yet.
    """

    base = CallRecord.objects.only("id", "phone", "gclid", "processed").get(id=record_id)
    phone = base.phone
    gclid = base.gclid

    logger.warning(f"📞 Processing CallRecord ID={record_id}, phone={phone}, gclid={gclid}")

    # Step 1 — Fetch Shopmonkey Orders
    try:
        orders = fetch_orders_by_phone(phone)
        if not orders:
            logger.warning(f"⏳ No orders found yet for {phone} — retrying in 24 hours.")
            raise self.retry(countdown=86400)
    except ShopmonkeyWAFBlocked:
        logger.error("🚫 Shopmonkey WAF blocked the request.")
        return "waf_blocked"
    except Exception as e:
        logger.exception(f"❌ Error fetching orders: {e}")
        raise self.retry(exc=e, countdown=3600)

    created_any = False
    qualifying_found = False
    now_dt = timezone.now()

    with transaction.atomic():
        record = CallRecord.objects.select_for_update().get(id=record_id)
        if record.processed:
            logger.info("ℹ️ Already processed — skipping duplicate run.")
            return "already_processed"

        for o in orders:
            archived = bool(o.get("archived"))
            paid = bool(o.get("paid"))
            invoiced = bool(o.get("invoiced"))

            try:
                total_cents = int(o.get("totalCostCents") or 0)
            except (TypeError, ValueError):
                total_cents = 0

            logger.warning(
                f"🧾 Order: archived={archived}, paid={paid}, invoiced={invoiced}, total_cents={total_cents}"
            )

            if not ((archived or paid or invoiced) and total_cents > 0):
                logger.info("⏭️ Order not finalized yet — will recheck later.")
                continue

            qualifying_found = True
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

            # ========================================
            # Upload logic — GCLID or Enhanced Conversion
            # ========================================
            if gclid:
                logger.warning(f"📤 Uploading conversion via GCLID={gclid}")
                completed_iso = o.get("completedAt") or o.get("completed_at")
                completed_dt = parse_datetime(completed_iso) if isinstance(completed_iso, str) else None
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
                    logger.info(f"✅ GCLID upload response: {getattr(resp, 'results', 'No results')}")
                except GoogleAdsException as ex:
                    logger.error(f"⚠️ Google Ads upload failed: {ex}")
                    raise self.retry(exc=ex, countdown=7200)

            else:
                # Enhanced Conversion Upload (Hashed Identifiers)
                phone_hash = hash_identifier(normalize_phone(record.phone))
                email_hash = hash_identifier(record.payload.get("customer_email"))

                if not (phone_hash or email_hash):
                    logger.warning("⚠️ Skipping upload — no GCLID or user identifiers available.")
                    continue

                logger.warning("📤 Uploading via Enhanced Conversion (hashed phone/email)")
                resp = upload_enhanced_conversion(
                    phone_hash=phone_hash,
                    email_hash=email_hash,
                    value=float(value),
                    conversion_time=format_ads_datetime(now_dt),
                    order_id=order_id,
                )

                # ✅ Match-rate logging
                if resp and hasattr(resp, "partial_failure_error"):
                    logger.warning(f"⚠️ Partial Failure: {resp.partial_failure_error.message}")
                elif not resp:
                    logger.warning("⚠️ No response — likely no user data matched (Google couldn’t find user).")
                else:
                    logger.info("✅ Enhanced Conversion accepted by Google Ads (match pending confirmation).")

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

        # ✅ Mark record processed only if qualifying or uploaded
        if created_any or qualifying_found:
            record.processed = True
            record.save(update_fields=["processed"])
        else:
            logger.warning(f"🕓 Orders exist but none closed yet for {phone} — retrying in 24h.")
            raise self.retry(countdown=86400)

    logger.info(f"✅ Finished processing CallRecord {record_id} — created_any={created_any}")
    return "ok" if created_any else "pending_orders"
