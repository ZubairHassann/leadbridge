from decimal import Decimal, InvalidOperation
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from celery import shared_task
from django.db import transaction
from google.ads.googleads.errors import GoogleAdsException

from .models import CallRecord, ShopmonkeyOrder, OfflineConversion
from .services.shopmonkey import fetch_orders_by_phone, ShopmonkeyWAFBlocked
from ads.services.google_ads import upload_gclid_conversion, format_ads_datetime
from django.conf import settings


@shared_task(bind=True, max_retries=3, default_retry_delay=30)
def process_call_record(self, record_id: int):
    """
    1) Read CallRecord (no lock) to get phone/gclid.
    2) Fetch Shopmonkey orders (no DB lock held).
    3) Open atomic block, lock CallRecord, re-check processed, upsert orders,
       upload conversions (network), mark processed. Minimal lock window.
    """
    # 1) Read lightweight fields without locking
    base = CallRecord.objects.only("id", "phone", "gclid", "processed").get(id=record_id)
    phone = base.phone
    gclid = base.gclid

    # 2) External IO first (no DB lock held)
    try:
        orders = fetch_orders_by_phone(phone)
    except ShopmonkeyWAFBlocked:
        # Cloudflare/WAF HTML 403 — don't keep hammering; surface and stop.
        return "waf_blocked"
    except Exception as e:
        # Network/transient problems — retry
        raise self.retry(exc=e, countdown=60)

    created_any = False

    # 3) Critical section: lock the row and re-check processed
    with transaction.atomic():
        record = CallRecord.objects.select_for_update().get(id=record_id)
        if record.processed:
            return "already_processed"

        now_dt = timezone.now()

        for o in orders or []:
            archived = bool(o.get("archived"))
            try:
                total_cents = int(o.get("totalCostCents") or 0)
            except (TypeError, ValueError):
                total_cents = 0

            if not (archived and total_cents > 0):
                continue

            order_id = str(o.get("id") or "")

            # Mirror order
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
                continue

            try:
                value = Decimal(total_cents) / Decimal(100)
            except (InvalidOperation, ZeroDivisionError):
                value = Decimal("0")

            conv, created = OfflineConversion.objects.get_or_create(
                gclid=gclid,
                order=order,
                defaults={"value": value, "uploaded_at": None},
            )
            if not created and conv.uploaded_at:
                # already uploaded
                continue

            # Parse completion time if present (e.g., "completedAt": ISO string)
            completed_iso = o.get("completedAt") or o.get("completed_at")
            completed_dt = parse_datetime(completed_iso) if isinstance(completed_iso, str) else None
            conv_time = format_ads_datetime(completed_dt or now_dt)

            # Upload to Google Ads (allow retry on transient Ads errors)
            try:
                resp = upload_gclid_conversion(
                    customer_id=str(settings.GOOGLE_CUSTOMER_ID),  # no dashes
                    conversion_action_resource=settings.GOOGLE_CONVERSION_ACTION_RESOURCE,
                    gclid=gclid,
                    conversion_date_time=conv_time,
                    value=float(value),
                    currency=getattr(settings, "GOOGLE_CURRENCY_CODE", "USD"),
                    order_id=order_id or None,
                )
            except GoogleAdsException as ex:
                # Retry only on transient / rate-limit / 5xx-like issues (let caller decide)
                raise self.retry(exc=ex, countdown=90)

            # Persist upload result (store resource names for audit)
            results = getattr(resp, "results", []) or []
            try:
                serialized_results = [
                    getattr(r, "conversion_action", None) or getattr(r, "gclid", None) or str(r)
                    for r in results
                ]
            except Exception:
                serialized_results = [str(results)]

            conv.upload_response = serialized_results
            conv.uploaded_at = timezone.now()
            conv.save(update_fields=["upload_response", "uploaded_at"])
            created_any = True

        # Mark processed to avoid re-firing on the same call record.
        record.processed = True
        record.save(update_fields=["processed"])

    return "ok" if created_any else "no_matching_orders"
