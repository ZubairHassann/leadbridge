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
    1. Read CallRecord.
    2. Fetch Shopmonkey orders via phone → customerId → orders.
    3. Lock record, upsert orders + upload conversions.
    """

    # Step 1: Fetch base record
    base = CallRecord.objects.only("id", "phone", "gclid", "processed").get(id=record_id)
    phone = base.phone
    gclid = base.gclid

    # Step 2: External API call (Shopmonkey)
    try:
        orders = fetch_orders_by_phone(phone)
        if not orders:
            return "no_orders_found"
    except ShopmonkeyWAFBlocked:
        return "waf_blocked"
    except Exception as e:
        # Retry for transient network errors
        raise self.retry(exc=e, countdown=60)

    created_any = False

    # Step 3: Critical section
    with transaction.atomic():
        record = CallRecord.objects.select_for_update().get(id=record_id)
        if record.processed:
            return "already_processed"

        now_dt = timezone.now()

        for o in orders:
            archived = bool(o.get("archived"))
            try:
                total_cents = int(o.get("totalCostCents") or o.get("totalCostCents") or 0)
            except (TypeError, ValueError):
                total_cents = 0

            # Only process archived + paid orders with value
            if not (archived and total_cents > 0):
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
                continue

            # Compute conversion value
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
                continue  # already uploaded

            completed_iso = (
                o.get("completedAt") or
                o.get("completed_at") or
                o.get("orderCreatedDate")
            )
            completed_dt = parse_datetime(completed_iso) if isinstance(completed_iso, str) else None
            conv_time = format_ads_datetime(completed_dt or now_dt)

            # Upload conversion to Google Ads
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
                raise self.retry(exc=ex, countdown=90)

            # Save upload response
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
            conv.uploaded = True
            conv.save(update_fields=["upload_response", "uploaded_at", "uploaded"])
            created_any = True

        # Mark as processed
        record.processed = True
        record.save(update_fields=["processed"])

    return "ok" if created_any else "no_matching_orders"
