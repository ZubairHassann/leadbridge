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
    Process a call record by:
    1) Fetching the CallRecord.
    2) Looking up Shopmonkey orders for the phone.
    3) Uploading offline conversions to Google Ads.
    """

    # 1) Fetch base record
    base = CallRecord.objects.only("id", "phone", "gclid", "processed").get(id=record_id)
    phone = base.phone
    gclid = base.gclid

    # 2) Fetch orders from Shopmonkey
    try:
        orders = fetch_orders_by_phone(phone)

        if orders is None:
            return f"fetch_failed:{phone}"
        if isinstance(orders, dict) and orders.get("no_customer"):
            return f"no_customer_found:{phone}"
        if not orders:
            return f"customer_found_but_no_orders:{phone}"

    except ShopmonkeyWAFBlocked:
        return "waf_blocked"
    except Exception as e:
        raise self.retry(exc=e, countdown=60)

    created_any = False

    # 3) Process inside a DB transaction
    with transaction.atomic():
        record = CallRecord.objects.select_for_update().get(id=record_id)
        if record.processed:
            return "already_processed"

        now_dt = timezone.now()

        for o in orders:
            archived = bool(o.get("archived"))
            try:
                total_cents = int(o.get("totalCostCents") or 0)
            except (TypeError, ValueError):
                total_cents = 0

            # Only process completed orders with value
            if not (archived and total_cents > 0):
                continue

            order_id = str(o.get("id") or "")

            # Save or update Shopmonkey order
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

            # Calculate value
            try:
                value = Decimal(total_cents) / Decimal(100)
            except (InvalidOperation, ZeroDivisionError):
                value = Decimal("0")

            # Save or update conversion record
            conv, created = OfflineConversion.objects.get_or_create(
                gclid=gclid,
                order=order,
                defaults={"value": value, "uploaded_at": None},
            )
            if not created and conv.uploaded_at:
                continue  # already uploaded

            # Parse conversion time
            completed_iso = o.get("completedAt") or o.get("completed_at")
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

            # Save Google Ads response
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

        # Mark as processed
        record.processed = True
        record.save(update_fields=["processed"])

    if created_any:
        return f"orders_processed:{phone}"
    else:
        return f"orders_found_but_not_qualified:{phone}"
