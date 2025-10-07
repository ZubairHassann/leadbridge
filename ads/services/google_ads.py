from __future__ import annotations

from typing import Optional
from datetime import datetime

from django.conf import settings
from django.utils import timezone

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def format_ads_datetime(dt: datetime) -> str:
    """
    Google Ads expects: 'YYYY-MM-DD HH:MM:SS±HH:MM' (account timezone offset included).
    If dt is naive, we make it aware using Django's current timezone.
    """
    if dt.tzinfo is None:
        dt = timezone.make_aware(dt, timezone.get_current_timezone())

    # Python's %z is like +0500; convert to +05:00
    offset = dt.strftime("%z")  # e.g. '+0500'
    offset_colon = f"{offset[:-2]}:{offset[-2:]}" if offset else "+00:00"
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')}{offset_colon}"


def build_client() -> GoogleAdsClient:
    """
    Builds a GoogleAdsClient from Django settings.

    Required:
      - GOOGLE_DEVELOPER_TOKEN
      - GOOGLE_REFRESH_TOKEN
      - GOOGLE_CLIENT_ID
      - GOOGLE_CLIENT_SECRET

    Optional:
      - GOOGLE_LOGIN_CUSTOMER_ID (manager account). If absent, we log in directly to the customer.
    """
    login_customer_id = getattr(settings, "GOOGLE_LOGIN_CUSTOMER_ID", None)
    cfg = {
        "developer_token": settings.GOOGLE_DEVELOPER_TOKEN,
        "use_proto_plus": True,
        "refresh_token": settings.GOOGLE_REFRESH_TOKEN,
        "client_id": settings.GOOGLE_CLIENT_ID,
        "client_secret": settings.GOOGLE_CLIENT_SECRET,
    }
    if login_customer_id:
        cfg["login_customer_id"] = str(login_customer_id)

    return GoogleAdsClient.load_from_dict(cfg)


def upload_gclid_conversion(
    *,
    customer_id: str,
    conversion_action_resource: str,  # e.g. "customers/1234567890/conversionActions/1122334455"
    gclid: str,
    conversion_date_time: str,        # formatted via format_ads_datetime(...)
    value: float,
    currency: str = None,
    order_id: Optional[str] = None,
    validate_only: Optional[bool] = None,
):
    """
    Upload a single GCLID-based offline conversion (ClickConversion).

    Args:
        customer_id: The Ad account numeric ID without dashes, e.g. "1234567890".
        conversion_action_resource: Full resource name for the conversion action.
        gclid: The GCLID captured at click time.
        conversion_date_time: 'YYYY-MM-DD HH:MM:SS±HH:MM' (use format_ads_datetime()).
        value: Monetary value of the conversion.
        currency: ISO currency code (default comes from settings.GOOGLE_CURRENCY_CODE or 'USD').
        order_id: Optional order/external ID for deduplication.
        validate_only: If True, Google will validate but not persist (dry-run).

    Returns:
        UploadClickConversionsResponse
    """
    if not currency:
        currency = getattr(settings, "GOOGLE_CURRENCY_CODE", "USD")

    if validate_only is None:
        validate_only = bool(getattr(settings, "GOOGLE_VALIDATE_ONLY", False))

    client = build_client()
    service = client.get_service("ConversionUploadService")

    conversion = client.get_type("ClickConversion")
    conversion.gclid = gclid
    conversion.conversion_action = conversion_action_resource
    conversion.conversion_date_time = conversion_date_time
    conversion.conversion_value = float(value)
    conversion.currency_code = currency
    if order_id:
        conversion.order_id = str(order_id)

    request = client.get_type("UploadClickConversionsRequest")
    request.customer_id = str(customer_id)
    request.conversions.append(conversion)
    request.partial_failure = True  #required for Ads v21
    request.validate_only = validate_only



    try:
        response = service.upload_click_conversions(request=request)
        # Optional: You can inspect response.results for partial errors handling per-conversion.
        return response
    except GoogleAdsException as ex:
        # Bubble up so caller (e.g., Celery task) can retry/backoff
        raise


# ---- Backward compatibility shim ----
def send_offline_conversion_to_google(
    gclid: str,
    conversion_time: str,
    value: float,
    external_id: Optional[str] = None,
):
    """
    Legacy wrapper for existing code paths.
    Uses settings.GOOGLE_CUSTOMER_ID and settings.GOOGLE_CONVERSION_ACTION_RESOURCE.
    """
    customer_id = str(settings.GOOGLE_CUSTOMER_ID)  # upload account ID (no dashes)
    action_resource = settings.GOOGLE_CONVERSION_ACTION_RESOURCE

    return upload_gclid_conversion(
        customer_id=customer_id,
        conversion_action_resource=action_resource,
        gclid=gclid,
        conversion_date_time=conversion_time,
        value=float(value),
        currency=getattr(settings, "GOOGLE_CURRENCY_CODE", "USD"),
        order_id=external_id,
        validate_only=bool(getattr(settings, "GOOGLE_VALIDATE_ONLY", False)),
    )
