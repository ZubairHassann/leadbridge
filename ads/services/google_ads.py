from __future__ import annotations
from typing import Optional
from datetime import datetime
import json

from django.conf import settings
from django.utils import timezone

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


# ==============================================================
# 🕒 Helper — Format date for Google Ads
# ==============================================================
def format_ads_datetime(dt: datetime) -> str:
    """
    Google Ads expects: 'YYYY-MM-DD HH:MM:SS±HH:MM' (account timezone offset included).
    If dt is naive, we make it aware using Django's current timezone.
    """
    if dt.tzinfo is None:
        dt = timezone.make_aware(dt, timezone.get_current_timezone())

    # Python's %z gives +0500; convert to +05:00
    offset = dt.strftime("%z")  # e.g. '+0500'
    offset_colon = f"{offset[:-2]}:{offset[-2:]}" if offset else "+00:00"
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')}{offset_colon}"


# ==============================================================
# ⚙️ Helper — Build Google Ads API Client
# ==============================================================
def build_client() -> GoogleAdsClient:
    """
    Builds a GoogleAdsClient from Django settings,
    including manager (MCC) and client Ads account IDs.
    """
    cfg = {
        "developer_token": settings.GOOGLE_DEVELOPER_TOKEN,
        "use_proto_plus": True,
        "refresh_token": settings.GOOGLE_REFRESH_TOKEN,
        "client_id": settings.GOOGLE_CLIENT_ID,
        "client_secret": settings.GOOGLE_CLIENT_SECRET,

        # ⚙️ Add these 2 keys 👇
        "login_customer_id": "7671185681",   # your MCC / manager account ID
        "linked_customer_id": "4066522290",  # your actual Ads client account
    }

    return GoogleAdsClient.load_from_dict(cfg)



# ==============================================================
# 🚀 Upload a single offline conversion (GCLID-based)
# ==============================================================
def upload_gclid_conversion(
    *,
    customer_id: str,
    conversion_action_resource: str,
    gclid: str,
    conversion_date_time: str,
    value: float,
    currency: str = None,
    order_id: Optional[str] = None,
    validate_only: Optional[bool] = None,
):
    """
    Upload a single GCLID-based offline conversion (ClickConversion).

    Returns:
        dict: JSON-serializable response with results and/or partial failure.
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
    request.partial_failure = True
    request.validate_only = validate_only

    try:
        response = service.upload_click_conversions(request=request)
        results = []

        # Extract detailed results
        for res in response.results:
            res_data = {
                "gclid": getattr(res, "gclid", None),
                "conversion_action": getattr(res, "conversion_action", None),
                "success": bool(getattr(res, "gclid", None)),
            }
            results.append(res_data)

        # Partial failure handling
        partial_error = None
        if response.partial_failure_error:
            partial_error = response.partial_failure_error.message
            print(f"⚠️ Partial failure from Google Ads: {partial_error}")
        else:
            print("✅ Conversion upload successful:", results)

        # Return structured dict (easy to log or save in DB)
        return {
            "results": results,
            "partial_failure": partial_error,
        }

    except GoogleAdsException as ex:
        # Capture full failure info for logs
        error_info = {
            "request_id": ex.request_id,
            "failure": str(ex.failure),
            "error_code": ex.error.code().name if ex.error else None,
        }
        print("🚨 GoogleAdsException:", json.dumps(error_info, indent=2))
        return {"error": error_info}


# ==============================================================
# 🧩 Legacy Wrapper (Backward Compatibility)
# ==============================================================
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
    customer_id = str(settings.GOOGLE_CUSTOMER_ID)
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
