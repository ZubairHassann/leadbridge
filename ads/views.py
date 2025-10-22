import json
import time
import logging
from django.conf import settings
from rest_framework.pagination import PageNumberPagination
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.db.models import Q
from .models import CallRecord, ShopmonkeyOrder, OfflineConversion
from .tasks import process_call_record
from .serializers import (
    CallRecordSerializer,
    ShopmonkeyOrderSerializer,
    OfflineConversionSerializer,
)

# ---------------------------------------------------------
# Logger setup
# ---------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# Helper function — determines if a call is qualified
# ---------------------------------------------------------
def is_call_qualified(lead_status: str, payload: dict) -> bool:
    """
    Determines whether a call qualifies for processing.
    Either by lead_status or by presence of 'qualified' in milestones.
    """
    milestones = (payload.get("milestones") or {})
    has_qualified_milestone = "qualified" in milestones

    return (
        lead_status in [
            "good",
            "good_lead",
            "qualified",
            "qualified_lead",
            "previously_marked_good_lead",
        ]
        or has_qualified_milestone
    )

# ---------------------------------------------------------
# CallRail Webhook
# ---------------------------------------------------------
@csrf_exempt
def callrail_webhook(request):
    """
    Webhook endpoint to receive call data from CallRail.
    Accepts token via query string (?token=...) or header (X-Token).
    Supports both JSON body and query params.
    """
    if request.method != "POST":
        return JsonResponse({"error": "Invalid method"}, status=405)

    # --- Security: token check
    query_token = request.GET.get("token")
    header_token = request.headers.get("X-Token")
    if (
        query_token != settings.CALLRAIL_WEBHOOK_TOKEN
        and header_token != settings.CALLRAIL_WEBHOOK_TOKEN
    ):
        logger.warning(
            "Unauthorized webhook attempt. Headers: %s | Params: %s",
            request.headers,
            request.GET.dict(),
        )
        return JsonResponse({"error": "Unauthorized"}, status=401)

    # --- Parse body
    data = {}
    try:
        if request.body:
            data = json.loads(request.body.decode("utf-8"))
            logger.info("Webhook received JSON body: %s", data)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON body. Falling back to form/query params.")

    if not data and request.POST:
        data = request.POST.dict()
        logger.info("Webhook received form-encoded body: %s", data)

    if not data:
        data = request.GET.dict()
        logger.info("Webhook received query params: %s", data)

    # --- Extract fields
    call_id = data.get("id") or data.get("resource_id") or f"call-{int(time.time())}"
    phone = (
        data.get("caller_number")
        or data.get("callernum")
        or data.get("customer_phone_number")
    )
    gclid = data.get("gclid")
    lead_status = (data.get("lead_status") or data.get("callsource") or "").lower()
    duration = data.get("duration")

    caller_name = data.get("callername")
    caller_city = data.get("callercity")
    caller_state = data.get("callerstate")
    caller_country = data.get("callercountry")
    tracking_number = data.get("trackingnum")
    recording_url = data.get("recording")

    logger.info(
        "Parsed webhook fields: call_id=%s, phone=%s, gclid=%s, lead_status=%s",
        call_id,
        phone,
        gclid,
        lead_status,
    )

    # --- Create or reuse CallRecord
    record, created = CallRecord.objects.get_or_create(
        callrail_id=call_id,
        defaults={
            "phone": phone,
            "gclid": gclid,
            "lead_status": lead_status,
            "duration": int(duration) if duration else None,
            "caller_name": caller_name,
            "caller_city": caller_city,
            "caller_state": caller_state,
            "caller_country": caller_country,
            "tracking_number": tracking_number,
            "recording_url": recording_url,
            "payload": data,
        },
    )

    if created:
        logger.info("New CallRecord created: %s", record.id)
    else:
        logger.info("Duplicate CallRecord received: %s", record.id)

    # --- Qualification Logic (lead_status OR milestone)
    if is_call_qualified(lead_status, data):
        logger.info(
            "✅ Qualified call detected — status=%s | milestones=%s | record_id=%s",
            lead_status,
            list((data.get('milestones') or {}).keys()),
            record.id,
        )
        process_call_record.delay(record.id)
    else:
        logger.info(
            "❌ Skipped unqualified call — status=%s | milestones=%s",
            lead_status,
            list((data.get('milestones') or {}).keys()),
        )

    return JsonResponse({"status": "received"})

# ---------------------------------------------------------
# Paginated list of CallRail records
# ---------------------------------------------------------
@api_view(["GET"])
def callrail_records(request):
    """
    List all CallRail webhook records with pagination.
    """
    records = CallRecord.objects.all().order_by("-created_at")

    paginator = PageNumberPagination()
    paginator.page_size = 20
    result_page = paginator.paginate_queryset(records, request)
    serializer = CallRecordSerializer(result_page, many=True)

    return paginator.get_paginated_response(serializer.data)

# ---------------------------------------------------------
# List Shopmonkey Orders
# ---------------------------------------------------------
@api_view(["GET"])
def shopmonkey_orders(request):
    """
    List all Shopmonkey orders fetched from API.
    Includes computed total_cost in dollars.
    """
    orders = ShopmonkeyOrder.objects.all().order_by("-fetched_at")

    paginator = PageNumberPagination()
    paginator.page_size = 20
    result_page = paginator.paginate_queryset(orders, request)

    formatted_orders = []
    for order in result_page:
        formatted_orders.append({
            "id": order.id,
            "phone": order.phone,
            "archived": order.archived,
            "fetched_at": order.fetched_at,
            "total_cost": round(order.total_cents / 100, 2) if order.total_cents else 0.0,
        })

    return paginator.get_paginated_response(formatted_orders)


# ---------------------------------------------------------
# List Offline Conversions
# ---------------------------------------------------------
@api_view(["GET"])
def offline_conversions(request):
    """
    List all Google Ads conversions uploaded.
    """
    conversions = OfflineConversion.objects.all().order_by("-created_at")
    paginator = PageNumberPagination()
    paginator.page_size = 20
    result_page = paginator.paginate_queryset(conversions, request)
    serializer = OfflineConversionSerializer(result_page, many=True)
    return paginator.get_paginated_response(serializer.data)

# ---------------------------------------------------------
# Qualified Calls View (Paginated)
# ---------------------------------------------------------
@api_view(["GET"])
def qualified_calls(request):
    """
    Paginated list of qualified calls with related order/conversion info.
    """
    records = CallRecord.objects.all().order_by("-created_at")

    qualified_records = []
    for record in records:
        if is_call_qualified(record.lead_status, record.payload):
            conversion = (
    OfflineConversion.objects.filter(
        Q(order__phone=record.phone) | Q(gclid=record.gclid)
    ).first()
)

            order = ShopmonkeyOrder.objects.filter(phone=record.phone).first()

            qualified_records.append({
                "id": record.id,
                "phone": record.phone,
                "lead_status": record.lead_status,
                "caller_name": record.caller_name,
                "created_at": record.created_at,
                "processed": record.processed,
                "has_shopmonkey_order": bool(order),
                "has_offline_conversion": bool(conversion),
                "conversion_uploaded": bool(conversion and conversion.uploaded),
                "conversion_value": float(conversion.value) if conversion else None,
            })

    paginator = PageNumberPagination()
    paginator.page_size = 20
    result_page = paginator.paginate_queryset(qualified_records, request)
    return paginator.get_paginated_response(result_page)
