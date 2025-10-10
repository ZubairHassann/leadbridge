import json
import time
import logging
from django.conf import settings
from rest_framework.pagination import PageNumberPagination
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import CallRecord
from .tasks import process_call_record
from rest_framework.response import Response
from rest_framework.decorators import api_view
from .models import CallRecord, ShopmonkeyOrder, OfflineConversion
from .serializers import CallRecordSerializer, ShopmonkeyOrderSerializer, OfflineConversionSerializer
# Configure a logger for this module
logger = logging.getLogger(__name__)

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
    if query_token != settings.CALLRAIL_WEBHOOK_TOKEN and header_token != settings.CALLRAIL_WEBHOOK_TOKEN:
        logger.warning("Unauthorized webhook attempt. Headers: %s | Params: %s", request.headers, request.GET.dict())
        return JsonResponse({"error": "Unauthorized"}, status=401)

    # --- Try to parse JSON or fallback to query params
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
    phone = data.get("caller_number") or data.get("callernum") or data.get("customer_phone_number")
    gclid = data.get("gclid")
    lead_status = (data.get("lead_status") or data.get("callsource") or "").lower()
    duration = data.get("duration")

    # Extra metadata
    caller_name = data.get("callername")
    caller_city = data.get("callercity")
    caller_state = data.get("callerstate")
    caller_country = data.get("callercountry")
    tracking_number = data.get("trackingnum")
    recording_url = data.get("recording")

    logger.info("Parsed webhook fields: call_id=%s, phone=%s, gclid=%s, lead_status=%s",
                call_id, phone, gclid, lead_status)

    # --- Save or reuse CallRecord
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

    # --- Trigger async processing if lead is qualified
    if lead_status in ["good", "qualified"]:
        logger.info("Lead is qualified. Triggering process_call_record for record %s", record.id)
        process_call_record.delay(record.id)
    else:
        logger.info("Lead status not qualified: %s", lead_status)

    return JsonResponse({"status": "received"})




@api_view(["GET"])
def callrail_records(request):
    """
    List all CallRail webhook records with pagination.
    """
    records = CallRecord.objects.all().order_by("-created_at")

    # Initialize paginator
    paginator = PageNumberPagination()
    paginator.page_size = 20  # you can adjust this (e.g., 10, 50, etc.)
    
    # Paginate queryset
    result_page = paginator.paginate_queryset(records, request)
    serializer = CallRecordSerializer(result_page, many=True)
    
    # Return paginated response
    return paginator.get_paginated_response(serializer.data)


@api_view(["GET"])
def shopmonkey_orders(request):
    """
    List all Shopmonkey orders fetched from API.
    """
    orders = ShopmonkeyOrder.objects.all().order_by("-fetched_at")
    serializer = ShopmonkeyOrderSerializer(orders, many=True)
    return Response(serializer.data)


@api_view(["GET"])
def offline_conversions(request):
    """
    List all Google Ads conversions uploaded.
    """
    conversions = OfflineConversion.objects.all().order_by("-created_at")
    serializer = OfflineConversionSerializer(conversions, many=True)
    return Response(serializer.data)
