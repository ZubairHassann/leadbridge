import json
import time
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import CallRecord
from .tasks import process_call_record


import json
import time
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import CallRecord
from .tasks import process_call_record


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
        return JsonResponse({"error": "Unauthorized"}, status=401)

    # --- Try to parse JSON or fallback to query params
    data = {}
    try:
        if request.body:
            data = json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        pass

    if not data:  # fallback to query params
        data = request.GET.dict()

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

    # --- Trigger async processing if lead is qualified
    if lead_status in ["good", "qualified"]:
        process_call_record.delay(record.id)

    return JsonResponse({"status": "received"})

