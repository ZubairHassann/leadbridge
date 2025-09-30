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
    """

    if request.method != "POST":
        return JsonResponse({"error": "Invalid method"}, status=405)

    query_token = request.GET.get("token")
    header_token = request.headers.get("X-Token")
    if query_token != settings.CALLRAIL_WEBHOOK_TOKEN and header_token != settings.CALLRAIL_WEBHOOK_TOKEN:
        return JsonResponse({"error": "Unauthorized"}, status=401)

    try:
        data = json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    call_id = data.get("id") or f"call-{int(time.time())}"
    phone = data.get("caller_number")
    gclid = data.get("gclid")
    lead_status = (data.get("lead_status") or "").lower()

    record, created = CallRecord.objects.get_or_create(
        callrail_id=call_id,
        defaults={
            "phone": phone,
            "gclid": gclid,
            "lead_status": lead_status,
            "payload": data,
        },
    )

    if lead_status in ["good", "qualified"]:
        process_call_record.delay(record.id)

    return JsonResponse({"status": "received"})
