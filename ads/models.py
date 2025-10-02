from django.db import models
from django.utils import timezone

from django.db import models
from django.utils import timezone

class CallRecord(models.Model):
    callrail_id = models.CharField(max_length=255, unique=True)
    phone = models.CharField(max_length=32)
    gclid = models.CharField(max_length=512, blank=True, null=True)
    lead_status = models.CharField(max_length=64, blank=True, null=True)
    duration = models.IntegerField(null=True, blank=True)

    # New fields for richer reporting
    caller_name = models.CharField(max_length=255, blank=True, null=True)
    caller_city = models.CharField(max_length=128, blank=True, null=True)
    caller_state = models.CharField(max_length=64, blank=True, null=True)
    caller_country = models.CharField(max_length=64, blank=True, null=True)
    tracking_number = models.CharField(max_length=32, blank=True, null=True)
    recording_url = models.URLField(blank=True, null=True)

    payload = models.JSONField()  # full raw data
    created_at = models.DateTimeField(default=timezone.now)
    processed = models.BooleanField(default=False)

    def __str__(self):
        return f"Call {self.callrail_id} from {self.phone}"


class ShopmonkeyOrder(models.Model):
    order_id = models.CharField(max_length=255, unique=True)
    phone = models.CharField(max_length=32)
    total_cents = models.BigIntegerField()
    archived = models.BooleanField(default=False)
    raw = models.JSONField()
    fetched_at = models.DateTimeField(auto_now_add=True)

class OfflineConversion(models.Model):
    gclid = models.CharField(max_length=512)
    order = models.ForeignKey(ShopmonkeyOrder, on_delete=models.CASCADE)
    value = models.DecimalField(max_digits=12, decimal_places=2)
    uploaded = models.BooleanField(default=False)
    upload_response = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
