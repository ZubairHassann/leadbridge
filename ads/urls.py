from django.urls import path
from .views import callrail_records, callrail_webhook, offline_conversions, shopmonkey_orders

urlpatterns = [
    # Webhook endpoint that CallRail will post to
    path('webhooks/callrail/', callrail_webhook, name='callrail_webhook'),
        # New API endpoints
    path("callrail-records/", callrail_records, name="callrail_records"),
    path("shopmonkey-orders/", shopmonkey_orders, name="shopmonkey_orders"),
    path("offline-conversions/", offline_conversions, name="offline_conversions"),
]

