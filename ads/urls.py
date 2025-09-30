from django.urls import path
from .views import callrail_webhook

urlpatterns = [
    # Webhook endpoint that CallRail will post to
    path('webhooks/callrail/', callrail_webhook, name='callrail_webhook'),
]
