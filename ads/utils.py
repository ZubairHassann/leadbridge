import requests
from django.conf import settings

def fetch_orders_from_shopmonkey(phone):
    url = "https://api.shopmonkey.io/v1/orders"
    headers = {"Authorization": f"Bearer {settings.SHOPMONKEY_API_KEY}"}
    resp = requests.get(url, headers=headers, params={"customerPhone": phone})
    resp.raise_for_status()
    return resp.json()  # adjust to actual API structure

def send_google_conversion(conv_id, gclid, value):
    # TODO: Implement Google Ads API client upload
    # Use google-ads-python or REST endpoint
    pass
