import requests
import certifi
from django.conf import settings

def fetch_orders_from_shopmonkey(phone):
    url = "https://api.shopmonkey.cloud/v3/orders"
    headers = {
        "Authorization": f"Bearer {settings.SHOPMONKEY_API_KEY}",
        "Content-Type": "application/json"
    }
    params = {"customerPhone": phone}

    try:
        resp = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=15,
            verify=certifi.where()  # use certifi bundle
        )
        print("Shopmonkey API status:", resp.status_code)
        print("Shopmonkey API response:", resp.text)

        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print("Shopmonkey API error:", str(e))
        return None
