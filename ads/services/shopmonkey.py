import requests
from django.conf import settings
from typing import List, Dict, Any


class ShopmonkeyWAFBlocked(Exception):
    """Raised when Shopmonkey returns a WAF / 403 block."""


def _get_customer_id_by_phone(phone: str, headers: dict) -> str | None:
    """
    Step 1: Look up a customer ID from phone number using Shopmonkey API.
    Returns None if no customer is found.
    """
    url = "https://api.shopmonkey.cloud/v3/customer/phone_number/search"
    payload = {
        "phoneNumbers": [
            {"number": phone}  # ✅ must be object with number
        ]
    }

    print("=== Shopmonkey Customer Lookup ===")
    print("URL:", url)
    print("Payload:", payload)

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15, verify=False)
    except requests.exceptions.RequestException as e:
        print("Shopmonkey API error during customer lookup:", str(e))
        return None  # graceful fallback

    print("Status:", resp.status_code)
    print("Response:", resp.text[:500])

    if resp.status_code == 403 and "cloudflare" in resp.text.lower():
        raise ShopmonkeyWAFBlocked("Shopmonkey WAF blocked the customer lookup request")

    # If 404 or 400, don’t crash — just return None
    if resp.status_code in (400, 404):
        print(f"⚠ No customer found for phone {phone}")
        return None

    resp.raise_for_status()
    data = resp.json().get("data", [])

    if data and isinstance(data, list) and data[0].get("id"):
        customer_id = data[0]["id"]
        print("✅ Found customer ID:", customer_id)
        return customer_id

    print("⚠ No customer found for phone:", phone)
    return None


def fetch_orders_by_phone(phone: str) -> List[Dict[str, Any]]:
    """
    Step 1: Find customer by phone
    Step 2: Fetch all orders for that customer
    Returns [] if no customer or no orders found.
    """
    headers = {
        "Authorization": f"Bearer {settings.SHOPMONKEY_API_KEY}",
        "Content-Type": "application/json",
    }

    # Step 1: Get customer ID
    customer_id = _get_customer_id_by_phone(phone, headers)
    if not customer_id:
        print(f"⚠ No Shopmonkey customer for {phone}, skipping orders.")
        return []

    # Step 2: Fetch orders for customer
    url = f"https://api.shopmonkey.cloud/v3/customer/{customer_id}/order"
    print("=== Shopmonkey Orders Fetch ===")
    print("URL:", url)

    try:
        resp = requests.get(url, headers=headers, timeout=15, verify=False)
    except requests.exceptions.RequestException as e:
        print("Shopmonkey API error during order fetch:", str(e))
        return []  # fallback

    print("Status:", resp.status_code)
    print("Response:", resp.text[:500])

    if resp.status_code == 403 and "cloudflare" in resp.text.lower():
        raise ShopmonkeyWAFBlocked("Shopmonkey WAF blocked the orders fetch request")

    if resp.status_code in (400, 404):
        print(f"⚠ No orders found for customer {customer_id}")
        return []

    resp.raise_for_status()
    return resp.json().get("data", [])
