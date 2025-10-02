import requests
import certifi
from django.conf import settings

class ShopmonkeyWAFBlocked(Exception):
    """Raised when Shopmonkey returns a WAF / 403 block."""


def _get_customer_id_by_phone(phone: str, headers: dict):
    """
    Find a customer by phone. Returns first customer_id or None.
    """
    customer_url = "https://api.shopmonkey.cloud/v3/customers/search"
    params = {"phone": phone}

    print("=== Customer Lookup ===")
    print("URL:", customer_url, "Params:", params)

    resp = requests.get(customer_url, headers=headers, params=params, timeout=15, verify=certifi.where())
    print("Status:", resp.status_code, "Response:", resp.text[:300])

    if resp.status_code == 403 and "cloudflare" in resp.text.lower():
        raise ShopmonkeyWAFBlocked("Blocked by WAF")

    resp.raise_for_status()
    customers = resp.json()

    if isinstance(customers, list) and customers:
        return customers[0].get("id")

    return None


def fetch_orders_by_phone(phone: str):
    """
    Two-step fetch: customer lookup → order fetch.
    """
    headers = {
        "Authorization": f"Bearer {settings.SHOPMONKEY_API_KEY}",
        "Content-Type": "application/json",
    }

    # Step 1: Lookup customer
    customer_id = _get_customer_id_by_phone(phone, headers)
    if not customer_id:
        print("No customer found for phone:", phone)
        return []

    # Step 2: Fetch orders
    orders_url = "https://api.shopmonkey.cloud/v3/orders"
    params = {"customerId": customer_id}

    print("=== Order Fetch ===")
    print("URL:", orders_url, "Params:", params)

    resp = requests.get(orders_url, headers=headers, params=params, timeout=15, verify=certifi.where())
    print("Status:", resp.status_code, "Response:", resp.text[:300])

    if resp.status_code == 403 and "cloudflare" in resp.text.lower():
        raise ShopmonkeyWAFBlocked("Blocked by WAF")

    resp.raise_for_status()
    return resp.json()
