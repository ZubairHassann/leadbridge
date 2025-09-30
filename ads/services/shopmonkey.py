import re
import time
import certifi
import requests
from typing import Dict, List, Optional
from django.conf import settings

API_BASE = "https://api.shopmonkey.cloud/v3"

class ShopmonkeyAPIError(Exception):
    pass

class ShopmonkeyWAFBlocked(ShopmonkeyAPIError):
    """Raised when Cloudflare/WAF challenge page is returned (HTML 403)."""

def normalize_us_phone(raw: Optional[str]) -> str:
    digits = re.sub(r"\D", "", (raw or ""))
    if not digits:
        return ""
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}" if not (raw or "").startswith("+") else (raw or "")

def _default_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.SHOPMONKEY_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",  # ✅
        "User-Agent": getattr(settings, "SHOPMONKEY_USER_AGENT", "leadbridge/1.0"),  # ✅
    }

def fetch_orders_by_phone(phone: str, page_size: int = 100, max_pages: int = 50) -> List[Dict]:
    norm = normalize_us_phone(phone)
    if not norm:
        return []

    session = requests.Session()
    url = f"{API_BASE}/orders"
    params = {"customerPhone": norm, "limit": page_size}
    headers = _default_headers()

    out: List[Dict] = []
    page = 0
    next_token_key = None

    while True:
        try:
            resp = session.get(
                url,
                headers=headers,
                params=params,
                timeout=20,
                verify=certifi.where(),
            )
        except requests.exceptions.SSLError as e:
            raise ShopmonkeyAPIError(f"SSL verification failed: {e}") from e
        except requests.RequestException as e:
            raise ShopmonkeyAPIError(f"Network error contacting Shopmonkey: {e}") from e

        # WAF/Cloudflare detection: HTML content with 403
        content_type = resp.headers.get("Content-Type", "")
        if resp.status_code == 403 and "text/html" in content_type.lower():
            # Body often contains 'Cloudflare' or challenge markup
            body_head = resp.text[:300].lower()
            if "cloudflare" in body_head or "<html" in body_head:
                raise ShopmonkeyWAFBlocked("Cloudflare/WAF blocked this request (403 HTML). Ask Shopmonkey to allowlist your IP / add API bypass.")

        if resp.status_code == 401:
            raise ShopmonkeyAPIError("Unauthorized (401): Check SHOPMONKEY_API_KEY and scopes.")

        if resp.status_code == 429:
            # simple backoff
            time.sleep(2 + page)
            continue

        if not (200 <= resp.status_code < 300):
            raise ShopmonkeyAPIError(f"HTTP {resp.status_code}: {resp.text[:500]}")

        payload = resp.json()

        items = (
            payload.get("data")
            or payload.get("orders")
            or payload.get("items")
            or (payload if isinstance(payload, list) else [])
        )
        if not isinstance(items, list):
            items = []
        out.extend(items)

        if next_token_key is None and isinstance(payload, dict):
            if "next" in payload:
                next_token_key = "next"
            elif "nextPageToken" in payload:
                next_token_key = "nextPageToken"
            elif "pageToken" in payload:
                next_token_key = "pageToken"

        next_token = payload.get(next_token_key) if isinstance(payload, dict) and next_token_key else None
        if not next_token:
            break

        params["pageToken"] = next_token
        page += 1
        if page >= max_pages:
            break

    return out
