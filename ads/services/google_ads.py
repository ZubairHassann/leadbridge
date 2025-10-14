import hashlib
import logging
import re
from django.conf import settings
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v21.enums.types import (
    OfflineUserDataJobTypeEnum,
    UserIdentifierSourceEnum,
)
from google.ads.googleads.v21.resources.types import (
    OfflineUserDataJob,
    UserData,
    UserIdentifier,
    TransactionAttribute,
)
from google.ads.googleads.v21.services.types import (
    AddOfflineUserDataJobOperationsRequest,
)
from google.ads.googleads.v21.services.services.offline_user_data_job_service import (
    OfflineUserDataJobServiceClient,
)

logger = logging.getLogger(__name__)

# ===================================
# Helper: Normalize + Hash Identifiers
# ===================================
def normalize_phone(phone: str):
    """Remove symbols, spaces, country code, and return digits only."""
    if not phone:
        return None
    digits = re.sub(r"[^0-9]", "", phone)
    return digits.lstrip("1")  # Remove leading country code (like 1 for US)


def hash_identifier(value: str):
    """Lowercase + SHA256 hash for phone/email."""
    if not value:
        return None
    normalized = value.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# ===================================
# Enhanced Conversion Upload
# ===================================
def upload_enhanced_conversion(
    phone_hash=None,
    email_hash=None,
    value=0.0,
    conversion_time=None,
    order_id=None,
):
    """
    Upload an enhanced conversion for leads using hashed user data.
    Works without GCLID. Logs all Google Ads responses for debugging.
    """

    if not (phone_hash or email_hash):
        logger.warning("⚠️ No identifiers provided — skipping enhanced conversion upload.")
        return None

    try:
        client = GoogleAdsClient.load_from_storage(settings.GOOGLEADS_YAML_PATH)
        service = client.get_service("OfflineUserDataJobService")
        offline_user_data_job_service = OfflineUserDataJobServiceClient()

        # ✅ Create a new OfflineUserDataJob
        job = OfflineUserDataJob(
            type_=OfflineUserDataJobTypeEnum.OFFLINE_USER_DATA_JOB_TYPE_STORE_SALES_UPLOAD_FIRST_PARTY,
        )

        # Prepare UserData with identifiers
        user_data = UserData()

        if phone_hash:
            user_id = UserIdentifier()
            user_id.hashed_phone_number = phone_hash
            user_id.user_identifier_source = UserIdentifierSourceEnum.FIRST_PARTY
            user_data.user_identifiers.append(user_id)

        if email_hash:
            user_id = UserIdentifier()
            user_id.hashed_email = email_hash
            user_id.user_identifier_source = UserIdentifierSourceEnum.FIRST_PARTY
            user_data.user_identifiers.append(user_id)

        # Add transaction attributes
        transaction = TransactionAttribute()
        transaction.transaction_amount_micros = int(value * 1_000_000)
        transaction.currency_code = getattr(settings, "GOOGLE_CURRENCY_CODE", "USD")
        transaction.transaction_date_time = conversion_time
        if order_id:
            transaction.order_id = order_id
        user_data.transaction_attribute = transaction

        # Build the AddOfflineUserDataJobOperationsRequest
        request = AddOfflineUserDataJobOperationsRequest(
            resource_name=job.resource_name,
            enable_partial_failure=True,
            operations=[{"create": user_data}],
        )

        response = offline_user_data_job_service.add_offline_user_data_job_operations(request=request)

        logger.info("✅ Enhanced Conversion uploaded successfully.")
        logger.debug(f"Google Ads Response: {response}")
        return response

    except GoogleAdsException as ex:
        logger.error(f"❌ Google Ads API Error: {ex.failure}")
        for error in ex.failure.errors:
            logger.error(f"  → {error.error_code} | {error.message}")
        return None
    except Exception as e:
        logger.exception(f"❌ Unexpected error uploading enhanced conversion: {e}")
        return None
