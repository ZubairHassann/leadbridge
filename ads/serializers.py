from rest_framework import serializers
from .models import CallRecord, ShopmonkeyOrder, OfflineConversion

class CallRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = CallRecord
        fields = "__all__"

class ShopmonkeyOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShopmonkeyOrder
        fields = "__all__"

class OfflineConversionSerializer(serializers.ModelSerializer):
    class Meta:
        model = OfflineConversion
        fields = "__all__"
