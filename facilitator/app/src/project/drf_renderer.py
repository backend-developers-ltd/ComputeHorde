from enum import Enum

from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.renderers import JSONRenderer


class EnumJsonEncoder(DjangoJSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class EnumJSONRenderer(JSONRenderer):
    encoder_class = EnumJsonEncoder
