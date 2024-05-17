from django.conf import settings
from django.contrib.admin import AdminSite


class MinerAdminSite(AdminSite):
    def each_context(self, request):
        context = super().each_context(request)
        context['hotkey'] = settings.BITTENSOR_WALLET().hotkey.ss58_address
        context['netuid'] = settings.BITTENSOR_NETUID
        return context
