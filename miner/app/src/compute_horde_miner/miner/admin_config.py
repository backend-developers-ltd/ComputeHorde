from django.contrib.admin.apps import AdminConfig


class MinerAdminConfig(AdminConfig):
    default_site = "compute_horde_miner.miner.admin_site.MinerAdminSite"
