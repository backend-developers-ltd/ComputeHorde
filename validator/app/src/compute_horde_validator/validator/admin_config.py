from django.contrib.admin.apps import AdminConfig


class ValidatorAdminConfig(AdminConfig):
    default_site = "compute_horde_validator.validator.admin_site.ValidatorAdminSite"
