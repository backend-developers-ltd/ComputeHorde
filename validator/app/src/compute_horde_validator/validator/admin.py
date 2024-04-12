import logging
import os

from django.contrib import admin  # noqa
from django.contrib.admin import register  # noqa
from django.contrib.auth.models import User  # noqa

from compute_horde_validator.validator.models import Miner, OrganicJob, SyntheticJob

logger = logging.getLogger(__name__)

admin.site.site_header = "compute_horde_validator Administration"
admin.site.site_title = "compute_horde_validator"
admin.site.index_title = "Welcome to compute_horde_validator Administration"

class AddOnlyAdmin(admin.ModelAdmin):
    def has_change_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False

class ReadOnlyAdmin(AddOnlyAdmin):
    change_form_template = "admin/read_only_view.html"

    def has_add_permission(self, *args, **kwargs):
        return False

admin.site.register(Miner, admin_class=ReadOnlyAdmin)
admin.site.register(SyntheticJob, admin_class=AddOnlyAdmin)
admin.site.register(OrganicJob, admin_class=AddOnlyAdmin)

def maybe_create_default_admin():
    # Create default admin user if missing
    if not User.objects.filter(is_superuser=True).exists():
        admin_password = os.getenv("DEFAULT_ADMIN_PASSWORD")
        if admin_password is None:
            logger.warning("Not creating Admin user - please set DEFAULT_ADMIN_PASSWORD env variable")
        else:
            logger.info("Creating Admin user with DEFAULT_ADMIN_PASSWORD")
            admin_user = User.objects.create_superuser(username='admin', email='admin@admin.com', password=admin_password)
            admin_user.save()

maybe_create_default_admin()
