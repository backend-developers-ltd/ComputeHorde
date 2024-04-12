import logging
import os

from django.contrib import admin  # noqa
from django.contrib.admin import register  # noqa
from django.contrib.auth.models import User  # noqa

from compute_horde_miner.miner.models import AcceptedJob, Validator

logger = logging.getLogger(__name__)

admin.site.site_header = "compute_horde_miner Administration"
admin.site.site_title = "compute_horde_miner"
admin.site.index_title = "Welcome to compute_horde_miner Administration"

class ReadOnlyAdmin(admin.ModelAdmin):
    change_form_template = "admin/read_only_view.html"

    def has_add_permission(self, *args, **kwargs):
        return False

    def has_change_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False

admin.site.register(AcceptedJob, admin_class=ReadOnlyAdmin)
admin.site.register(Validator, admin_class=ReadOnlyAdmin)

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
