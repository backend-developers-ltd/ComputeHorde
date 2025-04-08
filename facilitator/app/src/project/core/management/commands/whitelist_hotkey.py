from django.core.management import BaseCommand

from project.core.models import HotkeyWhitelist


class Command(BaseCommand):
    help = "Add a hotkey to the facilitator hotkey whitelist"

    def add_arguments(self, parser):
        parser.add_argument("public_key", type=str, help="a public key to add to the whitelist")

    def handle(self, *args, **options):
        HotkeyWhitelist.objects.get_or_create(
            ss58_address=options["public_key"],
        )
