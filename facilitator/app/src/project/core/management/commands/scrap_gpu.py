import itertools
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand
from django.utils.timezone import now

from ...models import Channel, Job, Miner

User = get_user_model()


class Command(BaseCommand):
    help = "Run jobs to scrap info about miners GPUs"

    def add_arguments(self, parser):
        parser.add_argument("--job-tag", type=str, help="string to tag created jobs", required=True)
        parser.add_argument("--username", type=str, help="assign jobs to user", required=True)

    def handle(self, *args, **options):
        user = User.objects.get(username=options["username"])
        job_tag = options["job_tag"]
        validator_ids = Channel.objects.filter(last_heartbeat__gte=now() - timedelta(minutes=3)).values_list(
            "validator_id", flat=True
        )
        cycle_validator_ids = itertools.cycle(validator_ids)
        miners = Miner.objects.filter(is_active=True)
        for miner in miners:
            job = Job(
                user_id=user.pk,
                miner_id=miner.pk,
                validator_id=next(cycle_validator_ids, None),
                raw_script="""import subprocess
print(subprocess.getoutput("nvidia-smi -L"))
""",
                use_gpu=True,
                tag=job_tag,
            )
            job.save()
            self.stdout.write(self.style.SUCCESS(f"Added scrap job for miner: {miner.ss58_address}"))
