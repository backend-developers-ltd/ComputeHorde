import wandb
from django.conf import settings


def fetch_compute_subnet_hardware() -> dict:
    """
    Retrieve hardware specs for the compute subnet.

    This info is also displayed here: https://opencompute.streamlit.app
    """

    wandb.login(key=settings.WANDB_API_KEY)
    api = wandb.Api()

    # https://github.com/nauttiilus/opencompute/blob/main/main.py
    db_specs_dict = {}
    project_path = "neuralinternet/opencompute"
    runs = api.runs(project_path)
    for run in runs:
        run_config = run.config
        hotkey = run_config.get("hotkey")
        details = run_config.get("specs")
        role = run_config.get("role")
        if hotkey and details and role == "miner":
            db_specs_dict[hotkey] = details

    return db_specs_dict
