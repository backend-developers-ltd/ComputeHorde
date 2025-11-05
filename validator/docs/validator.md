# Validator Help

## Debugging Organic Jobs

Validators can manually execute jobs via cli


To trigger a job through the cli:

first ssh into the machine running the validator and into the container:
```
ssh user@validator-host
docker exec -it valdiator-celery-worker-1 /bin/bash
```

then trigger the job with the following command:
```
python3 manage.py debug_run_organic_job --miner_hotkey 5EHGR... --docker_image python:3.11-slim --cmd_args "python3 --version"
```

The job progress can be monitored via the generated logs or alternatively in the admin panel by checking the `OrganicJob` view.
