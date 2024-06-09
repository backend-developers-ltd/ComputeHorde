# Validator Help

## Debugging Organic Jobs

Validators can manually execute jobs either from the admin panel or via cli


To trigger a job through the cli:

first ssh into the machine running the validator and into the container:
```
ssh user@validator-host
docker exec -it valdiator-celery-worker-1 /bin/bash
```

then trigger the job with the following command:
```
python3 manage.py debug_run_organic_job --miner_hotkey 5EHGR... --timeout 60 --docker_image python:3.11-slim  --raw_scrip "print('hello world')"
```

or alternatively to the raw script you can provide `cmd_args`, as well as

```
python3 manage.py debug_run_organic_job --miner_hotkey 5EHGR... --timeout 99 --docker_image python:3.11-slim --cmd_args "python3 --version"
```

The job progress can be monitored via the generated logs or alternatively in the admin panel by checking the `AdminJobRequest` and `OrganicJob` views



To trigger a job through the admin panel:

Access the validator admin panel by tunneling the port to your local machine, then access the admin panel at `localhost:8000/admin`:
```
ssh -L 8000:localhost:8000 user@validator-host
```

First, create a `AdminJobRequest` object - this will trigger the creation and execution of a new `OrganicJob` with the specified uuid.

Then, go to the `OrganicJob` details page to see the job status as well as `stdout` generated by the job execution.

Any errors from the job execution can be checked in the `OrganicJob` view, in the `stderr` field. Likewise details about the job failing to trigger or errors previous to the job execution will be in the `AdminJobRequest` view in the admin panel.