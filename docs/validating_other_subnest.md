
```
v = Validator(
wallet,
trusted_miner_adress_and_stuff,
trusted_facilitator_url,  # facilitator used for contacting the trusted miner
public_facilitator_url,  # facilitator used to route jobs sent to compute_horde
deposition_facilitator_url,  # facilitator used to deliver depositions about public miner faults
)

await v.list_ongoing_jobs()  # will get all both from trusted_facilitator_url and public_facilitator_url

job = v.submit_job(
executor_class,
job_details,
public_volumes,
trusted_volumes,
cross_validation_chance,
)

musi być fabryka tych trusted volumes?


await job
job.status
job.job_details
job.depostion
job.public_result
job.trusted_result
job.public_volumes
job.trusted_volumes


```