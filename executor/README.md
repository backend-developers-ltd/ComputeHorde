# executor

ComputeHorde Executor.  See repositories main README for an explanation of what that means.

- - -

Skeleton of this project was generated with `cookiecutter-rt-django`, which sometimes gets upgrades that are easy to retrofit into already older projects.

# Base requirements

- docker
- docker-compose
- python 3.11
- [uv](https://docs.astral.sh/uv/)
- [nox](https://nox.thea.codes)
- 500GB shared disk space for docker images and job data stored in `/tmp`

## Shared HTTP input cache

**All submitters on a host, including different validators, share one key space.**
A submitter can populate a key or reuse its bytes. Enable this on inputs intended for that sharing boundary.
On a hit the executor makes zero origin requests. A revoked or rotated URL does not revoke a cached file.

The native runner stores completed files in the `/tmp/executor-cache` directory on the Docker host.
Each opted-in input gets a read-only file bind at its requested path under the workload's normal
`/volume` mount.

Set `EXECUTOR_CACHE_MAX_BYTES` in the miner's `.env` to override the default 20 GiB payload
budget per executor host. The miner supplies the budget whenever it launches an executor.
Executors launched without a miner-supplied budget use ordinary uncached downloads.
The executor caches each successful opted-in download. A job can use more than the cache budget.
At startup and after each job, cleanup evicts entries closest to expiry first until the cache fits
within budget.

Data filenames are SHA-256 hashes of the exact UTF-8 cache keys. The index records sizes,
completion and expiry times, and inode change times. The cache stores no source URLs or credentials.
A completed hit must pass file type, link count, change-time and age checks.
The executor checks age on every lookup, using both the original and requested TTL.
At most once every five minutes per host, cleanup also removes up to 128 expired entries.
An idle host reclaims space on a later startup. Operators can remove the whole cache between jobs.
The next opted-in download recreates it.

# Setup development environment

```sh
# 1st tab
$ ./setup-dev.sh
```

```sh
# 2nd tab
docker-compose up
```

```sh
# 1st tab
cd app/src
uv run manage.py wait_for_database --timeout 10
uv run manage.py migrate
uv run manage.py runserver
```
