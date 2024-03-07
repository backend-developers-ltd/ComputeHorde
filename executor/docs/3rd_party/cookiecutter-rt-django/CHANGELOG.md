# cookiecutter-rt-django Changelog

Main purpose of this file is to provide a changelog for the template itself.
It is not intended to be used as a changelog for the generated project.

This changelog will document any know **BREAKING** changes between versions of the template.
Please review this new entries carefully after applying `cruft update` before committing the changes.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Currently, `cookiecutter-rt-django` has no explicit versioning amd we purely rely on `cruft` diff.

## [Unreleased]

* **BREAKING** Added formatting with ruff.
* **BREAKING** Started using [pdm](https://github.com/pdm-project/pdm) for managing Python dependencies.
* **BREAKING** root of repository is used as docker build context instead of just `./app/`.
* **BREAKING** Updated django-environ from 0.4.5 to 0.10 (https://django-environ.readthedocs.io/en/latest/changelog.html)
* **BREAKING** Updated redis python package from 3.5.3 to 4.6 (breaking changes listed in https://github.com/redis/redis-py/releases/tag/v4.0.0b1)
* **BREAKING** Updated Python from 3.9 to 3.11
* **BREAKING** Updated Django from 3.2 to 4.2 (https://docs.djangoproject.com/en/4.2/releases/4.0/#backwards-incompatible-changes-in-4-0)
* **BREAKING** Updated django-cors-headers from 3.7 to 4.0 (https://github.com/adamchainz/django-cors-headers/blob/main/CHANGELOG.rst#400-2023-05-12)
* **BREAKING** Updated django-environ from 0.7 to 0.10 (https://django-environ.readthedocs.io/en/latest/changelog.html)
