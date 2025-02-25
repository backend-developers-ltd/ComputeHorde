# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This project uses [*towncrier*](https://towncrier.readthedocs.io/) and the changes for the
upcoming release can be found in [changelog.d](changelog.d).

<!-- towncrier release notes start -->

## [0.0.7](https://github.com/backend-developers-ltd/compute-horde-facilitator-sdk/releases/tag/v0.0.7) - 2025-01-27


### Changed

- Updated dependencies.


## [0.0.6](https://github.com/backend-developers-ltd/compute-horde-facilitator-sdk/releases/tag/v0.0.6) - 2024-10-25


### Added

- Add support for HuggingFace volumes.


## [0.0.5](https://github.com/backend-developers-ltd/compute-horde-facilitator-sdk/releases/tag/v0.0.5) - 2024-10-18


### Changed

- Updated dependencies.

## [0.0.4](https://github.com/backend-developers-ltd/compute-horde-facilitator-sdk/releases/tag/v0.0.4) - 2024-07-08


### Added

- Add `submit_job_feedback` method to FacilitatorClient.
- Add `uploads` and `volumes` support in job creation.
- Add `wait_for_job` method to FacilitatorClient.
- Add support for signing job requests with Bittensor wallet.

### Infrastructure

- Link repository to `cookiecutter-rt-pkg` template for more robust CI and easier updates.
