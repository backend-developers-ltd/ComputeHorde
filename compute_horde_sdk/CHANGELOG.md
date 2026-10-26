# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This project uses [*towncrier*](https://towncrier.readthedocs.io/) and the changes for the
upcoming release can be found in [changelog.d](changelog.d).

<!-- towncrier release notes start -->

## [0.0.19](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.19) - 2026-10-26

### Changed

- `HTTPOutputVolume` mount paths must be `/output/` followed by a canonical relative path; paths such as `/output//x`, `/output/../x` or `/output/x/` now raise `ValueError` when the job is created.

### Fixed

- HTTP PUT output uploads send the file in chunks of at most 64 KiB instead of one chunk per line, so a file without newlines is no longer read into memory whole.
- Output uploads are confined to the output directory: `relative_path` of single-file uploads must be a canonical relative path with no component longer than 255 bytes, the uploader opens output files without following symlinks and rejects anything that is not a regular file or exceeds the size limit, zipped uploads leave out symlinks, other non-regular entries and empty directories, and the HTTP response of an upload receiver is cut off at the 10 MiB cap.
- The SDK requires `certifi>=2026.10.19`, because earlier certifi releases trust the compromised eMudhra/emSign CAs.
- When one file of a `MultiUpload` fails to upload, the uploads still in progress are cancelled before `OutputUploadFailed` is raised; uploads that already finished stay delivered.
- Zipped output uploads accept files modified after 2107 and fail with `OutputUploadFailed` instead of an unhandled exception when a file name is not valid UTF-8. An output directory with directories nested more than 100 levels deep also fails with `OutputUploadFailed`. `MultiUpload` builds its system output zip before it sends any file.

### Added

- Added `compute_horde_core.untrusted_files` for reading files from, and writing files into, a directory populated by an untrusted process without following symlinks and with a size bound checked before reading.


## [0.0.18](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.18) - 2025-12-18


Added a new CPU-only executor class.


## [0.0.17](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.17) - 2025-10-21


### Breaking Change

The API for reporting a cheated job has changed:
- It now **requires** a `trusted_job_uuid` parameter.
- A new **optional** field `details` has been added for providing additional information about the cheated job.


## [0.0.14](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.14) - 2025-08-13


No significant changes.


## [0.0.13](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.13) - 2025-08-07


### Fixed

Fixed fallback client instabilities.


## [0.0.12](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.12) - 2025-07-11

### Fixed

Added exponential backoff to request retries.

## [0.0.11](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.11) - 2025-07-01

## [0.0.10](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.10) - 2025-07-01

## [0.0.9](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.9) - 2025-06-13


No significant changes.


## [0.0.8](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.8) - 2025-05-29

## [0.0.7](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.7) - 2025-05-27

## [0.0.6](https://github.com/backend-developers-ltd/compute-horde-sdk/releases/tag/v0.0.6) - 2025-05-20


No significant changes.
