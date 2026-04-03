# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-03-29

### Added

- Added APT-generated `XXXQuery` classes and static index metadata generation.
- Added `JSON / HASH` dual storage support with nested object and collection-field indexing.
- Added `list / page / one / count` repository APIs and chain-style query execution.
- Added `UpdateChain` with `where / and / or / set / setIf / expect / expectVersion / advanceVersion`.
- Added atomic `increment / decrement` DSL for numeric fields.
- Added standalone, read/write split, and cluster topology support.
- Added structured exception hierarchy and stable error codes.
- Added Micrometer Observation integration, slow-log pipeline, command failure metrics, index metrics, and partial-update metrics.
- Added script caching with `EVALSHA` fallback for complex single-key atomic updates.
- Added `NONE / VALIDATE / RECREATE` index management modes.

### Changed

- Changed the runtime query execution path to use real `FT.SEARCH` commands consistently, including pagination, sorting, and count fallback.
- Changed APT recursive field scanning and metadata generation to better support JSON paths, nested fields, and collection tags.
- Changed Spring Boot integration to align with Spring Boot `4.0.x`.
- Changed index governance from implicit startup behavior to explicit mode-based control.
- Changed local and integration test coverage to include topology, write-path atomicity, and index-governance behaviors.

### Removed

- Removed dangerous automatic append/update index path based on `FT.ALTER`.
- Removed over-complicated startup-time manual plan/apply requirement from the default developer workflow.

## [0.9.0] - 2026-03-28

### Added

- Added index diff model, migration-plan model, and index governance facade.
- Added optimistic-lock style version helpers for partial updates.
- Added cluster-aware search merge and routing fallback handling.

### Changed

- Changed repository write path from full-entity overwrite focus to partial-update-first design.
- Changed observability implementation to a zero-overhead style invoker with async slow-log delivery.

## [0.5.0] - 2026-03-20

### Added

- Added Spring Boot starter auto-configuration and package scanning.
- Added generated repository demo application and real Valkey integration tests.
- Added JSON entity deserialization, nested merchant field support, and collection tag search verification.

### Changed

- Changed project from proof-of-concept query wrapper to modular framework layout.

## [0.1.0] - 2026-03-01

### Added

- Added initial annotations, core query model, and basic Valkey Search repository abstraction.
