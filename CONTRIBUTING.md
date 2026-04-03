# Contributing

Thanks for contributing to `Valkey QueryDSL`.

## Development Baseline

- Java `17+`
- Maven `3.9+`
- Spring Boot `4.0.x`
- Valkey Glide `2.2.x`

## Local Setup

1. Install JDK 17 or newer.
2. Ensure Maven is available.
3. Clone the repository.
4. Prefer running the real Valkey integration suites documented in:

- [docs/真实环境测试手册.md](/Users/momao/dm/java/demo/valkey-demo/docs/真实环境测试手册.md)

5. If you only need a fast compile / model sanity check, run:

```bash
mvn clean test
```

This default run is only a fast sanity check. Runtime correctness must be validated against real Valkey.

## Running Integration Tests

Runtime changes must be verified against a local Valkey instance with Search / JSON capability.

No runtime feature counts as delivered until the affected real Valkey suites pass.

Run them with:

```bash
./scripts/run-real-tests.sh all
```

Default local assumptions:

- host: `localhost`
- port: `6379`
- no password

If you use another topology, set the corresponding environment variables already supported by the sample application.

Recommended real test commands are maintained in:

- [docs/真实环境测试手册.md](/Users/momao/dm/java/demo/valkey-demo/docs/真实环境测试手册.md)
- [docs/测试分层说明.md](/Users/momao/dm/java/demo/valkey-demo/docs/测试分层说明.md)

CI uses the same runtime validation baseline image as the local project:

- `ghcr.io/lmemory123/valkey-bundle:9.1.0`

## Pull Request Checklist

Every pull request should include:

- A clear description of the problem and the change.
- Tests that cover the new behavior.
- Documentation updates when behavior, configuration, or public APIs change.

## Expected Test Coverage By Change Type

- Query DSL / APT changes:
  Add or update generator tests and query behavior tests.
- Runtime adapter changes:
  Add real integration tests. Pure tests can be supplementary only.
- Spring Boot starter changes:
  Add configuration-path coverage and a real topology test when behavior changes.
- Index governance changes:
  Add tests for all relevant modes and failure paths.

## Coding Notes

- Keep ASCII by default unless the file already uses non-ASCII content.
- Avoid destructive git operations.
- Do not silently change compatibility claims without updating `README.md` and `CHANGELOG.md`.

## PR Style

- Keep PRs focused.
- Prefer one concern per PR.
- If a change is intentionally deferred, say so in the PR description.
