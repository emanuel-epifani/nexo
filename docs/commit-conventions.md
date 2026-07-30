# Commit Conventions

All commits follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>
```

## Types

### Shown in changelog

| Type | Category | Use for | Example |
|---|---|---|---|
| `feat` | Added | New user-facing functionality | `feat(queue): add priority queue with deadlines` |
| `fix` | Fixed | Bug fixes | `fix(store): handle null key crash` |
| `perf` | Performance | Performance improvements | `perf(stream): reduce batch allocation overhead` |
| `refactor` | Changed | Code restructuring without behavior change | `refactor(tcp): split connection handler` |
| `revert` | Removed | Reverting a previous change | `revert: pubsub delivery tokens` |

### Not shown in changelog

| Type | Use for | Example |
|---|---|---|
| `chore` | Cleanup, formatting, renaming, tooling | `chore: remove unused imports` |
| `docs` | Documentation only | `docs: update quickstart guide` |
| `test` | Test-only changes | `test(queue): add consumer group tests` |
| `ci` | CI/CD pipeline changes | `ci: add docker publish workflow` |
| `build` | Build system, dependencies | `build: update Cargo dependencies` |

## Scopes

| Scope | Covers |
|---|---|
| `store` | Store broker |
| `queue` | Queue broker |
| `pubsub` | Pub/Sub broker |
| `stream` | Stream broker |
| `transport` | TCP, codec, protocol, wire |
| `sdk-ts` | TypeScript SDK |
| `sdk-py` | Python SDK |
| `docs` | VitePress documentation site |
| `ci` | CI/CD pipeline |
| _(none)_ | Cross-cutting or repo-wide |

## Rules

- **One commit per feature/refactor.** If a change touches broker + SDKs + tests + docs, it goes in a single commit with the primary scope.
- **Amend while in progress.** Use `git commit --amend` to keep squashing until the change is complete and coherent.
- **Description is lowercase, no trailing period.** `feat(queue): add priority queue` not `feat(queue): Add priority queue.`
- **Scope is optional but recommended.** Omit only for cross-cutting changes (e.g. `docs: update version numbers`).
