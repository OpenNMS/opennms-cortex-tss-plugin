# Contributing

Thanks for your interest in improving the OpenNMS Prometheus RemoteWrite Plugin.

## Workflow

1. Start from an issue. Open one (bug or enhancement) before writing code so the
   change can be discussed and tracked. Drive-by PRs without an issue may be asked
   to open one first.
2. Branch from `main`.
3. Keep changes focused and follow the existing code style.
4. Open a PR that references its issue with a closing keyword (`Closes #123`).

## Building and testing

Requires JDK 17 and Maven.

```bash
mvn clean install                       # compile + unit tests
mvn -DskipITs=false clean verify        # include integration tests (Docker required)
```

## Developer Certificate of Origin (DCO)

Every commit must be signed off, certifying the [DCO](https://developercertificate.org/):

```bash
git commit -s
```

This adds a `Signed-off-by: Your Name <you@example.com>` trailer using your real
identity. PRs whose commits are not signed off cannot be merged.

## AI-assisted contributions

AI assistance is welcome. When a commit was produced with an AI coding tool, record
it with an `Assisted-by` trailer so provenance is clear:

```
Assisted-by: ClaudeCode:claude-opus-4-8
Signed-off-by: Your Name <you@example.com>
```

The `Assisted-by` trailer names the tool and model; the `Signed-off-by` trailer is
always a human. The human signer remains responsible for reviewing the change and
for its license compliance — AI output is not exempt from review.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): summary`
where type is one of `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `build`,
`ci`, `chore`. Breaking changes append `!` or add a `BREAKING CHANGE:` footer.
