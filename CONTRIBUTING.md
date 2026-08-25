# Contributing

Thanks for your interest in improving the OpenNMS Prometheus RemoteWrite Plugin.

## Workflow

Plugin software management is tracked in the
[PNNMS Jira project](https://opennms.atlassian.net/browse/PNNMS).

1. Start from an issue. Open a PNNMS Jira issue before writing code so the change
   can be discussed and tracked. Drive-by PRs without an issue may be asked to
   open one first.
2. Set the **Fix Version** on the PNNMS issue to the next unreleased patch release.
3. Branch from `main`.
4. Keep changes focused and follow the existing code style.
5. Open a PR whose title is prefixed with the issue key:
   `PNNMS-1234: short description`.
6. Link back to the PR from a comment on the PNNMS issue.

If any of this is unclear, say so in the PR and we can help get it set up.

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

Prefix the subject with the Jira issue key, as in the PR title:

```
PNNMS-1234: short description of the change
```

Keep the subject on one line and put the reasoning in the body. Breaking changes
should say so explicitly, either with a `!` after the key or a `BREAKING CHANGE:`
footer.

Automated dependency updates from Dependabot keep their own `build(deps):` subjects
and are exempt.
