# Releasing

This document describes how a release of the OpenNMS Prometheus RemoteWrite Plugin
is produced.

## Cutting a release

1. Ensure `main` is green in CircleCI and up to date. Only changes intended for
   this release should be on `main`.
2. Set the release version on `main` (via a PR):
   ```bash
   mvn versions:set -DnewVersion=2.1.1
   git commit -s -am "chore(release): 2.1.1"
   ```
3. Merge the PR.
4. Tag the release commit with a GPG-signed tag and push it. CircleCI builds the
   tag and deploys the signed artifacts to OSSRH:
   ```bash
   git tag -u opennms@opennms.org -s v2.1.1 -m "v2.1.1"
   git push origin v2.1.1
   ```
5. Bump `main` to the next snapshot:
   ```bash
   mvn versions:set -DnewVersion=2.1.2-SNAPSHOT
   ```

## Publishing the GitHub Release

Create a GitHub Release for the `vX.Y.Z` tag with curated notes (highlights,
breaking changes, fixes — not a raw commit dump). Attach the assembly archive,
renamed for download:

```bash
mv org.opennms.plugins.timeseries.prometheus.remotewrite.assembly.kar-<VERSION>.kar \
   opennms-prometheus-remotewrite-plugin.kar
```
