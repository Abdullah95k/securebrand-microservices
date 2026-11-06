# fbs status

**Updated:** 7 November 2026, Iraq time. The master session reads this first.

## Now

- **REQ-007 fix 2 (keep everything):** done, [#54](https://github.com/Abdullah95k/securebrand-microservices/pull/54).
- **REQ-007 fix 4 (production wiring):** in [#55](https://github.com/Abdullah95k/securebrand-microservices/pull/55):
  - the chart reads `platform-credentials` (as files) and `proxies`;
  - no topic creation;
  - raw files under `fbs/keep/` and `fbs/30d/`, media under `fbs/`;
  - `values-production.yaml`;
  - `make chart-smoke` runs it against fakefb.
- **Next:**
  - fix 3: the per-IP cost model;
  - the REQ-001 answers;
  - fixes 5–13.

## Production pin (once #55 is merged)

| Field | Value |
|---|---|
| Commit | #55's merge commit |
| Image | `fbs`: `deploy/docker/Dockerfile`, context `.`, target `""` (the last stage, no browser) |
| Chart | `deploy/helm/fbs` |
| Values | `values-production.yaml` |
| Parameters | `image.repository: "{registry}/fbs"`, `image.tag: "{commit}"` |

## Waiting on

- **Canary pages (the owner):** at least three public pages that post daily and get reply threads. Without them, production can't find the real query ids and collects nothing.
- **`PROXIES_FBS` (the owner):** the format of `phase0/proxies.example.yaml`, with the tier named `isp`, and IPs not in use by the pilot (question E).
- **Valkey (the master):** confirm that fbs may use logical database 1 of the shared Valkey (`SELECT 1`). If not, fbs needs a key prefix in code: several of its keys have no `fbs:` prefix.
- **Kafka permissions (the master):** the scheduler reads its consumer groups' lag, so `fbs-kafka` needs to describe groups `fbs-*`.
- **The backups bucket** (REQ-007 fix 1): its name and endpoint, and how the owner signs in to it from the shared server.
- **The owner's answers:**
  - **F:** which page list;
  - **G:** the mention-checker trial week in production with a cap;
  - **H:** fixes 5–13 while the cluster isn't ready;
  - **I:** who changes the mention checker.

## Tracking

`docs/master/fbs-fixes.md`: one row per fix, with its PR, status and evidence.
