# fbs status

**Updated:** 6 November 2026, Iraq time. The master session reads this first.

## Now

- **REQ-007 fix 2 (keep everything):** in a PR. Every retention default is 0, and fbs installs no lifecycle rule on raw files.
- **Next:**
  - fix 4's production wiring: the chart reads `platform-credentials` and `proxies`, no topic creation, raw files under `fbs/keep/` and `fbs/30d/`, media under `fbs/`;
  - fix 3: the per-IP cost model;
  - the REQ-001 answers.

## Waiting on

- **The backups bucket** (REQ-007 fix 1): its name, endpoint and how the owner signs in to it from the shared server.
- **The owner's answers:**
  - **E:** which proxy IPs production uses;
  - **F:** which page list;
  - **G:** the mention-checker trial week in production with a cap;
  - **H:** fixes 5–13 while the cluster isn't ready;
  - **I:** who changes the mention checker.

## Tracking

`docs/master/fbs-fixes.md`: one row per fix, with its PR, status and evidence.
