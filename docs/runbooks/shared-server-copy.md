# Copying the pilot's data off the shared server (REQ-007 fix 1)

**Status:** agreed by the master on 7 November: the method, a temporary key limited to the backups bucket, and the bucket `securebrand-prod-backups` at `https://fra1.digitaloceanspaces.com`.

**Who runs it:** the owner, on the shared server (201.79.36.209), in the fbs folder (where `make pilot-up` runs).

**When:** only after the master confirms that the production cluster is up and fbs is deployed at its pin, so the pilot stops only when production can take over. Then, in this order:

1. The owner runs this copy. Its last step stops the pilot.
2. The owner adds `PROXIES_FBS`.
3. The master re-runs the setup that turns it into the Secret `proxies`. fbs's crawlers and registry-sync, waiting for that Secret, then start on their own.

Production uses the pilot's proxy IPs, so the pilot must stop before production collects. Stopping it loses what only lives in memory: the raw responses in its in-memory S3. This copy keeps them.

**What it copies**, to the production backups bucket under `fbs/shared-server/<date>/`:

| What | Why |
|---|---|
| `fbs.dump` | The whole database (`pg_dump`, custom format): pages, posts, comments, every version, metrics, jobs, the query registry. |
| `fbs.dump.sha256` | Its checksum, to check the copy. |
| `counts.txt` | Row counts at the time of the copy, for the record. |
| `raw/` | The raw responses kept since the pilot last started: failures only, `fb.capture_raw: failures`. |
| `phase0-out.tar.gz` | `phase0/out`: the Phase 0 results, reports and registry. It never contains `proxies.yaml`, which is in `phase0/`, not in `phase0/out/`. |

**Not copied:**
- **Kafka (Redpanda):** everything in it is already in the database once the persister has caught up (step 3 checks this).
- **Valkey:** only rate limits and sessions, rebuilt by any start.
- **Media files:** temporary by design.

**Nothing is deleted.** Stopping keeps the database and Kafka volumes, and nothing is removed until the owner confirms the copy is complete.

## How the copy reaches the bucket

The data goes straight from the shared server to the bucket over HTTPS, without passing through a chat or a local file. It uses a temporary key the owner creates and deletes:

- the key can write only the backups bucket;
- the owner types it at a hidden prompt on the server, so it is never in a chat, a file or the shell history.

The alternative the master suggested also works, but needs more set-up:
- a Job in the `fbs` namespace, using fbs's own key;
- it reads the database through an SSH tunnel;
- that needs a public key added to the shared server for the duration.

## Steps

### 1. Create the temporary key (DigitalOcean)

1. Go to **Spaces Object Storage → Access Keys → Create Access Key**.
2. Choose **Limited access**: only the backups bucket, with the permission that allows writing.
3. Name it `fbs-shared-server-copy`.
4. Keep the window open: the secret is shown once.

### 2. Open a terminal on the shared server, in the fbs folder

Fail on any error in a pipe:

```bash
set -o pipefail
```

The bucket and the day's folder. The bucket is the platform's backups bucket (`S3_BUCKET_BACKUPS`), confirmed by the master: `securebrand-prod-backups` at `https://fra1.digitaloceanspaces.com`.

```bash
export BUCKET=securebrand-prod-backups ENDPOINT=https://fra1.digitaloceanspaces.com DAY=$(TZ=Asia/Baghdad date +%F)
```

The key, typed at hidden prompts (paste the key ID, press Enter; then the secret):

```bash
read -rsp 'Key ID: ' AWS_ACCESS_KEY_ID; echo; read -rsp 'Secret: ' AWS_SECRET_ACCESS_KEY; echo; export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
```

A folder for what the copy writes on the server, then a short name for the upload tool (a container that reads the key from those variables):

```bash
mkdir -p phase0/copy
```

```bash
s3() { docker run --rm -i -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION=fra1 -v "$PWD/phase0/copy:/copy" amazon/aws-cli:2.22.0 --endpoint-url "$ENDPOINT" "$@"; }
```

Check that the key reaches the bucket (it prints `OK`):

```bash
s3 s3api head-bucket --bucket "$BUCKET" && echo OK
```

### 3. Stop collecting, and let the database catch up

Stop everything that reads Facebook or plans work. The database, Kafka, Valkey, the in-memory S3 and the persister keep running:

```bash
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/pilot.yml stop crawler scheduler registry-sync media-downloader media-janitor api
```

Wait a minute, then check that the persister has written everything Kafka holds:

```bash
docker exec fbs-redpanda-1 rpk group describe fbs-persister
```

- **`TOTAL-LAG` must be 0.** If it isn't, wait a minute and check again.
- Then stop the persister:

```bash
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/pilot.yml stop persister
```

### 4. Copy the database

Row counts, for the record:

```bash
docker exec fbs-postgres-1 psql -U fbs -d fbs -c "SELECT now() AS at, (SELECT count(*) FROM page_schedule) AS pages, (SELECT count(*) FROM posts) AS posts, (SELECT count(*) FROM comments) AS comments, (SELECT count(*) FROM jobs) AS jobs" > phase0/copy/counts.txt && cat phase0/copy/counts.txt
```

The dump, streamed to the bucket with its checksum. It takes a few minutes and uses no disk:

```bash
docker exec fbs-postgres-1 pg_dump -U fbs -Fc fbs | tee >(sha256sum | cut -d' ' -f1 > phase0/copy/fbs.dump.sha256) | s3 s3 cp - "s3://$BUCKET/fbs/shared-server/$DAY/fbs.dump"
```

Upload the checksum and the counts:

```bash
s3 s3 cp /copy/fbs.dump.sha256 "s3://$BUCKET/fbs/shared-server/$DAY/fbs.dump.sha256" && s3 s3 cp /copy/counts.txt "s3://$BUCKET/fbs/shared-server/$DAY/counts.txt"
```

**Check the copy:** read it back and compare the checksums. The two lines must be the same.

```bash
cat phase0/copy/fbs.dump.sha256; s3 s3 cp "s3://$BUCKET/fbs/shared-server/$DAY/fbs.dump" - | sha256sum | cut -d' ' -f1
```

### 5. Copy the raw responses

From the pilot's in-memory S3 to a folder, then to the bucket:

```bash
docker run --rm --network host -e AWS_ACCESS_KEY_ID=fbs -e AWS_SECRET_ACCESS_KEY=fbs-secret -e AWS_DEFAULT_REGION=us-east-1 -v "$PWD/phase0/copy:/copy" amazon/aws-cli:2.22.0 --endpoint-url http://127.0.0.1:9000 s3 sync s3://fbs-raw /copy/raw
```

```bash
s3 s3 sync /copy/raw "s3://$BUCKET/fbs/shared-server/$DAY/raw/"
```

**Check:** the two counts must match.

```bash
find phase0/copy/raw -type f | wc -l; s3 s3 ls --recursive "s3://$BUCKET/fbs/shared-server/$DAY/raw/" | wc -l
```

### 6. Copy the Phase 0 results

```bash
tar czf - -C phase0 out | s3 s3 cp - "s3://$BUCKET/fbs/shared-server/$DAY/phase0-out.tar.gz"
```

### 7. List what is in the bucket

```bash
s3 s3 ls --recursive --human-readable "s3://$BUCKET/fbs/shared-server/$DAY/"
```

Send this list (names and sizes only) and the two checksum lines from step 4 to the fbs session. They contain nothing secret.

### 8. Stop the pilot

Every container stops. The database and Kafka volumes stay on disk, and nothing is deleted:

```bash
make pilot-down
```

### 9. Remove the key

Close the terminal; the key was only in that shell. Then delete the key `fbs-shared-server-copy` in DigitalOcean: **Spaces Object Storage → Access Keys**.

## Afterwards

- The pilot's volumes and `phase0/copy` stay on the shared server until the owner confirms the copy is complete. Freeing that space is a later step, with its own commands.
- Production may then start with the same proxy IPs. The owner adds `PROXIES_FBS`, and the master re-runs the setup.
