#!/usr/bin/env python3
"""Run the Helm chart's workloads in Docker, as the chart defines them.

Without a Kubernetes cluster, this checks what a cluster would run: it
renders the chart (deploy/helm/fbs/examples/fakefb.yaml by default) and
starts every hook Job and Deployment container with the rendered command,
environment, ConfigMap and Secret mounts, user, read-only root filesystem,
dropped capabilities and memory limit, on the network of `make infra-up`
(Postgres, Redpanda, Valkey), next to fakefb and an in-memory S3. Then it
registers fakefb's pages through the API (`fbctl pages import`) and waits
for their backfills: every job must complete, with comments and media
stored, and no component may log an error.

It does not exercise what only Kubernetes does: Services, probes, KEDA,
PodDisruptionBudgets. `kubeconform` checks those objects' schemas.

    python3 scripts/chart-smoke.py --image fbs:dev [--values FILE ...] [--keep]

With the production values (deploy/helm/fbs/values-production.yaml plus
examples/production-smoke.yaml), the script stands in for the platform: it
writes the Secrets platform-credentials (one file per key, for the local
services) and proxies, and creates every topic beforehand, as the platform
does; fbs must then create no topic of its own, and keep its media files
under s3.media_prefix.

It uses its own database (fbs_smoke), Redis DB 15 and topic prefix
(chartsmoke.), so it can run next to the tests.
"""
import argparse
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request

import yaml

PREFIX = "fbs-smoke-"
TOPIC_PREFIX = "chartsmoke."
DB = "fbs_smoke"
PAGES = ["brand0", "brand1", "brand2"]


def sh(*args, check=True, capture=True, timeout=600):
    r = subprocess.run(args, capture_output=capture, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise SystemExit(f"command failed ({r.returncode}): {' '.join(args)}\n{r.stdout}\n{r.stderr}")
    return r


def psql(sql, db=DB):
    return sh("docker", "exec", "fbs-postgres-1", "psql", "-U", "fbs", "-d", db, "-tAc", sql).stdout.strip()


def render(args):
    repo, _, tag = args.image.rpartition(":")
    if not repo or "/" in tag:
        raise SystemExit(f"--image {args.image}: want repository:tag")
    values = []
    for v in args.values or ["deploy/helm/fbs/examples/fakefb.yaml"]:
        values += ["-f", v]
    out = sh(args.helm, "template", "fbs", "deploy/helm/fbs", *values,
             "--set", f"image.repository={repo}",
             "--set", f"image.tag={tag}",
             "--set", "image.pullPolicy=Never",
             "--set", f"config.kafka.topic_prefix={TOPIC_PREFIX}",
             "--set", "config.redis.db=15").stdout
    return [d for d in yaml.safe_load_all(out) if d]


def docker_args(name, alias, pod, container, configmaps, secret_dirs, secret_env, network):
    """docker run arguments for one pod container, from its rendered spec;
    configmaps and secret_dirs map a ConfigMap's or Secret's name to the
    directory holding its files."""
    psc = pod.get("securityContext") or {}
    csc = container.get("securityContext") or {}
    a = ["docker", "run", "-d", "--name", PREFIX + name, "--network", network, "--network-alias", alias,
         "--env-file", secret_env]
    if psc.get("runAsUser"):
        a += ["--user", f"{psc['runAsUser']}:{psc.get('fsGroup', psc['runAsUser'])}"]
    if csc.get("readOnlyRootFilesystem"):
        a.append("--read-only")
    for cap in (csc.get("capabilities") or {}).get("drop") or []:
        a += ["--cap-drop", cap]
    if csc.get("allowPrivilegeEscalation") is False:
        a += ["--security-opt", "no-new-privileges"]
    mem = ((container.get("resources") or {}).get("limits") or {}).get("memory")
    if mem:
        a += ["--memory", mem.replace("Mi", "m").replace("Gi", "g")]
    for e in container.get("env") or []:
        a += ["-e", f"{e['name']}={e['value']}"]
    volumes = {v["name"]: v for v in pod.get("volumes") or []}
    for m in container.get("volumeMounts") or []:
        v = volumes[m["name"]]
        if "configMap" in v:
            a += ["-v", f"{configmaps[v['configMap']['name']]}:{m['mountPath']}:ro"]
        elif "secret" in v and v["secret"]["secretName"] in secret_dirs:
            a += ["-v", f"{secret_dirs[v['secret']['secretName']]}:{m['mountPath']}:ro"]
        elif "emptyDir" in v:
            a += ["--tmpfs", f"{m['mountPath']}:uid={psc.get('runAsUser', 0)},gid={psc.get('fsGroup', 0)}"]
        else:
            raise SystemExit(f"{name}: volume {m['name']} has no Docker equivalent here: {v}")
    for p in container.get("ports") or []:
        a += ["-p", f"127.0.0.1::{p['containerPort']}"]
    cmd = list(container.get("command") or []) + list(container.get("args") or [])
    a += ["--entrypoint", cmd[0], container["image"]] + cmd[1:]
    return a


def host_port(name, port):
    out = sh("docker", "port", PREFIX + name, str(port)).stdout.strip().splitlines()[0]
    return int(out.rsplit(":", 1)[1])


def http(url, key=None, body=None, method=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, method=method)
    if key:
        req.add_header("Authorization", "Bearer " + key)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except OSError as e:
        return 0, str(e)


# What the platform's Secrets would hold, for the local services: the
# credentials one file per key (infra/README.md of the platform), and a
# proxies.yaml whose tier reaches fakefb directly.
PLATFORM_CREDENTIALS = {
    "POSTGRES_HOST": "postgres", "POSTGRES_PORT": "5432", "POSTGRES_DATABASE": DB, "POSTGRES_USER": "fbs",
    "POSTGRES_PASSWORD": "fbs", "POSTGRES_SSLMODE": "disable", "POSTGRES_CA_CERT": "",
    "KAFKA_BROKERS": "redpanda:9092", "KAFKA_TLS": "false", "KAFKA_CA_CERT": "",
    "VALKEY_HOST": "valkey", "VALKEY_PORT": "6379", "VALKEY_TLS": "false", "VALKEY_CA_CERT": "",
    "S3_ENDPOINT": "http://s3:9000", "S3_REGION": "us-east-1", "S3_ACCESS_KEY": "smoke", "S3_SECRET_KEY": "smokesmoke",
    "S3_BUCKET_RAW": "fbs-raw", "S3_BUCKET_MEDIA": "fbs-media", "S3_BUCKET_BACKUPS": "fbs-backups",
}
PROXIES_YAML = "fb:\n  proxy:\n    tiers: [{name: isp, kind: direct}]\n"


def write_secret(work, name, files):
    d = os.path.join(work, "secret-" + name)
    os.makedirs(d)
    os.chmod(d, 0o755)
    for fname, body in files.items():
        with open(os.path.join(d, fname), "w") as f:
            f.write(body)
        os.chmod(os.path.join(d, fname), 0o644)
    return d


def topics(prefix):
    out = sh("docker", "exec", "fbs-redpanda-1", "rpk", "topic", "list").stdout
    return {line.split()[0] for line in out.splitlines()[1:] if line.split() and line.split()[0].startswith(prefix)}


def cleanup():
    names = sh("docker", "ps", "-aq", "--filter", f"name={PREFIX}").stdout.split()
    if names:
        sh("docker", "rm", "-f", *names, check=False)
    sh("docker", "exec", "fbs-postgres-1", "psql", "-U", "fbs", "-d", "postgres", "-c", f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)", check=False)
    sh("docker", "exec", "fbs-redpanda-1", "rpk", "topic", "delete", "-r", "^" + TOPIC_PREFIX.replace(".", r"\.") + ".*", check=False)
    sh("docker", "exec", "fbs-valkey-1", "valkey-cli", "-n", "15", "flushdb", check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", default="fbs:dev")
    ap.add_argument("--values", action="append",
                    help="values file, repeatable (default deploy/helm/fbs/examples/fakefb.yaml)")
    ap.add_argument("--network", default="fbs_default")
    ap.add_argument("--helm", default="helm")
    ap.add_argument("--timeout", type=int, default=300, help="seconds for the backfills")
    ap.add_argument("--keep", action="store_true", help="leave the containers running")
    args = ap.parse_args()

    # make infra-up returns once Postgres answers; Redpanda may still be
    # starting. A fresh broker also answers its first producer-ID requests
    # with NOT_COORDINATOR for a while: produce once (idempotently, as the
    # components do) before starting them.
    sh("docker", "exec", "fbs-redpanda-1", "rpk", "cluster", "health", "--exit-when-healthy", timeout=180)
    cleanup()
    deadline = time.time() + 120
    while sh("docker", "exec", "fbs-redpanda-1", "sh", "-c",
             f"rpk topic create {TOPIC_PREFIX}warmup >/dev/null 2>&1; echo warmup | rpk topic produce {TOPIC_PREFIX}warmup", check=False).returncode != 0:
        if time.time() > deadline:
            raise SystemExit("Redpanda does not accept idempotent produce requests")
        time.sleep(2)
    docs = render(args)
    configmaps = {d["metadata"]["name"]: d.get("data") or {} for d in docs if d["kind"] == "ConfigMap"}
    hooks = [d for d in docs if d["kind"] == "Job"]
    post_hooks = [j for j in hooks if "post-" in j["metadata"].get("annotations", {}).get("helm.sh/hook", "")]
    pre_hooks = [j for j in hooks if j not in post_hooks]
    deployments = [d for d in docs if d["kind"] == "Deployment"]
    print(f"rendered: {len(deployments)} deployments, {len(hooks)} hook jobs, configmaps {sorted(configmaps)}")
    pods = [d["spec"]["template"]["spec"] for d in hooks + deployments]
    secrets_used = {v["secret"]["secretName"] for p in pods for v in p.get("volumes") or [] if "secret" in v}
    uses_secret_env = any(c.get("envFrom") for p in pods for c in p["containers"])
    config = yaml.safe_load(configmaps["fbs-config"]["config.yaml"])
    media_prefix = (config.get("s3") or {}).get("media_prefix") or ""
    seeded = [p.lower() for p in (config.get("pages") or {}).get("seed") or []] if post_hooks else []
    # Without the chart's topics hook the platform makes the topics: so does
    # this script, and fbs must not add any.
    platform_topics = not any(j["metadata"]["name"].endswith("-topics") for j in pre_hooks)
    if platform_topics:
        with open("configs/topics.yaml") as f:
            names = [TOPIC_PREFIX + t["name"] for t in yaml.safe_load(f)["topics"]]
        sh("docker", "exec", "fbs-redpanda-1", "rpk", "topic", "create", "-p", "3", "-r", "1", *names)
        made = topics(TOPIC_PREFIX)
        print(f"topics made as the platform would: {len(names)}")

    psql(f"CREATE DATABASE {DB}", db="postgres")
    work = tempfile.mkdtemp(prefix="chart-smoke-")
    os.chmod(work, 0o755)
    # Each ConfigMap as a directory, written once: rewriting one while a
    # container reads it would hand that container an empty file.
    for cm, data in list(configmaps.items()):
        d = os.path.join(work, cm)
        os.makedirs(d)
        os.chmod(d, 0o755)
        for fname, body in data.items():
            with open(os.path.join(d, fname), "w") as f:
                f.write(body)
            os.chmod(os.path.join(d, fname), 0o644)
        configmaps[cm] = d
    # What the cluster's Secret fbs-env (secretEnv.existingSecret) would hold,
    # when the values use one.
    secret_env = os.path.join(work, "fbs-env")
    with open(secret_env, "w") as f:
        if uses_secret_env:
            f.write(f"FBS_POSTGRES__DSN=postgres://fbs:fbs@postgres:5432/{DB}?sslmode=disable\n"
                    "FBS_S3__ACCESS_KEY=smoke\nFBS_S3__SECRET_KEY=smokesmoke\n"
                    f"FBS_PRIVACY__AUTHOR_HMAC_KEY={secrets.token_hex(32)}\n")
    secret_dirs = {}
    for name in secrets_used:
        if name == "platform-credentials":
            secret_dirs[name] = write_secret(work, name, PLATFORM_CREDENTIALS)
        elif name == "proxies":
            secret_dirs[name] = write_secret(work, name, {"proxies.yaml": PROXIES_YAML})
        else:
            raise SystemExit(f"the values use Secret {name}, which this script can't stand in for")
    # The platform adds the Secret proxies (the owner's PROXIES_FBS) only once
    # the pilot has stopped, and that waits for the master to see fbs up: so
    # the hooks and every pod outside secretFiles.components must start
    # without it. It is added once they are ready; the pods that mount it
    # start then.
    def mounts(spec, secret):
        return any((v.get("secret") or {}).get("secretName") == secret for v in spec.get("volumes") or [])
    proxies_dir = secret_dirs.pop("proxies", None)
    for job in hooks:
        if proxies_dir and mounts(job["spec"]["template"]["spec"], "proxies"):
            raise SystemExit(f"hook {job['metadata']['name']} mounts the Secret proxies: it would wait for PROXIES_FBS")
    later = [d for d in deployments if proxies_dir and mounts(d["spec"]["template"]["spec"], "proxies")]
    first = [d for d in deployments if d not in later]
    # The Services the values expect besides the infrastructure.
    sh("docker", "run", "-d", "--name", PREFIX + "s3", "--network", args.network, "--network-alias", "s3",
       "--entrypoint", "/usr/local/bin/devs3", args.image, "--addr", ":9000")
    sh("docker", "run", "-d", "--name", PREFIX + "fakefb", "--network", args.network, "--network-alias", "fakefb",
       "--entrypoint", "/usr/local/bin/fakefb", args.image, "--addr", ":8090", "--pages", str(len(PAGES)), "--posts", "10")

    failures = []
    try:
        def run_hook(job):
            name = job["metadata"]["name"]
            pod = job["spec"]["template"]["spec"]
            a = docker_args(name, name, pod, pod["containers"][0], configmaps, secret_dirs, secret_env, args.network)
            cid = sh(*a).stdout.strip()
            code = sh("docker", "wait", cid, timeout=180).stdout.strip()
            r = sh("docker", "logs", cid, check=False)
            logs = r.stdout + r.stderr
            print(f"hook {name}: exit {code}")
            if code != "0":
                raise SystemExit(f"hook {name} failed:\n{logs[-3000:]}")

        # Pre-install hooks, in order, each must exit 0.
        for job in sorted(pre_hooks, key=lambda j: j["metadata"]["name"].endswith("-topics")):
            run_hook(job)

        def start(wave):
            for d in wave:
                name = d["metadata"]["name"]
                pod = d["spec"]["template"]["spec"]
                sh(*docker_args(name, name, pod, pod["containers"][0], configmaps, secret_dirs, secret_env, args.network))
            # Readiness, as the readinessProbe would check it.
            deadline = time.time() + 120
            pending = {d["metadata"]["name"] for d in wave}
            while pending and time.time() < deadline:
                for name in sorted(pending):
                    status, _ = http(f"http://127.0.0.1:{host_port(name, 9090)}/readyz")
                    if status == 200:
                        pending.discard(name)
                time.sleep(1)
            if pending:
                raise SystemExit(f"not ready after 120s: {sorted(pending)}")

        start(first)
        if later:
            print(f"{len(first)} deployments ready without the Secret proxies")
            secret_dirs["proxies"] = proxies_dir
            start(later)
            print(f"then {len(later)} with it: {sorted(d['metadata']['name'] for d in later)}")
        print(f"all {len(deployments)} deployments ready")
        # Post-install hooks, once the release is up (the page seed).
        for job in post_hooks:
            run_hook(job)

        # An API key, as the go-live runbook creates it.
        out = sh("docker", "exec", PREFIX + "fbs-api", "/usr/local/bin/fbctl", "apikey", "create", "--name", "smoke",
                 "--scopes", "pages:write,jobs:write,data:read").stdout
        key = json.loads(out[out.index("{"):])["api_key"]
        pages_file = os.path.join(work, "pages.csv")
        with open(pages_file, "w") as f:
            f.write("page\n" + "\n".join(PAGES) + "\n")
        os.chmod(pages_file, 0o644)
        out = sh("docker", "run", "--rm", "--network", args.network, "-v", f"{pages_file}:/pages.csv:ro",
                 "--entrypoint", "/usr/local/bin/fbctl", args.image, "pages", "import",
                 "--api", "http://fbs-api:8080", "--key", key, "/pages.csv").stdout
        imported = json.loads(out[out.index("{"):])
        print(f"pages import: {imported}")
        # The pages the seed hook registered are there already.
        want = len([p for p in PAGES if p not in seeded])
        if imported.get("created") != want or imported.get("existing", 0) != len(PAGES) - want:
            failures.append(f"pages import: {imported}, want {want} created, {len(PAGES) - want} there already")
        # Importing again changes nothing.
        out = sh("docker", "run", "--rm", "--network", args.network, "-v", f"{pages_file}:/pages.csv:ro",
                 "--entrypoint", "/usr/local/bin/fbctl", args.image, "pages", "import",
                 "--api", "http://fbs-api:8080", "--key", key, "/pages.csv").stdout
        again = json.loads(out[out.index("{"):])
        if again.get("existing") != len(PAGES) or again.get("created"):
            failures.append(f"second import: {again}")

        deadline = time.time() + args.timeout
        while time.time() < deadline:
            done = int(psql("SELECT count(*) FROM jobs WHERE status IN ('completed','partial','failed','cancelled')") or 0)
            if done >= len(PAGES):
                break
            time.sleep(2)
        statuses = psql("SELECT string_agg(kind || ':' || status, ',') FROM jobs")
        posts = int(psql("SELECT count(*) FROM posts"))
        comments = int(psql("SELECT count(*) FROM comments"))
        media = psql("SELECT coalesce(string_agg(status || '=' || n, ','), '') FROM (SELECT status, count(*) n FROM media GROUP BY status) m")
        print(f"jobs {statuses}; posts {posts}; comments {comments}; media {media}")
        if statuses is None or statuses.count(":completed") != len(PAGES):
            failures.append(f"jobs: {statuses}")
        if posts == 0 or comments == 0:
            failures.append("nothing stored")
        if "ready=" not in media and "deleted=" not in media:
            failures.append(f"no media downloaded: {media}")
        if media_prefix:
            outside = psql(f"SELECT count(*) FROM media WHERE s3_key IS NOT NULL AND left(s3_key, {len(media_prefix)}) <> '{media_prefix}'")
            inside = psql(f"SELECT count(*) FROM media WHERE left(s3_key, {len(media_prefix)}) = '{media_prefix}'")
            print(f"media files under {media_prefix}: {inside}; elsewhere: {outside}")
            if outside != "0" or inside == "0":
                failures.append(f"media keys: {inside} under {media_prefix}, {outside} elsewhere")
        if platform_topics:
            extra = topics(TOPIC_PREFIX) - made
            if extra:
                failures.append(f"fbs created topics the platform didn't: {sorted(extra)}")
        api_port = host_port("fbs-api", 8080)
        status, body = http(f"http://127.0.0.1:{api_port}/v1/pages", key)
        if status != 200:
            failures.append(f"GET /v1/pages: {status} {body[:200]}")

        for d in deployments + hooks:
            name = d["metadata"]["name"]
            r = sh("docker", "logs", PREFIX + name, check=False)
            errors = [l for l in (r.stdout + r.stderr).splitlines() if '"level":"ERROR"' in l]
            if errors:
                failures.append(f"{name} logged {len(errors)} errors, e.g. {errors[0][:300]}")
    finally:
        if not args.keep:
            cleanup()

    if failures:
        print("FAIL")
        for f in failures:
            print(" -", f)
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
