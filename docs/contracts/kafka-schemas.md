# Kafka record schemas (fb.v1)

fbs publishes its results on the public topics below. Topic names, and the
Schema Registry subjects, get `kafka.topic_prefix` prepended.

| Topic | Key | Value message | Proto file |
|---|---|---|---|
| `fb.page.v1` | page id | `securebrand.fb.v1.Page` | `page.proto` |
| `fb.post.v1` | post id | `securebrand.fb.v1.Post` | `post.proto` |
| `fb.post_metrics.v1` | post id | `securebrand.fb.v1.PostMetrics` | `post.proto` |
| `fb.comment.v1` | post id (a post's comments stay in order) | `securebrand.fb.v1.Comment` | `comment.proto` |
| `fb.media.v1` | media id | `securebrand.fb.v1.Media` | `media.proto` |
| `fb.media.ready.v1` | media id | `securebrand.fb.v1.MediaReady` | `media.proto` |
| `fb.media.processed.v1` | media id | `securebrand.fb.v1.MediaProcessed` (the analysis service produces it) | `media.proto` |
| `fb.media.deleted.v1` | media id | `securebrand.fb.v1.MediaDeleted` | `media.proto` |
| `fb.job.v1` | job id | `securebrand.fb.v1.JobEvent` | `job.proto` |
| `fb.erasure.v1` | erasure request id | `securebrand.fb.v1.ErasureEvent` | `erasure.proto` |

The files are in `api/proto/securebrand/fb/v1/`. Generated Go is in `gen/go/securebrand/fb/v1`.

## Wire format

- A value is the plain protobuf encoding of the message. There is no Schema Registry framing: no magic byte, schema id or message indexes.
- The `fbs-type` header holds the full message name, for example `securebrand.fb.v1.Comment`.
- `content-type` is `application/x-protobuf`.

A consumer decodes by `fbs-type`: with the generated code, or with a descriptor built from the registered schema.

## Schema Registry

`fbctl schemas apply` registers the contract with a Confluent-compatible registry (`kafka.schema_registry.url`). Redpanda's registry, on port 8081, works.

- **Subjects:**
  - each topic's value schema is registered under `<topic>-value`;
  - `securebrand/fb/v1/common.proto`, which the other files import, has a subject of its own, and the topic subjects reference it;
  - `google/protobuf/*` imports are the registry's built-in well-known types.
- **Compatibility:** every subject is set to `BACKWARD_TRANSITIVE`. A consumer built against any earlier version can still read new data, so it can upgrade at its own pace.
- **Idempotent:** an unchanged contract registers nothing new.
- **`fbctl schemas check`** is a dry run. It checks each schema against the subject's latest version.

The registry does not change what is on the wire. It documents the schemas for consumers and refuses breaking changes at deploy time. The Helm hook (`hooks.schemas: true`) runs `fbctl schemas apply` before each upgrade, so an incompatible contract change fails the upgrade before any pod produces it. Docker Compose runs it as the `schemas` service.

## Changing the contract

1. Only add fields, with new field numbers. Never change a field's type or number, and never reuse a removed number: mark it `reserved`.
2. CI runs `buf breaking` against the base branch (`FILE` rules), so a breaking change fails the pull request.
3. On deploy, `fbctl schemas apply` registers the new version. The registry refuses it if it is not backward compatible with every earlier version.
4. A real break needs a new topic and package (`fb.comment.v2`, `securebrand.fb.v2`), produced alongside v1 until consumers move.
