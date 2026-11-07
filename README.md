# Multi-tenant record ingestion

A working Java 21 service with **versioned Protocol Buffers schemas**, asynchronous
HTTP batches, strict per-record validation, SQLite persistence, idempotent retries,
bounded storage retries, and fair scheduling between customers.

## Run

Prerequisites: JDK 21+, a POSIX shell, `curl`, `tar`, and `shasum`. The included
`mvnw` bootstraps pinned Maven into `.tools/` and downloads dependencies into `.m2/`.
Internet access is needed for the first build. No database service, cloud account,
Docker, Python, or installed Maven/protoc is required.

```sh
./mvnw test
./mvnw package
java -jar target/ingestion.jar --port 8080 --db data/ingestion.sqlite3
```

The service binds to `127.0.0.1`. Stop with Ctrl-C; shutdown normally drains
accepted work before closing SQLite. Persistent metadata errors get bounded
shutdown retries, leaving unfinished batches resumable after restart. Schemas,
records and completed reports survive restarts. One service process owns each
canonical database path (symbolic aliases resolve to the same lock; dangling
database symlinks are rejected).

Run the standalone demonstration from the project root:

```sh
./mvnw package
java -cp target/test-classes:target/ingestion.jar io.atlan.ingestion.Demo
```

The demo starts an isolated temporary service and database under `target/`. It
checks mixed-version validation, partial success, unchanged completed reports,
transient/exhausted storage retries, and customer isolation. Customer A sends 100
records with **25 ms injected delay per write**; B sends one with no delay after
A starts processing. It asserts B completes while A is still processing and
prints actual acknowledgment/completion timings. Fault injection lives only in
`src/test/`; there is no production fault-control endpoint.

## Try the API

These fixtures include actual `protoc`-compiled descriptors and their `.proto`
sources. Register both immutable versions, then submit a mixed batch:

```sh
curl -sS http://127.0.0.1:8080/health
curl -sS -H 'Content-Type: application/json' --data-binary @fixtures/register-v1.json http://127.0.0.1:8080/schemas
curl -sS -H 'Content-Type: application/json' --data-binary @fixtures/register-v2.json http://127.0.0.1:8080/schemas
curl -sS -H 'Content-Type: application/json' --data-binary @fixtures/mixed-batch.json http://127.0.0.1:8080/batches
curl -sS http://127.0.0.1:8080/batches/acme/orders/mixed-001
curl -sS 'http://127.0.0.1:8080/records/acme/orders?limit=100&offset=0'
curl -sS 'http://127.0.0.1:8080/schemas/acme/orders?limit=100&offset=0'
```

Poll the batch endpoint until `status` is `completed`. The fixture's expected
report is **total=7, successful=2, failed=5**. Stored IDs are `valid-v1` and
`valid-v2`; their original payloads and base64 protobuf bytes are inspectable.
Resubmitting the fixture returns the existing status/report. Changing its content
without changing its batch ID returns 409. Batch IDs are scoped by customer and
dataset; a different batch may store the same record ID again.

| Method and route | Result |
| --- | --- |
| `POST /schemas` | 201 new immutable version; 200 identical registration |
| `GET /schemas/{customer}/{dataset}?limit=100&offset=0` | Version-ordered schema page |
| `POST /batches` | 202 accepted/in progress; 200 completed identical retry |
| `GET /batches/{customer}/{dataset}/{batch}` | Durable status or final report; 404 if absent |
| `GET /records/{customer}/{dataset}?limit=100&offset=0` (or `&after=0`) | Stored records in insertion order |
| `GET /health` | Liveness |

For offset pages, follow `next_offset`. For record cursor pages, start with
`?limit=100&after=0`, then pass each `next_after` as the next request's `after`.
A null continuation means the current scan is complete. Use `next_after` in cursor
mode; the response's offset metadata does not locate a cursor page. `after` is a
nonnegative signed 64-bit row ID; it cannot be combined with a nonzero offset,
and is supported only by `/records`. Byte budgets can return fewer rows than
`limit`. Inspection is live, not a snapshot across concurrent writes.

Errors use `{ "error": "machine_code", "message": "explanation" }`: 400 malformed
JSON/batch structure; 413 size limit; 415 content type; 422 malformed/incompatible
schema; 409 conflicting identity/version ordering; 429 accepted-work quota;
503 unavailable database or saturated HTTP queue (`server_busy`). Malformed individual records are accepted with the
batch and reported independently, including unknown datasets or versions.

## Protobuf schema contract

Registration format:

```json
{
  "customer_id": "acme",
  "dataset_id": "orders",
  "version": 1,
  "message_type": "customer.Order",
  "descriptor_set": "<base64-encoded FileDescriptorSet>"
}
```

The schema is a **proto2** message, so required/optional rules come directly from
protobuf. There is no custom field-definition format and no server-side .proto
parser. Example from `fixtures/proto/v1/orders.proto`:

```proto
syntax = "proto2";
package customer;
message Order {
  required string order_id = 1;
  required double amount = 2;
  optional string currency = 3;
}
```

To compile your own schema, use protoc (optional development tool):

```sh
protoc -I path/to/protos --include_imports --descriptor_set_out=orders.desc orders.proto
base64 < orders.desc | tr -d '\n'
```

Put the base64 output in `descriptor_set`. If using timestamps, add
`import "google/protobuf/timestamp.proto";` and supply protoc's standard include
path. A registration accepts one flat dataset message, plus the standard Timestamp
import. Unsupported: proto3/editions, repeated fields, maps, arbitrary nested
messages, enums, oneofs, extensions and explicit defaults. Field names must match
the identifier rule below. Use protobuf **field names** in JSON, not camelCase
aliases. Both binary protobuf and original JSON are persisted per successful row.

| Logical type | Protobuf type | JSON validation |
| --- | --- | --- |
| string | `string` | String only, including empty string |
| integer | `int64` | Integer token in signed 64-bit range; `1.0`, strings and bool fail |
| float | `double` | JSON number representable as a finite double; integer tokens allowed; bool/strings fail |
| boolean | `bool` | Literal true/false only |
| timestamp | `google.protobuf.Timestamp` | Real date/time string `YYYY-MM-DDTHH:MM:SS[.fffffffff](Z\|±HH:MM)` |

Timestamps require uppercase T/Z, seconds and a timezone; fractions have 1–9
digits (up to nanosecond precision). No leap seconds, year zero or time-only/date-only values. Offsets must be
valid `ZoneOffset` values (within ±18:00), and the instant must fit protobuf's
years 0001–9999. JSON retains the submitted offset; protobuf stores epoch seconds
and nanoseconds. Decimal JSON values retain their precision in stored JSON;
protobuf `double` has normal IEEE-754 rounding. No coercion from strings occurs.

String values must contain valid Unicode scalar sequences. Escaped unmatched
surrogates fail only their record as `invalid_unicode`; valid surrogate pairs
retain their original value in JSON and protobuf. Unknown descriptor wire fields
(including options), Editions feature overrides and proto3 presence flags are
rejected. Reserved ranges may span protobuf's implementation-reserved tag numbers;
actual fields still cannot use those numbers.

All values, including optional fields, reject explicit null. Unknown fields,
arrays and nested objects fail (Timestamp itself is a JSON string). Required
fields containing `0`, `false` or `""` count as present and are encoded with
protobuf presence. This is intentionally stricter than protobuf's general JSON
mapping, which permits several coercions and quoted integer representations.

Initial schema versions can be any positive 32-bit integer; new versions must
increase. Existing fields retain names, field numbers, types and requiredness, and
previously declared `reserved` field names/ranges cannot be removed or reused;
only optional additions are allowed. Protobuf's weaker wire-compatibility rules
do not override this policy. Old versions remain available. Repeating a
registration with identical normalized descriptor content (ignoring `SourceCodeInfo`
compiler location metadata) is safe and idempotent; conflicting schema definitions
for an existing version return 409.

## Batch/report contract and bounds

The example batch format is in `fixtures/mixed-batch.json`. Every record must have
exactly `record_id`, `schema_version` and `payload`; the batch envelope must have
exactly `customer_id`, `dataset_id`, `batch_id` and `records`. Each batch has one
tenant/dataset. Each record explicitly selects its version. Duplicate
syntactically valid record IDs reject the whole batch, even if one payload would
later fail validation. Missing/invalid IDs fail that record by zero-based position.

Each completed report contains tenant/dataset/batch IDs, total/successful/failed
counts, timestamps and one failure entry per failed position. Failure entries
include category (`validation`, `storage` or `internal`), code, reason, position
and a valid record ID when available. Storage failures include attempt count.
Unexpected validation/encoding bugs use `internal/validation_error`, unexpected
lookup bugs use `internal/schema_lookup_error`, and unexpected write bugs
(including unchecked `Error`s) use `internal/storage_write_error`; none are retried.
Their stack traces are logged with batch/position context rather than being
reported as registry outages or invalid customer data. Successful
records are durable before a report becomes completed. Final counts reconcile
and completed reports never change. Rows can be visible while a batch processes.

- Request: 2 MiB, 1,000 records/batch, JSON nesting depth 32.
- Schema: 256 KiB decoded descriptor, 100 fields.
- IDs: `[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}`.
- Pending work: 100 batches globally, 10/customer across datasets, 32 MiB total
  normalized batch JSON in memory globally, and 8 MiB per customer
  (`maxPendingBytesPerCustomer`). Active/completed retries are checked before
  record deep-copying and quotas; interrupted retries must reserve capacity again.
  Retries still parse, serialize for byte accounting, and hash the supplied body;
  they avoid the additional records tree copy.
- Processing: 4 workers, at most 1 active record/customer, FIFO batches/customer,
  round robin between ready customers. JSON preparation and durable admission
  happen outside the scheduler lock. In-flight admissions count against quotas
  and graceful shutdown; failed reservations release their capacity. An error
  after a committed admission is reconciled before enqueueing.
- SQLite: 1 writer connection with a fair write lock, 4 pooled metadata readers,
  2 separate pooled inspection readers. WAL readers run concurrently with writes.
  The schema LRU has a separate short lock; cache hits never wait for a DB lock.
  Inspection fetches bounded raw pages, releases the cursor/connection, then
  parses payload JSON and Base64-encodes blobs. Responses emit supplementary
  Unicode as UTF-8, matching stored-byte page estimates. SQLite still permits one writer.
- HTTP: 16 request workers, 64 queued exchanges, connection/time limits, HTTP/1.1
  keep-alive for `2xx` responses. A separate bounded overload pool (2 workers,
  128 queued tasks) returns `503 server_busy` with `Retry-After: 1` without parsing
  requests on the dispatcher thread. Slow overload requests can delay overload
  responses; connection limits and timeouts still apply.
- Inspection: 1–1,000 rows requested per page (default 100), 8 MiB page byte budget,
  supporting both `offset` pagination and `after` keyset cursor pagination on `records`.
- Retry: schema lookup and record write each have an independent budget of
  3 total attempts, with 10 ms then 20 ms timer-scheduled backoff that immediately
  releases the worker thread for other customers. Validated protobuf bytes are cached
  across write retries. SQLite BUSY/LOCKED primary codes (including extended codes)
  are transient. Exhausted lookup failures use
  `storage/schema_lookup_retries_exhausted`; permanent SQL lookup failures use
  `storage/schema_lookup_failed` after one attempt. Unknown schemas, ordinary
  validation failures and unexpected internal bugs are never retried. Write
  failure codes and attempt counts remain separate from lookup outcomes.

Equivalent batch submissions are defined by SHA-256 of a length-delimited, typed
encoding of parsed JSON with recursively sorted object keys. Whitespace/object-key
order are ignored; array order, strings, IDs, versions and numeric token kind remain
significant. Decimal/exponent tokens with the same numeric value are equivalent
to each other, but distinct from integer tokens (including `1e0` versus `1`).
Retaining a decimal value does not
mean preserving the original HTTP whitespace or number spelling. Duplicate keys,
non-JSON numeric literals (`NaN`, `Infinity`, `-Infinity`) and non-UTF-8 input reject
the submission. Valid JSON numbers outside a field's supported range, such as
`1e309` for protobuf `double`, fail only that record during schema validation.
An overflow value in an undeclared field likewise fails only its record as an
unknown field. Other valid records are still stored. This includes extreme
BigDecimal exponents whose normalized scale exceeds Java's integer scale range.

After an unclean restart, unfinished batches report `status: "interrupted"`,
`retryable: true`, `interrupted_at` and retry instructions. Resubmit an equivalent
request with the same tenant/dataset/batch ID to resume (202); different content
still returns 409. Committed positions count as successes without being validated
or written again. All other positions are processed from the supplied request,
including failures whose outcomes were not finalized before interruption. Final
completed reports remain immutable. An interrupted batch stays available until
retried; a queued admission with no in-memory job after an uncertain database
outcome is also safely re-admitted by an identical retry; the server does not replay work without the client request.

Only fingerprints and report metadata are retained for batches. Valid record
payloads/protobuf remain in `records`; full request bodies are not stored. Startup
migrates legacy databases by dropping `batches.body`, preserving schemas, records,
identities and reports. Migration may rewrite the table; freed pages are reusable.
To shrink an existing database file physically, run SQLite `VACUUM` offline after
stopping the service. Startup does not automatically compact the database.

## Verification and scope

`./mvnw test` exercises real localhost HTTP and temporary SQLite files. Fixtures
and expected counts/payloads are hand-authored; tests do not reuse the service's
validation logic to compute expectations. Tests cover schema compatibility and
malformed descriptors, mixed/all-invalid records, strict types and timestamps,
protobuf decoding, concurrent retries/conflicts, tenant separation, retry
exhaustion, isolation/admission limits, pagination, decimal retention, normal
restart, graceful drain and forced child-JVM termination before enqueue and after
a partial commit. Interrupted retries preserve committed rows and handle concurrent
resubmissions, quota rejection and failed resumption updates. Deterministic latch tests also pause admission copying,
a JDBC write and an inspection cursor to verify unrelated scheduler turns, cached
and uncached metadata reads, and HTTP ingestion continue respectively. Test-only
JDBC decorators delegate to real SQLite; no production test hooks are needed.

This is a single-process local prototype. Authentication, automatic crash replay, external
brokers, multi-instance deployment and a 50k records/sec benchmark are out of
scope. Permanent metadata errors prevent publishing a durable completed report;
shutdown abandons the in-memory attempt after three consecutive metadata failures,
leaving durable unfinished state for restart/resubmission. An indefinitely blocked
storage call can still delay shutdown. See
[DECISIONS.md](DECISIONS.md) for architecture, production evolution and limits,
and [AI_SESSION.md](AI_SESSION.md) for session provenance and transcript scope.
