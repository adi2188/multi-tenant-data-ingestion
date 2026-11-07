# Multi-tenant ingestion prototype

Build a local HTTP service that accepts batches without waiting for processing,
validates each record against its explicitly requested immutable schema, persists
valid records, and exposes immutable completed reports. The attached take-home
supplies the acceptance criteria; the interview PDF supplies production context.

## Approach and scope

Implementation choice (updated at the user's request): Java 21, Protocol Buffers,
Jackson, SQLite JDBC, bounded HTTP threads,
and a record-granularity fair scheduler. This needs no external services or cloud
accounts and makes concurrency and transactions explicit. Maven bootstraps locally.
A framework plus an
external broker would add setup without improving the local acceptance criteria;
one thread per tenant would not bound resource usage. No UI, authentication,
broker, distributed workers, cross-batch deduplication, or automatic crash replay.

## Contracts

- POST /schemas registers `{customer_id,dataset_id,version,message_type,descriptor_set}`.
  `descriptor_set` is a base64 FileDescriptorSet for one proto2 message. Supported
  protobuf types: string, int64, double, bool, google.protobuf.Timestamp.
  Proto2 required/optional labels define requiredness; field names and numbers are
  immutable. Initial positive integer version is arbitrary; new
  versions must increase. Only optional additions are compatible. Identical
  existing registrations are safe; conflicting ones return 409.
- POST /batches accepts `{customer_id,dataset_id,batch_id,records}` and returns
  202 without waiting. Each record contains `record_id,schema_version,payload`.
  Identical retries return the persisted status (200 when already complete);
  conflicting content returns 409. Object order and whitespace are irrelevant;
  array order and integer versus decimal/exponent token kind are significant.
  Decimal tokens with equivalent values are equivalent regardless of scale.
- GET /batches/{customer}/{dataset}/{batch} returns status and final report.
  GET /schemas/{customer}/{dataset} and GET /records/{customer}/{dataset}
  expose persisted data; records are paginated and include original payloads plus
  base64 protobuf bytes.
- Strict JSON rejects duplicate object keys, non-JSON numeric literals (`NaN`,
  `Infinity`, `-Infinity`), and malformed
  UTF-8. Structural batch problems reject the entire submission. Malformed
  individual records fail independently. Duplicate syntactically valid record
  IDs reject a batch before any processing.
- Integer means a JSON integer in signed 64-bit range (excluding bool); float
  accepts JSON numbers representable as finite doubles (excluding bool). Numeric
  overflow fails only its record during schema validation. No string coercion. Timestamp
  means a real RFC3339 date/time with uppercase T, timezone, seconds, and up to
  nine fractional digits; no leap seconds. Optional fields may be absent but
  never null. Payloads are flat; unknown fields fail.
- Limits: 2 MiB request, 1,000 records/batch, 100 schema fields, 128-character
  ASCII identifiers, 100 unfinished batches globally, 10/customer, 32 MiB globally / 8 MiB per customer of
  normalized unfinished batch content. HTTP pool: 16; record workers: 4.

## Processing and persistence

Bounded dataset and batch-identity lock stripes serialize schema registration
and duplicate admissions. JSON preparation and persistence run outside the
scheduler monitor; short quota updates count in-flight admissions and roll back
failed reservations. Individual SQLite writes commit atomically through one fair
writer lock. Four metadata readers and two separate inspection readers use
query-only WAL connections. The bounded schema LRU has a cache-only lock, and
descriptor compilation happens outside all database/cache locks. An OS file lock
enforces one service process per canonical database path, resolving symbolic aliases. Inspection releases its bounded raw
read snapshot before JSON parsing/Base64 encoding.
Batch identity is (customer,dataset,batch); a canonical SHA256 binds its content.
Batch identities/fingerprints and queued reports are durable before acknowledgment;
request bodies stay in bounded memory. All persistence keys include
tenant and dataset. Completed records are unique by batch position.

The scheduler rotates ready customers, dispatches one record per turn, and allows
one record in flight per customer. A slow write sleeps outside database locks.
Four bounded workers permit unrelated customers to progress. Batch order within
a customer is FIFO. Admission quotas prevent unbounded accepted work.

Transient schema lookups and record writes each have three total attempts with
10/20 ms timer delays that release workers; validated bytes survive write retries. Retryable SQLite primary codes include extended BUSY/LOCKED
codes. Unknown schemas and expected validation failures have one outcome and
no retry. Unexpected validation/encoding or lookup runtime exceptions are
`internal` failures with stage-specific codes and stack-trace diagnostics, rather
than storage outages; they are not retried. Every processed position
contributes exactly one success or failure. Report completion is committed only
after valid records are durable. Graceful shutdown normally drains accepted batches; three consecutive metadata
failures during closing release an unfinalizable attempt for restart/resubmission.
Indefinitely blocked storage calls remain outside this bound.
On startup, unfinished reports become interrupted/retryable. An identical client
retry re-admits work under the same identity, skipping committed positions and
re-evaluating remaining records. Full bodies are not retained; legacy bodies are
removed by migration. Automatic background replay is outside prototype scope.

## Verification and deliverables

JUnit public-HTTP integration tests use temporary SQLite databases and handwritten
expected values. A test-only storage wrapper supplies deterministic delays,
transient failures, and exhausted failures. A standalone demo prints batch sizes,
write delays, completion timings, reports, and assertions. README documents exact
commands and API examples. DECISIONS.md explains tradeoffs, scale, recovery, and
uncertain commits. Session provenance documents distinguish a summary from an
authentic transcript export.
