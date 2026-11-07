# Progress: docs/superpowers/plans/2026-11-06-ingestion.md

- Read both supplied PDFs. Workspace initially contained only PDFs, with no Git
  repository or existing code. Wrote public-API acceptance tests first.
- User steering: switched to Java and Protocol Buffers. Removed draft Python
  tests; no Python product implementation was built.
- Choice: proto2 descriptors express requiredness directly and retain stable
  field numbers. No runtime compiler or custom field-schema language. Original
  JSON and real DynamicMessage bytes are stored together.
- Implemented strict validation, SQLite persistence, fair scheduling, limits,
  retries, HTTP API and graceful shutdown. Initial suite: 9/9 passing (JUnit).
- Build downloads require sandbox escalation; build state stays in `.tools` and
  `.m2` in this workspace.
- Independent review identified missing protobuf field-number/reservation checks,
  UTF-8 autodetection and unbounded inspection. Reproduced regressions first and
  fixed each; Timestamp now requires canonical descriptor identity.
- Decimal retention and integer-versus-exponent fingerprints received failing
  regressions before corrections. Fingerprints use typed length-delimited
  encoding; stored JSON uses BigDecimal instead of lossy double parsing.
- Initial delivery `./mvnw -B -q package`: 15 tests, zero failures/errors/skips. Packaged demo
  passed: B=1 completed in 3.7 ms while A=100 with 25 ms/write remained processing;
  A completed in 2937.2 ms. Full output is in DEMO_OUTPUT.txt.
- Actual executable JVM stop/restart retained the same report, two unchanged
  payloads and schemas v1/v2. Both smoke-test processes were stopped afterward.
- Source formatted; README, decisions, fixtures, genuine visible transcript and
  verification notes delivered. No deferred review findings; production
  limitations are documented rather than claimed implemented.

## Follow-up: record-local double overflow

- User identified that `Json.checkFinite()` treated valid JSON `1e309` as malformed
  JSON by converting BigDecimal to double before batch admission.
- Added raw-HTTP regressions first. Both mixed-record and 999-valid-plus-one-overflow
  submissions reproduced 400 instead of 202; non-JSON literal rejection already passed.
- Removed the global conversion/check. Schema validation now owns double range
  checks; protobuf's existing finite-double guard reports the affected record.
- Focused regressions: 3/3 pass. Includes positive/negative overflow, unknown fields,
  unchanged valid payloads, 999 valid writes, retries/conflicts and NaN/Infinity rejection.
- Full `./mvnw -B -q package`: 18 tests, zero failures/errors/skips; executable
  jar rebuilt. README, design contract and verification notes now distinguish
  valid JSON numeric overflow from invalid numeric literals.

## Follow-up: separate admission, scheduler, cache and database locks

- Reproduced all three reported lock couplings using deterministic test gates:
  admission deep-copy prevented another customer's next turn; a JDBC write
  prevented cache/metadata reads; an open inspection cursor blocked HTTP admission.
- Moved preparation and persistence outside the scheduler monitor. Identity lock
  stripes preserve deduplication; quota reservations count in-flight admissions
  and roll back on insertion failure. Drain includes those reservations.
- Replaced the Store monitor with one fair writer lock, four pooled metadata
  readers, two pooled inspection readers, a separate cache LRU lock and bounded
  schema-registration stripes. Descriptor compilation and response JSON/Base64
  conversion happen outside database connections/locks.
- Seven concurrency/inspection tests pass, including quota rollback, drain,
  concurrent schema registration and Unicode response sizing. Full package:
  25 tests, zero failures/errors/skips.
- Final packaged demo passed: B completed in 3.5 ms while A=100 records with
  25 ms/write remained processing; A completed in 2906.8 ms. SQLite still has
  one writer, so commit/fsync contention remains explicitly documented.
- Independent concurrency review found no other actionable races/deadlocks. Its
  one finding was reproduced as a 10,400,366-byte emoji response and fixed by
  emitting supplementary Unicode as UTF-8. All page conversion remains outside
  database locks/connections. No review findings remain deferred.

## Follow-up: schema lookup retries and accurate failure stages

- User identified that cache-miss SQL lookup failures were not retried and that
  validation runtime bugs were caught as `storage/schema_lookup_failed`.
- Added five focused tests before implementation: transient recovery (BUSY,
  LOCKED and extended codes), exhaustion/permanent failure, unknown versions,
  validation bugs and unexpected lookup bugs. Four reproduced the failures;
  the unknown-schema no-retry control already passed.
- Separated envelope validation, lookup retry, validation/encoding and write
  retry stages. Lookup/write have independent 3-attempt budgets and 10/20 ms
  backoff. Internal bugs are logged and reported as `internal` without retry.
- Focused regressions pass 5/5, including recovery with both lookup and write
  retries, accurate failed-attempt counts, unchanged payloads and later progress.
- Full `./mvnw -B -q package`: 30 tests, zero failures/errors/skips; executable
  jar rebuilt. Failure categories and lookup retry policy are documented.


## Follow-up: interrupted batches and request-body retention

- User identified never-read `batches.body` and unrecoverable queued/processing
  identities after an abnormal exit. Four regressions failed before implementation,
  including forced child-JVM kills after durable reservation and partial commit.
- Removed retained batch bodies, with an atomic legacy column migration preserving
  metadata, schemas and records. Admission no longer sorts/serializes canonical
  bodies for storage; typed fingerprints still bind the full request content.
- Startup marks unfinished reports interrupted/retryable. Equivalent client retries
  re-admit once under identity stripes, respect quotas/conflicts, and skip committed
  positions. Remaining records are evaluated again from the supplied request.
  Completed reports remain immutable; automatic background replay is outside scope.
- A fifth test covers repeated startup, quota rejection and SQL resume failure,
  preserving interrupted reports and releasing failed admission capacity.
- Independent review reproduced uncertain admission outcomes after an actual queued
  commit: releasing quotas could leave no job. A sixth regression first failed;
  exact durable-report/fingerprint reconciliation now enqueues fresh/resumed work
  once when commit succeeded. The amended review found no further issues.
- Focused regressions pass 6/6; final package passes all 36 tests and the packaged
  demo passes. Full verification is recorded in
  VERIFICATION.md; legacy freed pages are reusable, with optional offline VACUUM
  for physical file shrinking.


## Comprehensive audit after user changes

The user's changes added per-customer pending bytes, delayed-copy retries,
worker-releasing backoff, encoded-record reuse, broader poison-record guards,
bounded metadata shutdown, HTTP keep-alive/overload handling, nanosecond timestamp
precision, descriptor hardening and cursor pagination. The 39-test baseline passed.

The audit reproduced and fixed eight defects; [REVIEW.md](REVIEW.md) records each
finding, regression, cleanup and residual limit. Two independent reviewers checked
the amended scheduling/storage/schema paths. Removed two unused methods and the
redundant Maven bootstrap archive; retained original PDFs, fixtures, provenance
and necessary ignored development tools. README/DECISIONS now match the code,
including bounded shutdown, overload handling and remaining retry serialization.
The final suite contains 49 tests. Exact results are in VERIFICATION.md.
