# Verification — November 6, 2026

Environment: macOS arm64, Temurin OpenJDK 21.0.1, Maven 3.9.16.

Commands from the project root:

```sh
./mvnw test
./mvnw package
java -jar target/ingestion.jar --help
java -cp target/test-classes:target/ingestion.jar io.atlan.ingestion.Demo
```

Expanded integration suite: **49 tests, 0 failures, 0 errors, 0 skipped**.
`target/surefire-reports/` contains JUnit results for ApiTest (24),
ConcurrencyTest (9), ProcessingFailureTest (7), InterruptedBatchTest (7), and
HttpDispatchTest (2). Tests use actual localhost HTTP,
a fresh temporary SQLite database
per case and literal expected results. No production validator calculates the
expected results. Focused regression runs first reproduced decimal rounding,
Timestamp impersonation, illegal protobuf field numbers/reservations and missing
inspection pagination. An exponent-versus-integer fingerprint collision also
received a failing regression before correction; the final suite passed.

The earlier follow-up sections below are historical runs with their original
counts. The latest comprehensive audit is [REVIEW.md](REVIEW.md); it covers the
user's subsequent implementation changes and eight additional defect fixes.

Covered acceptance cases:

| Area | Evidence |
| --- | --- |
| Schema evolution | Optional addition succeeds; deletion, type/number/requiredness changes, duplicate fields and unsupported types fail without changing registry |
| Explicit old versions | v2-only field fails against v1; requested unknown version fails independently |
| Partial and all-invalid batches | Exact counts, positions/reasons, unchanged valid payloads; invalid records absent |
| Scalar rules/presence | int64 bounds, bool/numeric distinction, optional omission/null, timestamps, nested values; stored binary protobuf decoded independently |
| Admission | Malformed/duplicate-key JSON, invalid UTF-8, invalid envelopes, empty/duplicate/oversized batches leave no reserved batch |
| Safe retry | Eight concurrent identical HTTP requests yield one set of rows; conflicts fail; completed reports stay identical |
| Tenant/dataset scoping | Different customer schemas share `orders`; schemas and records remain independent; cross-batch duplicate IDs intentionally permitted |
| Storage failures | Two transient errors recover on third attempt, exhaustion fails on third, permanent failure tries once, other records continue |
| Isolation/bounds | Delayed A is still processing when B completes; global/customer/byte quotas reject new work but allow known retries |
| Persistence | New server/store instances retain schemas/rows/final reports; close drains work; forced JVM kills leave interrupted batches that resume from identical client input |
| Restart/migration | Legacy body column removed without losing data; large invalid payload not retained; queued/partial retries resume once, skip committed positions and preserve completed reports; repeated startup, quota rejection and failed readmission keep interrupted state retryable |
| Descriptor boundaries | Invalid/reserved wire tags and reserved declarations rejected; canonical Timestamp cannot be impersonated |
| Inspection | Schema pagination and byte-budgeted record pagination preserve all large records |
| Double overflow | `1e309`/`-1e309` fail per record; undeclared overflow fields report `unknown_field`; 999 valid records alongside one overflow persist unchanged; retries remain safe |
| Lock isolation | Paused admission copying allows another customer's next turn; paused writer allows warm/cold schema and batch reads; open inspection cursor allows another customer's HTTP admission and durable completion |
| Admission concurrency | Failed reservations release quotas; in-flight reservations count against quotas and shutdown drain; simultaneous conflicting schema registrations preserve one immutable version |
| Unicode response bounds | Two large emoji records retain exact payloads and keep actual HTTP response bytes within 8 MiB |
| Lookup retries/error stages | Cold lookups retry BUSY/LOCKED and extended codes; exhaustion/permanent failures retain correct counts; unknown schemas and internal bugs are not retried; unexpected validation/lookup bugs are logged as internal |

## Follow-up: remove global monitor coupling

Three deterministic regressions first failed against the old monitors: another
customer could not advance while admission copying was paused; cache hits and
cold metadata reads waited behind a paused JDBC write; an open inspection cursor
prevented another customer's HTTP batch admission. The revised implementation
passes all three while each gate remains closed. Test-only JDBC driver decorators
delegate operations to real SQLite; the inspection test holds a real WAL read
snapshot open while the other customer's report becomes durably completed.

Three additional checks cover failed-reservation quota rollback and safe retry,
shutdown waiting for in-flight admissions, and simultaneous identical/conflicting
schema registrations. Independent review found no further concurrency defects,
but identified a Unicode response-budget mismatch: two valid emoji records
produced 10,400,366 HTTP bytes because supplementary characters were escaped.
A failing wire-size regression preceded enabling UTF-8 surrogate combination;
response encoding now matches stored-byte estimates without adding JSON work
inside database connections. `./mvnw -B -q package` passed all 25 tests and rebuilt the
jar; the packaged demo also passed. See ConcurrencyTest for the gates and deadlines.
This proves the lock separation and WAL overlap, not production throughput or
isolation from physical disk/CPU contention.

## Follow-up: valid JSON double overflow

The user reported that the global `Json.checkFinite()` incorrectly rejected
valid JSON numbers outside Java double's range before admission. Two new public
HTTP regressions reproduced 400 instead of 202 before the fix. Removing that
global conversion lets BigDecimal parsing preserve the number until the selected
schema rejects only the affected record. A third regression confirms `NaN`,
`Infinity` and `-Infinity` still produce 400 `invalid_json` without reserving work.
The mixed-version case checks both valid payloads, failure categories/positions,
and retry/conflict behavior; the full-sized case verifies 999 unique unchanged
records and one validation failure. Focused tests passed 3/3; `./mvnw -B -q package`
then passed all 18 tests and rebuilt the executable jar.

## Follow-up: schema lookup retries and error classification

Five focused tests cover cold-cache lookup recovery for SQLite BUSY/LOCKED and
extended codes, exhausted/permanent SQL failures, unknown versions, unexpected
validation bugs and unexpected lookup bugs. Four tests failed before the fix;
the unknown-schema no-retry control already passed. After splitting processing
stages, all five focused tests passed. Real SQLite queries are decorated only in
tests, and unexpected validation exceptions are injected through a test-only JSON
node. No production fault-control API was added.

Recovery tests exhaust two lookup failures and two independent write failures
before succeeding, then check exact rows/payloads and safe batch retry. Exhaustion
tests verify stage-specific storage codes and attempt counts while later records
continue. Runtime bugs are counted once as `internal`, never retried or written,
and retain diagnostic stack traces; unknown schemas remain ordinary validation
failures with one lookup.

Final `./mvnw -B -q package` passed all 30 tests and rebuilt `target/ingestion.jar`.

## Follow-up: interrupted batches and unused request bodies

Four regressions first reproduced the unused body column and queued/processing
reports surviving restart without an in-memory job. Two cases kill a real child
JVM forcibly: after durable reservation but before enqueue, and after one record
commits while the next record is paused. After the fix, startup reports both as
interrupted/retryable; equivalent submissions resume and complete with three
unique records. The committed row remains byte-for-byte unchanged and its write
interceptor is never called on resume. Legacy migration preserves schema/record
and immutable completed reports; a large invalid payload leaves only metadata.
Eight concurrent retries create one processing attempt. A fifth regression
checks unchanged interrupted reports over repeated startup, quota rejection and
an injected failed resume UPDATE, then successful retry with restored capacity.
Independent review reproduced an error-after-commit admission edge case: releasing
capacity after a durable queued UPDATE left no in-memory job. A sixth regression
first failed, then passed after exact durable-report reconciliation under the
identity stripe, covering both fresh reserve and resume. All six focused tests
pass. Final `./mvnw -B -q package` passed all 36 tests and rebuilt the jar;
the packaged demo also passed. Automatic replay without client input is outside
scope; nonfinal failures may be re-evaluated during resumption.

## Comprehensive follow-up audit

The 39-test baseline passed before audit changes. Eight newly reproduced bugs
covered orphaned admissions, extreme decimal scale overflow, unmatched-surrogate
corruption/hash collisions, presence overrides, legal reserved ranges, HTTP
slow-header overload blocking, runtime write error classification and symbolic
ownership aliases. Their regressions failed before correction. Added independent
checks for Timestamp nanoseconds and keyset cursor boundaries. The backoff test
now gates the actual retry timer while a healthy tenant advances, rather than
relying on a deadline longer than blocking sleeps.

Final package results: 49 tests, zero failures/errors/skips (6.13 seconds total
reported JUnit case time). The rebuilt executable jar help and packaged demo passed. The clean build also
removed stale target artifacts. Source formatting and `sh -n mvnw` passed.
Expected fault-injection diagnostics on stderr are not test failures. The
independent amended reviews found no additional actionable findings.

## Measured demonstration

One observed run of the packaged demo:

```text
Mixed batch: total=7 successful=2 failed=5; retry returned identical report.
Storage faults: recover succeeded on attempt 3; exhaust failed after 3; after succeeded.
Isolation: A=100 records, 25 ms delay/write; B=1 record, 0 ms delay/write.
A acknowledgment=7.4 ms; B completed in 3.5 ms after B submission; A status then=processing.
A completed in 2906.8 ms after A submission. All assertions passed.
```

The demo waits for A's first write attempt before submitting B. It asserts B's
completed report is observable while A's status is still `processing`, then waits
for A and checks exact storage counts (100/1). Timings include HTTP/polling and
SQLite overhead and vary by machine. Full final output is in
[DEMO_OUTPUT.txt](DEMO_OUTPUT.txt). These are local correctness/isolation
measurements, not production throughput benchmarks.

An additional smoke test started the executable jar on an ephemeral port with
`target/cli-smoke.sqlite3`, submitted the README fixtures, saved the final report,
stopped with Ctrl-C, and started a fresh JVM on the same file. Report JSON matched
exactly; both schema versions and two original valid payloads were unchanged.
Both smoke-test processes were stopped.

Approximate total time spent: **5–6 hours**, as estimated by the author, including
implementation, AI-assisted review, manual changes, fixes, testing and documentation.
Remaining work is automatic production crash replay, bounded/cancellable
downstream I/O, snapshot-consistent inspection, deployment authentication and load
testing, as described in DECISIONS.md.
