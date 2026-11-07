# Design decisions

## Architecture and schemas

Java 21, the JDK HTTP server, Jackson, protobuf-java and SQLite keep the prototype
local and reproducible without hosted services. Maven produces one executable
jar. Protocol Buffers implements the requested versioned schemas; proto2 labels
supply required/optional presence without a separate metadata language. Successful
records retain both parsed JSON and encoded protobuf.

Schemas are immutable and keyed by customer/dataset/version. Registration compares
against the latest version: existing names, tags, types and requiredness remain
fixed; additions must be optional and reservations must remain protected.
Malformed descriptors, unsupported structures, feature overrides and redefined
Timestamp types are rejected. Identical registrations are safe; compiler source
locations are ignored when comparing descriptors. A 256-entry LRU caches compiled
descriptors. Cache hits hold only a short cache lock; cold compilation follows a
pooled database read. Missing versions fail individual records, without fallback.

## Validation, identity and durability

Every record selects its schema version. Payloads are flat and reject unknown
fields, explicit null, missing required fields and coercion. Integers require
integer tokens within int64; doubles require finite numeric values. BigDecimal
preserves JSON decimal precision; protobuf doubles use IEEE-754 rounding. Numeric
overflow fails its record. Timestamps require valid dates, seconds, timezone and
at most nine fractional digits. Unmatched Unicode surrogates fail string records
rather than being silently replaced. README documents the complete contract.

Admission rejects malformed JSON, invalid batch structure, empty batches,
duplicate valid record IDs and submissions exceeding 2 MiB or 1,000 records.
A typed, length-delimited SHA-256 traversal binds each customer/dataset/batch ID to
content. Object order and whitespace are ignored; array order and integer versus
decimal token kinds matter. Equivalent decimal spellings match. Changed content
returns 409; active/completed retries return existing results without copying
records again. Parsing, hashing and byte accounting still inspect retry bodies.

Identity lock stripes serialize duplicate admissions. Quotas are reserved before
persistence, queued metadata commits before enqueue/acknowledgment, and failed
admissions release capacity. Uncertain admission commits are reconciled; identical
retries can safely readmit durable identities with no live job. JSON preparation
and database operations never hold the scheduler monitor.

SQLite uses WAL and FULL synchronous commits. One fair writer lock protects its
connection; four metadata readers and two inspection readers use independent
connections. Records are unique by customer/dataset/batch/position. Reports
complete only after every outcome exists and successes are stored; completed
reports remain immutable. Full batch bodies are not retained. A canonical-path
ownership lock prevents concurrent service owners, including symbolic aliases.

## Failures and customer isolation

Schema lookup and record writes independently allow three attempts with 10/20 ms
backoff for transient BUSY/LOCKED failures. Timers release workers during backoff;
validated protobuf bytes survive write retries. Validation failures are never
retried. Exhaustion/permanent storage errors fail only the affected record;
unexpected bugs are logged and reported separately as internal failures.

Four workers dispatch one record per turn, round robin across ready customers,
with one active record and FIFO batches per customer. Defaults bound pending
batches to 100 globally/10 per customer and normalized input bytes to 32 MiB
globally/8 MiB per customer. In-flight admissions count toward quotas and drain.
HTTP has 16 workers, a 64-task queue, connection/time limits and a separate bounded
overload pool returning retryable 503 responses without blocking the dispatcher.
Inspection releases bounded read pages before JSON/Base64 conversion; indexed
record cursors avoid offset scans. These bounds include work and input sizes,
not exact heap usage or hard latency guarantees. SQLite still serializes writes;
four blocked storage calls can occupy all workers. Slow overload clients can
delay overload responses; inspection is live across pages.

## Recovery and production evolution

Normal shutdown drains work. Repeated metadata errors have three consecutive
closing attempts before releasing unfinished jobs; indefinitely blocked I/O can
still delay shutdown. Startup marks unfinished batches interrupted/retryable.
Identical client resubmission supplies the body, skips committed positions and
reprocesses remaining records. Completed reports survive unchanged. Automatic
crash replay and durable per-position failure history are not implemented.

An error after a record commit remains uncertain: uniqueness prevents duplicates,
but a stored row could be counted failed. Production needs atomic outcomes with
writes or an outbox and sink idempotency, plus reconciliation. For 50k records/sec,
replace in-memory admission with a durable log, partition tenant/dataset ownership,
cache immutable schemas and batch writes. Horizontal scaling needs fenced leases,
checkpointed outcomes, fair tenant budgets, cancellable I/O and replay. At 10x,
measure hot partitions, sink capacity, lag and reporting overhead. No throughput
claim is made. Authentication, brokers and cross-batch deduplication remain outside
prototype scope.

## Verification, AI use and time

All 49 tests pass using real HTTP/SQLite and handwritten expectations. Tests check
exactly-once rows, unchanged payloads, protobuf decoding, failures, retries,
isolation, concurrency and restarts. The reproducible demo delays A's 100 writes
by 25 ms each and shows B's single record completing while A processes.
[Verification](docs/VERIFICATION.md) and [audit](docs/REVIEW.md) retain detailed evidence.

AI proposed SQLite and per-record scheduling; I chose Java/protobuf and adapted
presence semantics. I added quota, retry, HTTP and schema improvements, then used
AI reviews and failing regressions to check them. A broker/cloud stack and a
separate validation checker were unnecessary. [AI_SESSION.md](AI_SESSION.md) links
the interaction history and distinguishes contributions. Approximately **5–6 hours**
were spent on implementation, review, fixes, testing and documentation. More time
would go toward automatic replay, storage deadlines and load/failure testing.
