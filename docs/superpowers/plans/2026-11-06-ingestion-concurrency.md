# Remove ingestion head-of-line blocking

The user identified global monitors coupling HTTP admission, scheduler turns,
schema-cache hits, reads and durable writes. Keep the public API and correctness
contracts while separating those resources.

- [x] Add deterministic regressions before implementation. Gate admission's JSON
  copy, a real JDBC record write and a real inspection cursor with latches. Verify
  unrelated scheduler turns, schema hits/cold metadata reads and HTTP ingestion
  progress respectively while the gated operation remains paused. JDBC probes
  delegate to real SQLite and live only in tests.
- [x] Prepare/copy/hash input outside scheduler locking. Use bounded admission
  lock stripes to serialize identical batch identities. Reserve quotas under the
  scheduler lock before database admission; enqueue only after persistence; roll
  back quotas on failure. Count in-flight admissions during drain.
- [x] Replace Store's global monitor with one fair writer lock, four bounded
  metadata read connections, two bounded inspection read connections, and a small
  cache-only LRU monitor. Compile descriptors outside database/cache locks.
  Serialize schema evolution with bounded dataset lock stripes.
- [x] Fetch bounded raw inspection pages, close cursors and return connections,
  then parse JSON and Base64-encode blobs. Keep conservative 8 MiB output budgets
  and next_offset semantics. No schema migration or database format change.
- [x] Verify failed admissions release capacity and concurrent duplicate/schema
  registrations remain safe. Run the full suite, independent review, package and
  demo. Update architecture/limits/verification notes and visible transcript.

Verification: original three blocking regressions RED→GREEN; seven new concurrency/
inspection tests pass; full package 25/25; independent review completed. Its
Unicode response-byte finding received a failing regression and UTF-8 encoding fix.

SQLite remains a single writer and CPU/disk remain shared. This change removes
the avoidable global Java lock and reader contention; it does not claim hard
latency isolation or production throughput.
