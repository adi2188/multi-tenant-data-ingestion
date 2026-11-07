# Interrupted batch retries without retained request bodies

The user found unused persisted bodies and batch identities wedged after abnormal
exit. Implement client-assisted recovery, with no background request replay.

- [x] Reproduce dead body storage, legacy migration and interrupted queued/partial
  batches through HTTP. Add real child-JVM kill tests for durable admission before
  enqueue and for a partially committed processing batch.
- [x] Remove `batches.body` from new databases and migrate old databases by dropping
  that unused column. Keep fingerprints, statuses, report JSON, schemas and records.
  A first migration may rewrite pages; VACUUM remains an optional offline operation.
- [x] Before HTTP startup, mark unfinished queued/processing batches `interrupted`
  with an explicit reason and retry instructions. Leave completed and already
  interrupted reports unchanged. Do not invent final counts for unfinished work.
- [x] Identical interrupted retries reserve capacity and atomically transition to
  queued, then enqueue once under existing identity stripes. Conflicts stay 409;
  active duplicate submissions remain idempotent. Admission failures roll back quotas.
- [x] Load only already-stored positions on resume; count/skip them without calling
  validation or storage again. Reprocess remaining positions from the retry body.
  Unknown/failed/unprocessed positions may be retried in a new processing attempt;
  only completed reports are immutable across attempts.
- [x] Run focused/full tests, independent review and package verification. Document
  interrupted status, partial-record inspection, retry semantics and migration.

No full accepted-body disk retention or automatic crash replay is needed. A client
must provide an equivalent body to resume. SQLite/WAL and the process ownership
lock remain the durability/concurrency boundary. No database schema/body format
is inferred from a Java object's in-memory state after restart.

Verified with six focused regressions and the full `./mvnw -B -q package` run:
36 tests, no failures/errors/skips. Independent review found an uncertain
admission-after-commit edge case; a failing regression preceded reconciliation
for both reserve and resume. Amended review found no additional issues.
