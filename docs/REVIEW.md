# Comprehensive review — November 6, 2026

Reviewed all production Java sources, build configuration/bootstrap, fixtures,
tests, README and decisions after the user's quota, retry, HTTP, descriptor and
pagination changes. Two independent read-only reviews covered scheduling and
schema/storage. Findings were reproduced before fixes; amended reviews found no
additional actionable defects in those changes.

## Defects corrected

| Finding | Correction and regression |
| --- | --- |
| A committed admission plus failed reconciliation SELECT left a queued row with no job | Identical retries check for an actual queued job and safely readmit orphaned work. Rereading durable status protects concurrent completion. `InterruptedBatchTest.retryReadmitsAnOrphanedAdmissionWhenCommitAndReconciliationBothFail` |
| Valid extreme decimals overflowed fingerprint normalization, rejecting the batch | Normalize with a long exponent when trailing-zero removal exceeds BigDecimal scale bounds; existing ordinary hashes stay unchanged. `ApiTest.extremeDecimalExponentsFailOnlyTheirRecordAndRetrySafely` |
| Unmatched Unicode surrogates collided with `?` in hashes and were silently changed during successful storage | Hash malformed UTF-16 losslessly and reject affected string records as `invalid_unicode`; valid pairs survive. `ApiTest.unmatchedSurrogatesFailIndependentlyAndCannotImpersonateOtherContent` |
| Editions feature options overrode proto2 field presence despite root syntax checks | Reject file/message/field feature overrides and proto3 presence flags; recursively reject unknown descriptor wire fields, including options. `ApiTest.editionsFeaturesAndUnknownOptionFieldsCannotOverrideProto2Rules` |
| Legal reserved ranges spanning tags 19000–19999 were rejected | Restrict actual field numbers, while accepting valid reserved ranges. `ApiTest.reservedRangesMaySpanImplementationReservedNumbers` |
| Caller-runs overload handling parsed incomplete HTTP headers on the dispatcher | Dispatch overload responses through a separate bounded pool; normal dispatch keeps progressing. `HttpDispatchTest.slowOverflowHeadersDoNotBlockTheHttpDispatcher` |
| Unchecked write exceptions were still classified as storage failures | Classify runtime write bugs as `internal/storage_write_error`, without retry; later records continue. `ProcessingFailureTest.unexpectedRuntimeWriteBugIsInternalAndLaterRecordsContinue` |
| A symbolic database alias bypassed the sidecar ownership lock | Canonicalize the database and parent path before opening SQLite or its lock; reject dangling database symlinks. `HttpDispatchTest.symbolicDatabaseAliasesCannotBypassTheOwnershipLock` |

## Coverage and cleanup

- Strengthened the worker-release regression by holding the actual retry timer
  paused while another tenant advances; the old two-second deadline alone could
  pass with sleeping workers.
- Added explicit tests for one/nine-digit Timestamp fractions, exact encoded
  nanoseconds, excess precision, tenant-scoped cursor continuation, empty scans,
  invalid cursors and offset/cursor conflicts.
- Removed unused `Json.stringify` and the test-only four-argument `Store.records`
  forwarding overload; tests use the same pagination entry point as HTTP.
- Removed the redundant 9,278,065-byte Maven bootstrap archive and made the
  bootstrap delete its archive after successful verified extraction.
- A clean build removed stale generated classes/demo artifacts. `.m2/`, the Maven
  runtime, formatter and optional fixture compiler are intentional ignored local
  build/development tools. Source PDFs, fixture proto sources/compiled descriptors,
  demo/fault utilities and historical review/session documents remain useful and
  were retained. No stray Python implementation, shell helper scripts or backup
  files remain outside ignored build/tool directories.
- Updated README/DECISIONS for shutdown behavior, overload bounds, cursor usage,
  retry work, Unicode, descriptor options, orphan recovery and canonical ownership.
  Updated provenance to acknowledge the user's implementation changes.

## Limits that remain explicit

SQLite still has one writer. Four independently slow storage calls can occupy all
workers; timers release backoff waits, not blocked I/O. Permanent metadata errors
have bounded closing retries, but indefinitely blocked calls can delay shutdown.
Two slow overload clients can delay overload responses until request timeout;
normal dispatch stays independent. Connection limits can reject connections before
an application JSON response is possible. Cursor inspection is live across pages.

Automatic crash replay, cross-batch deduplication and distributed ownership remain
outside scope. Hard-link aliases are outside SQLite's supported file-identity
model; use a single canonical path. Uncertain record writes still need production
outcome reconciliation: uniqueness prevents duplicate rows, but an error after
commit can leave a stored row counted as failed. Previously corrupted Unicode
rows cannot be reconstructed from the retained metadata.

Fresh command results and test counts are in [VERIFICATION.md](VERIFICATION.md).
