# Multi-tenant ingestion implementation plan

> Execute inline using superpowers:executing-plans, with public-API tests first.

**Goal:** A reproducible local ingestion service satisfying the attached take-home.

**Architecture:** Strict HTTP admission persists immutable protobuf descriptors and
batch identities in SQLite. A bounded fair scheduler processes one record per
customer at a time. Durable storage precedes immutable report completion.

**Tech stack:** Java 21, Maven, Protocol Buffers, Jackson, SQLite JDBC, JUnit.

**Spec:** `docs/superpowers/specs/2026-11-06-ingestion-design.md`.

## Global constraints

No external service, UI, or authentication. Runtime dependencies are Maven-pinned.
Use trusted customer identifiers. Bound requests, batches, backlog and workers.
Version selection is explicit; schema and batch content are immutable.

## Review focus

1. JSON booleans must fail both numeric types.
2. Concurrent duplicate requests: reserve identity before enqueueing exactly once.
3. Slow writes must hold neither scheduler nor SQLite locks.
4. Reports must count malformed records without valid identifiers by position.
5. Shutdown must drain accepted work before database closure.

## Tasks

- [x] Write public HTTP integration tests in `src/test/java/io/atlan/ingestion/ApiTest.java`, supported by a
  temporary server harness and test-only injectable `FaultWriter`. Verify
  tests fail before product implementation. Hand-author expected records/counts.
- [x] Implement `ProtobufSchema.java` and `Json.java`: strict JSON, protobuf descriptors,
  additive evolution, explicit record-version validation and DynamicMessage encoding.
- [x] Implement `Store.java`: SQLite registry, serialized reservation,
  idempotent record writes, report commits and paginated inspection.
- [x] Implement `IngestionService.java`: admission quotas, per-customer FIFO,
  fair record scheduling, bounded retry and graceful drain.
- [x] Implement `IngestionServer.java` and `Main.java`: bounded HTTP
  pool, body limits, strict errors, documented routes and CLI.
- [x] Run `./mvnw test`: initial 9 tests pass, zero failures/errors.
- [x] Add JSON/protobuf fixtures, a Java demo, README, DECISIONS and honest
  AI provenance. Run complete tests and demo, inspect source, then deliver.
- [x] Independent source review and corrections: descriptor constraints, UTF-8,
  decimal precision, bounded inspection and typed numeric fingerprints.
- [x] Final package: 15/15 tests; demo passes; actual JVM stop/restart preserves
  identical final report, original payloads and both schema versions.

There is no Git repository in the supplied workspace; leave concrete files for
review without creating commits or an unrelated repository.
