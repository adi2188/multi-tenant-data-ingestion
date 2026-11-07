package io.atlan.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/** Customer round robin, one record per turn, no unbounded executor queue. */
final class IngestionService implements AutoCloseable {
  static final int WORKERS = 4;
  private static final int STORAGE_ATTEMPTS = 3;
  final Store store;
  private final WriteInterceptor interceptor;
  private final Limits limits;
  private final ExecutorService workers = Executors.newFixedThreadPool(WORKERS);
  private final ScheduledExecutorService retryTimer =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            var thread = new Thread(runnable, "customer-retry-timer");
            thread.setDaemon(true);
            return thread;
          });
  private final Map<String, Deque<Job>> queues = new HashMap<>();
  private final Deque<String> ready = new ArrayDeque<>();
  private final Set<String> active = new HashSet<>();
  private final Set<String> delayed = new HashSet<>();
  private final Map<String, Integer> pendingByCustomer = new HashMap<>();
  private final Map<String, Long> pendingBytesByCustomer = new HashMap<>();
  private final Object[] admissionLocks = new Object[64];
  private final Thread dispatcher;
  private boolean closing;
  private int pending;
  private long pendingBytes;

  IngestionService(Store store, WriteInterceptor interceptor, Limits limits) throws SQLException {
    this.store = store;
    this.interceptor = interceptor == null ? (c, r, a) -> {} : interceptor;
    this.limits = limits;
    Arrays.setAll(admissionLocks, ignored -> new Object());
    store.interruptUnfinishedBatches();
    dispatcher = new Thread(this::dispatch, "customer-scheduler");
    dispatcher.start();
  }

  record Submission(int status, JsonNode report) {}

  Submission submit(JsonNode body) throws Exception {
    Json.keys(body, Set.of("customer_id", "dataset_id", "batch_id", "records"), 400);
    String customer = Json.id(body, "customer_id"),
        dataset = Json.id(body, "dataset_id"),
        batch = Json.id(body, "batch_id");
    var records = body.get("records");
    if (!records.isArray() || records.isEmpty())
      throw new ApiException(400, "invalid_batch", "records must be a nonempty array");
    if (records.size() > 1000)
      throw new ApiException(413, "batch_too_large", "Maximum 1000 records per batch");
    var ids = new HashSet<String>();
    for (var record : records)
      if (record.isObject()
          && Json.validId(record.get("record_id"))
          && !ids.add(record.get("record_id").textValue()))
        throw new ApiException(400, "duplicate_record_id", "Duplicate record identifier in batch");
    int bytes = body.toString().getBytes(StandardCharsets.UTF_8).length;
    String hash = Json.fingerprint(body);
    var report =
        Json.object()
            .put("customer_id", customer)
            .put("dataset_id", dataset)
            .put("batch_id", batch)
            .put("status", "queued")
            .put("total", records.size())
            .put("submitted_at", Instant.now().toString());
    ObjectNode response;
    String key = customer + "/" + dataset + "/" + batch;
    // Identity serialization is independent of scheduling. Bounded stripes avoid
    // an ever-growing lock map; hash/copy/DB work never holds the scheduler monitor.
    synchronized (admissionLocks[Math.floorMod(key.hashCode(), admissionLocks.length)]) {
      var existing = store.batch(customer, dataset, batch);
      if (existing != null) {
        if (!existing.fingerprint().equals(hash))
          throw new ApiException(
              409, "batch_conflict", "Batch identifier is already bound to different content");
        String status = existing.report().path("status").asText();
        if (status.equals("completed")) return new Submission(200, existing.report());
        if (!status.equals("interrupted")) {
          synchronized (this) {
            var queue = queues.get(customer);
            if (queue != null
                && queue.stream().anyMatch(j -> j.dataset.equals(dataset) && j.batch.equals(batch)))
              return new Submission(202, existing.report());
          }
          // A commit followed by a failed reconciliation read can leave a durable
          // admission without a job. The identity stripe excludes another submit;
          // reread after checking the queue in case a worker just completed it.
          existing = store.batch(customer, dataset, batch);
          if (existing.report().path("status").asText().equals("completed"))
            return new Submission(200, existing.report());
        }
        report.put("submitted_at", existing.report().path("submitted_at").asText());
        report.put("resumed_at", Instant.now().toString());
      }
      var preparedRecords = ((ArrayNode) records).deepCopy();
      Set<Integer> storedPositions =
          existing == null ? Set.of() : store.storedPositions(customer, dataset, batch);
      var job = new Job(customer, dataset, batch, preparedRecords, bytes, report, storedPositions);
      response = report.deepCopy();
      synchronized (this) {
        if (closing)
          throw new ApiException(503, "shutting_down", "Service is draining accepted batches");
        if (pending >= limits.maxPending()
            || pendingByCustomer.getOrDefault(customer, 0) >= limits.maxPendingPerCustomer()
            || pendingBytes + bytes > limits.maxPendingBytes()
            || pendingBytesByCustomer.getOrDefault(customer, 0L) + bytes
                > limits.maxPendingBytesPerCustomer())
          throw new ApiException(429, "backlog_full", "Pending work limit reached; retry later");
        // Includes admissions still waiting for durability: concurrent requests
        // cannot oversubscribe quotas and graceful drain cannot exit too early.
        pending++;
        pendingBytes += bytes;
        pendingByCustomer.merge(customer, 1, Integer::sum);
        pendingBytesByCustomer.merge(customer, (long) bytes, Long::sum);
      }
      try {
        if (existing == null) store.reserve(customer, dataset, batch, hash, report);
        else store.resume(customer, dataset, batch, hash, report);
      } catch (Exception ex) {
        // JDBC may report an error after the commit. Under this identity stripe,
        // only this attempt can have installed the exact queued report. Reconcile
        // before releasing capacity, or a durable admission could have no job.
        boolean committed = false;
        try {
          var persisted = store.batch(customer, dataset, batch);
          committed =
              persisted != null
                  && persisted.fingerprint().equals(hash)
                  && persisted.report().equals(report);
        } catch (Exception lookupFailure) {
          ex.addSuppressed(lookupFailure);
        }
        if (!committed) {
          synchronized (this) {
            releaseCapacity(job);
            notifyAll();
          }
          throw ex;
        }
      }
      synchronized (this) {
        var queue = queues.get(customer);
        if (queue == null) {
          queue = new ArrayDeque<>();
          queues.put(customer, queue);
          ready.addLast(customer);
        }
        queue.addLast(job);
        notifyAll();
      }
    }
    return new Submission(202, response);
  }

  // Called only with the scheduler monitor held.
  private void releaseCapacity(Job job) {
    pending--;
    pendingBytes -= job.bytes;
    pendingByCustomer.compute(job.customer, (key, count) -> count == 1 ? null : count - 1);
    pendingBytesByCustomer.compute(
        job.customer,
        (key, total) -> total == null || total <= job.bytes ? null : total - job.bytes);
  }

  private void dispatch() {
    while (true) {
      synchronized (this) {
        while (ready.isEmpty() || active.size() >= WORKERS) {
          if (closing && pending == 0) return;
          try {
            wait();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        String customer = ready.removeFirst();
        Job job = queues.get(customer).getFirst();
        active.add(customer);
        workers.execute(() -> runTurn(job));
      }
    }
  }

  private void runTurn(Job job) {
    boolean done = false;
    long retryDelayMs = 0;
    try {
      if (job.position == 0 && !job.started) {
        job.report.put("status", "processing").put("started_at", Instant.now().toString());
        store.status(job.customer, job.dataset, job.batch, job.report);
        job.started = true;
        job.metadataFailures = 0;
      }
      if (job.position < job.records.size()) {
        if (job.storedPositions.contains(job.position)) {
          job.successful++;
          job.advancePosition();
        } else {
          var step = process(job, job.records.get(job.position));
          if (step.completedRecord()) {
            if (step.failure() == null) job.successful++;
            else job.failures.add(step.failure());
            job.advancePosition();
          } else {
            retryDelayMs = step.retryDelayMs();
          }
        }
      }
      if (job.position == job.records.size()) {
        job.report
            .put("status", "completed")
            .put("successful", job.successful)
            .put("failed", job.failures.size());
        if (!job.report.has("completed_at"))
          job.report.put("completed_at", Instant.now().toString());
        job.report.set("failures", job.failures);
        store.status(job.customer, job.dataset, job.batch, job.report);
        done = true;
      }
    } catch (Throwable ex) {
      // Metadata cannot be reported durable while unavailable. Release the worker and
      // schedule a timer retry; during shutdown, bound retries so close() cannot hang forever.
      job.metadataFailures++;
      synchronized (this) {
        if (closing && job.metadataFailures >= STORAGE_ATTEMPTS) {
          System.err.println(
              "Aborting unfinalizable batch during shutdown "
                  + job.customer
                  + "/"
                  + job.batch
                  + ": "
                  + ex.getClass().getSimpleName());
          done = true;
        }
      }
      if (!done) {
        System.err.println(
            "Metadata unavailable; will retry batch "
                + job.customer
                + "/"
                + job.batch
                + ": "
                + ex.getClass().getSimpleName());
        retryDelayMs = 100L;
      }
    } finally {
      synchronized (this) {
        active.remove(job.customer);
        var queue = queues.get(job.customer);
        if (done) {
          queue.removeFirst();
          releaseCapacity(job);
        }
        if (queue.isEmpty()) {
          queues.remove(job.customer);
        } else if (!done && retryDelayMs > 0) {
          scheduleRetry(job.customer, retryDelayMs);
        } else {
          ready.addLast(job.customer);
        }
        notifyAll();
      }
    }
  }

  // Called only with the scheduler monitor held.
  private void scheduleRetry(String customer, long delayMs) {
    delayed.add(customer);
    try {
      retryTimer.schedule(
          () -> {
            synchronized (IngestionService.this) {
              if (delayed.remove(customer) && queues.containsKey(customer)) {
                ready.addLast(customer);
                IngestionService.this.notifyAll();
              }
            }
          },
          delayMs,
          TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException ex) {
      delayed.remove(customer);
      ready.addLast(customer);
    }
  }

  private record Step(boolean completedRecord, ObjectNode failure, long retryDelayMs) {}

  private Step process(Job job, JsonNode record) {
    if (job.encodedRecord == null) {
      int version;
      try {
        Json.keys(record, Set.of("record_id", "schema_version", "payload"), 422);
        if (!Json.validId(record.get("record_id")))
          throw new ApiException(422, "invalid_record_id", "record_id must be a valid identifier");
        version = Json.positiveInt(record.get("schema_version"), 422, "schema_version");
      } catch (ApiException ex) {
        return new Step(true, failure(job, record, "validation", ex.code, ex.getMessage(), 0), 0);
      } catch (Throwable ex) {
        return new Step(
            true,
            internalFailure(job, record, "validation_error", "record validation/encoding", ex),
            0);
      }

      ProtobufSchema schema;
      int lookupAttempt = ++job.lookupAttempts;
      try {
        schema = store.schema(job.customer, job.dataset, version);
      } catch (SQLException ex) {
        boolean transientFailure = isTransientStorageFailure(ex);
        if (!transientFailure || lookupAttempt >= STORAGE_ATTEMPTS)
          return new Step(
              true,
              failure(
                  job,
                  record,
                  "storage",
                  transientFailure ? "schema_lookup_retries_exhausted" : "schema_lookup_failed",
                  transientFailure
                      ? "Transient schema lookup failed after bounded retries"
                      : "Permanent schema lookup failure",
                  lookupAttempt),
              0);
        return new Step(false, null, 10L << (lookupAttempt - 1));
      } catch (Throwable ex) {
        return new Step(
            true, internalFailure(job, record, "schema_lookup_error", "schema lookup", ex), 0);
      }
      if (schema == null)
        return new Step(
            true,
            failure(
                job,
                record,
                "validation",
                "unknown_schema",
                "Unknown dataset or schema version " + version,
                0),
            0);

      try {
        job.encodedRecord = schema.validateAndEncode(record.get("payload"));
      } catch (ApiException ex) {
        return new Step(true, failure(job, record, "validation", ex.code, ex.getMessage(), 0), 0);
      } catch (Throwable ex) {
        return new Step(
            true,
            internalFailure(job, record, "validation_error", "record validation/encoding", ex),
            0);
      }
    }

    int writeAttempt = ++job.writeAttempts;
    try {
      interceptor.beforeWrite(job.customer, record.get("record_id").textValue(), writeAttempt);
      store.write(job.customer, job.dataset, job.batch, job.position, record, job.encodedRecord);
      return new Step(true, null, 0);
    } catch (RuntimeException ex) {
      return new Step(
          true, internalFailure(job, record, "storage_write_error", "record write", ex), 0);
    } catch (Exception ex) {
      boolean transientFailure = isTransientStorageFailure(ex);
      if (!transientFailure || writeAttempt >= STORAGE_ATTEMPTS)
        return new Step(
            true,
            failure(
                job,
                record,
                "storage",
                transientFailure ? "storage_retries_exhausted" : "storage_write_failed",
                transientFailure
                    ? "Transient write failed after bounded retries"
                    : "Permanent storage write failure",
                writeAttempt),
            0);
      return new Step(false, null, 10L << (writeAttempt - 1));
    } catch (Throwable ex) {
      return new Step(
          true, internalFailure(job, record, "storage_write_error", "record write", ex), 0);
    }
  }

  private static boolean isTransientStorageFailure(Exception ex) {
    if (ex instanceof TransientStorageException) return true;
    if (ex instanceof SQLException sql) {
      // SQLite extended result codes retain BUSY/LOCKED in the low byte.
      int primaryCode = sql.getErrorCode() & 0xff;
      return primaryCode == 5 || primaryCode == 6;
    }
    return false;
  }

  private static ObjectNode internalFailure(
      Job job, JsonNode record, String code, String phase, Throwable ex) {
    System.err.printf(
        "Unexpected %s failure in batch %s/%s/%s at position %d%n",
        phase, job.customer, job.dataset, job.batch, job.position);
    ex.printStackTrace(System.err);
    return failure(job, record, "internal", code, "Unexpected " + phase + " failure", 0);
  }

  private static ObjectNode failure(
      Job job, JsonNode record, String category, String code, String reason, int attempts) {
    var failure =
        Json.object()
            .put("position", job.position)
            .put("category", category)
            .put("code", code)
            .put("reason", reason);
    if (record.isObject() && Json.validId(record.get("record_id")))
      failure.put("record_id", record.get("record_id").textValue());
    if (attempts > 0) failure.put("attempts", attempts);
    return failure;
  }

  @Override
  public void close() {
    synchronized (this) {
      closing = true;
      notifyAll();
    }
    boolean interrupted = false;
    while (dispatcher.isAlive())
      try {
        dispatcher.join();
      } catch (InterruptedException ex) {
        interrupted = true;
      }
    retryTimer.shutdownNow();
    workers.shutdown();
    for (; ; )
      try {
        if (workers.awaitTermination(1, TimeUnit.DAYS)) break;
      } catch (InterruptedException ex) {
        interrupted = true;
      }
    if (interrupted) Thread.currentThread().interrupt();
  }

  private static final class Job {
    final String customer, dataset, batch;
    final ArrayNode records, failures = Json.MAPPER.createArrayNode();
    final long bytes;
    final ObjectNode report;
    final Set<Integer> storedPositions;
    int position, successful;
    int lookupAttempts, writeAttempts, metadataFailures;
    byte[] encodedRecord;
    boolean started;

    Job(
        String customer,
        String dataset,
        String batch,
        ArrayNode records,
        long bytes,
        ObjectNode report,
        Set<Integer> storedPositions) {
      this.customer = customer;
      this.dataset = dataset;
      this.batch = batch;
      this.records = records;
      this.bytes = bytes;
      this.report = report;
      this.storedPositions = storedPositions;
    }

    void advancePosition() {
      position++;
      lookupAttempts = 0;
      writeAttempts = 0;
      metadataFailures = 0;
      encodedRecord = null;
    }
  }
}
