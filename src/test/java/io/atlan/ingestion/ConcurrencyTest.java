package io.atlan.ingestion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConcurrencyTest {
  @TempDir Path directory;

  static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("gate timed out");
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new AssertionError(ex);
    }
  }

  @Test
  void supplementaryUnicodeInspectionPagesRespectTheWireByteBudget() throws Exception {
    try (var server = new IngestionServer(directory.resolve("unicode.db"), 0)) {
      server.start();
      var client = new Client(server.port());
      var schema =
          ApiTest.schema(
              "a",
              1,
              ApiTest.f(
                  "s",
                  1,
                  com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                  true));
      assertEquals(201, client.call("POST", "/schemas", schema).statusCode());
      String text = "😀".repeat(300_000);
      for (int i = 0; i < 2; i++) {
        var record =
            ApiTest.JSON.createObjectNode().put("record_id", "r" + i).put("schema_version", 1);
        record.putObject("payload").put("s", text);
        assertEquals(
            202, client.call("POST", "/batches", ApiTest.batch("a", "b" + i, record)).statusCode());
        client.completed("a", "b" + i);
      }
      int offset = 0;
      var ids = new ArrayList<String>();
      do {
        var response = client.call("GET", "/records/a/orders?limit=1000&offset=" + offset, null);
        assertEquals(200, response.statusCode());
        int bytes = response.body().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        assertTrue(bytes <= 8 * 1024 * 1024, "inspection response exceeded 8 MiB: " + bytes);
        var page = ApiTest.JSON.readTree(response.body());
        for (var row : page.path("records")) {
          ids.add(row.path("record_id").textValue());
          assertEquals(text, row.path("payload").path("s").textValue());
        }
        offset = page.path("next_offset").asInt(-1);
      } while (offset >= 0);
      assertEquals(List.of("r0", "r1"), ids);
    }
  }

  @Test
  void failedReservationReturnsQuotaAndRetryStoresExactlyOnce() throws Exception {
    var failFirst = new AtomicBoolean(true);
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void beforeUpdate(String sql) throws SQLException {
                    if (sql.startsWith("INSERT INTO batches")
                        && failFirst.compareAndSet(true, false))
                      throw new SQLException("injected reservation failure");
                  }
                });
        var server =
            new IngestionServer(
                directory.resolve("rollback.db"), 0, null, new Limits(1, 1, 32 * 1024 * 1024))) {
      server.start();
      var client = new Client(server.port());
      client.call("POST", "/schemas", ApiTest.schema("a", 1, ApiTest.AMOUNT));
      var body = ApiTest.batch("a", "retry", ApiTest.rec("row"));
      assertEquals(503, client.call("POST", "/batches", body).statusCode());
      assertEquals(404, client.call("GET", "/batches/a/orders/retry", null).statusCode());
      assertEquals(202, client.call("POST", "/batches", body).statusCode());
      var completed = client.completed("a", "retry");
      assertEquals(1, completed.path("successful").asInt());
      var retry = client.call("POST", "/batches", body);
      assertEquals(200, retry.statusCode());
      assertEquals(completed, ApiTest.JSON.readTree(retry.body()));
      var rows =
          ApiTest.JSON
              .readTree(client.call("GET", "/records/a/orders", null).body())
              .path("records");
      assertEquals(List.of("row"), ApiTest.ids(rows));
      assertEquals(
          202,
          client
              .call("POST", "/batches", ApiTest.batch("a", "next", ApiTest.rec("next")))
              .statusCode());
      assertEquals(1, client.completed("a", "next").path("successful").asInt());
    }
  }

  @Test
  void concurrentSchemaRegistrationKeepsCompatibilityAndImmutabilityAtomic() throws Exception {
    try (var server = new IngestionServer(directory.resolve("schemas.db"), 0);
        var pool = Executors.newFixedThreadPool(6)) {
      server.start();
      var client = new Client(server.port());
      assertEquals(
          201,
          client.call("POST", "/schemas", ApiTest.schema("a", 1, ApiTest.AMOUNT)).statusCode());
      var start = new CountDownLatch(1);
      var replies = new ArrayList<Future<Integer>>();
      for (int i = 0; i < 6; i++) {
        var addition =
            i % 2 == 0
                ? ApiTest.NOTE
                : ApiTest.f(
                    "flag",
                    3,
                    com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL,
                    false);
        var body = ApiTest.schema("a", 2, ApiTest.AMOUNT, addition);
        replies.add(
            pool.submit(
                () -> {
                  await(start);
                  return client.call("POST", "/schemas", body).statusCode();
                }));
      }
      start.countDown();
      var statuses = new ArrayList<Integer>();
      for (var reply : replies) statuses.add(reply.get(5, TimeUnit.SECONDS));
      Collections.sort(statuses);
      assertEquals(List.of(200, 200, 201, 409, 409, 409), statuses);
      var schemas =
          ApiTest.JSON
              .readTree(client.call("GET", "/schemas/a/orders", null).body())
              .path("schemas");
      assertEquals(2, schemas.size());
      assertEquals(1, schemas.get(0).path("version").asInt());
      assertEquals(2, schemas.get(1).path("version").asInt());
    }
  }

  @Test
  void inFlightAdmissionsCountAgainstQuotasAndKeepDrainAlive() throws Exception {
    var reserving = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void beforeUpdate(String sql) {
                    if (sql.startsWith("INSERT INTO batches")) {
                      reserving.countDown();
                      await(release);
                    }
                  }
                });
        var store = new Store(directory.resolve("pending.db"));
        var service = new IngestionService(store, null, new Limits(1, 1, 32 * 1024 * 1024));
        var pool = Executors.newFixedThreadPool(3)) {
      try {
        store.register(ProtobufSchema.parse(ApiTest.schema("a", 1, ApiTest.AMOUNT)));
        var admission =
            pool.submit(() -> service.submit(ApiTest.batch("a", "slow", ApiTest.rec("row"))));
        assertTrue(reserving.await(2, TimeUnit.SECONDS));
        var quota =
            pool.submit(
                () ->
                    assertThrows(
                        ApiException.class,
                        () -> service.submit(ApiTest.batch("b", "slow", ApiTest.rec("other")))));
        assertEquals(429, quota.get(2, TimeUnit.SECONDS).status);
        var drain =
            pool.submit(
                () -> {
                  service.close();
                  return null;
                });
        assertThrows(
            TimeoutException.class,
            () -> drain.get(100, TimeUnit.MILLISECONDS),
            "shutdown must wait for an admission that already reserved capacity");
        release.countDown();
        assertEquals(202, admission.get(2, TimeUnit.SECONDS).status());
        drain.get(2, TimeUnit.SECONDS);
        var report = store.batch("a", "orders", "slow").report();
        assertEquals("completed", report.path("status").asText());
        assertEquals(1, report.path("successful").asInt());
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void admissionPreparationDoesNotStopAnotherCustomersTurns() throws Exception {
    var firstWrite = new CountDownLatch(1);
    var releaseFirst = new CountDownLatch(1);
    var secondWrite = new CountDownLatch(1);
    var preparing = new CountDownLatch(1);
    var releasePreparation = new CountDownLatch(1);
    try (var store = new Store(directory.resolve("copy.db"))) {
      store.register(ProtobufSchema.parse(ApiTest.schema("a", 1, ApiTest.AMOUNT)));
      store.register(ProtobufSchema.parse(ApiTest.schema("b", 1, ApiTest.AMOUNT)));
      try (var service =
              new IngestionService(
                  store,
                  (customer, id, attempt) -> {
                    if (customer.equals("b") && id.equals("first")) {
                      firstWrite.countDown();
                      await(releaseFirst);
                    }
                    if (customer.equals("b") && id.equals("second")) secondWrite.countDown();
                  },
                  Limits.defaults());
          var pool = Executors.newSingleThreadExecutor()) {
        try {
          service.submit(
              ApiTest.batch("b", "running", ApiTest.rec("first"), ApiTest.rec("second")));
          assertTrue(firstWrite.await(2, TimeUnit.SECONDS));
          var payload =
              new ObjectNode(JsonNodeFactory.instance) {
                @Override
                public ObjectNode deepCopy() {
                  preparing.countDown();
                  await(releasePreparation);
                  return super.deepCopy();
                }
              };
          payload.put("amount", 42.5);
          var record = ApiTest.rec("large");
          record.set("payload", payload);
          var admission =
              pool.submit(() -> service.submit(ApiTest.batch("a", "admitting", record)));
          assertTrue(preparing.await(2, TimeUnit.SECONDS));
          releaseFirst.countDown();
          assertTrue(
              secondWrite.await(2, TimeUnit.SECONDS),
              "B must advance to its next turn while A's admission copy is paused");
          assertFalse(admission.isDone());
        } finally {
          releaseFirst.countDown();
          releasePreparation.countDown();
        }
      }
    }
  }

  @Test
  void schemaCacheHitsAndColdMetadataReadsDoNotWaitForAWriter() throws Exception {
    Path path = directory.resolve("cache.db");
    try (var seed = new Store(path)) {
      seed.register(ProtobufSchema.parse(ApiTest.schema("a", 1, ApiTest.AMOUNT)));
      seed.register(ProtobufSchema.parse(ApiTest.schema("b", 1, ApiTest.AMOUNT)));
    }
    var writing = new CountDownLatch(1);
    var releaseWrite = new CountDownLatch(1);
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void beforeUpdate(String sql) {
                    if (sql.startsWith("INSERT INTO records")) {
                      writing.countDown();
                      await(releaseWrite);
                    }
                  }
                });
        var store = new Store(path);
        var pool = Executors.newFixedThreadPool(4)) {
      try {
        var warm = store.schema("a", "orders", 1);
        var report = Json.object().put("status", "queued");
        store.reserve("a", "orders", "existing", "hash", report);
        var record = ApiTest.rec("row");
        var protobuf = warm.validateAndEncode(record.get("payload"));
        var writer =
            pool.submit(
                () -> {
                  store.write("a", "orders", "existing", 0, record, protobuf);
                  return null;
                });
        assertTrue(writing.await(2, TimeUnit.SECONDS));
        var cacheHit = pool.submit(() -> store.schema("a", "orders", 1));
        var coldRead = pool.submit(() -> store.schema("b", "orders", 1));
        var batchRead = pool.submit(() -> store.batch("a", "orders", "existing"));
        assertSame(warm, cacheHit.get(2, TimeUnit.SECONDS));
        assertNotNull(coldRead.get(2, TimeUnit.SECONDS));
        assertEquals("queued", batchRead.get(2, TimeUnit.SECONDS).report().path("status").asText());
        assertFalse(writer.isDone());
      } finally {
        releaseWrite.countDown();
      }
    }
  }

  @Test
  void anOpenInspectionCursorDoesNotBlockHttpAdmissionOrCompletion() throws Exception {
    var reading = new CountDownLatch(1);
    var releaseRead = new CountDownLatch(1);
    var armed = new AtomicBoolean(false);
    var gated = new AtomicBoolean(false);
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void afterNext(String sql, boolean hasRow) {
                    if (armed.get()
                        && hasRow
                        && sql.contains("FROM records WHERE")
                        && gated.compareAndSet(false, true)) {
                      reading.countDown();
                      await(releaseRead);
                    }
                  }
                });
        var server = new IngestionServer(directory.resolve("inspection.db"), 0);
        var pool = Executors.newFixedThreadPool(2)) {
      server.start();
      var client = new Client(server.port());
      try {
        assertEquals(
            201,
            client.call("POST", "/schemas", ApiTest.schema("a", 1, ApiTest.AMOUNT)).statusCode());
        assertEquals(
            201,
            client.call("POST", "/schemas", ApiTest.schema("b", 1, ApiTest.AMOUNT)).statusCode());
        client.call("POST", "/batches", ApiTest.batch("a", "seed", ApiTest.rec("row")));
        client.completed("a", "seed");
        armed.set(true);
        var inspection =
            pool.submit(() -> client.call("GET", "/records/a/orders?limit=1000", null));
        assertTrue(reading.await(2, TimeUnit.SECONDS));
        var admission =
            pool.submit(
                () ->
                    client.call(
                        "POST",
                        "/batches",
                        ApiTest.batch("b", "during-read", ApiTest.rec("other"))));
        assertEquals(202, admission.get(2, TimeUnit.SECONDS).statusCode());
        assertEquals(1, client.completed("b", "during-read").path("successful").asInt());
        assertFalse(
            inspection.isDone(), "inspection must still be paused during B's durable completion");
      } finally {
        releaseRead.countDown();
      }
    }
  }

  @Test
  void perCustomerByteQuotaIsolatesTenantsAndRetriesSkipDeepCopy() throws Exception {
    var pauseA = new CountDownLatch(1);
    var releaseA = new CountDownLatch(1);
    try (var store = new Store(directory.resolve("customer-bytes.db"))) {
      store.register(ProtobufSchema.parse(ApiTest.schema("a", 1, ApiTest.AMOUNT)));
      store.register(ProtobufSchema.parse(ApiTest.schema("b", 1, ApiTest.AMOUNT)));
      try (var service =
          new IngestionService(
              store,
              (customer, id, attempt) -> {
                if (customer.equals("a")) {
                  pauseA.countDown();
                  await(releaseA);
                }
              },
              new Limits(100, 10, 10_000, 250))) {
        try {
          var firstA = ApiTest.batch("a", "b1", ApiTest.rec("r1"), ApiTest.rec("r2"));
          assertEquals(202, service.submit(firstA).status());
          assertTrue(pauseA.await(2, TimeUnit.SECONDS));

          var retryCopied = new AtomicBoolean(false);
          var retryPayload =
              new ObjectNode(JsonNodeFactory.instance) {
                @Override
                public ObjectNode deepCopy() {
                  retryCopied.set(true);
                  return super.deepCopy();
                }
              };
          retryPayload.put("amount", 42.5);
          var retryRec1 = ApiTest.rec("r1");
          retryRec1.set("payload", retryPayload);
          var retryBatch = ApiTest.batch("a", "b1", retryRec1, ApiTest.rec("r2"));
          assertEquals(202, service.submit(retryBatch).status());
          assertFalse(retryCopied.get(), "idempotent retry must not deep-copy records");

          var secondA = ApiTest.batch("a", "b2", ApiTest.rec("r3"), ApiTest.rec("r4"));
          assertEquals(429, assertThrows(ApiException.class, () -> service.submit(secondA)).status);

          assertEquals(202, service.submit(ApiTest.batch("b", "b-ok", ApiTest.rec("rb"))).status());
        } finally {
          releaseA.countDown();
        }
      }
      assertEquals("completed", store.batch("a", "orders", "b1").report().path("status").asText());
      assertEquals(
          "completed", store.batch("b", "orders", "b-ok").report().path("status").asText());
    }
  }

  @Test
  void retryBackoffReleasesWorkersSoOtherCustomersProgress() throws Exception {
    try (var store = new Store(directory.resolve("backoff-workers.db"))) {
      for (String c : List.of("c1", "c2", "c3", "c4", "fast"))
        store.register(ProtobufSchema.parse(ApiTest.schema(c, 1, ApiTest.AMOUNT)));
      var allFourFailedOnce = new CountDownLatch(4);
      var fastCompleted = new CountDownLatch(1);
      var retryTimerHeld = new CountDownLatch(1);
      var releaseTimer = new CountDownLatch(1);
      var retried = new AtomicInteger();
      try (var service =
          new IngestionService(
              store,
              (customer, id, attempt) -> {
                if (customer.startsWith("c") && attempt > 1) retried.incrementAndGet();
                if (customer.startsWith("c") && attempt == 1) {
                  allFourFailedOnce.countDown();
                  throw new TransientStorageException("backoff release check");
                }
                if (customer.equals("fast")) fastCompleted.countDown();
              },
              Limits.defaults())) {
        var timerField = IngestionService.class.getDeclaredField("retryTimer");
        timerField.setAccessible(true);
        ((ScheduledExecutorService) timerField.get(service))
            .execute(
                () -> {
                  retryTimerHeld.countDown();
                  await(releaseTimer);
                });
        try {
          assertTrue(retryTimerHeld.await(2, TimeUnit.SECONDS));
          for (String c : List.of("c1", "c2", "c3", "c4"))
            service.submit(ApiTest.batch(c, "b", ApiTest.rec("r")));
          assertTrue(allFourFailedOnce.await(2, TimeUnit.SECONDS));
          service.submit(ApiTest.batch("fast", "b", ApiTest.rec("r")));
          assertTrue(fastCompleted.await(2, TimeUnit.SECONDS));
          assertEquals(
              0, retried.get(), "fast customer must progress while all retry timers remain gated");
        } finally {
          releaseTimer.countDown();
        }
      }
      for (String c : List.of("c1", "c2", "c3", "c4", "fast"))
        assertEquals(1, store.batch(c, "orders", "b").report().path("successful").asInt());
    }
  }

  private static final class Client {
    final String base;
    final HttpClient http = HttpClient.newHttpClient();

    Client(int port) {
      base = "http://127.0.0.1:" + port;
    }

    HttpResponse<String> call(String method, String path, JsonNode body) throws Exception {
      return http.send(
          HttpRequest.newBuilder(URI.create(base + path))
              .timeout(Duration.ofSeconds(5))
              .header("Content-Type", "application/json")
              .method(
                  method,
                  body == null
                      ? HttpRequest.BodyPublishers.noBody()
                      : HttpRequest.BodyPublishers.ofString(body.toString()))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }

    JsonNode completed(String customer, String batch) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (System.nanoTime() < deadline) {
        var response = call("GET", "/batches/" + customer + "/orders/" + batch, null);
        assertEquals(200, response.statusCode());
        var report = ApiTest.JSON.readTree(response.body());
        if (report.path("status").asText().equals("completed")) return report;
        Thread.sleep(5);
      }
      throw new AssertionError("batch did not complete while inspection was paused");
    }
  }
}
