package io.atlan.ingestion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProcessingFailureTest {
  @TempDir Path directory;

  private void seed(Path database) throws Exception {
    try (var store = new Store(database)) {
      store.register(ProtobufSchema.parse(ApiTest.schema("a", 1, ApiTest.AMOUNT)));
      store.register(ProtobufSchema.parse(ApiTest.schema("a", 2, ApiTest.AMOUNT, ApiTest.NOTE)));
    }
  }

  @Test
  void transientColdSchemaLookupsHaveTheirOwnBoundedRetryBudget() throws Exception {
    for (int code : new int[] {5, 6, 517, 262}) {
      Path database = directory.resolve("recover-" + code + ".db");
      seed(database);
      var reads = new AtomicInteger();
      var writes = new FaultWriter(Map.of(), Map.of("a/recover", 2), Set.of());
      try (var probe =
              new JdbcProbe(
                  new JdbcProbe.Hooks() {
                    @Override
                    public void beforeQuery(String sql) throws SQLException {
                      if (sql.startsWith("SELECT body FROM schemas WHERE")
                          && reads.incrementAndGet() <= 2)
                        throw new SQLException("injected lookup contention", "", code);
                    }
                  });
          var server = new IngestionServer(database, 0, writes, Limits.defaults())) {
        server.start();
        var client = new Client(server.port());
        var body = ApiTest.batch("a", "recover", ApiTest.rec("recover"), ApiTest.rec("after"));
        assertEquals(202, client.call("POST", "/batches", body).status);
        var report = client.completed("recover");
        counts(report, 2, 2, 0);
        assertEquals(3, reads.get(), "cold lookup must recover on the third attempt");
        assertEquals(3, writes.attempts("a/recover"), "write retries have a separate budget");
        assertEquals(1, writes.attempts("a/after"));
        var rows = client.call("GET", "/records/a/orders", null).body.path("records");
        assertEquals(List.of("recover", "after"), ApiTest.ids(rows));
        for (var row : rows)
          assertEquals(ApiTest.JSON.readTree("{\"amount\":42.5}"), row.path("payload"));
        assertEquals(report, client.call("POST", "/batches", body).body);
        assertEquals(3, reads.get());
      }
    }
  }

  @Test
  void exhaustedAndPermanentLookupFailuresDoNotStopLaterRecords() throws Exception {
    for (int code : new int[] {5, 6, 19}) {
      Path database = directory.resolve("failed-" + code + ".db");
      seed(database);
      int expectedAttempts = code == 19 ? 1 : 3;
      var reads = new AtomicInteger();
      var writes = new FaultWriter(Map.of(), Map.of(), Set.of());
      try (var probe =
              new JdbcProbe(
                  new JdbcProbe.Hooks() {
                    @Override
                    public void beforeQuery(String sql) throws SQLException {
                      if (sql.startsWith("SELECT body FROM schemas WHERE")
                          && reads.incrementAndGet() <= expectedAttempts)
                        throw new SQLException("injected registry failure", "", code);
                    }
                  });
          var server = new IngestionServer(database, 0, writes, Limits.defaults())) {
        server.start();
        var client = new Client(server.port());
        var body =
            ApiTest.batch(
                "a",
                "failed",
                ApiTest.rec("failed"),
                ApiTest.rec("after", 2, "{\"amount\":1,\"note\":\"later\"}"));
        assertEquals(202, client.call("POST", "/batches", body).status);
        var report = client.completed("failed");
        counts(report, 2, 1, 1);
        var failure = report.path("failures").get(0);
        assertEquals("storage", failure.path("category").asText());
        assertEquals(
            code == 19 ? "schema_lookup_failed" : "schema_lookup_retries_exhausted",
            failure.path("code").asText());
        assertEquals(expectedAttempts, failure.path("attempts").asInt());
        assertEquals("failed", failure.path("record_id").asText());
        assertEquals(0, failure.path("position").asInt());
        assertEquals(expectedAttempts + 1, reads.get());
        assertEquals(0, writes.attempts("a/failed"));
        var rows = client.call("GET", "/records/a/orders", null).body.path("records");
        assertEquals(List.of("after"), ApiTest.ids(rows));
        assertEquals(
            ApiTest.JSON.readTree("{\"amount\":1,\"note\":\"later\"}"),
            rows.get(0).path("payload"));
        assertEquals(report, client.call("POST", "/batches", body).body);
      }
    }
  }

  @Test
  void unknownSchemaIsNotRetried() throws Exception {
    Path database = directory.resolve("unknown.db");
    seed(database);
    var reads = new AtomicInteger();
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void beforeQuery(String sql) {
                    if (sql.startsWith("SELECT body FROM schemas WHERE")) reads.incrementAndGet();
                  }
                });
        var server = new IngestionServer(database, 0)) {
      server.start();
      var client = new Client(server.port());
      client.call(
          "POST", "/batches", ApiTest.batch("a", "unknown", ApiTest.rec("unknown", 99, "{}")));
      var report = client.completed("unknown");
      counts(report, 1, 0, 1);
      var failure = report.path("failures").get(0);
      assertEquals("validation", failure.path("category").asText());
      assertEquals("unknown_schema", failure.path("code").asText());
      assertFalse(failure.has("attempts"));
      assertEquals(1, reads.get());
    }
  }

  @Test
  void unexpectedValidationBugIsInternalAndNeverRetried() throws Exception {
    var validations = new AtomicInteger();
    var log = new ByteArrayOutputStream();
    PrintStream previous = System.err;
    try (var captured = new PrintStream(log, true, StandardCharsets.UTF_8);
        var store = new Store(directory.resolve("validation.db"))) {
      System.setErr(captured);
      try {
        store.register(ProtobufSchema.parse(ApiTest.schema("a", 1, ApiTest.AMOUNT)));
        var writes = new FaultWriter(Map.of(), Map.of(), Set.of());
        try (var service = new IngestionService(store, writes, Limits.defaults())) {
          var source =
              new ObjectNode(JsonNodeFactory.instance) {
                @Override
                public ObjectNode deepCopy() {
                  var copy =
                      new ObjectNode(JsonNodeFactory.instance) {
                        @Override
                        public JsonNode get(String name) {
                          if (name.equals("amount")) {
                            validations.incrementAndGet();
                            throw new IllegalStateException("injected validator bug");
                          }
                          return super.get(name);
                        }
                      };
                  copy.put("amount", 42.5);
                  return copy;
                }
              };
          source.put("amount", 42.5);
          var bad = ApiTest.rec("bug");
          bad.set("payload", source);
          service.submit(ApiTest.batch("a", "bug", bad, ApiTest.rec("after")));
          service.close();
          var report = store.batch("a", "orders", "bug").report();
          counts(report, 2, 1, 1);
          var failure = report.path("failures").get(0);
          assertEquals("internal", failure.path("category").asText());
          assertEquals("validation_error", failure.path("code").asText());
          assertFalse(failure.has("attempts"));
          assertEquals(1, validations.get());
          assertEquals(0, writes.attempts("a/bug"));
          assertEquals(
              List.of("after"),
              ApiTest.ids(store.records("a", "orders", 100, 0, null).path("records")));
        }
      } finally {
        System.setErr(previous);
      }
    }
    assertTrue(
        log.toString(StandardCharsets.UTF_8).contains("injected validator bug"),
        "unexpected bugs must be visible in diagnostics");
  }

  @Test
  void unexpectedLookupBugIsInternalAndNeverRetried() throws Exception {
    Path database = directory.resolve("lookup-bug.db");
    seed(database);
    var reads = new AtomicInteger();
    var log = new ByteArrayOutputStream();
    PrintStream previous = System.err;
    try (var captured = new PrintStream(log, true, StandardCharsets.UTF_8);
        var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void beforeQuery(String sql) {
                    if (sql.startsWith("SELECT body FROM schemas WHERE")
                        && reads.incrementAndGet() == 1)
                      throw new IllegalStateException("injected lookup bug");
                  }
                });
        var server = new IngestionServer(database, 0)) {
      System.setErr(captured);
      try {
        server.start();
        var client = new Client(server.port());
        client.call(
            "POST",
            "/batches",
            ApiTest.batch(
                "a", "bug", ApiTest.rec("bug"), ApiTest.rec("after", 2, "{\"amount\":1}")));
        var report = client.completed("bug");
        counts(report, 2, 1, 1);
        var failure = report.path("failures").get(0);
        assertEquals("internal", failure.path("category").asText());
        assertEquals("schema_lookup_error", failure.path("code").asText());
        assertFalse(failure.has("attempts"));
        assertEquals(2, reads.get());
      } finally {
        System.setErr(previous);
      }
    }
    assertTrue(log.toString(StandardCharsets.UTF_8).contains("injected lookup bug"));
  }

  @Test
  void unexpectedRuntimeWriteBugIsInternalAndLaterRecordsContinue() throws Exception {
    Path database = directory.resolve("runtime-write.db");
    seed(database);
    try (var store = new Store(database)) {
      try (var service =
          new IngestionService(
              store,
              (customer, id, attempt) -> {
                if (id.equals("bug"))
                  throw new IllegalStateException("injected unchecked write bug");
              },
              Limits.defaults())) {
        service.submit(ApiTest.batch("a", "runtime", ApiTest.rec("bug"), ApiTest.rec("after")));
      }
      var report = store.batch("a", "orders", "runtime").report();
      counts(report, 2, 1, 1);
      assertEquals("internal", report.path("failures").get(0).path("category").asText());
      assertEquals("storage_write_error", report.path("failures").get(0).path("code").asText());
      assertEquals(
          List.of("after"),
          ApiTest.ids(store.records("a", "orders", 100, 0, null).path("records")));
    }
  }

  @Test
  void uncheckedErrorInWriteAndPermanentMetadataFailureDoNotHangShutdown() throws Exception {
    Path database = directory.resolve("error-and-drain.db");
    seed(database);
    var log = new ByteArrayOutputStream();
    PrintStream previous = System.err;
    try (var captured = new PrintStream(log, true, StandardCharsets.UTF_8);
        var store = new Store(database)) {
      System.setErr(captured);
      try {
        try (var service =
            new IngestionService(
                store,
                (customer, recordId, attempt) -> {
                  if (recordId.equals("boom")) throw new AssertionError("injected write Error");
                },
                Limits.defaults())) {
          service.submit(
              ApiTest.batch("a", "err-batch", ApiTest.rec("boom"), ApiTest.rec("after")));
          service.close();
        }
        var report = store.batch("a", "orders", "err-batch").report();
        counts(report, 2, 1, 1);
        var failure = report.path("failures").get(0);
        assertEquals("internal", failure.path("category").asText());
        assertEquals("storage_write_error", failure.path("code").asText());
        assertEquals(
            List.of("after"),
            ApiTest.ids(store.records("a", "orders", 100, 0, null).path("records")));
      } finally {
        System.setErr(previous);
      }
    }

    Path brokenDb = directory.resolve("broken-status.db");
    seed(brokenDb);
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void beforeUpdate(String sql) throws SQLException {
                    if (sql.startsWith("UPDATE batches SET status=?,report=?"))
                      throw new SQLException("permanent disk full on metadata update", "", 13);
                  }
                });
        var store = new Store(brokenDb);
        var service = new IngestionService(store, null, Limits.defaults())) {
      service.submit(ApiTest.batch("a", "stuck", ApiTest.rec("r1")));
      service.close();
    }
  }

  private static void counts(JsonNode report, int total, int success, int failed) {
    assertEquals(total, report.path("total").asInt());
    assertEquals(success, report.path("successful").asInt());
    assertEquals(failed, report.path("failed").asInt());
    assertEquals(failed, report.path("failures").size());
  }

  private static final class Client {
    final String base;
    final HttpClient http = HttpClient.newHttpClient();

    Client(int port) {
      base = "http://127.0.0.1:" + port;
    }

    record Reply(int status, JsonNode body) {}

    Reply call(String method, String path, JsonNode body) throws Exception {
      var response =
          http.send(
              HttpRequest.newBuilder(URI.create(base + path))
                  .timeout(Duration.ofSeconds(5))
                  .header("Content-Type", "application/json")
                  .method(
                      method,
                      body == null
                          ? HttpRequest.BodyPublishers.noBody()
                          : HttpRequest.BodyPublishers.ofString(body.toString()))
                  .build(),
              HttpResponse.BodyHandlers.ofByteArray());
      return new Reply(response.statusCode(), ApiTest.JSON.readTree(response.body()));
    }

    JsonNode completed(String batch) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (System.nanoTime() < deadline) {
        var reply = call("GET", "/batches/a/orders/" + batch, null);
        assertEquals(200, reply.status);
        if (reply.body.path("status").asText().equals("completed")) return reply.body;
        Thread.sleep(5);
      }
      throw new AssertionError("batch did not complete");
    }
  }
}
