package io.atlan.ingestion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InterruptedBatchTest {
  @TempDir Path directory;

  @Test
  void legacyBodyMigrationPreservesCompletedData() throws Exception {
    Path database = directory.resolve("migration.db");
    var body = ApiTest.batch("a", "completed", ApiTest.rec("committed"));
    var report = completedReport("completed");
    legacy(database, List.of(new Seed(body, "completed", report, true)));
    try (var server = new IngestionServer(database, 0)) {
      server.start();
      var client = new Client(server.port());
      assertFalse(
          columns(database).contains("body"),
          "the unused legacy batch body column must be removed");
      assertEquals(report, client.call("GET", "/batches/a/orders/completed", null).body);
      assertEquals(200, client.call("POST", "/batches", body).status);
      var rows = client.records();
      assertEquals(List.of("committed"), ApiTest.ids(rows));
      assertEquals(ApiTest.JSON.readTree("{\"amount\":42.5}"), rows.get(0).path("payload"));
      assertEquals(1, client.call("GET", "/schemas/a/orders", null).body.path("schemas").size());
    }
    try (var server = new IngestionServer(database, 0)) {
      server.start();
      assertEquals(
          report, new Client(server.port()).call("GET", "/batches/a/orders/completed", null).body);
    }
  }

  @Test
  void freshAdmissionsRetainMetadataWithoutRetainingInvalidPayloads() throws Exception {
    Path database = directory.resolve("new.db");
    try (var server = new IngestionServer(database, 0)) {
      server.start();
      var client = new Client(server.port());
      client.call("POST", "/schemas", ApiTest.schema("a", 1, ApiTest.AMOUNT));
      var record = ApiTest.rec("bad");
      ((ObjectNode) record.path("payload")).put("extra", "unused-payload-".repeat(50_000));
      assertEquals(
          202, client.call("POST", "/batches", ApiTest.batch("a", "large-invalid", record)).status);
      var report = client.completed("large-invalid");
      assertEquals(1, report.path("failed").asInt());
      assertTrue(client.records().isEmpty());
      assertFalse(
          columns(database).contains("body"), "new batches must store no full request body");
      try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
          var statement = connection.createStatement();
          var rows =
              statement.executeQuery(
                  "SELECT"
                      + " length(customer)+length(dataset)+length(batch)+length(fingerprint)+length(status)+length(report)"
                      + " FROM batches")) {
        assertTrue(rows.next());
        assertTrue(rows.getLong(1) < 4096, "only small batch metadata should remain");
      }
    }
  }

  @Test
  void legacyInterruptedRetriesResumeOnceAndSkipCommittedPositions() throws Exception {
    Path database = directory.resolve("legacy-pending.db");
    var queued = ApiTest.batch("a", "queued", ApiTest.rec("q1"), ApiTest.rec("q2"));
    var partial =
        ApiTest.batch(
            "a",
            "partial",
            ApiTest.rec("committed"),
            ApiTest.rec("invalid", 1, "{\"amount\":true}"),
            ApiTest.rec("after"));
    legacy(
        database,
        List.of(
            new Seed(queued, "queued", pendingReport("queued", "queued", 2), false),
            new Seed(partial, "processing", pendingReport("partial", "processing", 3), true)));
    JsonNode queuedReport, partialReport;
    var writes = new FaultWriter(Map.of(), Map.of(), Set.of("a/committed"));
    try (var server = new IngestionServer(database, 0, writes, Limits.defaults());
        var pool = Executors.newFixedThreadPool(8)) {
      server.start();
      var client = new Client(server.port());
      for (String batch : List.of("queued", "partial")) {
        var status = client.call("GET", "/batches/a/orders/" + batch, null);
        assertEquals(200, status.status);
        assertEquals("interrupted", status.body.path("status").asText());
        assertTrue(status.body.path("retryable").asBoolean());
        assertTrue(status.body.has("interrupted_at"));
      }
      assertEquals(
          409,
          client.call("POST", "/batches", ApiTest.batch("a", "partial", ApiTest.rec("different")))
              .status);
      assertEquals(202, client.call("POST", "/batches", queued).status);
      var replies = new ArrayList<Future<Client.Reply>>();
      for (int i = 0; i < 8; i++)
        replies.add(pool.submit(() -> client.call("POST", "/batches", partial)));
      for (var reply : replies)
        assertTrue(Set.of(200, 202).contains(reply.get(5, TimeUnit.SECONDS).status));
      queuedReport = client.completed("queued");
      partialReport = client.completed("partial");
      assertEquals(2, queuedReport.path("successful").asInt());
      assertEquals(3, partialReport.path("total").asInt());
      assertEquals(2, partialReport.path("successful").asInt());
      assertEquals(1, partialReport.path("failed").asInt());
      assertEquals("invalid", partialReport.path("failures").get(0).path("record_id").asText());
      assertEquals(0, writes.attempts("a/committed"));
      assertEquals(1, writes.attempts("a/after"));
      assertEquals(4, client.records().size());
      assertEquals(partialReport, client.call("POST", "/batches", partial).body);
    }
    try (var server = new IngestionServer(database, 0)) {
      server.start();
      var client = new Client(server.port());
      assertEquals(queuedReport, client.call("GET", "/batches/a/orders/queued", null).body);
      assertEquals(partialReport, client.call("GET", "/batches/a/orders/partial", null).body);
      assertEquals(4, client.records().size());
    }
  }

  @Test
  void interruptedReportsSurviveRepeatedStartupAndFailedReadmission() throws Exception {
    Path database = directory.resolve("resume-failure.db");
    var body = ApiTest.batch("a", "resume", ApiTest.rec("resumed"));
    legacy(
        database, List.of(new Seed(body, "queued", pendingReport("resume", "queued", 1), false)));
    JsonNode interrupted;
    try (var server = new IngestionServer(database, 0)) {
      server.start();
      interrupted = new Client(server.port()).call("GET", "/batches/a/orders/resume", null).body;
      assertEquals("interrupted", interrupted.path("status").asText());
    }
    var failResume = new AtomicBoolean(true);
    var writing = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void beforeUpdate(String sql) throws SQLException {
                    if (sql.startsWith("UPDATE batches SET status='queued'")
                        && failResume.compareAndSet(true, false))
                      throw new SQLException("injected resume failure", "", 10);
                  }
                });
        var server =
            new IngestionServer(
                database,
                0,
                (customer, id, attempt) -> {
                  if (id.equals("hold")) {
                    writing.countDown();
                    if (!release.await(5, TimeUnit.SECONDS))
                      throw new IllegalStateException("gate timeout");
                  }
                },
                new Limits(1, 1, 2 * 1024 * 1024))) {
      server.start();
      var client = new Client(server.port());
      try {
        assertEquals(interrupted, client.call("GET", "/batches/a/orders/resume", null).body);
        assertEquals(
            202,
            client.call("POST", "/batches", ApiTest.batch("a", "holding", ApiTest.rec("hold")))
                .status);
        assertTrue(writing.await(2, TimeUnit.SECONDS));
        assertEquals(429, client.call("POST", "/batches", body).status);
        assertEquals(interrupted, client.call("GET", "/batches/a/orders/resume", null).body);
        release.countDown();
        client.completed("holding");
        assertEquals(503, client.call("POST", "/batches", body).status);
        assertEquals(interrupted, client.call("GET", "/batches/a/orders/resume", null).body);
        assertEquals(202, client.call("POST", "/batches", body).status);
        assertEquals(1, client.completed("resume").path("successful").asInt());
        assertEquals(List.of("hold", "resumed"), ApiTest.ids(client.records()));
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void committedAdmissionsAreEnqueuedEvenWhenJdbcReportsAnUncertainOutcome() throws Exception {
    for (boolean resume : List.of(false, true)) {
      Path database = directory.resolve("uncertain-" + resume + ".db");
      var body = ApiTest.batch("a", "uncertain", ApiTest.rec("once"));
      if (resume)
        legacy(
            database,
            List.of(
                new Seed(body, "processing", pendingReport("uncertain", "processing", 1), false)));
      var failOnce = new AtomicBoolean(true);
      var writes = new FaultWriter(Map.of(), Map.of(), Set.of());
      try (var probe =
              new JdbcProbe(
                  new JdbcProbe.Hooks() {
                    @Override
                    public void afterUpdate(String sql) throws SQLException {
                      if (sql.startsWith(
                              resume ? "UPDATE batches SET status='queued'" : "INSERT INTO batches")
                          && failOnce.compareAndSet(true, false))
                        throw new SQLException("injected error after actual commit", "", 10);
                    }
                  });
          var server = new IngestionServer(database, 0, writes, Limits.defaults())) {
        server.start();
        var client = new Client(server.port());
        if (!resume) client.call("POST", "/schemas", ApiTest.schema("a", 1, ApiTest.AMOUNT));
        assertEquals(202, client.call("POST", "/batches", body).status);
        assertEquals(1, client.completed("uncertain").path("successful").asInt());
        assertEquals(List.of("once"), ApiTest.ids(client.records()));
        assertEquals(1, writes.attempts("a/once"));
        assertEquals(200, client.call("POST", "/batches", body).status);
      }
    }
  }

  @Test
  void retryReadmitsAnOrphanedAdmissionWhenCommitAndReconciliationBothFail() throws Exception {
    var uncertain = new AtomicBoolean(false);
    var readFailure = new AtomicBoolean(false);
    try (var probe =
            new JdbcProbe(
                new JdbcProbe.Hooks() {
                  @Override
                  public void afterUpdate(String sql) throws SQLException {
                    if (sql.startsWith("INSERT INTO batches")
                        && uncertain.compareAndSet(false, true)) {
                      readFailure.set(true);
                      throw new SQLException("commit response lost", "", 10);
                    }
                  }

                  @Override
                  public void beforeQuery(String sql) throws SQLException {
                    if (sql.startsWith("SELECT fingerprint,report")
                        && readFailure.compareAndSet(true, false))
                      throw new SQLException("reconciliation unavailable", "", 5);
                  }
                });
        var server = new IngestionServer(directory.resolve("orphan.db"), 0)) {
      server.start();
      var client = new Client(server.port());
      client.call("POST", "/schemas", ApiTest.schema("a", 1, ApiTest.AMOUNT));
      var body = ApiTest.batch("a", "orphan", ApiTest.rec("once"));
      assertEquals(503, client.call("POST", "/batches", body).status);
      assertEquals(202, client.call("POST", "/batches", body).status);
      assertEquals(1, client.completed("orphan").path("successful").asInt());
      assertEquals(List.of("once"), ApiTest.ids(client.records()));
    }
  }

  @Test
  void actualJvmKillsLeaveResumableQueuedAndPartialBatches() throws Exception {
    for (String mode : List.of("queued", "processing")) {
      Path database = directory.resolve("killed-" + mode + ".db");
      String classpath =
          System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
      var process =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-cp",
                  classpath,
                  CrashFixtureProcess.class.getName(),
                  database.toString(),
                  mode)
              .redirectError(ProcessBuilder.Redirect.INHERIT)
              .start();
      var body =
          ApiTest.batch(
              "a", "killed", ApiTest.rec("committed"), ApiTest.rec("pause"), ApiTest.rec("after"));
      JsonNode committed = null;
      try (var output =
              new BufferedReader(
                  new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
          var pool = Executors.newSingleThreadExecutor()) {
        try {
          String ready = pool.submit(output::readLine).get(10, TimeUnit.SECONDS);
          assertNotNull(ready);
          assertTrue(ready.startsWith("READY "), ready);
          var client = new Client(Integer.parseInt(ready.substring(6)));
          assertEquals(
              201, client.call("POST", "/schemas", ApiTest.schema("a", 1, ApiTest.AMOUNT)).status);
          var submission = client.send("POST", "/batches", body);
          assertEquals("PAUSED", pool.submit(output::readLine).get(10, TimeUnit.SECONDS));
          if (mode.equals("processing")) {
            assertEquals(202, submission.get(5, TimeUnit.SECONDS).statusCode());
            var rows = client.records();
            assertEquals(1, rows.size());
            committed = rows.get(0);
          }
        } finally {
          process.destroyForcibly();
          assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        }
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(5, TimeUnit.SECONDS);
        }
      }
      var writes =
          new FaultWriter(
              Map.of(), Map.of(), mode.equals("processing") ? Set.of("a/committed") : Set.of());
      try (var server = new IngestionServer(database, 0, writes, Limits.defaults())) {
        server.start();
        var client = new Client(server.port());
        assertEquals(
            "interrupted",
            client.call("GET", "/batches/a/orders/killed", null).body.path("status").asText());
        assertEquals(202, client.call("POST", "/batches", body).status);
        var report = client.completed("killed");
        assertEquals(3, report.path("successful").asInt());
        assertEquals(0, report.path("failed").asInt());
        var rows = client.records();
        assertEquals(List.of("committed", "pause", "after"), ApiTest.ids(rows));
        if (committed != null) {
          assertEquals(committed, rows.get(0));
          assertEquals(0, writes.attempts("a/committed"));
        }
        assertEquals(200, client.call("POST", "/batches", body).status);
        assertEquals(report, client.call("POST", "/batches", body).body);
      }
    }
  }

  private record Seed(ObjectNode body, String status, ObjectNode report, boolean firstStored) {}

  private static ObjectNode pendingReport(String batch, String status, int total) {
    return ApiTest.JSON
        .createObjectNode()
        .put("customer_id", "a")
        .put("dataset_id", "orders")
        .put("batch_id", batch)
        .put("status", status)
        .put("total", total)
        .put("submitted_at", "2026-11-06T12:00:00Z");
  }

  private static ObjectNode completedReport(String batch) {
    var report =
        pendingReport(batch, "completed", 1)
            .put("successful", 1)
            .put("failed", 0)
            .put("completed_at", "2026-11-06T12:01:00Z");
    report.putArray("failures");
    return report;
  }

  private static void legacy(Path database, List<Seed> seeds) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE schemas(customer TEXT NOT NULL,dataset TEXT NOT NULL,version INTEGER NOT"
              + " NULL,body TEXT NOT NULL,PRIMARY KEY(customer,dataset,version))");
      statement.execute(
          "CREATE TABLE batches(customer TEXT NOT NULL,dataset TEXT NOT NULL,batch TEXT NOT"
              + " NULL,fingerprint TEXT NOT NULL,body TEXT NOT NULL,status TEXT NOT NULL,report"
              + " TEXT NOT NULL,PRIMARY KEY(customer,dataset,batch))");
      statement.execute(
          "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,customer TEXT NOT NULL,dataset"
              + " TEXT NOT NULL,batch TEXT NOT NULL,position INTEGER NOT NULL,record_id TEXT NOT"
              + " NULL,schema_version INTEGER NOT NULL,payload TEXT NOT NULL,protobuf BLOB NOT"
              + " NULL,UNIQUE(customer,dataset,batch,position))");
      try (var insert =
          connection.prepareStatement("INSERT INTO schemas VALUES('a','orders',1,?)")) {
        insert.setString(1, ApiTest.schema("a", 1, ApiTest.AMOUNT).toString());
        insert.executeUpdate();
      }
      for (var seed : seeds) {
        try (var insert =
            connection.prepareStatement("INSERT INTO batches VALUES('a','orders',?,?,?,?,?)")) {
          insert.setString(1, seed.body.path("batch_id").asText());
          insert.setString(2, Json.fingerprint(seed.body));
          insert.setString(3, seed.body.toString());
          insert.setString(4, seed.status);
          insert.setString(5, seed.report.toString());
          insert.executeUpdate();
        }
        if (seed.firstStored)
          try (var insert =
              connection.prepareStatement(
                  "INSERT INTO"
                      + " records(customer,dataset,batch,position,record_id,schema_version,payload,protobuf)"
                      + " VALUES('a','orders',?,0,?,1,?,?)")) {
            insert.setString(1, seed.body.path("batch_id").asText());
            insert.setString(2, seed.body.path("records").get(0).path("record_id").asText());
            insert.setString(3, "{\"amount\":42.5}");
            insert.setBytes(
                4,
                ByteBuffer.allocate(9)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .put((byte) 9)
                    .putDouble(42.5)
                    .array());
            insert.executeUpdate();
          }
      }
    }
  }

  private static Set<String> columns(Path database) throws Exception {
    var names = new HashSet<String>();
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        var statement = connection.createStatement();
        var rows = statement.executeQuery("PRAGMA table_info(batches)")) {
      while (rows.next()) names.add(rows.getString("name"));
    }
    return names;
  }

  private static final class Client {
    final String base;
    final HttpClient http = HttpClient.newHttpClient();

    Client(int port) {
      base = "http://127.0.0.1:" + port;
    }

    record Reply(int status, JsonNode body) {}

    CompletableFuture<HttpResponse<byte[]>> send(String method, String path, JsonNode body) {
      return http.sendAsync(
          HttpRequest.newBuilder(URI.create(base + path))
              .timeout(Duration.ofSeconds(8))
              .header("Content-Type", "application/json")
              .method(
                  method,
                  body == null
                      ? HttpRequest.BodyPublishers.noBody()
                      : HttpRequest.BodyPublishers.ofString(body.toString()))
              .build(),
          HttpResponse.BodyHandlers.ofByteArray());
    }

    Reply call(String method, String path, JsonNode body) throws Exception {
      var response = send(method, path, body).get(10, TimeUnit.SECONDS);
      return new Reply(response.statusCode(), ApiTest.JSON.readTree(response.body()));
    }

    JsonNode records() throws Exception {
      return call("GET", "/records/a/orders?limit=1000", null).body.path("records");
    }

    JsonNode completed(String batch) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
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
