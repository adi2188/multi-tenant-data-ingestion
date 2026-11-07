package io.atlan.ingestion;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Standalone, real-HTTP acceptance demonstration; no JUnit runtime needed. */
public final class Demo {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final HttpClient client =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private final String base;

  private Demo(int port) {
    base = "http://127.0.0.1:" + port;
  }

  private record Reply(int status, JsonNode body) {}

  private Reply call(String method, String path, JsonNode body) throws Exception {
    var builder =
        HttpRequest.newBuilder(URI.create(base + path))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json");
    var request =
        builder
            .method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)))
            .build();
    var reply = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    return new Reply(reply.statusCode(), JSON.readTree(reply.body()));
  }

  private JsonNode waitFor(String customer, String batch) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (System.nanoTime() < deadline) {
      var reply = call("GET", "/batches/" + customer + "/orders/" + batch, null);
      check(reply.status == 200, "batch lookup");
      if (reply.body.path("status").asText().equals("completed")) return reply.body;
      Thread.sleep(5);
    }
    throw new AssertionError("batch completion timed out");
  }

  private static void check(boolean condition, String description) {
    if (!condition) throw new AssertionError(description);
  }

  private static ObjectNode record(String id) {
    var result = JSON.createObjectNode().put("record_id", id).put("schema_version", 1);
    result.putObject("payload").put("order_id", id).put("amount", 42.5);
    return result;
  }

  private static ObjectNode batch(String customer, String batch, int count) {
    var result =
        JSON.createObjectNode()
            .put("customer_id", customer)
            .put("dataset_id", "orders")
            .put("batch_id", batch);
    var records = result.putArray("records");
    for (int i = 0; i < count; i++) records.add(record("r" + i));
    return result;
  }

  public static void main(String[] args) throws Exception {
    Files.createDirectories(Path.of("target"));
    Path database =
        Files.createTempDirectory(Path.of("target"), "demo-").resolve("records.sqlite3");
    var faults =
        new FaultWriter(
            Map.of("slow-a", 25L), Map.of("acme/recover", 2, "acme/exhaust", 10), Set.of());
    try (var server = new IngestionServer(database, 0, faults, Limits.defaults())) {
      server.start();
      var demo = new Demo(server.port());
      for (int version : List.of(1, 2)) {
        var registration =
            JSON.readTree(Files.readAllBytes(Path.of("fixtures/register-v" + version + ".json")));
        check(demo.call("POST", "/schemas", registration).status == 201, "register v" + version);
      }
      var mixed = JSON.readTree(Files.readAllBytes(Path.of("fixtures/mixed-batch.json")));
      check(demo.call("POST", "/batches", mixed).status == 202, "asynchronous acceptance");
      var report = demo.waitFor("acme", "mixed-001");
      check(
          report.path("successful").asInt() == 2 && report.path("failed").asInt() == 5,
          "mixed report");
      check(demo.call("POST", "/batches", mixed).body.equals(report), "retry preserves report");
      check(
          demo.call("GET", "/records/acme/orders", null).body.path("records").size() == 2,
          "exactly two stored records");
      System.out.println(
          "Mixed batch: total=7 successful=2 failed=5; retry returned identical report.");
      System.out.println(
          JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report.path("failures")));

      var retryBatch = batch("acme", "write-failures", 0);
      ((ArrayNode) retryBatch.path("records"))
          .add(record("recover"))
          .add(record("exhaust"))
          .add(record("after"));
      demo.call("POST", "/batches", retryBatch);
      var retries = demo.waitFor("acme", "write-failures");
      check(
          retries.path("successful").asInt() == 2 && retries.path("failed").asInt() == 1,
          "bounded storage retries");
      check(
          faults.attempts("acme/recover") == 3 && faults.attempts("acme/exhaust") == 3,
          "attempt bounds");
      System.out.println(
          "Storage faults: recover succeeded on attempt 3; exhaust failed after 3; after"
              + " succeeded.");

      var registration =
          (ObjectNode) JSON.readTree(Files.readAllBytes(Path.of("fixtures/register-v1.json")));
      for (String customer : List.of("slow-a", "fast-b")) {
        registration.put("customer_id", customer);
        check(demo.call("POST", "/schemas", registration).status == 201, "tenant schema");
      }
      long start = System.nanoTime();
      check(
          demo.call("POST", "/batches", batch("slow-a", "large", 100)).status == 202,
          "large batch accepted");
      double acknowledgmentMs = (System.nanoTime() - start) / 1_000_000.0;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (faults.attempts("slow-a/r0") == 0 && System.nanoTime() < deadline) Thread.sleep(1);
      check(faults.attempts("slow-a/r0") > 0, "slow A started before B submission");
      long bStart = System.nanoTime();
      demo.call("POST", "/batches", batch("fast-b", "small", 1));
      demo.waitFor("fast-b", "small");
      double bMs = (System.nanoTime() - bStart) / 1_000_000.0;
      String aStatus =
          demo.call("GET", "/batches/slow-a/orders/large", null).body.path("status").asText();
      check(aStatus.equals("processing"), "B must complete while A is still processing");
      var aReport = demo.waitFor("slow-a", "large");
      double aMs = (System.nanoTime() - start) / 1_000_000.0;
      check(aReport.path("successful").asInt() == 100, "all A records stored");
      check(
          demo.call("GET", "/records/slow-a/orders?limit=1000", null).body.path("records").size()
              == 100,
          "A exact storage count");
      check(
          demo.call("GET", "/records/fast-b/orders", null).body.path("records").size() == 1,
          "B exact storage count");
      System.out.printf(
          Locale.ROOT,
          "Isolation: A=100 records, 25 ms delay/write; B=1 record, 0 ms delay/write.%n");
      System.out.printf(
          Locale.ROOT,
          "A acknowledgment=%.1f ms; B completed in %.1f ms after B submission; A status"
              + " then=%s.%n",
          acknowledgmentMs,
          bMs,
          aStatus);
      System.out.printf(
          Locale.ROOT, "A completed in %.1f ms after A submission. All assertions passed.%n", aMs);
      System.out.println("Demo database: " + database.toAbsolutePath());
    }
  }
}
