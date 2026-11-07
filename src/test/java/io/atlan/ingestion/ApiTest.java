package io.atlan.ingestion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.TimestampProto;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class ApiTest {
  static final ObjectMapper JSON = new ObjectMapper();
  @TempDir Path directory;
  IngestionServer server;
  HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  @BeforeEach
  void start() throws Exception {
    server = new IngestionServer(directory.resolve("test.db"), 0);
    server.start();
  }

  @AfterEach
  void stop() {
    if (server != null) server.close();
  }

  record Reply(int status, JsonNode body) {}

  Reply raw(String method, String path, byte[] bytes) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path))
            .timeout(Duration.ofSeconds(8))
            .header("Content-Type", "application/json")
            .method(
                method,
                bytes == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(bytes))
            .build();
    var result = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    return new Reply(result.statusCode(), JSON.readTree(result.body()));
  }

  Reply call(String method, String path, JsonNode body) throws Exception {
    return raw(method, path, body == null ? null : JSON.writeValueAsBytes(body));
  }

  static FieldDescriptorProto f(
      String name, int number, FieldDescriptorProto.Type type, boolean required) {
    return FieldDescriptorProto.newBuilder()
        .setName(name)
        .setNumber(number)
        .setType(type)
        .setLabel(
            required
                ? FieldDescriptorProto.Label.LABEL_REQUIRED
                : FieldDescriptorProto.Label.LABEL_OPTIONAL)
        .build();
  }

  static FileDescriptorSet descriptor(FieldDescriptorProto... fields) {
    return FileDescriptorSet.newBuilder()
        .addFile(TimestampProto.getDescriptor().toProto())
        .addFile(
            FileDescriptorProto.newBuilder()
                .setName("orders.proto")
                .setPackage("customer")
                .setSyntax("proto2")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(
                    DescriptorProto.newBuilder()
                        .setName("Order")
                        .addAllField(Arrays.asList(fields))))
        .build();
  }

  static ObjectNode schema(String customer, int version, FieldDescriptorProto... fields) {
    return JSON.createObjectNode()
        .put("customer_id", customer)
        .put("dataset_id", "orders")
        .put("version", version)
        .put("message_type", "customer.Order")
        .put(
            "descriptor_set", Base64.getEncoder().encodeToString(descriptor(fields).toByteArray()));
  }

  static final FieldDescriptorProto AMOUNT =
      f("amount", 1, FieldDescriptorProto.Type.TYPE_DOUBLE, true);
  static final FieldDescriptorProto NOTE =
      f("note", 2, FieldDescriptorProto.Type.TYPE_STRING, false);

  void register(String customer, int version, FieldDescriptorProto... fields) throws Exception {
    int status = call("POST", "/schemas", schema(customer, version, fields)).status;
    assertTrue(status == 200 || status == 201, "registration status " + status);
  }

  static ObjectNode rec(String id, int version, String payload) throws Exception {
    ObjectNode result = JSON.createObjectNode().put("record_id", id).put("schema_version", version);
    result.set("payload", JSON.readTree(payload));
    return result;
  }

  static ObjectNode rec(String id) throws Exception {
    return rec(id, 1, "{\"amount\":42.5}");
  }

  static ObjectNode batch(String customer, String id, JsonNode... records) {
    ObjectNode result =
        JSON.createObjectNode()
            .put("customer_id", customer)
            .put("dataset_id", "orders")
            .put("batch_id", id);
    ArrayNode array = result.putArray("records");
    for (JsonNode r : records) array.add(r);
    return result;
  }

  JsonNode waitFor(String customer, String batch) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      var reply = call("GET", "/batches/" + customer + "/orders/" + batch, null);
      assertEquals(200, reply.status, reply.body.toString());
      if (reply.body.path("status").asText().equals("completed")) return reply.body;
      Thread.sleep(5);
    }
    throw new AssertionError("batch did not complete");
  }

  JsonNode records(String customer) throws Exception {
    return call("GET", "/records/" + customer + "/orders", null).body.path("records");
  }

  void counts(JsonNode report, int total, int success, int failed) {
    assertEquals(total, report.path("total").asInt());
    assertEquals(success, report.path("successful").asInt());
    assertEquals(failed, report.path("failed").asInt());
    assertEquals(failed, report.path("failures").size());
  }

  @Test
  void schemaEvolutionIsAtomicAndOlderVersionsAreUsed() throws Exception {
    register("a", 1, AMOUNT);
    assertEquals(200, call("POST", "/schemas", schema("a", 1, AMOUNT)).status);
    register("a", 2, AMOUNT, NOTE);
    for (var fields :
        List.of(
            new FieldDescriptorProto[] {},
            new FieldDescriptorProto[] {
              f("amount", 1, FieldDescriptorProto.Type.TYPE_STRING, true)
            },
            new FieldDescriptorProto[] {
              f("amount", 1, FieldDescriptorProto.Type.TYPE_DOUBLE, false)
            },
            new FieldDescriptorProto[] {
              f("amount", 9, FieldDescriptorProto.Type.TYPE_DOUBLE, true)
            },
            new FieldDescriptorProto[] {
              AMOUNT, f("new_field", 3, FieldDescriptorProto.Type.TYPE_INT64, true)
            })) {
      assertEquals(422, call("POST", "/schemas", schema("a", 3, fields)).status);
    }
    assertEquals(409, call("POST", "/schemas", schema("a", 1, AMOUNT, NOTE)).status);
    assertEquals(422, call("POST", "/schemas", schema("a", 3, AMOUNT, AMOUNT)).status);
    assertEquals(
        422,
        call(
                "POST",
                "/schemas",
                schema("a", 3, AMOUNT, f("bytes", 3, FieldDescriptorProto.Type.TYPE_BYTES, false)))
            .status);
    assertEquals(2, call("GET", "/schemas/a/orders", null).body.path("schemas").size());
    assertEquals(
        202,
        call(
                "POST",
                "/batches",
                batch(
                    "a",
                    "batch",
                    rec("old"),
                    rec("new", 2, "{\"amount\":1,\"note\":\"ok\"}"),
                    rec("wrong-old", 1, "{\"amount\":1,\"note\":\"no\"}"),
                    rec("unknown", 99, "{}")))
            .status);
    counts(waitFor("a", "batch"), 4, 2, 2);
    assertEquals(List.of("old", "new"), ids(records("a")));
    assertEquals(
        JSON.readTree("{\"amount\":1,\"note\":\"ok\"}"), records("a").get(1).path("payload"));
  }

  static List<String> ids(JsonNode records) {
    var ids = new ArrayList<String>();
    records.forEach(r -> ids.add(r.path("record_id").asText()));
    return ids;
  }

  @Test
  void mixedMalformedAndAllInvalidRecordsAreAccountedFor() throws Exception {
    register("a", 1, AMOUNT);
    var submission =
        batch(
            "a",
            "batch",
            rec("ok"),
            rec("null", 1, "{\"amount\":null}"),
            rec("bool", 1, "{\"amount\":true}"),
            rec("string", 1, "{\"amount\":\"42\"}"),
            rec("missing", 1, "{}"),
            rec("extra", 1, "{\"amount\":1,\"other\":2}"),
            JSON.nullNode(),
            JSON.createObjectNode(),
            rec("", 1, "{}"),
            JSON.createObjectNode().put("record_id", "bad-envelope"),
            rec("nested", 1, "{\"amount\":[1]}"));
    assertEquals(202, call("POST", "/batches", submission).status);
    var report = waitFor("a", "batch");
    counts(report, 11, 1, 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(i + 1, report.path("failures").get(i).path("position").asInt());
      assertEquals("validation", report.path("failures").get(i).path("category").asText());
    }
    assertFalse(report.path("failures").get(5).has("record_id"));
    assertEquals(List.of("ok"), ids(records("a")));
    assertEquals(JSON.readTree("{\"amount\":42.5}"), records("a").get(0).path("payload"));
    call("POST", "/batches", batch("unknown", "bad", rec("unknown"), JSON.nullNode()));
    counts(waitFor("unknown", "bad"), 2, 0, 2);
    assertTrue(records("unknown").isEmpty());
  }

  @Test
  void scalarRulesPresenceAndProtobufBytesAreReal() throws Exception {
    var integer = f("i", 1, FieldDescriptorProto.Type.TYPE_INT64, true);
    var floating = f("f", 2, FieldDescriptorProto.Type.TYPE_DOUBLE, true);
    var bool = f("b", 3, FieldDescriptorProto.Type.TYPE_BOOL, true);
    var string = f("s", 4, FieldDescriptorProto.Type.TYPE_STRING, true);
    var timestamp =
        f("t", 5, FieldDescriptorProto.Type.TYPE_MESSAGE, true).toBuilder()
            .setTypeName(".google.protobuf.Timestamp")
            .build();
    register(
        "a",
        1,
        integer,
        floating,
        bool,
        string,
        timestamp,
        f("o", 6, FieldDescriptorProto.Type.TYPE_STRING, false));
    var good =
        (ObjectNode)
            JSON.readTree(
                "{\"i\":9223372036854775807,\"f\":1,\"b\":false,\"s\":\"\",\"t\":\"2024-02-29T10:11:12.123456789+05:30\"}");
    var inputs = new ArrayList<JsonNode>();
    inputs.add(rec("ok", 1, good.toString()));
    String[][] bad = {
      {"i", "true"},
      {"i", "1.0"},
      {"i", "9223372036854775808"},
      {"f", "false"},
      {"b", "1"},
      {"t", "\"2023-02-29T00:00:00Z\""},
      {"t", "\"2024-01-01\""},
      {"t", "\"2024-01-01T00:00:00\""},
      {"t", "\"2024-01-01T00:00:60Z\""},
      {"t", "\"2024-01-01T00:00:00.1234567890Z\""},
      {"s", "{}"},
      {"o", "null"}
    };
    for (int n = 0; n < bad.length; n++) {
      var p = good.deepCopy();
      p.set(bad[n][0], JSON.readTree(bad[n][1]));
      inputs.add(rec("bad" + n, 1, p.toString()));
    }
    call("POST", "/batches", batch("a", "types", inputs.toArray(JsonNode[]::new)));
    counts(waitFor("a", "types"), 13, 1, 12);
    var stored = records("a").get(0);
    assertEquals(good, stored.path("payload"));
    var file =
        Descriptors.FileDescriptor.buildFrom(
            descriptor(integer, floating, bool, string, timestamp).getFile(1),
            new Descriptors.FileDescriptor[] {TimestampProto.getDescriptor()});
    var parsed =
        DynamicMessage.parseFrom(
            file.findMessageTypeByName("Order"),
            Base64.getDecoder().decode(stored.path("protobuf_base64").asText()));
    assertTrue(parsed.hasField(parsed.getDescriptorForType().findFieldByName("b")));
    assertEquals(false, parsed.getField(parsed.getDescriptorForType().findFieldByName("b")));
    assertEquals(
        Long.MAX_VALUE, parsed.getField(parsed.getDescriptorForType().findFieldByName("i")));
    var tsMsg =
        (DynamicMessage) parsed.getField(parsed.getDescriptorForType().findFieldByName("t"));
    assertEquals(
        123_456_789, tsMsg.getField(tsMsg.getDescriptorForType().findFieldByName("nanos")));
  }

  @Test
  void doubleOverflowFailsOnlyItsRecordAndKeepsRetriesSafe() throws Exception {
    register("a", 1, AMOUNT);
    register("a", 2, AMOUNT, NOTE);
    String submission =
        """
        {
          "customer_id": "a", "dataset_id": "orders", "batch_id": "overflow",
          "records": [
            {"record_id":"before","schema_version":1,"payload":{"amount":42.5}},
            {"record_id":"positive","schema_version":1,"payload":{"amount":1e309}},
            {"record_id":"negative","schema_version":2,"payload":{"amount":-1e309}},
            {"record_id":"undeclared","schema_version":1,"payload":{"amount":1,"extra":1e309}},
            {"record_id":"after","schema_version":2,"payload":{"amount":0,"note":"unchanged"}}
          ]
        }
        """;
    byte[] bytes = submission.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    assertEquals(202, raw("POST", "/batches", bytes).status);
    var report = waitFor("a", "overflow");
    counts(report, 5, 2, 3);
    for (int i = 0; i < 3; i++) {
      var failure = report.path("failures").get(i);
      assertEquals(i + 1, failure.path("position").asInt());
      assertEquals("validation", failure.path("category").asText());
      assertFalse(failure.has("attempts"));
    }
    assertEquals("incorrect_type", report.path("failures").get(0).path("code").asText());
    assertEquals("incorrect_type", report.path("failures").get(1).path("code").asText());
    assertEquals("unknown_field", report.path("failures").get(2).path("code").asText());
    var stored = records("a");
    assertEquals(List.of("before", "after"), ids(stored));
    assertEquals(JSON.readTree("{\"amount\":42.5}"), stored.get(0).path("payload"));
    assertEquals(
        JSON.readTree("{\"amount\":0,\"note\":\"unchanged\"}"), stored.get(1).path("payload"));
    var retry = raw("POST", "/batches", bytes);
    assertEquals(200, retry.status);
    assertEquals(report, retry.body);
    assertEquals(
        409,
        raw(
                "POST",
                "/batches",
                submission
                    .replace("1e309", "1e310")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .status);
    assertEquals(stored, records("a"));
  }

  @Test
  void oneOverflowDoesNotDiscard999ValidRecords() throws Exception {
    register("a", 1, AMOUNT);
    var entries = new StringJoiner(",");
    var expectedIds = new ArrayList<String>();
    for (int i = 0; i < 999; i++) {
      if (i == 500)
        entries.add(
            "{\"record_id\":\"overflow\",\"schema_version\":1,\"payload\":{\"amount\":1e309}}");
      String id = "r" + i;
      expectedIds.add(id);
      entries.add(rec(id).toString());
    }
    String submission =
        "{\"customer_id\":\"a\",\"dataset_id\":\"orders\",\"batch_id\":\"large-overflow\",\"records\":["
            + entries
            + "]}";
    assertEquals(
        202,
        raw("POST", "/batches", submission.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .status);
    var report = waitFor("a", "large-overflow");
    counts(report, 1000, 999, 1);
    var failure = report.path("failures").get(0);
    assertEquals(500, failure.path("position").asInt());
    assertEquals("overflow", failure.path("record_id").asText());
    assertEquals("validation", failure.path("category").asText());
    assertEquals("incorrect_type", failure.path("code").asText());
    var stored = call("GET", "/records/a/orders?limit=1000", null).body.path("records");
    assertEquals(expectedIds, ids(stored));
    var expectedPayload = JSON.readTree("{\"amount\":42.5}");
    for (var row : stored) assertEquals(expectedPayload, row.path("payload"));
  }

  @Test
  void nonJsonNumericTokensStillRejectTheEntireSubmission() throws Exception {
    register("a", 1, AMOUNT);
    for (String token : List.of("NaN", "Infinity", "-Infinity")) {
      String submission =
          "{\"customer_id\":\"a\",\"dataset_id\":\"orders\",\"batch_id\":\"non-json\",\"records\":[{\"record_id\":\"r\",\"schema_version\":1,\"payload\":{\"amount\":"
              + token
              + "}}]}";
      var reply =
          raw("POST", "/batches", submission.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      assertEquals(400, reply.status, token);
      assertEquals("invalid_json", reply.body.path("error").asText(), token);
      assertEquals(404, call("GET", "/batches/a/orders/non-json", null).status);
    }
    assertTrue(records("a").isEmpty());
  }

  @Test
  void malformedBatchesAreRejectedBeforeReservation() throws Exception {
    for (String raw : List.of("{", "[]", "null", "{\"x\":NaN}", "{\"x\":1,\"x\":2}", "{} {}"))
      assertEquals(
          400,
          raw("POST", "/batches", raw.getBytes(java.nio.charset.StandardCharsets.UTF_8)).status,
          raw);
    assertEquals(400, raw("POST", "/batches", new byte[] {(byte) 0xff}).status);
    for (ObjectNode b :
        List.of(
            batch("a", "batch"),
            batch("a", "batch", rec("same"), rec("same")),
            batch("", "batch", rec("x")),
            batch("a", "batch", rec("x")).put("extra", true)))
      assertEquals(400, call("POST", "/batches", b).status);
    var many = batch("a", "batch");
    for (int i = 0; i < 1001; i++) ((ArrayNode) many.path("records")).add(rec("r" + i));
    assertEquals(413, call("POST", "/batches", many).status);
    assertEquals(413, raw("POST", "/batches", new byte[2 * 1024 * 1024 + 1]).status);
    assertEquals(404, call("GET", "/batches/a/orders/batch", null).status);
  }

  @Test
  void concurrentRetriesAreSafeAndConflictsAndTenantsAreScoped() throws Exception {
    restart(new FaultWriter(Map.of("a", 50L), Map.of(), Set.of()), Limits.defaults());
    register("a", 1, AMOUNT);
    var b = batch("a", "batch", rec("r1"), rec("r2"));
    try (var pool = Executors.newFixedThreadPool(8)) {
      var futures = new ArrayList<Future<Reply>>();
      for (int i = 0; i < 8; i++) futures.add(pool.submit(() -> call("POST", "/batches", b)));
      for (var f : futures) assertTrue(Set.of(200, 202).contains(f.get().status));
    }
    assertEquals(409, call("POST", "/batches", batch("a", "batch", rec("other"))).status);
    var finalReport = waitFor("a", "batch");
    assertEquals(
        finalReport,
        raw("POST", "/batches", JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(b)).body);
    assertEquals(List.of("r1", "r2"), ids(records("a")));
    register("b", 1, f("name", 1, FieldDescriptorProto.Type.TYPE_STRING, true));
    call("POST", "/batches", batch("b", "batch", rec("r1", 1, "{\"name\":\"B\"}")));
    counts(waitFor("b", "batch"), 1, 1, 0);
    assertEquals(JSON.readTree("{\"name\":\"B\"}"), records("b").get(0).path("payload"));
    call("POST", "/batches", batch("a", "another", rec("r1")));
    waitFor("a", "another");
    assertEquals(3, records("a").size());
  }

  void restart(FaultWriter faults, Limits limits) throws Exception {
    server.close();
    server = new IngestionServer(directory.resolve("test.db"), 0, faults, limits);
    server.start();
  }

  @Test
  void transientAndExhaustedFailuresDoNotDropOtherRecords() throws Exception {
    var faults =
        new FaultWriter(Map.of(), Map.of("a/recover", 2, "a/exhaust", 10), Set.of("a/permanent"));
    restart(faults, Limits.defaults());
    register("a", 1, AMOUNT);
    call(
        "POST",
        "/batches",
        batch(
            "a",
            "batch",
            rec("recover"),
            rec("exhaust"),
            rec("permanent"),
            rec("after"),
            rec("invalid", 1, "{\"amount\":true}")));
    var report = waitFor("a", "batch");
    counts(report, 5, 2, 3);
    assertEquals("storage", report.path("failures").get(0).path("category").asText());
    assertEquals(3, report.path("failures").get(0).path("attempts").asInt());
    assertEquals(1, report.path("failures").get(1).path("attempts").asInt());
    assertEquals(List.of("recover", "after"), ids(records("a")));
    assertEquals(3, faults.attempts("a/recover"));
    assertEquals(0, faults.attempts("a/invalid"));
  }

  @Test
  void slowTenantCannotMonopolizeWorkersAndQuotaAllowsRetries() throws Exception {
    var faults = new FaultWriter(Map.of("a", 25L), Map.of(), Set.of());
    restart(faults, new Limits(100, 1, 32 * 1024 * 1024));
    register("a", 1, AMOUNT);
    register("b", 1, AMOUNT);
    var a = batch("a", "large");
    for (int i = 0; i < 60; i++) ((ArrayNode) a.path("records")).add(rec("r" + i));
    assertEquals(202, call("POST", "/batches", a).status);
    assertTrue(faults.started.await(2, TimeUnit.SECONDS));
    assertEquals(202, call("POST", "/batches", a).status);
    assertEquals(429, call("POST", "/batches", batch("a", "over", rec("x"))).status);
    call("POST", "/batches", batch("b", "small", rec("x")));
    counts(waitFor("b", "small"), 1, 1, 0);
    assertNotEquals(
        "completed", call("GET", "/batches/a/orders/large", null).body.path("status").asText());
    counts(waitFor("a", "large"), 60, 60, 0);
  }

  @Test
  void restartPreservesImmutableReportsSchemasAndRecords() throws Exception {
    register("a", 1, AMOUNT);
    var b = batch("a", "batch", rec("r"));
    call("POST", "/batches", b);
    var report = waitFor("a", "batch");
    var stored = records("a");
    var schemas = call("GET", "/schemas/a/orders", null).body;
    restart(null, Limits.defaults());
    assertEquals(report, waitFor("a", "batch"));
    assertEquals(stored, records("a"));
    assertEquals(schemas, call("GET", "/schemas/a/orders", null).body);
    assertEquals(200, call("POST", "/batches", b).status);
    assertEquals(report, call("POST", "/batches", b).body);
  }

  @Test
  void paginationAndErrorResponses() throws Exception {
    register("a", 1, AMOUNT);
    call("POST", "/batches", batch("a", "batch", rec("r0"), rec("r1"), rec("r2")));
    waitFor("a", "batch");
    assertEquals(
        List.of("r1", "r2"),
        ids(call("GET", "/records/a/orders?limit=2&offset=1", null).body.path("records")));
    var cursorPage1 = call("GET", "/records/a/orders?limit=2&after=0", null).body;
    assertEquals(List.of("r0", "r1"), ids(cursorPage1.path("records")));
    assertTrue(cursorPage1.path("next_after").asLong(-1) > 0);
    var cursorPage2 =
        call(
                "GET",
                "/records/a/orders?limit=2&after=" + cursorPage1.path("next_after").asLong(),
                null)
            .body;
    assertEquals(List.of("r2"), ids(cursorPage2.path("records")));
    assertTrue(cursorPage2.path("next_after").isNull());
    assertEquals(400, call("GET", "/records/a/orders?limit=-1", null).status);
    assertEquals(400, call("GET", "/records/a/orders?limit=2&offset=1&after=0", null).status);
    assertEquals(200, call("GET", "/health", null).status);
    assertEquals(404, call("GET", "/unknown", null).status);
  }

  @Test
  void originalDecimalPrecisionAndNumericTokenKindsSurvive() throws Exception {
    register("a", 1, AMOUNT);
    String original =
        "{\"customer_id\":\"a\",\"dataset_id\":\"orders\",\"batch_id\":\"decimal\",\"records\":[{\"record_id\":\"r\",\"schema_version\":1,\"payload\":{\"amount\":0.12345678901234567890123456789}}]}";
    assertEquals(
        202,
        raw("POST", "/batches", original.getBytes(java.nio.charset.StandardCharsets.UTF_8)).status);
    waitFor("a", "decimal");
    var response =
        client.send(
            HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + server.port() + "/records/a/orders"))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertTrue(response.body().contains("0.12345678901234567890123456789"), response.body());
    var integer = batch("a", "numeric-kind", rec("r", 1, "{\"amount\":1}"));
    call("POST", "/batches", integer);
    waitFor("a", "numeric-kind");
    assertEquals(
        409,
        call("POST", "/batches", batch("a", "numeric-kind", rec("r", 1, "{\"amount\":1.0}")))
            .status);
    var exponent = integer.deepCopy().put("batch_id", "exponent");
    String exponentJson = exponent.toString().replace("\"amount\":1", "\"amount\":1e0");
    assertEquals(
        202,
        raw("POST", "/batches", exponentJson.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .status);
    waitFor("a", "exponent");
    assertEquals(409, call("POST", "/batches", exponent).status);
  }

  @Test
  void aDatasetCannotImpersonateTheTimestampType() throws Exception {
    var forged =
        FileDescriptorProto.newBuilder()
            .setName("forged.proto")
            .setSyntax("proto2")
            .setPackage("google.protobuf")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("Timestamp")
                    .addField(
                        f("t", 1, FieldDescriptorProto.Type.TYPE_MESSAGE, false).toBuilder()
                            .setTypeName(".google.protobuf.Timestamp")))
            .build();
    var body =
        schema("a", 1, AMOUNT)
            .put("message_type", "google.protobuf.Timestamp")
            .put(
                "descriptor_set",
                Base64.getEncoder()
                    .encodeToString(
                        FileDescriptorSet.newBuilder().addFile(forged).build().toByteArray()));
    assertEquals(422, call("POST", "/schemas", body).status);
    assertTrue(call("GET", "/schemas/a/orders", null).body.path("schemas").isEmpty());
  }

  @Test
  void globalAndByteQuotasRejectBeforeReservingAndGracefulCloseDrains() throws Exception {
    var faults = new FaultWriter(Map.of("a", 60L), Map.of(), Set.of());
    restart(faults, new Limits(1, 10, 32 * 1024 * 1024));
    register("a", 1, AMOUNT);
    call("POST", "/batches", batch("a", "slow", rec("1"), rec("2"), rec("3")));
    assertEquals(429, call("POST", "/batches", batch("b", "blocked", rec("x"))).status);
    assertEquals(404, call("GET", "/batches/b/orders/blocked", null).status);
    // Close while work is accepted; reopening must observe all final outcomes.
    restart(null, Limits.defaults());
    counts(waitFor("a", "slow"), 3, 3, 0);
    restart(null, new Limits(100, 10, 20));
    assertEquals(429, call("POST", "/batches", batch("b", "bytes", rec("x"))).status);
    assertEquals(404, call("GET", "/batches/b/orders/bytes", null).status);
    assertEquals(3, records("a").size());
  }

  @Test
  void invalidProtobufWireFieldNumbersNeverEnterRegistry() throws Exception {
    register("a", 1, AMOUNT);
    for (int number : new int[] {19000, 19999, 536870912, Integer.MAX_VALUE}) {
      assertEquals(
          422,
          call(
                  "POST",
                  "/schemas",
                  schema(
                      "a",
                      2,
                      AMOUNT,
                      f("bad", number, FieldDescriptorProto.Type.TYPE_STRING, false)))
              .status,
          "field number " + number);
    }
    assertEquals(1, call("GET", "/schemas/a/orders", null).body.path("schemas").size());
  }

  @Test
  void reservedProtoFieldsAndNonUtf8JsonAreRejected() throws Exception {
    var set = descriptor(AMOUNT).toBuilder();
    var root = set.getFile(1).toBuilder();
    root.setMessageType(
        0,
        root.getMessageType(0).toBuilder()
            .addReservedName("amount")
            .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(1).setEnd(2)));
    set.setFile(1, root);
    var body =
        schema("a", 1, AMOUNT)
            .put("descriptor_set", Base64.getEncoder().encodeToString(set.build().toByteArray()));
    assertEquals(422, call("POST", "/schemas", body).status);

    var validReservedV1 = descriptor(AMOUNT).toBuilder();
    var rootV1 = validReservedV1.getFile(1).toBuilder();
    rootV1.setMessageType(
        0,
        rootV1.getMessageType(0).toBuilder()
            .addReservedName("legacy")
            .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(10).setEnd(15)));
    validReservedV1.setFile(1, rootV1);
    var v1Body =
        schema("a", 1, AMOUNT)
            .put(
                "descriptor_set",
                Base64.getEncoder().encodeToString(validReservedV1.build().toByteArray()));
    assertEquals(201, call("POST", "/schemas", v1Body).status);

    var withSourceInfo = validReservedV1.build().toBuilder();
    withSourceInfo.setFile(
        1,
        withSourceInfo.getFile(1).toBuilder()
            .setSourceCodeInfo(
                SourceCodeInfo.newBuilder()
                    .addLocation(SourceCodeInfo.Location.newBuilder().addPath(1))));
    var v1WithSourceInfo =
        schema("a", 1, AMOUNT)
            .put(
                "descriptor_set",
                Base64.getEncoder().encodeToString(withSourceInfo.build().toByteArray()));
    assertEquals(200, call("POST", "/schemas", v1WithSourceInfo).status);

    assertEquals(422, call("POST", "/schemas", schema("a", 2, AMOUNT, NOTE)).status);

    for (String encoding : List.of("UTF-16", "UTF-16LE", "UTF-32"))
      assertEquals(
          400,
          raw(
                  "POST",
                  "/batches",
                  batch("a", "utf", rec("r"))
                      .toString()
                      .getBytes(java.nio.charset.Charset.forName(encoding)))
              .status,
          encoding);
  }

  @Test
  void extremeDecimalExponentsFailOnlyTheirRecordAndRetrySafely() throws Exception {
    register("a", 1, AMOUNT);
    String text =
        "{\"customer_id\":\"a\",\"dataset_id\":\"orders\",\"batch_id\":\"extreme\",\"records\":["
            + "{\"record_id\":\"bad\",\"schema_version\":1,\"payload\":{\"amount\":100e2147483647}},"
            + rec("good")
            + "]}";
    assertEquals(
        202,
        raw("POST", "/batches", text.getBytes(java.nio.charset.StandardCharsets.UTF_8)).status);
    counts(waitFor("a", "extreme"), 2, 1, 1);
    assertEquals(List.of("good"), ids(records("a")));
    assertEquals(
        200,
        raw(
                "POST",
                "/batches",
                text.replace("100e2147483647", "1000e2147483646")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .status);
  }

  @Test
  void unmatchedSurrogatesFailIndependentlyAndCannotImpersonateOtherContent() throws Exception {
    register("a", 1, f("s", 1, FieldDescriptorProto.Type.TYPE_STRING, true));
    String text =
        "{\"customer_id\":\"a\",\"dataset_id\":\"orders\",\"batch_id\":\"unicode\",\"records\":["
            + "{\"record_id\":\"bad\",\"schema_version\":1,\"payload\":{\"s\":\"\\ud800\"}},"
            + "{\"record_id\":\"good\",\"schema_version\":1,\"payload\":{\"s\":\"\\ud83d\\ude00\"}}]}";
    assertEquals(
        202,
        raw("POST", "/batches", text.getBytes(java.nio.charset.StandardCharsets.UTF_8)).status);
    var report = waitFor("a", "unicode");
    counts(report, 2, 1, 1);
    assertEquals("invalid_unicode", report.path("failures").get(0).path("code").asText());
    assertEquals("😀", records("a").get(0).path("payload").path("s").asText());
    assertEquals(
        409,
        raw(
                "POST",
                "/batches",
                text.replace("\\ud800", "?").getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .status);
    assertEquals(
        409,
        raw(
                "POST",
                "/batches",
                text.replace("\\ud800", "\\udc00")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .status);
    assertEquals(
        200,
        raw("POST", "/batches", text.getBytes(java.nio.charset.StandardCharsets.UTF_8)).status);
  }

  @Test
  void editionsFeaturesAndUnknownOptionFieldsCannotOverrideProto2Rules() throws Exception {
    var unknown =
        com.google.protobuf.UnknownFieldSet.newBuilder()
            .addField(
                500, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
            .build();
    var base = descriptor(AMOUNT);
    var feature =
        FeatureSet.newBuilder().setFieldPresence(FeatureSet.FieldPresence.IMPLICIT).build();
    var root = base.getFile(1);
    var message = root.getMessageType(0);
    var variants =
        List.of(
            root.toBuilder().setOptions(FileOptions.newBuilder().setFeatures(feature)).build(),
            root.toBuilder().setOptions(FileOptions.newBuilder().setUnknownFields(unknown)).build(),
            root.toBuilder()
                .setMessageType(
                    0,
                    message.toBuilder()
                        .setOptions(MessageOptions.newBuilder().setFeatures(feature)))
                .build(),
            root.toBuilder()
                .setMessageType(
                    0,
                    message.toBuilder()
                        .setOptions(MessageOptions.newBuilder().setUnknownFields(unknown)))
                .build(),
            root.toBuilder()
                .setMessageType(
                    0,
                    message.toBuilder()
                        .setField(
                            0,
                            AMOUNT.toBuilder()
                                .setOptions(FieldOptions.newBuilder().setFeatures(feature))))
                .build(),
            root.toBuilder()
                .setMessageType(
                    0,
                    message.toBuilder()
                        .setField(
                            0,
                            AMOUNT.toBuilder()
                                .setOptions(FieldOptions.newBuilder().setUnknownFields(unknown))))
                .build(),
            root.toBuilder()
                .setMessageType(
                    0, message.toBuilder().setField(0, AMOUNT.toBuilder().setProto3Optional(true)))
                .build());
    for (var variant : variants) {
      var registration =
          schema("a", 1, AMOUNT)
              .put(
                  "descriptor_set",
                  Base64.getEncoder()
                      .encodeToString(base.toBuilder().setFile(1, variant).build().toByteArray()));
      assertEquals(422, call("POST", "/schemas", registration).status, variant.toString());
    }
    assertTrue(call("GET", "/schemas/a/orders", null).body.path("schemas").isEmpty());
  }

  @Test
  void reservedRangesMaySpanImplementationReservedNumbers() throws Exception {
    var set = descriptor(AMOUNT).toBuilder();
    set.setFile(
        1,
        set.getFile(1).toBuilder()
            .setMessageType(
                0,
                set.getFile(1).getMessageType(0).toBuilder()
                    .addReservedRange(
                        DescriptorProto.ReservedRange.newBuilder().setStart(2).setEnd(536870912))));
    var registration =
        schema("a", 1, AMOUNT)
            .put("descriptor_set", Base64.getEncoder().encodeToString(set.build().toByteArray()));
    assertEquals(201, call("POST", "/schemas", registration).status);
    assertEquals(202, call("POST", "/batches", batch("a", "reserved", rec("good"))).status);
    counts(waitFor("a", "reserved"), 1, 1, 0);
  }

  @Test
  void timestampFractionsRetainNanosecondsAndRejectExcessPrecision() throws Exception {
    var timestamp =
        f("when", 2, FieldDescriptorProto.Type.TYPE_MESSAGE, false).toBuilder()
            .setTypeName(".google.protobuf.Timestamp")
            .build();
    register("a", 1, AMOUNT, timestamp);
    var body =
        batch(
            "a",
            "nanos",
            rec("one", 1, "{\"amount\":1,\"when\":\"2026-11-06T01:02:03.1Z\"}"),
            rec("nine", 1, "{\"amount\":1,\"when\":\"2026-11-06T01:02:03.123456789+01:00\"}"),
            rec("too-many", 1, "{\"amount\":1,\"when\":\"2026-11-06T01:02:03.1234567890Z\"}"));
    assertEquals(202, call("POST", "/batches", body).status);
    counts(waitFor("a", "nanos"), 3, 2, 1);
    var rows = records("a");
    var descriptor =
        com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
                descriptor(AMOUNT, timestamp).getFile(1),
                new Descriptors.FileDescriptor[] {TimestampProto.getDescriptor()})
            .getMessageTypes()
            .getFirst();
    int[] nanos = {100_000_000, 123_456_789};
    for (int i = 0; i < rows.size(); i++) {
      var decoded =
          DynamicMessage.parseFrom(
              descriptor, Base64.getDecoder().decode(rows.get(i).path("protobuf_base64").asText()));
      var time = (DynamicMessage) decoded.getField(descriptor.findFieldByName("when"));
      assertEquals(nanos[i], time.getField(time.getDescriptorForType().findFieldByName("nanos")));
      assertEquals(body.path("records").get(i).path("payload"), rows.get(i).path("payload"));
    }
  }

  @Test
  void keysetPagesStayTenantScopedAndValidateCursorParameters() throws Exception {
    register("a", 1, AMOUNT);
    register("b", 1, AMOUNT);
    call("POST", "/batches", batch("a", "first", rec("a1")));
    waitFor("a", "first");
    call("POST", "/batches", batch("b", "middle", rec("b1")));
    waitFor("b", "middle");
    call("POST", "/batches", batch("a", "last", rec("a2"), rec("a3")));
    waitFor("a", "last");
    var seen = new ArrayList<String>();
    long after = 0;
    while (true) {
      var page = call("GET", "/records/a/orders?limit=1&after=" + after, null).body;
      assertEquals(1, page.path("records").size());
      seen.add(page.path("records").get(0).path("record_id").asText());
      if (page.path("next_after").isNull()) break;
      long next = page.path("next_after").longValue();
      assertTrue(next > after);
      after = next;
    }
    assertEquals(List.of("a1", "a2", "a3"), seen);
    assertTrue(
        call("GET", "/records/a/orders?after=9223372036854775807", null)
            .body
            .path("records")
            .isEmpty());
    for (String query :
        List.of("after=-1", "after=1&offset=1", "after=0&after=1", "after=9223372036854775808"))
      assertEquals(400, call("GET", "/records/a/orders?" + query, null).status);
    assertEquals(400, call("GET", "/schemas/a/orders?after=0", null).status);
  }

  @Test
  void inspectionHasSchemaPaginationAndByteBoundedRecordPages() throws Exception {
    register("a", 1, f("s", 1, FieldDescriptorProto.Type.TYPE_STRING, true));
    register("a", 2, f("s", 1, FieldDescriptorProto.Type.TYPE_STRING, true), NOTE);
    var first = call("GET", "/schemas/a/orders?limit=1", null).body;
    assertEquals(1, first.path("schemas").size());
    assertEquals(1, first.path("next_offset").asInt(-1));
    var second = call("GET", "/schemas/a/orders?limit=1&offset=1", null).body;
    assertEquals(2, second.path("schemas").get(0).path("version").asInt());
    assertTrue(second.path("next_offset").isNull());
    String large = "x".repeat(600_000);
    for (int i = 0; i < 8; i++) {
      call("POST", "/batches", batch("a", "b" + i, rec("r" + i, 1, "{\"s\":\"" + large + "\"}")));
      waitFor("a", "b" + i);
    }
    int count = 0, pages = 0, offset = 0;
    do {
      var page = call("GET", "/records/a/orders?limit=1000&offset=" + offset, null).body;
      pages++;
      for (var r : page.path("records")) {
        assertEquals(600_000, r.path("payload").path("s").textValue().length());
        count++;
      }
      offset = page.path("next_offset").asInt(-1);
    } while (offset >= 0);
    assertEquals(8, count);
    assertTrue(pages > 1, "response byte budget must split large records across pages");
  }
}
