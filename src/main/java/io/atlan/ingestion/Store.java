package io.atlan.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/** Independent WAL readers, serialized SQLite writes, and a cache-only LRU lock. */
final class Store implements AutoCloseable {
  private static final int PAGE_BYTES = 8 * 1024 * 1024;
  private final Connection writer;
  private final ReentrantLock writeLock = new ReentrantLock(true);
  private final ReadPool metadataReads;
  private final ReadPool inspectionReads;
  private final Object[] schemaLocks = new Object[64];
  private final Map<String, ProtobufSchema> cache =
      new LinkedHashMap<>(32, .75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, ProtobufSchema> e) {
          return size() > 256;
        }
      };

  Store(Path path) throws Exception {
    Files.createDirectories(path.toAbsolutePath().getParent());
    Arrays.setAll(schemaLocks, ignored -> new Object());
    String url = "jdbc:sqlite:" + path.toAbsolutePath();
    Connection opened = DriverManager.getConnection(url);
    ReadPool metadata = null, inspection = null;
    try {
      initialize(opened);
      metadata = new ReadPool(url, 4);
      inspection = new ReadPool(url, 2);
    } catch (Exception ex) {
      if (inspection != null) inspection.close();
      if (metadata != null) metadata.close();
      opened.close();
      throw ex;
    }
    writer = opened;
    metadataReads = metadata;
    inspectionReads = inspection;
  }

  private static void initialize(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute("PRAGMA journal_mode=WAL");
      statement.execute("PRAGMA synchronous=FULL");
      statement.execute("PRAGMA busy_timeout=1000");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS schemas (customer TEXT NOT NULL,dataset TEXT NOT NULL,version"
              + " INTEGER NOT NULL,body TEXT NOT NULL,PRIMARY KEY(customer,dataset,version))");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS batches (customer TEXT NOT NULL,dataset TEXT NOT NULL,batch"
              + " TEXT NOT NULL,fingerprint TEXT NOT NULL,status TEXT NOT"
              + " NULL,report TEXT NOT NULL,PRIMARY KEY(customer,dataset,batch))");
      boolean legacyBody = false;
      try (var columns = statement.executeQuery("PRAGMA table_info(batches)")) {
        while (columns.next()) legacyBody |= columns.getString("name").equals("body");
      }
      // SQLite makes this migration atomic; existing identities and reports survive.
      if (legacyBody) statement.execute("ALTER TABLE batches DROP COLUMN body");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY AUTOINCREMENT,customer TEXT"
              + " NOT NULL,dataset TEXT NOT NULL,batch TEXT NOT NULL,position INTEGER NOT"
              + " NULL,record_id TEXT NOT NULL,schema_version INTEGER NOT NULL,payload TEXT NOT"
              + " NULL,protobuf BLOB NOT NULL,UNIQUE(customer,dataset,batch,position))");
      statement.execute(
          "CREATE INDEX IF NOT EXISTS records_tenant_dataset ON records(customer,dataset,id)");
    }
  }

  private static PreparedStatement query(Connection connection, String sql, Object... values)
      throws SQLException {
    var statement = connection.prepareStatement(sql);
    for (int i = 0; i < values.length; i++) statement.setObject(i + 1, values[i]);
    return statement;
  }

  private int execute(String sql, Object... values) throws SQLException {
    writeLock.lock();
    try (var statement = query(writer, sql, values)) {
      return statement.executeUpdate();
    } finally {
      writeLock.unlock();
    }
  }

  private static JsonNode fromText(String text) {
    return Json.parse(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  record Registration(boolean created, JsonNode body) {}

  Registration register(ProtobufSchema next) throws SQLException {
    String canonical = next.registration.toString();
    String key = next.customer + "/" + next.dataset;
    // Only registrations in this dataset (or the same bounded stripe) serialize.
    // Descriptor compilation/compatibility checks never hold the writer or cache lock.
    synchronized (schemaLocks[Math.floorMod(key.hashCode(), schemaLocks.length)]) {
      String existing = schemaBody(next.customer, next.dataset, next.version);
      if (existing != null) {
        var body = fromText(existing);
        if (!Json.canonical(body).toString().equals(canonical)
            && !ProtobufSchema.parse(body).sameSchemaAs(next))
          throw new ApiException(
              409, "schema_conflict", "This version already has different content");
        return new Registration(false, body);
      }
      String latest =
          metadataReads.use(
              connection -> {
                try (var statement =
                        query(
                            connection,
                            "SELECT body FROM schemas WHERE customer=? AND dataset=? ORDER BY"
                                + " version DESC LIMIT 1",
                            next.customer,
                            next.dataset);
                    var rows = statement.executeQuery()) {
                  return rows.next() ? rows.getString(1) : null;
                }
              });
      if (latest != null) next.checkEvolution(ProtobufSchema.parse(fromText(latest)));
      execute(
          "INSERT INTO schemas VALUES (?,?,?,?)",
          next.customer,
          next.dataset,
          next.version,
          canonical);
      synchronized (cache) {
        cache.put(next.customer + "/" + next.dataset + "/" + next.version, next);
      }
      return new Registration(true, next.registration.deepCopy());
    }
  }

  private String schemaBody(String customer, String dataset, int version) throws SQLException {
    return metadataReads.use(
        connection -> {
          try (var statement =
                  query(
                      connection,
                      "SELECT body FROM schemas WHERE customer=? AND dataset=? AND version=?",
                      customer,
                      dataset,
                      version);
              var rows = statement.executeQuery()) {
            return rows.next() ? rows.getString(1) : null;
          }
        });
  }

  ProtobufSchema schema(String customer, String dataset, int version) throws SQLException {
    String key = customer + "/" + dataset + "/" + version;
    synchronized (cache) {
      var hit = cache.get(key);
      if (hit != null) return hit;
    }
    String body = schemaBody(customer, dataset, version);
    if (body == null) return null;
    var compiled = ProtobufSchema.parse(fromText(body));
    synchronized (cache) {
      var existing = cache.get(key);
      if (existing != null) return existing;
      cache.put(key, compiled);
    }
    return compiled;
  }

  private record RawPage<T>(List<T> rows, boolean more) {}

  JsonNode schemas(String customer, String dataset, int limit, int offset) throws SQLException {
    RawPage<String> page =
        inspectionReads.use(
            connection -> {
              var bodies = new ArrayList<String>();
              long bytes = 512;
              boolean more = false;
              try (var statement =
                      query(
                          connection,
                          "SELECT body,length(CAST(body AS BLOB)) FROM schemas WHERE customer=? AND"
                              + " dataset=? ORDER BY version LIMIT ? OFFSET ?",
                          customer,
                          dataset,
                          limit + 1,
                          offset);
                  var rows = statement.executeQuery()) {
                while (rows.next()) {
                  long size = rows.getLong(2) + 1;
                  if (bodies.size() >= limit || (bytes + size > PAGE_BYTES && !bodies.isEmpty())) {
                    more = true;
                    break;
                  }
                  bodies.add(rows.getString(1));
                  bytes += size;
                }
              }
              return new RawPage<>(bodies, more);
            });
    // Release the read snapshot and pooled connection before parsing response JSON.
    var result = Json.object().put("limit", limit).put("offset", offset);
    var list = result.putArray("schemas");
    for (String body : page.rows()) list.add(fromText(body));
    if (page.more()) result.put("next_offset", offset + list.size());
    else result.putNull("next_offset");
    return result;
  }

  record Batch(String fingerprint, JsonNode report) {}

  private record RawBatch(String fingerprint, String report) {}

  Batch batch(String customer, String dataset, String batch) throws SQLException {
    RawBatch raw =
        metadataReads.use(
            connection -> {
              try (var statement =
                      query(
                          connection,
                          "SELECT fingerprint,report FROM batches WHERE customer=? AND dataset=?"
                              + " AND batch=?",
                          customer,
                          dataset,
                          batch);
                  var rows = statement.executeQuery()) {
                return rows.next() ? new RawBatch(rows.getString(1), rows.getString(2)) : null;
              }
            });
    return raw == null ? null : new Batch(raw.fingerprint(), fromText(raw.report()));
  }

  void reserve(String customer, String dataset, String batch, String hash, JsonNode report)
      throws SQLException {
    String encodedReport = report.toString();
    execute(
        "INSERT INTO batches(customer,dataset,batch,fingerprint,status,report)"
            + " VALUES (?,?,?,?,'queued',?)",
        customer,
        dataset,
        batch,
        hash,
        encodedReport);
  }

  void interruptUnfinishedBatches() throws SQLException {
    // The server's exclusive database ownership lock excludes live jobs from another process.
    execute(
        "UPDATE batches SET status='interrupted',report=json_set(report,'$.status','interrupted',"
            + "'$.retryable',json('true'),'$.interrupted_at',?,'$.reason',?)"
            + " WHERE status IN ('queued','processing')",
        Instant.now().toString(),
        "Processing stopped before completion; resubmit the identical batch to resume");
  }

  Set<Integer> storedPositions(String customer, String dataset, String batch) throws SQLException {
    return metadataReads.use(
        connection -> {
          var positions = new HashSet<Integer>();
          try (var statement =
                  query(
                      connection,
                      "SELECT position FROM records WHERE customer=? AND dataset=? AND batch=?",
                      customer,
                      dataset,
                      batch);
              var rows = statement.executeQuery()) {
            while (rows.next()) positions.add(rows.getInt(1));
          }
          return Set.copyOf(positions);
        });
  }

  void resume(String customer, String dataset, String batch, String hash, JsonNode report)
      throws SQLException {
    int changed =
        execute(
            "UPDATE batches SET status='queued',report=? WHERE customer=? AND dataset=? AND batch=?"
                + " AND fingerprint=? AND status IN ('interrupted','queued','processing')",
            report.toString(),
            customer,
            dataset,
            batch,
            hash);
    if (changed != 1)
      throw new ApiException(409, "batch_conflict", "Batch is no longer available for resumption");
  }

  void status(String customer, String dataset, String batch, JsonNode report) throws SQLException {
    String encodedReport = report.toString();
    execute(
        "UPDATE batches SET status=?,report=? WHERE customer=? AND dataset=? AND batch=?"
            + " AND status IN ('queued','processing')",
        report.path("status").asText(),
        encodedReport,
        customer,
        dataset,
        batch);
  }

  void write(
      String customer, String dataset, String batch, int position, JsonNode record, byte[] protobuf)
      throws SQLException {
    String payload = record.path("payload").toString();
    // JSON serialization happens before taking the writer lock. A committed
    // position still has exactly one row, including after an uncertain retry.
    execute(
        "INSERT INTO"
            + " records(customer,dataset,batch,position,record_id,schema_version,payload,protobuf)"
            + " VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(customer,dataset,batch,position) DO NOTHING",
        customer,
        dataset,
        batch,
        position,
        record.path("record_id").asText(),
        record.path("schema_version").asInt(),
        payload,
        protobuf);
  }

  private record StoredRecord(
      long rowId,
      String batch,
      int position,
      String id,
      int version,
      String payload,
      byte[] protobuf) {}

  JsonNode records(String customer, String dataset, int limit, int offset, Long after)
      throws SQLException {
    RawPage<StoredRecord> page =
        inspectionReads.use(
            connection -> {
              var records = new ArrayList<StoredRecord>();
              long bytes = 512;
              boolean more = false;
              String sql =
                  after == null
                      ? "SELECT id,batch,position,record_id,schema_version,length(CAST(payload AS"
                          + " BLOB)),length(protobuf),payload,protobuf FROM records WHERE"
                          + " customer=? AND dataset=? ORDER BY id LIMIT ? OFFSET ?"
                      : "SELECT id,batch,position,record_id,schema_version,length(CAST(payload AS"
                          + " BLOB)),length(protobuf),payload,protobuf FROM records WHERE"
                          + " customer=? AND dataset=? AND id>? ORDER BY id LIMIT ?";
              Object[] params =
                  after == null
                      ? new Object[] {customer, dataset, limit + 1, offset}
                      : new Object[] {customer, dataset, after, limit + 1};
              try (var statement = query(connection, sql, params);
                  var rows = statement.executeQuery()) {
                while (rows.next()) {
                  // Exact payload/Base64 sizes plus a conservative envelope bound for four
                  // <=128-byte ASCII identifiers and scalar metadata. Json.MAPPER emits
                  // supplementary characters as UTF-8 to match these sizes. No JSON work here.
                  long size = rows.getLong(6) + 4 * ((rows.getLong(7) + 2) / 3) + 1024;
                  if (records.size() >= limit
                      || (bytes + size > PAGE_BYTES && !records.isEmpty())) {
                    more = true;
                    break;
                  }
                  records.add(
                      new StoredRecord(
                          rows.getLong(1),
                          rows.getString(2),
                          rows.getInt(3),
                          rows.getString(4),
                          rows.getInt(5),
                          rows.getString(8),
                          rows.getBytes(9)));
                  bytes += size;
                }
              }
              return new RawPage<>(records, more);
            });
    // Parsing, Base64 and JSON construction hold neither a DB connection nor a lock.
    var result = Json.object().put("limit", limit).put("offset", offset);
    if (after != null) result.put("after", after);
    var records = result.putArray("records");
    for (var row : page.rows()) {
      var item =
          Json.object()
              .put("customer_id", customer)
              .put("dataset_id", dataset)
              .put("batch_id", row.batch())
              .put("position", row.position())
              .put("record_id", row.id())
              .put("schema_version", row.version());
      item.set("payload", fromText(row.payload()));
      item.put("protobuf_base64", Base64.getEncoder().encodeToString(row.protobuf()));
      records.add(item);
    }
    if (page.more()) {
      result.put("next_offset", offset + records.size());
      if (after != null) result.put("next_after", page.rows().getLast().rowId());
    } else {
      result.putNull("next_offset");
      if (after != null) result.putNull("next_after");
    }
    return result;
  }

  @FunctionalInterface
  private interface Read<T> {
    T run(Connection connection) throws SQLException;
  }

  private static final class ReadPool implements AutoCloseable {
    private final ArrayBlockingQueue<Connection> available;
    private final List<Connection> connections = new ArrayList<>();

    ReadPool(String url, int size) throws SQLException {
      available = new ArrayBlockingQueue<>(size);
      try {
        for (int i = 0; i < size; i++) {
          Connection connection = DriverManager.getConnection(url);
          connections.add(connection);
          try (var statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout=1000");
            statement.execute("PRAGMA query_only=ON");
          }
          available.add(connection);
        }
      } catch (SQLException ex) {
        close();
        throw ex;
      }
    }

    <T> T use(Read<T> work) throws SQLException {
      Connection connection;
      try {
        connection = available.take();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new SQLException("Interrupted waiting for a read connection", ex);
      }
      try {
        return work.run(connection);
      } finally {
        available.add(connection);
      }
    }

    @Override
    public void close() {
      SQLException failure = null;
      for (Connection connection : connections) {
        try {
          connection.close();
        } catch (SQLException ex) {
          if (failure == null) failure = ex;
          else failure.addSuppressed(ex);
        }
      }
      if (failure != null) throw new IllegalStateException(failure);
    }
  }

  @Override
  public void close() {
    // Owner drains HTTP requests, admissions and record workers before closing.
    inspectionReads.close();
    metadataReads.close();
    writeLock.lock();
    try {
      writer.close();
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    } finally {
      writeLock.unlock();
    }
  }
}
