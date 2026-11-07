package io.atlan.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

/** Public HTTP API. No framework, cloud service, or protobuf compiler at runtime. */
public final class IngestionServer implements AutoCloseable {
  public static final int MAX_REQUEST_BYTES = 2 * 1024 * 1024;
  private static final ThreadLocal<Boolean> OVERLOADED = ThreadLocal.withInitial(() -> false);

  static {
    System.setProperty("sun.net.httpserver.maxReqTime", "10");
    System.setProperty("sun.net.httpserver.maxRspTime", "10");
    System.setProperty("jdk.httpserver.maxConnections", "128");
  }

  private final HttpServer http;
  private final Store store;
  private final IngestionService service;
  private final ThreadPoolExecutor requests;
  // Parsing an overload response still reads HTTP headers. Keep that work off
  // the selector/dispatcher thread, and bound it independently of normal work.
  private final ThreadPoolExecutor overflowRequests =
      new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(128));
  private final FileChannel lockChannel;
  private final FileLock fileLock;
  private volatile boolean closed;

  public IngestionServer(Path database, int port) throws Exception {
    this(database, port, null, Limits.defaults());
  }

  public IngestionServer(Path database, int port, WriteInterceptor interceptor, Limits limits)
      throws Exception {
    java.nio.file.Files.createDirectories(database.toAbsolutePath().getParent());
    // Resolve aliases before choosing both the SQLite/WAL path and ownership lock.
    // Otherwise a symlink could open the same database with a different .lock file.
    database =
        java.nio.file.Files.exists(database, java.nio.file.LinkOption.NOFOLLOW_LINKS)
            ? database.toRealPath()
            : database.toAbsolutePath().getParent().toRealPath().resolve(database.getFileName());
    lockChannel =
        FileChannel.open(
            Path.of(database.toAbsolutePath() + ".lock"),
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.WRITE);
    FileLock acquired;
    try {
      acquired = lockChannel.tryLock();
    } catch (OverlappingFileLockException ex) {
      lockChannel.close();
      throw new IOException("Database is already owned by a running service", ex);
    }
    if (acquired == null) {
      lockChannel.close();
      throw new IOException("Database is already owned by a running service");
    }
    fileLock = acquired;
    Store opened = null;
    HttpServer bound = null;
    try {
      opened = new Store(database);
      bound = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 64);
      http = bound;
      store = opened;
      service = new IngestionService(store, interceptor, limits);
      requests =
          new ThreadPoolExecutor(
              16,
              16,
              0,
              TimeUnit.MILLISECONDS,
              new ArrayBlockingQueue<>(64),
              Executors.defaultThreadFactory(),
              (runnable, executor) -> {
                if (executor.isShutdown()) return;
                overflowRequests.execute(
                    () -> {
                      OVERLOADED.set(true);
                      try {
                        runnable.run();
                      } finally {
                        OVERLOADED.remove();
                      }
                    });
              });
      http.setExecutor(requests);
      http.createContext("/", this::handle);
    } catch (Exception ex) {
      if (bound != null) bound.stop(0);
      if (opened != null) opened.close();
      fileLock.release();
      lockChannel.close();
      throw ex;
    }
  }

  public void start() {
    http.start();
  }

  public int port() {
    return http.getAddress().getPort();
  }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      if (OVERLOADED.get())
        throw new ApiException(503, "server_busy", "HTTP request queue is full; retry later");
      if (closed)
        throw new ApiException(503, "shutting_down", "Service is draining accepted batches");
      String method = exchange.getRequestMethod();
      String path = exchange.getRequestURI().getPath();
      if (path.equals("/health") && method.equals("GET")) {
        send(exchange, 200, Json.object().put("status", "ok"));
        return;
      }
      if (path.equals("/schemas") && method.equals("POST")) {
        var registration = store.register(ProtobufSchema.parse(body(exchange)));
        send(exchange, registration.created() ? 201 : 200, registration.body());
        return;
      }
      if (path.equals("/batches") && method.equals("POST")) {
        var submission = service.submit(body(exchange));
        send(exchange, submission.status(), submission.report());
        return;
      }
      String[] parts = path.split("/", -1);
      if (method.equals("GET") && parts.length == 5 && parts[1].equals("batches")) {
        checkIds(parts, 2);
        var batch = store.batch(parts[2], parts[3], parts[4]);
        if (batch == null) throw new ApiException(404, "batch_not_found", "Batch was not found");
        send(exchange, 200, batch.report());
        return;
      }
      if (method.equals("GET")
          && parts.length == 4
          && (parts[1].equals("schemas") || parts[1].equals("records"))) {
        checkIds(parts, 2);
        boolean allowAfter = parts[1].equals("records");
        var query = query(exchange.getRequestURI().getRawQuery(), allowAfter);
        int limit = query.getOrDefault("limit", 100L).intValue();
        int offset = query.getOrDefault("offset", 0L).intValue();
        if (parts[1].equals("schemas"))
          send(exchange, 200, store.schemas(parts[2], parts[3], limit, offset));
        else {
          send(exchange, 200, store.records(parts[2], parts[3], limit, offset, query.get("after")));
        }
        return;
      }
      throw new ApiException(404, "not_found", "Route was not found");
    } catch (ApiException ex) {
      if (ex.status == 429 || ex.status == 503)
        exchange.getResponseHeaders().set("Retry-After", "1");
      send(
          exchange, ex.status, Json.object().put("error", ex.code).put("message", ex.getMessage()));
    } catch (SQLException ex) {
      exchange.getResponseHeaders().set("Retry-After", "1");
      send(
          exchange,
          503,
          Json.object()
              .put("error", "store_unavailable")
              .put("message", "Persistent store is unavailable; retry later"));
    } catch (Exception ex) {
      System.err.println("HTTP request failed: " + ex.getClass().getSimpleName());
      send(
          exchange,
          500,
          Json.object().put("error", "internal_error").put("message", "Unexpected service error"));
    } finally {
      exchange.close();
    }
  }

  private static JsonNode body(HttpExchange exchange) throws IOException {
    String type = exchange.getRequestHeaders().getFirst("Content-Type");
    if (type == null || !type.split(";", 2)[0].trim().equalsIgnoreCase("application/json"))
      throw new ApiException(415, "content_type", "Use application/json");
    String length = exchange.getRequestHeaders().getFirst("Content-Length");
    if (length != null) {
      try {
        if (Long.parseLong(length) > MAX_REQUEST_BYTES)
          throw new ApiException(413, "request_too_large", "Maximum request size is 2 MiB");
      } catch (NumberFormatException ex) {
        throw new ApiException(400, "invalid_length", "Invalid Content-Length");
      }
    }
    byte[] bytes = exchange.getRequestBody().readNBytes(MAX_REQUEST_BYTES + 1);
    if (bytes.length > MAX_REQUEST_BYTES)
      throw new ApiException(413, "request_too_large", "Maximum request size is 2 MiB");
    return Json.parse(bytes);
  }

  private static void checkIds(String[] parts, int start) {
    for (int i = start; i < parts.length; i++)
      if (!Json.ID.matcher(parts[i]).matches())
        throw new ApiException(400, "invalid_identifier", "Invalid path identifier");
  }

  private static Map<String, Long> query(String raw, boolean allowAfter) {
    var result = new HashMap<String, Long>();
    if (raw == null || raw.isEmpty()) return result;
    Set<String> allowed =
        allowAfter ? Set.of("limit", "offset", "after") : Set.of("limit", "offset");
    for (String pair : raw.split("&")) {
      String[] parts = pair.split("=", -1);
      try {
        if (parts.length != 2 || !allowed.contains(parts[0]) || result.containsKey(parts[0]))
          throw new IllegalArgumentException();
        long value = Long.parseLong(parts[1]);
        if (value < 0
            || (parts[0].equals("limit") && (value < 1 || value > 1000))
            || (parts[0].equals("offset") && value > Integer.MAX_VALUE))
          throw new IllegalArgumentException();
        result.put(parts[0], value);
      } catch (IllegalArgumentException ex) {
        throw new ApiException(
            400, "invalid_pagination", "Use limit=1..1000 and nonnegative offset or after cursor");
      }
    }
    if (result.containsKey("after") && result.getOrDefault("offset", 0L) != 0L)
      throw new ApiException(
          400, "invalid_pagination", "Cannot combine nonzero offset with after cursor");
    return result;
  }

  private void send(HttpExchange exchange, int status, JsonNode node) throws IOException {
    byte[] bytes = Json.MAPPER.writeValueAsBytes(node);
    exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    exchange.getResponseHeaders().set("Cache-Control", "no-store");
    if (closed || status >= 400) exchange.getResponseHeaders().set("Connection", "close");
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
  }

  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    requests.shutdown();
    overflowRequests.shutdown();
    boolean interrupted = false;
    for (var executor : List.of(requests, overflowRequests))
      for (; ; )
        try {
          if (executor.awaitTermination(15, TimeUnit.SECONDS)) break;
        } catch (InterruptedException ex) {
          interrupted = true;
        }
    http.stop(0);
    service.close();
    store.close();
    try {
      fileLock.release();
      lockChannel.close();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
    if (interrupted) Thread.currentThread().interrupt();
  }
}
