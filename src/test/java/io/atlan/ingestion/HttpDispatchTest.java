package io.atlan.ingestion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HttpDispatchTest {
  @TempDir Path directory;

  @Test
  void symbolicDatabaseAliasesCannotBypassTheOwnershipLock() throws Exception {
    Path dangling = directory.resolve("dangling.db");
    Files.createSymbolicLink(dangling, directory.resolve("missing.db"));
    assertThrows(
        IOException.class,
        () -> {
          try (var unexpected = new IngestionServer(dangling, 0)) {
            fail("A dangling database alias must be rejected before SQLite creates its target");
          }
        });
    Path database = directory.resolve("owner.db");
    try (var owner = new IngestionServer(database, 0)) {
      Path alias = directory.resolve("alias.db");
      Files.createSymbolicLink(alias, database);
      assertThrows(
          IOException.class,
          () -> {
            try (var second = new IngestionServer(alias, 0)) {
              fail("A symbolic alias must not create a second owner of the same database");
            }
          });
    }
  }

  @Test
  void slowOverflowHeadersDoNotBlockTheHttpDispatcher() throws Exception {
    var release = new CountDownLatch(1);
    var occupied = new CountDownLatch(16);
    var overflow = new CountDownLatch(1);
    try (var server = new IngestionServer(directory.resolve("http.db"), 0);
        var client = HttpClient.newHttpClient()) {
      var field = IngestionServer.class.getDeclaredField("requests");
      field.setAccessible(true);
      var requests = (ThreadPoolExecutor) field.get(server);
      var original = requests.getRejectedExecutionHandler();
      requests.setRejectedExecutionHandler(
          (task, executor) -> {
            overflow.countDown();
            original.rejectedExecution(task, executor);
          });
      for (int i = 0; i < 16; i++)
        requests.execute(
            () -> {
              occupied.countDown();
              ConcurrencyTest.await(release);
            });
      assertTrue(occupied.await(2, TimeUnit.SECONDS));
      for (int i = 0; i < 64; i++) requests.execute(() -> {});
      server.start();
      try (var slow = new Socket("127.0.0.1", server.port())) {
        slow.getOutputStream()
            .write(
                "GET /health HTTP/1.1\r\nHost: localhost\r\nX-Wait: "
                    .getBytes(StandardCharsets.US_ASCII));
        slow.getOutputStream().flush();
        assertTrue(overflow.await(2, TimeUnit.SECONDS));
        var response =
            client
                .sendAsync(
                    HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + server.port() + "/health"))
                        .timeout(Duration.ofSeconds(2))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString())
                .get(3, TimeUnit.SECONDS);
        assertEquals(503, response.statusCode());
        assertEquals("server_busy", ApiTest.JSON.readTree(response.body()).path("error").asText());
        assertEquals("1", response.headers().firstValue("Retry-After").orElseThrow());
      } finally {
        release.countDown();
      }
    } finally {
      release.countDown();
    }
  }
}
