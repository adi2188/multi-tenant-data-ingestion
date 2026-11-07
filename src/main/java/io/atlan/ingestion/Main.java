package io.atlan.ingestion;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

public final class Main {
  public static void main(String[] args) throws Exception {
    int port = 8080;
    Path database = Path.of("data/ingestion.sqlite3");
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--help")) {
        System.out.println(
            "java -jar target/ingestion.jar [--port 8080] [--db data/ingestion.sqlite3]");
        return;
      }
      if (i + 1 >= args.length) throw new IllegalArgumentException("Missing option value");
      switch (args[i]) {
        case "--port" -> port = Integer.parseInt(args[++i]);
        case "--db" -> database = Path.of(args[++i]);
        default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
      }
    }
    var server = new IngestionServer(database, port);
    Runtime.getRuntime().addShutdownHook(new Thread(server::close, "graceful-shutdown"));
    server.start();
    System.out.printf(
        "Ingestion listening on http://127.0.0.1:%d; database=%s%n",
        server.port(), database.toAbsolutePath());
    new CountDownLatch(1).await();
  }
}
