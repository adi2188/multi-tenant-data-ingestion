package io.atlan.ingestion;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/** Real child process for abnormal-exit tests; all pause hooks are test-only. */
public final class CrashFixtureProcess {
  public static void main(String[] args) throws Exception {
    String mode = args[1];
    var probe =
        new JdbcProbe(
            new JdbcProbe.Hooks() {
              @Override
              public void afterUpdate(String sql) throws Exception {
                if (mode.equals("queued") && sql.startsWith("INSERT INTO batches")) pause();
              }
            });
    var server =
        new IngestionServer(
            Path.of(args[0]),
            0,
            (customer, record, attempt) -> {
              if (mode.equals("processing") && record.equals("pause")) pause();
            },
            Limits.defaults());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.close();
                  try {
                    probe.close();
                  } catch (Exception ignored) {
                  }
                }));
    server.start();
    System.out.println("READY " + server.port());
    System.out.flush();
    new CountDownLatch(1).await();
  }

  private static void pause() throws InterruptedException {
    System.out.println("PAUSED");
    System.out.flush();
    new CountDownLatch(1).await();
  }
}
