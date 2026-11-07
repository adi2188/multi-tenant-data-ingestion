package io.atlan.ingestion;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** Deterministic test/demo-only injection, always before the real database write. */
final class FaultWriter implements WriteInterceptor {
  final Map<String, Long> delays;
  final Map<String, Integer> failures;
  final Set<String> permanent;
  final CountDownLatch started = new CountDownLatch(1);
  final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

  FaultWriter(Map<String, Long> delays, Map<String, Integer> failures, Set<String> permanent) {
    this.delays = Map.copyOf(delays);
    this.failures = Map.copyOf(failures);
    this.permanent = Set.copyOf(permanent);
  }

  @Override
  public void beforeWrite(String customer, String recordId, int attempt) throws Exception {
    String key = customer + "/" + recordId;
    int n = calls.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
    started.countDown();
    Thread.sleep(delays.getOrDefault(customer, 0L));
    if (permanent.contains(key)) throw new IOException("injected permanent write failure");
    if (n <= failures.getOrDefault(key, 0))
      throw new TransientStorageException("injected transient write failure");
  }

  int attempts(String key) {
    return calls.getOrDefault(key, new AtomicInteger()).get();
  }
}
