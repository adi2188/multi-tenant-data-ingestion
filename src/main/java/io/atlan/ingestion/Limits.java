package io.atlan.ingestion;

public record Limits(
    int maxPending,
    int maxPendingPerCustomer,
    long maxPendingBytes,
    long maxPendingBytesPerCustomer) {
  public Limits {
    if (maxPending < 1
        || maxPendingPerCustomer < 1
        || maxPendingBytes < 1
        || maxPendingBytesPerCustomer < 1
        || maxPendingBytesPerCustomer > maxPendingBytes)
      throw new IllegalArgumentException(
          "Limits must be positive and maxPendingBytesPerCustomer <= maxPendingBytes");
  }

  public Limits(int maxPending, int maxPendingPerCustomer, long maxPendingBytes) {
    this(
        maxPending,
        maxPendingPerCustomer,
        maxPendingBytes,
        Math.max(1L, Math.min(maxPendingBytes, 8L * 1024 * 1024)));
  }

  public static Limits defaults() {
    return new Limits(100, 10, 32L * 1024 * 1024, 8L * 1024 * 1024);
  }
}
