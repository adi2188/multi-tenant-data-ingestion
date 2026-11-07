package io.atlan.ingestion;

/** Optional storage boundary decorator; normal service operation uses a no-op. */
@FunctionalInterface
public interface WriteInterceptor {
  void beforeWrite(String customer, String recordId, int attempt) throws Exception;
}
