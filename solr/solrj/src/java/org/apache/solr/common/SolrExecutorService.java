package org.apache.solr.common;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

public class SolrExecutorService extends AbstractExecutorService {
  @Override
  public void shutdown() {

  }

  @Override
  public List<Runnable> shutdownNow() {
    return null;
  }

  @Override
  public boolean isShutdown() {
    return false;
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit)
      throws InterruptedException {
    return false;
  }

  @Override
  public void execute(Runnable runnable) {

  }
}
