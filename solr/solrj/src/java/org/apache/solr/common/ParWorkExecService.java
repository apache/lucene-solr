package org.apache.solr.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ParWorkExecService implements ExecutorService {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_AVAILABLE = Math.max(ParWork.PROC_COUNT / 2, 3);
  private final Semaphore available = new Semaphore(MAX_AVAILABLE, false);

  private final ExecutorService service;
  private final int maxSize;
  private volatile boolean terminated;
  private volatile boolean shutdown;

  public ParWorkExecService(ExecutorService service) {
    this(service, -1);
  }


  public ParWorkExecService(ExecutorService service, int maxSize) {
    assert service != null;
    if (maxSize == -1) {
      this.maxSize = MAX_AVAILABLE;
    } else {
      this.maxSize = maxSize;
    }
    this.service = service;
  }

  @Override
  public void shutdown() {
    this.shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    this.shutdown = true;
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return terminated;
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit)
      throws InterruptedException {
    while (available.hasQueuedThreads()) {
      Thread.sleep(100);
    }
    terminated = true;
    return true;
  }

  @Override
  public <T> Future<T> submit(Callable<T> callable) {
    return doSubmit(callable, false);
  }


  public <T> Future<T> doSubmit(Callable<T> callable, boolean requiresAnotherThread) {
    try {
      if (!requiresAnotherThread) {
        boolean success = checkLoad();
        if (success) {
          success = available.tryAcquire();
        }
        if (!success) {
          return CompletableFuture.completedFuture(callable.call());
        }
      } else {
        return service.submit(new Callable<T>() {
          @Override
          public T call() throws Exception {
            try {
              return callable.call();
            } finally {
              available.release();
            }
          }
        });
      }
      Future<T> future = service.submit(callable);
      return future;
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public <T> Future<T> submit(Runnable runnable, T t) {
    boolean success = checkLoad();
    if (success) {
      success = available.tryAcquire();
    }
    if (!success) {
      runnable.run();
      return CompletableFuture.completedFuture(null);
    }
    return service.submit(new Runnable() {
      @Override
      public void run() {
        try {
          runnable.run();
        } finally {
          available.release();
        }
      }
    }, t);

  }

  @Override
  public Future<?> submit(Runnable runnable) {
    return doSubmit(runnable, false);
  }

  public Future<?> doSubmit(Runnable runnable, boolean requiresAnotherThread) {
    try {
      if (!requiresAnotherThread) {
        boolean success = checkLoad();
        if (success) {
          success = available.tryAcquire();
        }
        if (!success) {
          runnable.run();
          return CompletableFuture.completedFuture(null);
        }
      } else {
        return service.submit(runnable);
      }
      Future<?> future = service.submit(new Runnable() {
        @Override
        public void run() {
          try {
            runnable.run();
          } finally {
            available.release();
          }
        }
      });

      return future;
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> collection)
      throws InterruptedException {

    List<Future<T>> futures = new ArrayList<>(collection.size());
    for (Callable c : collection) {
      futures.add(submit(c));
    }
    Exception exception = null;
    for (Future<T> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        log.error("invokeAll execution exception", e);
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exception);
    return futures;
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
      throws InterruptedException {
    // nocommit
    return invokeAll(collection);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> collection)
      throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l,
      TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(Runnable runnable) {
    execute(runnable, false);
  }


  public void execute(Runnable runnable, boolean requiresAnotherThread) {
    if (requiresAnotherThread) {
       service.submit(runnable);
       return;
    }

    boolean success = checkLoad();
    if (success) {
      success = available.tryAcquire();
    }
    if (!success) {
      runnable.run();
      return;
    }
    service.execute(new Runnable() {
      @Override
      public void run() {
        try {
          runnable.run();
        } finally {
          available.release();
        }
      }
    });

  }

  public Integer getMaximumPoolSize() {
    return maxSize;
  }

  public boolean checkLoad() {
    double load = ManagementFactory.getOperatingSystemMXBean()
        .getSystemLoadAverage();
    if (load < 0) {
      log.warn("SystemLoadAverage not supported on this JVM");
    }

    double ourLoad = ParWork.getSysStats().getAvarageUsagePerCPU();

    if (ourLoad > 99.0D) {
      return false;
    } else {
      double sLoad = load / (double) ParWork.PROC_COUNT;
      if (sLoad > ParWork.PROC_COUNT) {
        return false;
      }
      if (log.isDebugEnabled()) log.debug("ParWork, load:" + sLoad);

    }
    return true;
  }
}
