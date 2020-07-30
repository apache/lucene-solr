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

  private static final int MAX_AVAILABLE = ParWork.PROC_COUNT;
  private final Semaphore available = new Semaphore(MAX_AVAILABLE, true);

  private final Phaser phaser = new Phaser(1) {
    @Override
    protected boolean onAdvance(int phase, int parties) {
      return false;
    }
  };

  private final ExecutorService service;
  private volatile boolean terminated;
  private volatile boolean shutdown;

  public ParWorkExecService(ExecutorService service) {
    assert service != null;
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
      Thread.sleep(50);
    }
    terminated = true;
    return true;
  }

  public void awaitOutstanding(long l, TimeUnit timeUnit)
      throws InterruptedException {
    while (available.hasQueuedThreads()) {
      Thread.sleep(50);
    }
  }

  @Override
  public <T> Future<T> submit(Callable<T> callable) {
    return doSubmit(callable, false);
  }


  public <T> Future<T> doSubmit(Callable<T> callable, boolean requiresAnotherThread) {
    if (shutdown || terminated) {
      throw new RejectedExecutionException();
    }
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
        available.acquireUninterruptibly();
      }
      Future<T> future = service.submit(callable);
      return new Future<T>() {
        @Override
        public boolean cancel(boolean b) {
          return future.cancel(b);
        }

        @Override
        public boolean isCancelled() {
          return future.isCancelled();
        }

        @Override
        public boolean isDone() {
          return future.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
          T ret;
          try {
            ret = future.get();
          } finally {
            available.release();
          }

          return ret;
        }

        @Override
        public T get(long l, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
          T ret;
          try {
            ret = future.get(l, timeUnit);
          } finally {
            available.release();
          }
          return ret;
        }
      };
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public <T> Future<T> submit(Runnable runnable, T t) {
    if (shutdown || terminated) {
      throw new RejectedExecutionException();
    }
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
    if (shutdown || terminated) {
      throw new RejectedExecutionException();
    }
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
      available.acquireUninterruptibly();
    }
    Future<?> future = service.submit(runnable);

    return new Future<>() {
      @Override
      public boolean cancel(boolean b) {
        return future.cancel(b);
      }

      @Override
      public boolean isCancelled() {
        return future.isCancelled();
      }

      @Override
      public boolean isDone() {
        return future.isDone();
      }

      @Override
      public Object get() throws InterruptedException, ExecutionException {
        Object ret;
        try {
          ret = future.get();
        } finally {
          available.release();
        }

        return ret;
      }

      @Override
      public Object get(long l, TimeUnit timeUnit)
          throws InterruptedException, ExecutionException, TimeoutException {
        Object ret;
        try {
          ret = future.get();
        } finally {
          available.release();
        }
        return ret;
      }

    };

  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> collection)
      throws InterruptedException {
    if (shutdown || terminated) {
      throw new RejectedExecutionException();
    }
    List<Future<T>> futures = new ArrayList<>(collection.size());
    for (Callable c : collection) {
      futures.add(submit(c));
    }

    for (Future<T> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        log.error("invokeAll execution exception", e);
        //throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
    return futures;
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
      throws InterruptedException {
    throw new UnsupportedOperationException();
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
    if (shutdown || terminated) {
      throw new RejectedExecutionException();
    }
    boolean success = checkLoad();
    if (success) {
      success = available.tryAcquire();
    }
    if (!success) {
      try {
        runnable.run();
      } finally {
        available.release();
      }
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
    return MAX_AVAILABLE;
  }

  public boolean checkLoad() {
    double load = ManagementFactory.getOperatingSystemMXBean()
        .getSystemLoadAverage();
    if (load < 0) {
      log.warn("SystemLoadAverage not supported on this JVM");
    }

    double ourLoad = ParWork.getSysStats().getAvarageUsagePerCPU();
    if (ourLoad > 1) {
      return false;
    } else {
      double sLoad = load / (double) ParWork.PROC_COUNT;
      if (sLoad > 1.0D) {
        return false;
      }
      if (log.isDebugEnabled()) log.debug("ParWork, load:" + sLoad);

    }
    return true;
  }
}
