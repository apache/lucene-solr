package org.apache.solr.common;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SysStats;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerThreadExecService extends AbstractExecutorService {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_AVAILABLE = Math.max(ParWork.PROC_COUNT / 2, 3);
  private final Semaphore available = new Semaphore(MAX_AVAILABLE, false);

  private final ExecutorService service;
  private final int maxSize;
  private final boolean noCallerRunsAllowed;
  private final boolean noCallerRunsAvailableLimit;
  private volatile boolean terminated;
  private volatile boolean shutdown;

  private final AtomicInteger running = new AtomicInteger();

  private final Object awaitTerminate = new Object();

  private CloseTracker closeTracker;

  private SysStats sysStats = ParWork.getSysStats();
  private volatile boolean closeLock;

  public PerThreadExecService(ExecutorService service) {
    this(service, -1);
  }

  public PerThreadExecService(ExecutorService service, int maxSize) {
    this(service, maxSize, false, false);
  }
  
  public PerThreadExecService(ExecutorService service, int maxSize, boolean noCallerRunsAllowed, boolean noCallerRunsAvailableLimit) {
    assert service != null;
    assert (closeTracker = new CloseTracker()) != null;
    this.noCallerRunsAllowed = noCallerRunsAllowed;
    this.noCallerRunsAvailableLimit = noCallerRunsAvailableLimit;
    if (maxSize == -1) {
      this.maxSize = MAX_AVAILABLE;
    } else {
      this.maxSize = maxSize;
    }
    this.service = service;
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    if (noCallerRunsAllowed) {
      return (RunnableFuture) new ParWork.SolrFutureTask(runnable, value, false);
    }
    return (RunnableFuture) new ParWork.SolrFutureTask(runnable, value);
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    if (noCallerRunsAllowed || callable instanceof ParWork.NoLimitsCallable) {
      return (RunnableFuture) new ParWork.SolrFutureTask(callable, false);
    }
    return (RunnableFuture) new ParWork.SolrFutureTask(callable, true);
  }

  @Override
  public void shutdown() {
//    if (closeLock) {
//      throw new IllegalCallerException();
//    }
   // assert closeTracker.close();
    assert ObjectReleaseTracker.release(this);
    this.shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return !available.hasQueuedThreads() && shutdown;
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit)
      throws InterruptedException {
    TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (running.get() > 0) {
      if (timeout.hasTimedOut()) {
        log.warn("return before reaching termination, wait for {} {}, running={}", l, timeout, running);
        return false;
      }

      // System.out.println("WAIT : " + workQueue.size() + " " + available.getQueueLength() + " " + workQueue.toString());
      synchronized (awaitTerminate) {
        awaitTerminate.wait(500);
      }
    }

    if (isShutdown()) {
      terminated = true;
    }
    return true;
  }


  @Override
  public void execute(Runnable runnable) {

    if (shutdown) {
      throw new RejectedExecutionException();
    }
    running.incrementAndGet();
    if (runnable instanceof ParWork.SolrFutureTask && !((ParWork.SolrFutureTask) runnable).isCallerThreadAllowed()) {
      if (noCallerRunsAvailableLimit) {
        try {
          available.acquire();
        } catch (InterruptedException e) {
          throw new RejectedExecutionException("Interrupted");
        }
      }
      try {
        service.submit(() -> {
          runIt(runnable, noCallerRunsAvailableLimit, false);
          if (noCallerRunsAvailableLimit) {
            available.release();
          }
        });
      } catch (Exception e) {
        log.error("", e);
        running.decrementAndGet();
        synchronized (awaitTerminate) {
          awaitTerminate.notifyAll();
        }
      }
      return;
    }

    if (!checkLoad()) {
      runIt(runnable, false, false);
      return;
    }

    if (!available.tryAcquire()) {
      runIt(runnable, false, false);
      return;
    }

    Runnable finalRunnable = runnable;
    try {
      service.submit(() -> runIt(finalRunnable, true, false));
    } catch (Exception e) {
      running.decrementAndGet();
      synchronized (awaitTerminate) {
        awaitTerminate.notifyAll();
      }
    }
  }

  private void runIt(Runnable runnable, boolean acquired, boolean alreadyShutdown) {
    try {
      runnable.run();
    } finally {
      try {
        if (acquired) {
          available.release();
        }
      } finally {
        if (!alreadyShutdown) {
          running.decrementAndGet();
          synchronized (awaitTerminate) {
            awaitTerminate.notifyAll();
          }
        }
      }
    }
  }

  public Integer getMaximumPoolSize() {
    return maxSize;
  }

  public boolean checkLoad() {

    double ourLoad = ParWork.getSysStats().getTotalUsage();
    if (ourLoad > SysStats.OUR_LOAD_HIGH) {
      if (log.isDebugEnabled()) log.debug("Our cpu usage is too high, run in caller thread {}", ourLoad);
      return false;
    } else {
      double sLoad = sysStats.getSystemLoad();
      if (sLoad > 1) {
        if (log.isDebugEnabled()) log.debug("System load is too high, run in caller thread {}", sLoad);
        return false;
      }
    }
    return true;
  }
  
  public void closeLock(boolean lock) {
    if (lock) {
      closeTracker.enableCloseLock();
    } else {
      closeTracker.disableCloseLock();
    }
  }

}
