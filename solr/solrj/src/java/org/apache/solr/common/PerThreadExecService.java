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
  private final boolean callerThreadAllowed;
  private final boolean callerThreadUsesAvailableLimit;
  private volatile boolean terminated;
  private volatile boolean shutdown;

  private final AtomicInteger running = new AtomicInteger();

  private CloseTracker closeTracker;

  private SysStats sysStats = ParWork.getSysStats();
  private volatile boolean closeLock;

  public PerThreadExecService(ExecutorService service) {
    this(service, -1);
  }

  public PerThreadExecService(ExecutorService service, int maxSize) {
    this(service, maxSize, false, false);
  }
  
  public PerThreadExecService(ExecutorService service, int maxSize, boolean callerThreadAllowed, boolean callerThreadUsesAvailableLimit) {
    assert service != null;
    assert (closeTracker = new CloseTracker()) != null;
    this.callerThreadAllowed = callerThreadAllowed;
    this.callerThreadUsesAvailableLimit = callerThreadUsesAvailableLimit;
    if (maxSize == -1) {
      this.maxSize = MAX_AVAILABLE;
    } else {
      this.maxSize = maxSize;
    }
    this.service = service;
    running.incrementAndGet();
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return (RunnableFuture) new ParWork.SolrFutureTask(runnable, value, callerThreadAllowed);
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    if (callable instanceof ParWork.ParWorkCallableBase) {
      return (RunnableFuture) new ParWork.SolrFutureTask(callable, ((ParWork.ParWorkCallableBase) callable).isCallerThreadAllowed());
    } else {
      return (RunnableFuture) new ParWork.SolrFutureTask(callable, true);
    }
  }

  @Override
  public void shutdown() {
//    if (closeLock) {
//      throw new IllegalCallerException();
//    }
   // assert closeTracker.close();
    assert ObjectReleaseTracker.release(this);
    this.shutdown = true;
    running.decrementAndGet();
    synchronized (running) {
      running.notifyAll();
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    running.decrementAndGet();
    synchronized (running) {
      running.notifyAll();
    }
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
    synchronized (running) {

      while (running.get() > 0) {
        if (timeout.hasTimedOut()) {
          log.error("return before reaching termination, wait for {} {}, running={}", l, timeout, running);
          return false;
        }

        // System.out.println("WAIT : " + workQueue.size() + " " + available.getQueueLength() + " " + workQueue.toString());
        running.wait(500);
      }
    }
    if (isShutdown()) {
      terminated = true;
    }
    return true;
  }


  @Override
  public void execute(Runnable runnable) {

    running.incrementAndGet();
    if (runnable instanceof ParWork.SolrFutureTask && !((ParWork.SolrFutureTask) runnable).isCallerThreadAllowed()) {
      if (callerThreadUsesAvailableLimit) {
        try {
          available.acquire();
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          running.decrementAndGet();
          synchronized (running) {
            running.notifyAll();
          }
          throw new RejectedExecutionException("Interrupted");
        }
      }
      try {
        service.submit(new MyThreadCallable(runnable, available, running, callerThreadUsesAvailableLimit));
      } catch (Exception e) {
        log.error("", e);
        if (callerThreadUsesAvailableLimit) {
          available.release();
        }
        running.decrementAndGet();
        synchronized (running) {
          running.notifyAll();
        }
        throw e;
      }
      return;
    }

    boolean acquired = available.tryAcquire();
    if (!acquired && callerThreadAllowed) {
      runIt(runnable, available, running, false);
      return;
    }

    Runnable finalRunnable = runnable;
    try {
      service.submit(new MyThreadCallable(finalRunnable, available, running,true));
    } catch (Exception e) {
      log.error("Exception submitting", e);
      try {
        available.release();
      } finally {
        running.decrementAndGet();
        synchronized (running) {
          running.notifyAll();
        }
      }
      throw e;
    }
  }

  private static void runIt(Runnable runnable, Semaphore available, AtomicInteger running, boolean acquired) {
    try {
      runnable.run();
    } finally {
      try {
        if (acquired) {
          available.release();
        }
      } finally {
        running.decrementAndGet();
        synchronized (running) {
          running.notifyAll();
        }
      }
    }
  }

  public Integer getMaximumPoolSize() {
    return maxSize;
  }

  private boolean checkLoad() {

    double sLoad = sysStats.getSystemLoad();

    if (hiStateLoad(sLoad)) {
      return true;
    }
    return false;
  }

  private boolean hiStateLoad(double sLoad) {
    return sLoad > 0.8d && running.get() > 3;
  }

  public void closeLock(boolean lock) {
    if (lock) {
      closeTracker.enableCloseLock();
    } else {
      closeTracker.disableCloseLock();
    }
  }

  public static class MyThreadCallable implements Callable<Object> {
    private final Runnable runnable;
    private final boolean acquired;
    private final Semaphore available;
    private final AtomicInteger running;

    public MyThreadCallable(Runnable runnable, Semaphore available, AtomicInteger running,  boolean acquired) {
      this.runnable = runnable;
      this.acquired = acquired;
      this.available = available;
      this.running = running;
    }

    public Runnable getRunnable() {
      return runnable;
    }

    public Object call() {
      runIt(runnable, available, running, acquired);
      return null;
    }
  }
}
