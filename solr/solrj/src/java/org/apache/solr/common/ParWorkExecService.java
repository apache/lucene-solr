package org.apache.solr.common;

import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SysStats;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ParWorkExecService extends AbstractExecutorService {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_AVAILABLE = Math.max(ParWork.PROC_COUNT, 3);
  private final Semaphore available = new Semaphore(MAX_AVAILABLE, false);

  private final ExecutorService service;
  private final int maxSize;
  private volatile boolean terminated;
  private volatile boolean shutdown;

  private final AtomicInteger running = new AtomicInteger();

  private final Object awaitTerminate = new Object();

  private final BlockingArrayQueue<Runnable> workQueue = new BlockingArrayQueue<>(30, 0);
  private volatile Worker worker;
  private volatile Future<?> workerFuture;

  private SysStats sysStats = ParWork.getSysStats();

  private class Worker implements Runnable {

    Worker() {
    //  setName("ParExecWorker");
    }

    @Override
    public void run() {
      while (!terminated && !Thread.currentThread().isInterrupted()) {
        Runnable runnable = null;
        try {
          runnable = workQueue.poll(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
           ParWork.propegateInterrupt(e);
           return;
        }
        if (runnable == null) {
          running.decrementAndGet();
          synchronized (awaitTerminate) {
            awaitTerminate.notifyAll();
          }
          return;
        }

        if (runnable instanceof ParWork.SolrFutureTask) {

        } else {

          try {
            available.acquire();
          } catch (InterruptedException e) {
            ParWork.propegateInterrupt(e);
            running.decrementAndGet();
            synchronized (awaitTerminate) {
              awaitTerminate.notifyAll();
            }
            return;
          }

        }

        Runnable finalRunnable = runnable;
        service.execute(new Runnable() {
          @Override
          public void run() {
            runIt(finalRunnable, true, false, false);
          }
        });
      }
    }
  }

  public ParWorkExecService(ExecutorService service) {
    this(service, -1);
  }


  public ParWorkExecService(ExecutorService service, int maxSize) {
    assert service != null;
    //assert ObjectReleaseTracker.track(this);
    if (maxSize == -1) {
      this.maxSize = MAX_AVAILABLE;
    } else {
      this.maxSize = maxSize;
    }
    this.service = service;
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return new FutureTask(runnable, value);
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    if (callable instanceof ParWork.NoLimitsCallable) {
      return (RunnableFuture) new ParWork.SolrFutureTask(callable);
    }
    return new FutureTask(callable);
  }

  @Override
  public void shutdown() {
    assert ObjectReleaseTracker.release(this);
    this.shutdown = true;
   // worker.interrupt();
  //  workQueue.clear();
//    try {
//      workQueue.offer(new Runnable() {
//        @Override
//        public void run() {
//          // noop to wake from take
//        }
//      });
//      workQueue.offer(new Runnable() {
//        @Override
//        public void run() {
//          // noop to wake from take
//        }
//      });
//      workQueue.offer(new Runnable() {
//        @Override
//        public void run() {
//          // noop to wake from take
//        }
//      });


   //   workerFuture.cancel(true);
//    } catch (NullPointerException e) {
//      // okay
//    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown();
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
        throw new RuntimeException("Timeout");
      }

     //zaa System.out.println("WAIT : " + workQueue.size() + " " + available.getQueueLength() + " " + workQueue.toString());
      synchronized (awaitTerminate) {
        awaitTerminate.wait(500);
      }
    }
//    workQueue.clear();

//    workerFuture.cancel(true);
    terminated = true;
  //  workerFuture.cancel(true);

   // worker.interrupt();
    return true;
  }


  @Override
  public void execute(Runnable runnable) {

    if (shutdown) {
      runIt(runnable, false, true, true);
      return;
    }
    running.incrementAndGet();
    if (runnable instanceof ParWork.SolrFutureTask) {
      try {
        service.execute(new Runnable() {
          @Override
          public void run() {
            runIt(runnable, false, false, false);
          }
        });
      } catch (Exception e) {
        running.decrementAndGet();
        synchronized (awaitTerminate) {
          awaitTerminate.notifyAll();
        }
      }
      return;
    }

    boolean acquired = false;
    if (runnable instanceof ParWork.SolrFutureTask) {

    } else {
      if (!checkLoad()) {
        runIt(runnable, false, true, false);
        return;
      }

      if (!available.tryAcquire()) {
        runIt(runnable, false, true, false);
        return;
      } else {
        acquired = true;
      }
    }

    Runnable finalRunnable = runnable;
    try {
      boolean finalAcquired = acquired;
      service.execute(new Runnable() {
      @Override
      public void run() {
          runIt(finalRunnable, finalAcquired, false, false);
      }
    });
    } catch (Exception e) {
      running.decrementAndGet();
      synchronized (awaitTerminate) {
        awaitTerminate.notifyAll();
      }
    }


//    boolean success = this.workQueue.offer(runnable);
//    if (!success) {
//     // log.warn("No room in the queue, running in caller thread {} {} {} {}", workQueue.size(), isShutdown(), isTerminated(), worker.isAlive());
//      try {
//        runnable.run();
//      } finally {
//        running.decrementAndGet();
//        synchronized (awaitTerminate) {
//          awaitTerminate.notifyAll();
//        }
//      }
//    } else {
//      if (worker == null) {
//        synchronized (this) {
//          if (worker == null) {
//            worker = new Worker();
//
//            workerFuture = ParWork.getEXEC().submit(worker);
//          }
//        }
//      }
//    }
  }

  private void runIt(Runnable runnable, boolean acquired, boolean callThreadRuns, boolean alreadyShutdown) {
    try {
      runnable.run();
    } finally {
      try {
        if (acquired) {
          available.release();
        }
      } finally {
        if (!alreadyShutdown) {
          try {
            running.decrementAndGet();
            synchronized (awaitTerminate) {
              awaitTerminate.notifyAll();
            }
          } finally {
            if (!callThreadRuns) ParWork.closeExecutor();
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
      return false;
    } else {
      double sLoad = sysStats.getSystemLoad();
      if (sLoad > ParWork.PROC_COUNT) {
        return false;
      }
    }
    return true;
  }
}
