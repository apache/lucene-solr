package org.apache.solr.common;

import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.FuturePromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ParWorkExecutor extends ThreadPoolExecutor {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final long KEEP_ALIVE_TIME = 1;
    private static final int GROW_BY = 30;

    private final Object lock = new Object();

    private static AtomicInteger threadNumber = new AtomicInteger(0);

    public ParWorkExecutor(String name, int maxPoolsSize) {
        super(0,  maxPoolsSize,  KEEP_ALIVE_TIME, TimeUnit.SECONDS, new ArrayBlockingQueue<>(Integer.getInteger("solr.threadExecQueueSize", 30)), new ThreadFactory() {

            ThreadGroup group;

            {
                SecurityManager s = System.getSecurityManager();
                group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            }

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(group, r, name + threadNumber.getAndIncrement(), 0);
                t.setDaemon(true);
                // t.setPriority(priority);
                return t;
            }
        });

        setRejectedExecutionHandler(new RejectedExecutionHandler() {
            private volatile Runnable last;
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                throw new RejectedExecutionException();
//                if (last == r) {
//                    return;
//                }

//                log.warn("Task was rejected, running in caller thread name" + name + " shutdown:" + isShutdown() + " terminated:" + isTerminated() + " terminating:" + isTerminating());
//                if (executor.isShutdown() || executor.isTerminated() || executor.isTerminating() || Thread.currentThread().isInterrupted()) {
//                    return;
//                }
//
////                synchronized (lock) {
////                    try {
////                        Thread.sleep(1000);
////                    } catch (InterruptedException e) {
////                       ParWork.propegateInterrupt(e, true);
////                       return;
////                    }
//                try {
//                executor.submit(r);
//                last = r;
//                } catch (Exception e) {
//                    if (e instanceof InterruptedException) {
//                        Thread.currentThread().interrupt();
//                    }
//                }
             }
        });

        allowCoreThreadTimeOut(true);
    }

    public Future<?> submit(Runnable task) {
        if (task == null) {
            throw new NullPointerException();
        } else {
            if (getActiveCount() == getMaximumPoolSize() && getQueue().remainingCapacity() == 0) {
                try {
                    task.run();
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Exception running task in caller thread");
                    }
                }
                return CompletableFuture.completedFuture(new Object());
            }
            RunnableFuture<Object> ftask = super.newTaskFor(task, (Object)null);
            try {
                this.execute(ftask);
            } catch (RejectedExecutionException e) {
                task.run();
                return CompletableFuture.completedFuture(new Object());
            }
            return ftask;
        }
    }

    public <T> Future<T> submit(Runnable task, T result) {
        if (task == null) {
            throw new NullPointerException();
        } else {
            if (getActiveCount() == getMaximumPoolSize() && getQueue().remainingCapacity() == 0) {
                try {
                    task.run();
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Exception running task in caller thread");
                    }
                }
                return CompletableFuture.completedFuture(result);
            }
            RunnableFuture<T> ftask = this.newTaskFor(task, result);
            try {
                this.execute(ftask);
            } catch (RejectedExecutionException e) {
                ftask.run();
                return CompletableFuture.completedFuture(result);
            }
            return ftask;
        }
    }

    public <T> Future<T> submit(Callable<T> task) {
        if (task == null) {
            throw new NullPointerException();
        } else {
            if (getActiveCount() == getMaximumPoolSize() && getQueue().remainingCapacity() == 0) {
                T res = null;
                try {
                   res = task.call();
                } catch (Exception e) {
                   if (e instanceof InterruptedException) {
                       Thread.currentThread().interrupt();
                       throw new RuntimeException("Exception running task in caller thread");
                   }
                }
                CompletableFuture<Object> future = new CompletableFuture<>();
                future.complete(res);
                return (Future<T>) future;
            }
            RunnableFuture<T> ftask = this.newTaskFor(task);
            try {
                this.execute(ftask);
            } catch (RejectedExecutionException e) {
                ftask.run();
                CompletableFuture<Object> future = new CompletableFuture<>();
                future.complete(new Object());
                return (Future<T>) future;
            }
            return ftask;
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
//        synchronized (lock) {
//            lock.notifyAll();
//        }
    }

}
