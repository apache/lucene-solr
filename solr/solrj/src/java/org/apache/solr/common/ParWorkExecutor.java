package org.apache.solr.common;

import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ParWorkExecutor extends ThreadPoolExecutor {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final long KEEP_ALIVE_TIME = 1000;
    private static final int GROW_BY = 30;

    private final Object lock = new Object();

    private static AtomicInteger threadNumber = new AtomicInteger(1);

    public ParWorkExecutor(String name, int maxPoolsSize) {
        super(0,  maxPoolsSize,  KEEP_ALIVE_TIME, TimeUnit.SECONDS,   new BlockingArrayQueue<>(Integer.getInteger("solr.threadExecQueueSize", 80), GROW_BY), new ThreadFactory() {

            ThreadGroup group;

            {
                SecurityManager s = System.getSecurityManager();
                group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            }

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(group, r, name + threadNumber.getAndIncrement(), 0);
                t.setDaemon(false);
                // t.setPriority(priority);
                return t;
            }
        });

        setRejectedExecutionHandler(new RejectedExecutionHandler() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.warn("Task was rejected, running in caller thread");
                if (executor.isShutdown() || executor.isTerminated() || executor.isTerminating()) {
                    throw new AlreadyClosedException();
                }
                synchronized (lock) {
                    try {
                        lock.wait(10000);
                    } catch (InterruptedException e) {
                       ParWork.propegateInterrupt(e, true);
                    }
                }
                executor.execute(r);
            }
        });
    }



    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        synchronized (lock) {
            lock.notifyAll();
        }
    }

}
