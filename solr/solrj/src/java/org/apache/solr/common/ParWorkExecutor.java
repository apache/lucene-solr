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
                Thread t = new Thread(group, r, name + threadNumber.getAndIncrement(), 0) {
                    public void run() {
                        super.run();
                        ParWork.close(ParWork.getExecutor());
                    }
                };
                t.setDaemon(true);
                // t.setPriority(priority);
                return t;
            }
        });

        setRejectedExecutionHandler(new CallerRunsPolicy());

        allowCoreThreadTimeOut(true);
    }
}
