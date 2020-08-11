package org.apache.solr.common;

import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ParWorkExecutor extends ExecutorUtil.MDCAwareThreadPoolExecutor {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final int KEEP_ALIVE_TIME = 1;

  private static AtomicInteger threadNumber = new AtomicInteger(0);

  public ParWorkExecutor(String name, int maxPoolsSize) {
    this(name, 0, maxPoolsSize, KEEP_ALIVE_TIME);
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize) {
    this(name, corePoolsSize, maxPoolsSize, KEEP_ALIVE_TIME);
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize,
      int keepalive) {
    super(corePoolsSize, maxPoolsSize, keepalive, TimeUnit.MILLISECONDS,
        new SynchronousQueue<>(), new ThreadFactory() {

          ThreadGroup group;

          {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ?
                s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
          }

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                name + threadNumber.getAndIncrement(), 0) {
              public void run() {
                try {
                  r.run();
                } finally {
                  ParWork.closeExecutor();
                }
              }
            };
            t.setDaemon(true);

            // t.setPriority(priority);
            return t;
          }
        });

    setRejectedExecutionHandler(new CallerRunsPolicy());
  }

  public void shutdown() {
   // allowCoreThreadTimeOut(true);
    super.shutdown();
  }
}
