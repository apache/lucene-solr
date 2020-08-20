package org.apache.solr.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ParWorkExecutor extends ThreadPoolExecutor {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final int KEEP_ALIVE_TIME = 5000;

  private static AtomicInteger threadNumber = new AtomicInteger(0);
  private volatile boolean closed;

  public ParWorkExecutor(String name, int maxPoolsSize) {
    this(name, 0, maxPoolsSize, KEEP_ALIVE_TIME, new SynchronousQueue<>());
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize) {
    this(name, corePoolsSize, maxPoolsSize, KEEP_ALIVE_TIME,  new SynchronousQueue<>());
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize,
      int keepalive, BlockingQueue<Runnable> workQueue) {
    super(corePoolsSize, maxPoolsSize, keepalive, TimeUnit.MILLISECONDS, workQueue
    , new ThreadFactory() {

          ThreadGroup group;

          {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ?
                s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
          }

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(group,
                name + threadNumber.getAndIncrement()) {
              public void run() {
                try {
                  r.run();
                } finally {
                  ParWork.closeExecutor();
                }
              }
            };
            //t.setDaemon(true);

            // t.setPriority(priority);
            return t;
          }
        });

    //setRejectedExecutionHandler(new CallerRunsPolicy());
  }

  public void shutdown() {
    this.closed = true;
//    if (!isShutdown()) {
//      // wake up idle threads!
//      for (int i = 0; i < getPoolSize(); i++) {
//        submit(new Runnable() {
//          @Override
//          public void run() {
//
//          }
//        });
//      }
//    }
    super.shutdown();
  }
}
