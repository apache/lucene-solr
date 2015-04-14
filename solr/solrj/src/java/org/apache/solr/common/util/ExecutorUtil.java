package org.apache.solr.common.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class ExecutorUtil {
  public static Logger log = LoggerFactory.getLogger(ExecutorUtil.class);
  
  public static void shutdownNowAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    pool.shutdownNow(); // Cancel currently executing tasks
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
      if (!shutdown) {
        pool.shutdownNow(); // Cancel currently executing tasks
      }
    }
  }
  
  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    shutdownAndAwaitTermination(pool, 60, TimeUnit.SECONDS);
  }

  public static void shutdownAndAwaitTermination(ExecutorService pool, long timeout, TimeUnit timeUnit) {
    pool.shutdown(); // Disable new tasks from being submitted
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(timeout, timeUnit);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
      if (!shutdown) {
        pool.shutdownNow(); // Cancel currently executing tasks
      }
    }
  }

  /**
   * See {@link java.util.concurrent.Executors#newFixedThreadPool(int, ThreadFactory)}
   */
  public static ExecutorService newMDCAwareFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(nThreads, nThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  /**
   * See {@link java.util.concurrent.Executors#newSingleThreadExecutor(ThreadFactory)}
   */
  public static ExecutorService newMDCAwareSingleThreadExecutor(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            threadFactory);
  }

  /**
   * See {@link java.util.concurrent.Executors#newCachedThreadPool(ThreadFactory)}
   */
  public static ExecutorService newMDCAwareCachedThreadPool(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        threadFactory);
  }

  public static class MDCAwareThreadPoolExecutor extends ThreadPoolExecutor {

    private static final int MAX_THREAD_NAME_LEN = 512;

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    @Override
    public void execute(final Runnable command) {
      final Map<String, String> submitterContext = MDC.getCopyOfContextMap();
      String ctxStr = submitterContext != null && !submitterContext.isEmpty() ?
          submitterContext.toString().replace("/", "//") : "";
      final String submitterContextStr = ctxStr.length() <= MAX_THREAD_NAME_LEN ? ctxStr : ctxStr.substring(0, MAX_THREAD_NAME_LEN);
      super.execute(new Runnable() {
        @Override
        public void run() {
          Map<String, String> threadContext = MDC.getCopyOfContextMap();
          final Thread currentThread = Thread.currentThread();
          final String oldName = currentThread.getName();
          if (submitterContext != null && !submitterContext.isEmpty()) {
            MDC.setContextMap(submitterContext);
            currentThread.setName(oldName + "-processing-" + submitterContextStr);
          } else {
            MDC.clear();
          }
          try {
            command.run();
          } finally {
            if (threadContext != null) {
              MDC.setContextMap(threadContext);
            } else {
              MDC.clear();
            }
            currentThread.setName(oldName);
          }
        }
      });
    }
  }
}
