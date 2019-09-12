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
package org.apache.solr.common.util;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class ExecutorUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static volatile List<InheritableThreadLocalProvider> providers = new ArrayList<>();

  public synchronized static void addThreadLocalProvider(InheritableThreadLocalProvider provider) {
    for (InheritableThreadLocalProvider p : providers) {//this is to avoid accidental multiple addition of providers in tests
      if (p.getClass().equals(provider.getClass())) return;
    }
    List<InheritableThreadLocalProvider> copy = new ArrayList<>(providers);
    copy.add(provider);
    providers = copy;
  }

  /** Any class which wants to carry forward the threadlocal values to the threads run
   * by threadpools must implement this interface and the implementation should be
   * registered here
   */
  public interface InheritableThreadLocalProvider {
    /**This is invoked in the parent thread which submitted a task.
     * copy the necessary Objects to the ctx. The object that is passed is same
     * across all three methods
     */
    public void store(AtomicReference<?> ctx);

    /**This is invoked in the Threadpool thread. set the appropriate values in the threadlocal
     * of this thread.     */
    public void set(AtomicReference<?> ctx);

    /**This method is invoked in the threadpool thread after the execution
     * clean all the variables set in the set method
     */
    public void clean(AtomicReference<?> ctx);
  }

  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    if(pool == null) return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTermination(pool);
  }

  public static void awaitTermination(ExecutorService pool) {
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
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
            new LinkedBlockingQueue<>(),
            threadFactory);
  }

  /**
   * Create a cached thread pool using a named thread factory
   */
  public static ExecutorService newMDCAwareCachedThreadPool(String name) {
    return newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory(name));
  }

  /**
   * See {@link java.util.concurrent.Executors#newCachedThreadPool(ThreadFactory)}
   */
  public static ExecutorService newMDCAwareCachedThreadPool(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        threadFactory);
  }

  public static ExecutorService newMDCAwareCachedThreadPool(int maxThreads, ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(0, maxThreads,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(maxThreads),
        threadFactory);
  }

  @SuppressForbidden(reason = "class customizes ThreadPoolExecutor so it can be used instead")
  public static class MDCAwareThreadPoolExecutor extends ThreadPoolExecutor {

    private static final int MAX_THREAD_NAME_LEN = 512;

    private final boolean enableSubmitterStackTrace;

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
      this.enableSubmitterStackTrace = true;
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
      this.enableSubmitterStackTrace = true;
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, true);
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, boolean enableSubmitterStackTrace) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
      this.enableSubmitterStackTrace = enableSubmitterStackTrace;
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
      this.enableSubmitterStackTrace = true;
    }

    @Override
    public void execute(final Runnable command) {
      final Map<String, String> submitterContext = MDC.getCopyOfContextMap();
      StringBuilder contextString = new StringBuilder();
      if (submitterContext != null) {
        Collection<String> values = submitterContext.values();

        for (String value : values) {
          contextString.append(value).append(' ');
        }
        if (contextString.length() > 1) {
          contextString.setLength(contextString.length() - 1);
        }
      }

      String ctxStr = contextString.toString().replace("/", "//");
      final String submitterContextStr = ctxStr.length() <= MAX_THREAD_NAME_LEN ? ctxStr : ctxStr.substring(0, MAX_THREAD_NAME_LEN);
      final Exception submitterStackTrace = enableSubmitterStackTrace ? new Exception("Submitter stack trace") : null;
      final List<InheritableThreadLocalProvider> providersCopy = providers;
      final ArrayList<AtomicReference> ctx = providersCopy.isEmpty() ? null : new ArrayList<>(providersCopy.size());
      if (ctx != null) {
        for (int i = 0; i < providers.size(); i++) {
          AtomicReference reference = new AtomicReference();
          ctx.add(reference);
          providersCopy.get(i).store(reference);
        }
      }
      super.execute(() -> {
        isServerPool.set(Boolean.TRUE);
        if (ctx != null) {
          for (int i = 0; i < providersCopy.size(); i++) providersCopy.get(i).set(ctx.get(i));
        }
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
        } catch (Throwable t) {
          if (t instanceof OutOfMemoryError) {
            throw t;
          }
          if (enableSubmitterStackTrace)  {
            log.error("Uncaught exception {} thrown by thread: {}", t, currentThread.getName(), submitterStackTrace);
          } else  {
            log.error("Uncaught exception {} thrown by thread: {}", t, currentThread.getName());
          }
          throw t;
        } finally {
          isServerPool.remove();
          if (threadContext != null && !threadContext.isEmpty()) {
            MDC.setContextMap(threadContext);
          } else {
            MDC.clear();
          }
          if (ctx != null) {
            for (int i = 0; i < providersCopy.size(); i++) providersCopy.get(i).clean(ctx.get(i));
          }
          currentThread.setName(oldName);
        }
      });
    }
  }

  private static final ThreadLocal<Boolean> isServerPool = new ThreadLocal<>();

  /// this tells whether a thread is owned/run by solr or not.
  public static boolean isSolrServerThread() {
    return Boolean.TRUE.equals(isServerPool.get());
  }

  public static void setServerThreadFlag(Boolean flag) {
    if (flag == null) isServerPool.remove();
    else isServerPool.set(flag);

  }

}
