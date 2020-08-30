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

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.annotation.Name;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.Dumpable;
import org.eclipse.jetty.util.thread.Scheduler;

/**
 * Implementation of {@link Scheduler} based on Jetty's ScheduledExecutorScheduler which is based on the
 * on JDK's {@link ScheduledThreadPoolExecutor}.
 * <p>
 * While use of {@link ScheduledThreadPoolExecutor} creates futures that will not be used,
 * it has the advantage of allowing to set a property to remove cancelled tasks from its
 * queue even if the task did not fire, which provides a huge benefit in the performance
 * of garbage collection in young generation.
 */
public class SolrScheduledExecutorScheduler extends AbstractLifeCycle implements Scheduler, Dumpable {
  private final String name;
  private final boolean daemon;
  private final ClassLoader classloader;
  private final ThreadGroup threadGroup;
  private final int threads;
  private final AtomicInteger count = new AtomicInteger();
  private volatile ScheduledThreadPoolExecutor scheduler;
  private volatile Thread thread;

  public SolrScheduledExecutorScheduler() {
    this(null);
  }

  public SolrScheduledExecutorScheduler(String name) {
    this(name, null);
  }

  public SolrScheduledExecutorScheduler(@Name("name") String name, @Name("threads") int threads) {
    this(name, null, null, threads);
  }

  public SolrScheduledExecutorScheduler(String name, ClassLoader classLoader) {
    this(name, classLoader, null);
  }

  public SolrScheduledExecutorScheduler(String name, ClassLoader classLoader, ThreadGroup threadGroup) {
    this(name, classLoader, threadGroup, -1);
  }

  /**
   * @param name        The name of the scheduler threads or null for automatic name
   * @param classLoader The classloader to run the threads with or null to use the current thread context classloader
   * @param threadGroup The threadgroup to use or null for no thread group
   * @param threads     The number of threads to pass to the the core {@link ScheduledThreadPoolExecutor} or -1 for a
   *                    heuristic determined number of threads.
   */
  public SolrScheduledExecutorScheduler(@Name("name") String name, @Name("classLoader") ClassLoader classLoader, @Name("threadGroup") ThreadGroup threadGroup, @Name("threads") int threads) {
    this.name = StringUtil.isBlank(name) ? "Scheduler-" + hashCode() : name;
    this.daemon = true;
    this.classloader = classLoader == null ? Thread.currentThread().getContextClassLoader() : classLoader;
    this.threadGroup = threadGroup;
    this.threads = threads;
  }

  @Override
  protected void doStart() throws Exception {
    int size = threads > 0 ? threads : 1;
    assert  scheduler == null;
    scheduler = new ScheduledThreadPoolExecutor(size, r -> {
      Thread thread = SolrScheduledExecutorScheduler.this.thread = new Thread(threadGroup, r, name + "-" + count.incrementAndGet());
      thread.setDaemon(daemon);
      thread.setContextClassLoader(classloader);
      return thread;
    });
    scheduler.setRemoveOnCancelPolicy(true);
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    ScheduledThreadPoolExecutor fscheduler = scheduler;
    if (fscheduler != null) {
      fscheduler.shutdownNow();
      super.doStop();
      ExecutorUtil.awaitTermination(fscheduler);
    }
    scheduler = null;
  }

  @Override
  public Task schedule(Runnable task, long delay, TimeUnit unit) {
    ScheduledThreadPoolExecutor s = scheduler;
    if (s == null) return () -> false;
    ScheduledFuture<?> result = s.schedule(task, delay, unit);
    return new ScheduledFutureTask(result);
  }

  @Override
  public String dump() {
    return Dumpable.dump(this);
  }

  @Override
  public void dump(Appendable out, String indent) throws IOException {
    Thread thread = this.thread;
    if (thread == null) Dumpable.dumpObject(out, this);
    else Dumpable.dumpObjects(out, indent, this, (Object[]) thread.getStackTrace());
  }

  private static class ScheduledFutureTask implements Task {
    private final ScheduledFuture<?> scheduledFuture;

    ScheduledFutureTask(ScheduledFuture<?> scheduledFuture) {
      this.scheduledFuture = scheduledFuture;
    }

    @Override
    public boolean cancel() {
      return scheduledFuture.cancel(false);
    }
  }
}
