package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.component.Dumpable;
import org.eclipse.jetty.util.thread.Scheduler;

public class SolrHttpClientScheduler extends AbstractLifeCycle implements Scheduler, Dumpable {
  private final String name;
  private final boolean daemon;
  private final ClassLoader classloader;
  private final ThreadGroup threadGroup;
  private volatile ScheduledThreadPoolExecutor scheduler;
  private volatile Thread thread;
  private int coreThreads;

  public SolrHttpClientScheduler() {
    this(null, false);
  }

  public SolrHttpClientScheduler(String name, boolean daemon) {
    this(name, daemon, Thread.currentThread().getContextClassLoader());
  }

  public SolrHttpClientScheduler(String name, boolean daemon, ClassLoader threadFactoryClassLoader) {
    this(name, daemon, threadFactoryClassLoader, null, 1);
  }

  public SolrHttpClientScheduler(String name, boolean daemon, ClassLoader threadFactoryClassLoader,
      ThreadGroup threadGroup, int coreThreads) {
    this.name = name == null ? "Scheduler-" + hashCode() : name;
    this.coreThreads = coreThreads;
    this.daemon = daemon;
    this.classloader = threadFactoryClassLoader == null ? Thread.currentThread().getContextClassLoader()
        : threadFactoryClassLoader;
    this.threadGroup = threadGroup;
  }

  @Override
  protected void doStart() throws Exception {
    scheduler = new ScheduledThreadPoolExecutor(coreThreads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = SolrHttpClientScheduler.this.thread = new Thread(threadGroup, r, name);
        thread.setDaemon(daemon);
        thread.setContextClassLoader(classloader);
        return thread;
      }
    });
    scheduler.setRemoveOnCancelPolicy(true);
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    scheduler.shutdownNow();
    super.doStop();
    scheduler = null;
  }

  @Override
  public Task schedule(Runnable task, long delay, TimeUnit unit) {
    ScheduledThreadPoolExecutor s = scheduler;
    if (s == null)
      return () -> false;
    ScheduledFuture<?> result = s.schedule(task, delay, unit);
    return new ScheduledFutureTask(result);
  }

  @Override
  public String dump() {
    return ContainerLifeCycle.dump(this);
  }

  @Override
  public void dump(Appendable out, String indent) throws IOException {
    ContainerLifeCycle.dumpObject(out, this);
    Thread thread = this.thread;
    if (thread != null) {
      List<StackTraceElement> frames = Arrays.asList(thread.getStackTrace());
      ContainerLifeCycle.dump(out, indent, frames);
    }
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