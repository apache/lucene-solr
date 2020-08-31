//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.solr.handler.component;

import org.apache.solr.common.PerThreadExecService;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

public class SolrExecutorCompletionService<V> implements CompletionService<V> {
  private final PerThreadExecService executor;
  private final BlockingQueue<Future<V>> completionQueue;

  private RunnableFuture<V> newTaskFor(Callable<V> task) {
    return (RunnableFuture)new FutureTask(task);
  }

  private RunnableFuture<V> newTaskFor(Runnable task, V result) {
    return (RunnableFuture) new FutureTask(task, result);
  }

  public SolrExecutorCompletionService(PerThreadExecService executor) {
    if (executor == null) {
      throw new NullPointerException();
    } else {
      this.executor = executor;
      this.completionQueue = new LinkedBlockingQueue();
    }
  }

  public SolrExecutorCompletionService(PerThreadExecService executor, BlockingQueue<Future<V>> completionQueue) {
    if (executor != null && completionQueue != null) {
      this.executor = executor;
      this.completionQueue = completionQueue;
    } else {
      throw new NullPointerException();
    }
  }

  public Future<V> submit(Callable<V> task) {
    if (task == null) {
      throw new NullPointerException();
    } else {
      RunnableFuture<V> f = this.newTaskFor(task);
      this.executor.execute(new SolrExecutorCompletionService.QueueingFuture(f, this.completionQueue));
      return f;
    }
  }

  public Future<V> submit(Runnable task, V result) {
    if (task == null) {
      throw new NullPointerException();
    } else {
      RunnableFuture<V> f = this.newTaskFor(task, result);
      this.executor.submit(new SolrExecutorCompletionService.QueueingFuture(f, this.completionQueue)); // nocommit - dont limit thread usage as much
      return f;
    }
  }

  public Future<V> take() throws InterruptedException {
    return (Future)this.completionQueue.take();
  }

  public Future<V> poll() {
    return (Future)this.completionQueue.poll();
  }

  public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
    return (Future)this.completionQueue.poll(timeout, unit);
  }

  private static class QueueingFuture<V> extends FutureTask<Void> {
    private final Future<V> task;
    private final BlockingQueue<Future<V>> completionQueue;

    QueueingFuture(RunnableFuture<V> task, BlockingQueue<Future<V>> completionQueue) {
      super(task, null);
      this.task = task;
      this.completionQueue = completionQueue;
    }

    protected void done() {
      this.completionQueue.add(this.task);
    }
  }
}
