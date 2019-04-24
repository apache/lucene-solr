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

package org.apache.lucene.monitor;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.NamedThreadFactory;

/**
 * Utility class for concurrently loading queries into a Monitor.
 * <p>
 * This is useful to speed up startup times for a Monitor.  You can use multiple
 * threads to parse and index queries before starting matches.
 * <p>
 * Use as follows:
 * <pre class="prettyprint">
 *     List&lt;QueryError&gt; errors = new ArrayList&lt;&gt;();
 *     try (ConcurrentQueryLoader loader = new ConcurrentQueryLoader(monitor, errors)) {
 *         for (MonitorQuery mq : getQueries()) {
 *             loader.add(mq);
 *         }
 *     }
 * </pre>
 * <p>
 * The Monitor's MonitorQueryParser must be thread-safe for this to work correctly.
 */
public class ConcurrentQueryLoader implements Closeable {

  private final Monitor monitor;
  private final ExecutorService executor;
  private final CountDownLatch shutdownLatch;
  private final BlockingQueue<MonitorQuery> queue;

  private boolean shutdown = false;
  private List<IOException> errors = new ArrayList<>();

  public static final int DEFAULT_QUEUE_SIZE = 2000;

  /**
   * Create a new ConcurrentQueryLoader for a {@link Monitor}
   *
   * @param monitor Monitor
   */
  public ConcurrentQueryLoader(Monitor monitor) {
    this(monitor, Runtime.getRuntime().availableProcessors(), DEFAULT_QUEUE_SIZE);
  }

  /**
   * Create a new ConcurrentQueryLoader
   *
   * @param monitor   the Monitor to load queries to
   * @param threads   the number of threads to use
   * @param queueSize the size of the buffer to hold queries in
   */
  public ConcurrentQueryLoader(Monitor monitor, int threads, int queueSize) {
    this.monitor = monitor;
    this.queue = new LinkedBlockingQueue<>(queueSize);
    this.executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory("loader"));
    this.shutdownLatch = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      this.executor.submit(new Worker(queueSize / threads));
    }
  }

  /**
   * Add a MonitorQuery to the loader's internal buffer
   * <p>
   * If the buffer is full, this will block until there is room to add
   * the MonitorQuery
   *
   * @param mq the monitor query
   * @throws InterruptedException if interrupted while waiting
   */
  public void add(MonitorQuery mq) throws InterruptedException {
    if (shutdown)
      throw new IllegalStateException("ConcurrentQueryLoader has been shutdown, cannot add new queries");
    this.queue.put(mq);
  }

  @Override
  public void close() throws IOException {
    this.shutdown = true;
    this.executor.shutdown();
    try {
      this.shutdownLatch.await();
    } catch (InterruptedException e) {
      // fine
    }
    if (errors.size() > 0) {
      IOException e = new IOException();
      errors.forEach(e::addSuppressed);
      throw e;
    }
  }

  private class Worker implements Runnable {

    final List<MonitorQuery> workerQueue;
    final int queueSize;
    boolean running = true;

    Worker(int queueSize) {
      workerQueue = new ArrayList<>(queueSize);
      this.queueSize = queueSize;
    }

    @Override
    public void run() {
      try {
        while (running) {
          workerQueue.clear();
          drain(queue, workerQueue, queueSize, 100, TimeUnit.MILLISECONDS);
          if (workerQueue.size() == 0 && shutdown)
            running = false;
          if (workerQueue.size() > 0) {
            monitor.register(workerQueue);
          }
        }
      } catch (IOException e) {
        errors.add(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        shutdownLatch.countDown();
      }
    }
  }

  /**
   * Drains the queue as {@link BlockingQueue#drainTo(Collection, int)}, but if the requested
   * {@code numElements} elements are not available, it will wait for them up to the specified
   * timeout.
   * <p>
   * Taken from Google Guava 18.0 Queues
   *
   * @param q           the blocking queue to be drained
   * @param buffer      where to add the transferred elements
   * @param numElements the number of elements to be waited for
   * @param timeout     how long to wait before giving up, in units of {@code unit}
   * @param unit        a {@code TimeUnit} determining how to interpret the timeout parameter
   * @param <E>         the type of the queue
   * @return the number of elements transferred
   * @throws InterruptedException if interrupted while waiting
   */
  private static <E> int drain(BlockingQueue<E> q, Collection<? super E> buffer, int numElements,
                              long timeout, TimeUnit unit) throws InterruptedException {
    buffer = Objects.requireNonNull(buffer);
    /*
     * This code performs one System.nanoTime() more than necessary, and in return, the time to
     * execute Queue#drainTo is not added *on top* of waiting for the timeout (which could make
     * the timeout arbitrarily inaccurate, given a queue that is slow to drain).
     */
    long deadline = System.nanoTime() + unit.toNanos(timeout);
    int added = 0;
    while (added < numElements) {
      // we could rely solely on #poll, but #drainTo might be more efficient when there are multiple
      // elements already available (e.g. LinkedBlockingQueue#drainTo locks only once)
      added += q.drainTo(buffer, numElements - added);
      if (added < numElements) { // not enough elements immediately available; will have to poll
        E e = q.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
        if (e == null) {
          break; // we already waited enough, and there are no more elements in sight
        }
        buffer.add(e);
        added++;
      }
    }
    return added;
  }
}
