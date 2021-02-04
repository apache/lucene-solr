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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;

public class OrderedExecutor extends ExecutorCompletionService {
  private final ExecutorService delegate;
  private final SparseStripedLock<Integer> sparseStripedLock;

  public OrderedExecutor(int numThreads, ExecutorService delegate) {
    super(delegate);
    this.delegate = delegate;
    this.sparseStripedLock = new SparseStripedLock<>(numThreads);
  }

  /**
   * Execute the given command in the future.
   * If another command with same {@code lockId} is waiting in the queue or running,
   * this method will block until that command finish.
   * Therefore different commands with same {@code hash} will be executed in order of calling this method.
   *
   * If multiple caller are waiting for a command to finish, there are no guarantee that the earliest call will win.
   *
   * @param lockId of the {@code command}, if null then a random hash will be generated
   * @param command the runnable task
   *
   * @throws RejectedExecutionException if this task cannot be accepted for execution
   * @return future for task
   */
  public Future<?> submit(Integer lockId, Runnable command) {
    try {
      sparseStripedLock.add(lockId);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException(e);
    }

    try {
      return delegate.submit(new ParWork.SolrFutureTask("TLogOrderedExec", () -> {
        try {
          command.run();
        } finally {
          sparseStripedLock.remove(lockId);
        }

        return null;
      }));
    } catch (Exception e) {
      sparseStripedLock.remove(lockId);
      throw e;
    }
  }

  public void shutdown() {
    delegate.shutdown();
  }

  public void shutdownAndAwaitTermination() {
    ExecutorUtil.shutdownAndAwaitTermination(delegate);
  }

  public void awaitTermination() {
    ExecutorUtil.awaitTermination(delegate);
  }

  public void shutdownNow() {
    delegate.shutdownNow();
  }

  /** A set of locks by a key {@code T}, kind of like Google Striped but the keys are sparse/lazy. */
  private static class SparseStripedLock<T> {
    private final ConcurrentHashMap<T, CountDownLatch> map;
    private final Semaphore sizeSemaphore;

    SparseStripedLock(int maxSize) {
      map = new ConcurrentHashMap<>(maxSize, 0.90f, maxSize);
      this.sizeSemaphore = new Semaphore(maxSize);
    }

    public void add(T t) throws InterruptedException {
      if (t != null) {
        CountDownLatch myLock = new CountDownLatch(1);
        CountDownLatch existingLock = map.putIfAbsent(t, myLock);
        while (existingLock != null) {
          // wait for existing lock/permit to become available (see remove() below)
          existingLock.await();
          existingLock = map.putIfAbsent(t, myLock);
        }
        // myLock was successfully inserted
      }
      // won the lock
      sizeSemaphore.acquireUninterruptibly();
    }

    public void remove(T t) {
      if (t != null) {
        // remove and signal to any "await"-ers
        map.remove(t).countDown();
      }
      sizeSemaphore.release();
    }
  }
}