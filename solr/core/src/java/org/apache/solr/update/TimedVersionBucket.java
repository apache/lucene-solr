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
package org.apache.solr.update;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.common.SolrException;

/** @lucene.internal */
/**
 * This implementation uses lock and condition and will throw exception if it can't obtain the lock within
 * <code>lockTimeoutMs</code>.
 */
public class TimedVersionBucket extends VersionBucket {

  private final Lock lock = new ReentrantLock(true);
  private final Condition condition = lock.newCondition();

  /**
   * This will run the function with the lock. It will throw exception if it can't obtain the lock within
   * <code>lockTimeoutMs</code>.
   */
  @Override
  public <T,R> R runWithLock(int lockTimeoutMs, CheckedFunction<T,R> function) throws IOException {
    if (tryLock(lockTimeoutMs)) {
      return function.apply();
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unable to get version bucket lock in " + lockTimeoutMs + " ms");
    }
  }

  public void unlock() {
    lock.unlock();
  }

  public void signalAll() {
    condition.signalAll();
  }

  public void awaitNanos(long nanosTimeout) {
    try {
      if (nanosTimeout > 0) {
        condition.awaitNanos(nanosTimeout);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  protected boolean tryLock(int lockTimeoutMs) {
    try {
      return lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
