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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.ParWork;

// TODO: make inner?
// TODO: store the highest possible in the index on a commit (but how to not block adds?)
// TODO: could also store highest possible in the transaction log after a commit.
// Or on a new index, just scan "version" for the max?
/** @lucene.internal */
/**
 * The default implementation which uses the intrinsic object monitor.
 * It uses less memory but ignores the <code>lockTimeoutMs</code>.
 */
public class VersionBucket {
  public volatile long highest;

  private final ReentrantLock lock = new ReentrantLock(true);
  private final Condition lockCondition = lock.newCondition();

  private Map<BytesRef,LongAdder> blockedIds = new ConcurrentHashMap<>();

  public void updateHighest(long val) {
    if (highest != 0) {
      highest = Math.max(highest, Math.abs(val));
    }
  }
  
  @FunctionalInterface
  public interface CheckedFunction<T, R> {
     R apply() throws IOException;
  }

  public <T, R> R runWithLock(int lockTimeoutMs, CheckedFunction<T,R> function, BytesRef idBytes) throws IOException {
    lock.lock();
    try {
      if (!blockedIds.keySet().contains(idBytes)) {
        LongAdder adder = new LongAdder();
        adder.increment();
        blockedIds.put(idBytes, adder);
        lock.unlock();
      } else {
        LongAdder adder = blockedIds.get(idBytes);
        adder.increment();
      }
      return function.apply();
    } finally {
      try {
        if (!lock.isHeldByCurrentThread()) {
          lock.lock();
          LongAdder adder = blockedIds.get(idBytes);
          adder.decrement();
          if (adder.longValue() == 0L) {
            blockedIds.remove(idBytes);
          }
        }
      } finally {
        if (lock.isHeldByCurrentThread()) lock.unlock();
      }
    }
  }

  public void signalAll() {
    if (!lock.isHeldByCurrentThread()) {
      lock.lock();
      try {
        lockCondition.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  public void awaitNanos(long nanosTimeout) {
    try {
      if (nanosTimeout > 0) {
        lockCondition.awaitNanos(nanosTimeout);
      }
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new RuntimeException(e);
    }
  }
}
