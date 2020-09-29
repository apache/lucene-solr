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
package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulated {@link DistributedQueueFactory} that keeps all data in memory. Unlike
 * the {@link GenericDistributedQueueFactory} this queue implementation data is not
 * exposed anywhere.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class SimDistributedQueueFactory implements DistributedQueueFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Map<String, SimDistributedQueue> queues = new ConcurrentHashMap<>();

  public SimDistributedQueueFactory() {
  }

  @Override
  public DistributedQueue makeQueue(final String path) throws IOException {
    return queues.computeIfAbsent(path, p -> new SimDistributedQueue(path));
  }

  @Override
  public void removeQueue(String path) throws IOException {
    queues.remove(path);
  }

  public static class SimDistributedQueue implements DistributedQueue {
    private final Queue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock updateLock = new ReentrantLock();
    private final Condition changed = updateLock.newCondition();
    private final Stats stats = new Stats();
    private final String dir;
    private int seq = 0;

    public SimDistributedQueue(String dir) {
      this.dir = dir;
    }

    @Override
    public byte[] peek() throws Exception {
      Timer.Context time = stats.time(dir + "_peek");
      try {
        Pair<String, byte[]> pair = queue.peek();
        return pair != null ? pair.second() : null;
      } finally {
        time.stop();
      }
    }

    @Override
    public byte[] peek(boolean block) throws Exception {
      return block ? peek(Long.MAX_VALUE) : peek();
    }

    @Override
    public byte[] peek(long wait) throws Exception {
      Timer.Context time;
      if (wait == Long.MAX_VALUE) {
        time = stats.time(dir + "_peek_wait_forever");
      } else {
        time = stats.time(dir + "_peek_wait" + wait);
      }
      try {
        Pair<String, byte[]> pair = peekInternal(wait);
        return pair != null ? pair.second() : null;
      } finally {
        time.stop();
      }
    }

    private Pair<String, byte[]> peekInternal(long wait) throws Exception {
      Preconditions.checkArgument(wait > 0);
      long waitNanos = TimeUnit.MILLISECONDS.toNanos(wait);
      updateLock.lockInterruptibly();
      try {
        while (waitNanos > 0) {
          Pair<String, byte[]> pair = queue.peek();
          if (pair != null) {
            return pair;
          }
          waitNanos = changed.awaitNanos(waitNanos);
          if (waitNanos < 0) { // timed out
            return null;
          }
        }
      } finally {
        updateLock.unlock();
      }
      return null;
    }

    @Override
    public byte[] poll() throws Exception {
      Timer.Context time = stats.time(dir + "_poll");
      updateLock.lockInterruptibly();
      try {
        Pair<String, byte[]>  pair = queue.poll();
        if (pair != null) {
          changed.signalAll();
          return pair.second();
        } else {
          return null;
        }
      } finally {
        updateLock.unlock();
        time.stop();
      }
    }

    @Override
    public byte[] remove() throws Exception {
      Timer.Context time = stats.time(dir + "_remove");
      updateLock.lockInterruptibly();
      try {
        byte[] res = queue.remove().second();
        changed.signalAll();
        return res;
      } finally {
        updateLock.unlock();
        time.stop();
      }
    }

    @Override
    public byte[] take() throws Exception {
      Timer.Context timer = stats.time(dir + "_take");
      updateLock.lockInterruptibly();
      try {
        while (true) {
          byte[] result = poll();
          if (result != null) {
            return result;
          }
          changed.await();
        }
      } finally {
        updateLock.unlock();
        timer.stop();
      }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void offer(byte[] data) throws Exception {
      Timer.Context time = stats.time(dir + "_offer");
      updateLock.lockInterruptibly();
      try {
        queue.offer(new Pair(String.format(Locale.ROOT, "qn-%010d", seq), data));
        seq++;
        if (log.isTraceEnabled()) {
          log.trace("=== offer {}", System.nanoTime());
        }
        changed.signalAll();
      } finally {
        updateLock.unlock();
        time.stop();
      }
    }

    @Override
    public Collection<Pair<String, byte[]>> peekElements(int max, long waitMillis, Predicate<String> acceptFilter) throws Exception {
      updateLock.lockInterruptibly();
      try {
        List<Pair<String, byte[]>> res = new LinkedList<>();
        final int maximum = max < 0 ? Integer.MAX_VALUE : max;
        final AtomicReference<Pair<String, byte[]>> pairRef = new AtomicReference<>();
        queue.forEach(pair -> {
          if (acceptFilter != null && !acceptFilter.test(pair.first())) {
            return;
          }
          if (res.size() < maximum) {
            pairRef.set(pair);
            res.add(pair);
          }
        });
        if (res.size() < maximum && waitMillis > 0) {
          long waitNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
          waitNanos = changed.awaitNanos(waitNanos);
          if (waitNanos < 0) {
            return res;
          }
          AtomicBoolean seen = new AtomicBoolean(false);
          queue.forEach(pair -> {
            if (!seen.get()) {
              if (pairRef.get() == null) {
                seen.set(true);
              } else {
                if (pairRef.get().first().equals(pair.first())) {
                  seen.set(true);
                  return;
                }
              }
            }
            if (!seen.get()) {
              return;
            }
            if (!acceptFilter.test(pair.first())) {
              return;
            }
            if (res.size() < maximum) {
              res.add(pair);
              pairRef.set(pair);
            } else {
              return;
            }
          });
        }
        return res;
      } finally {
        updateLock.unlock();
      }
    }

    public Stats getZkStats() {
      return stats;
    }

    @Override
    public Map<String, Object> getStats() {
      if (stats == null) {
        return Collections.emptyMap();
      }
      Map<String, Object> res = new HashMap<>();
      res.put("queueLength", stats.getQueueLength());
      final Map<String, Object> statsMap = new HashMap<>();
      res.put("stats", statsMap);
      stats.getStats().forEach((op, stat) -> {
        final Map<String, Object> statMap = new HashMap<>();
        statMap.put("success", stat.success.get());
        statMap.put("errors", stat.errors.get());
        final List<Map<String, Object>> failed = new ArrayList<>(stat.failureDetails.size());
        statMap.put("failureDetails", failed);
        stat.failureDetails.forEach(failedOp -> {
          Map<String, Object> fo = new HashMap<>();
          fo.put("req", failedOp.req);
          fo.put("resp", failedOp.resp);
        });
        statsMap.put(op, statMap);
      });
      return res;
    }
  }
}
