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

package org.apache.lucene.luwak.util;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

public final class CollectionUtils {

  @SafeVarargs
  public static <T> List<T> makeUnmodifiableList(T... items) {
    ArrayList<T> list = new ArrayList<>(items.length);
    Collections.addAll(list, items);
    return Collections.unmodifiableList(list);
  }

  public static <T> List<List<T>> partition(List<T> items, int slices) {
    double size = items.size() / (double) slices;
    double accum = 0;
    int start = 0;
    List<List<T>> list = new ArrayList<>(slices);
    for (int i = 0; i < slices; i++) {
      int end = (int) Math.floor(accum + size);
      if (i == slices - 1)
        end = items.size();
      list.add(items.subList(start, end));
      accum += size;
      start = (int) Math.floor(accum);
    }
    return list;
  }

  public static BytesRef[] convertHash(BytesRefHash hash) {
    BytesRef terms[] = new BytesRef[hash.size()];
    for (int i = 0; i < terms.length; i++) {
      BytesRef t = new BytesRef();
      terms[i] = hash.get(i, t);
    }
    return terms;
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
  public static <E> int drain(BlockingQueue<E> q, Collection<? super E> buffer, int numElements,
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
