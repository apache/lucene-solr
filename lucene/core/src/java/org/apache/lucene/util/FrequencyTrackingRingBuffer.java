package org.apache.lucene.util;

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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A ring buffer that tracks the frequency of the items that it contains.
 * This is typically useful to track popular recently-used items.
 *
 * This class is thread-safe.
 *
 * @lucene.internal
 */
public final class FrequencyTrackingRingBuffer<T> {

  private final int maxSize;
  private final Deque<T> ringBuffer;
  private final ConcurrentMap<T, Integer> frequencies;

  /** Create a new ring buffer that will contain at most <code>size</code> items. */
  public FrequencyTrackingRingBuffer(int maxSize) {
    this.maxSize = maxSize;
    this.ringBuffer = new ArrayDeque<>(maxSize);
    this.frequencies = new ConcurrentHashMap<>();
  }

  /**
   * Add a new item to this ring buffer, potentially removing the oldest
   * entry from this buffer if it is already full.
   */
  public synchronized void add(T item) {
    // we need this method to be protected by a lock since it is important for
    // correctness that the ring buffer and the frequencies table have
    // consistent content
    if (item == null) {
      throw new IllegalArgumentException("null items are not supported");
    }
    assert ringBuffer.size() <= maxSize;
    if (ringBuffer.size() == maxSize) {
      // evict the oldest entry
      final T removed = ringBuffer.removeFirst();
      final int newFrequency = frequency(removed) - 1;
      if (newFrequency == 0) {
        // free for GC
        frequencies.remove(removed);
      } else {
        frequencies.put(removed, newFrequency);
      }
    }

    // add the new entry and update frequencies
    ringBuffer.addLast(item);
    frequencies.put(item, frequency(item) + 1);
  }

  /**
   * Returns the frequency of the provided item in the ring buffer.
   */
  public int frequency(T item) {
    // The use of a concurrent hash map allows us to not use a lock for this read-only method
    final Integer freq = frequencies.get(item);
    return freq == null ? 0 : freq;
  }

  // pkg-private for testing
  Map<T, Integer> asFrequencyMap() {
    return Collections.unmodifiableMap(frequencies);
  }

}
