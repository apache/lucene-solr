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
package org.apache.lucene.index;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

/**
 * An approximate priority queue, which attempts to poll items by decreasing
 * log of the weight, though exact ordering is not guaranteed.
 * This class doesn't support null elements.
 */
final class ApproximatePriorityQueue<T> {

  // Indexes between 0 and 63 are sparsely populated, and indexes that are
  // greater than or equal to 64 are densely populated
  // Items close to the beginning of this list are more likely to have a
  // higher weight.
  private final List<T> slots = new ArrayList<>(Long.SIZE);

  // A bitset where ones indicate that the corresponding index in `slots` is taken.
  private long usedSlots = 0L;

  ApproximatePriorityQueue() {
    for (int i = 0; i < Long.SIZE; ++i) {
      slots.add(null);
    }
  }

  /**
   * Add an entry to this queue that has the provided weight.
   */
  void add(T entry, long weight) {
    assert entry != null;

    // The expected slot of an item is the number of leading zeros of its weight,
    // ie. the larger the weight, the closer an item is to the start of the array.
    final int expectedSlot = Long.numberOfLeadingZeros(weight);

    // If the slot is already taken, we look for the next one that is free.
    // The above bitwise operation is equivalent to looping over slots until finding one that is free.
    final long freeSlots = ~usedSlots;
    int destinationSlot = expectedSlot + Long.numberOfTrailingZeros(freeSlots >>> expectedSlot);
    assert destinationSlot >= expectedSlot;
    if (destinationSlot < Long.SIZE) {
      usedSlots |= 1L << destinationSlot;
      T previous = slots.set(destinationSlot, entry);
      assert previous == null;
    } else {
      slots.add(entry);
    }
  }

  /**
   * Return an entry matching the predicate. This will usually be one of the
   * available entries that have the highest weight, though this is not
   * guaranteed.
   * This method returns {@code null} if no free entries are available.
   */
  T poll(Predicate<T> predicate) {
    // Look at indexes 0..63 first, which are sparsely populated.
    int nextSlot = 0;
    do {
      final int nextUsedSlot = nextSlot + Long.numberOfTrailingZeros(usedSlots >>> nextSlot);
      if (nextUsedSlot >= Long.SIZE) {
        break;
      }
      final T entry = slots.get(nextUsedSlot);
      if (predicate.test(entry)) {
        usedSlots &= ~(1L << nextUsedSlot);
        slots.set(nextUsedSlot, null);
        return entry;
      } else {
        nextSlot = nextUsedSlot + 1;
      }
    } while (nextSlot < Long.SIZE);

    // Then look at indexes 64.. which are densely populated.
    // Poll in descending order so that if the number of indexing threads
    // decreases, we keep using the same entry over and over again.
    // Resizing operations are also less costly on lists when items are closer
    // to the end of the list.
    for (ListIterator<T> lit = slots.listIterator(slots.size()); lit.previousIndex() >= Long.SIZE; ) {
      final T entry = lit.previous();
      if (predicate.test(entry)) {
        lit.remove();
        return entry;
      }
    }

    // No entry matching the predicate was found.
    return null;
  }

  // Only used for assertions
  boolean contains(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return slots.contains(o);
  }

  boolean isEmpty() {
    return usedSlots == 0 && slots.size() == Long.SIZE;
  }

  boolean remove(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
    int index = slots.indexOf(o);
    if (index == -1) {
      return false;
    }
    if (index >= Long.SIZE) {
      slots.remove(index);
    } else {
      usedSlots &= ~(1L << index);
      slots.set(index, null);
    }
    return true;
  }
}
