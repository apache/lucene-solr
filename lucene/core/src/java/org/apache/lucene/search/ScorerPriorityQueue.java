package org.apache.lucene.search;

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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.lucene.util.PriorityQueue;

/**
 * A priority queue of scorers that orders by current doc ID.
 * This specialization is needed over {@link PriorityQueue} because the
 * pluggable comparison function makes the rebalancing quite slow.
 */
final class ScorerPriorityQueue implements Iterable<org.apache.lucene.search.ScorerPriorityQueue.ScorerWrapper> {

  static class ScorerWrapper {
    final Scorer scorer;
    final long cost;
    int doc; // the current doc, used for comparison
    ScorerWrapper next; // reference to a next element, see #topList

    // An approximation of the scorer, or the scorer itself if it does not
    // support two-phase iteration
    final DocIdSetIterator approximation;
    // A two-phase view of the scorer, or null if the scorer does not support
    // two-phase iteration
    final TwoPhaseIterator twoPhaseView;

    ScorerWrapper(Scorer scorer) {
      this.scorer = scorer;
      this.cost = scorer.cost();
      this.doc = -1;
      this.twoPhaseView = scorer.asTwoPhaseIterator();
      if (twoPhaseView != null) {
        approximation = twoPhaseView.approximation();
      } else {
        approximation = scorer;
      }
    }
  }

  static int leftNode(int node) {
    return ((node + 1) << 1) - 1;
  }

  static int rightNode(int leftNode) {
    return leftNode + 1;
  }

  static int parentNode(int node) {
    return ((node + 1) >>> 1) - 1;
  }

  private final ScorerWrapper[] heap;
  private int size;

  ScorerPriorityQueue(int maxSize) {
    heap = new ScorerWrapper[maxSize];
    size = 0;
  }

  int size() {
    return size;
  }

  ScorerWrapper top() {
    return heap[0];
  }

  /** Get the list of scorers which are on the current doc. */
  ScorerWrapper topList() {
    final ScorerWrapper[] heap = this.heap;
    final int size = this.size;
    ScorerWrapper list = heap[0];
    list.next = null;
    if (size >= 3) {
      list = topList(list, heap, size, 1);
      list = topList(list, heap, size, 2);
    } else if (size == 2 && heap[1].doc == list.doc) {
      list = prepend(heap[1], list);
    }
    return list;
  }

  // prepend w1 (scorer) to w2 (list)
  private static ScorerWrapper prepend(ScorerWrapper w1, ScorerWrapper w2) {
    w1.next = w2;
    return w1;
  }

  private static ScorerWrapper topList(ScorerWrapper list, ScorerWrapper[] heap, int size, int i) {
    final ScorerWrapper w = heap[i];
    if (w.doc == list.doc) {
      list = prepend(w, list);
      final int left = leftNode(i);
      final int right = left + 1;
      if (right < size) {
        list = topList(list, heap, size, left);
        list = topList(list, heap, size, right);
      } else if (left < size && heap[left].doc == list.doc) {
        list = prepend(heap[left], list);
      }
    }
    return list;
  }

  ScorerWrapper add(ScorerWrapper entry) {
    final ScorerWrapper[] heap = this.heap;
    final int size = this.size;
    heap[size] = entry;
    upHeap(heap, size);
    this.size = size + 1;
    return heap[0];
  }

  ScorerWrapper pop() {
    final ScorerWrapper[] heap = this.heap;
    final ScorerWrapper result = heap[0];
    final int i = --size;
    heap[0] = heap[i];
    heap[i] = null;
    downHeap(heap, i);
    return result;
  }

  ScorerWrapper updateTop() {
    downHeap(heap, size);
    return heap[0];
  }

  ScorerWrapper updateTop(ScorerWrapper topReplacement) {
    heap[0] = topReplacement;
    return updateTop();
  }

  static void upHeap(ScorerWrapper[] heap, int i) {
    final ScorerWrapper node = heap[i];
    final int nodeDoc = node.doc;
    int j = parentNode(i);
    while (j >= 0 && nodeDoc < heap[j].doc) {
      heap[i] = heap[j];
      i = j;
      j = parentNode(j);
    }
    heap[i] = node;
  }

  static void downHeap(ScorerWrapper[] heap, int size) {
    int i = 0;
    final ScorerWrapper node = heap[0];
    int j = leftNode(i);
    if (j < size) {
      int k = rightNode(j);
      if (k < size && heap[k].doc < heap[j].doc) {
        j = k;
      }
      if (heap[j].doc < node.doc) {
        do {
          heap[i] = heap[j];
          i = j;
          j = leftNode(i);
          k = rightNode(j);
          if (k < size && heap[k].doc < heap[j].doc) {
            j = k;
          }
        } while (j < size && heap[j].doc < node.doc);
        heap[i] = node;
      }
    }
  }

  @Override
  public Iterator<ScorerWrapper> iterator() {
    return Arrays.asList(heap).subList(0, size).iterator();
  }

}
