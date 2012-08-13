package org.apache.lucene.index;

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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.util.PriorityQueue;

/**
 * Provides a merged sorted view from several sorted iterators, each
 * iterating over a unique set of elements.
 * <p>
 * If an element appears in multiple iterators, it is deduplicated,
 * that is this iterator returns the sorted union of elements.
 * <p>
 * Caveats:
 * <ul>
 *   <li>The behavior is undefined if the iterators are not actually 
 *       sorted according to their comparator, or if a single iterator
 *       contains duplicates.
 *   <li>Null elements are unsupported.
 *   <li>When an element E is a duplicate across multiple iterators,
 *       only one is returned, but it is undefined which one: not
 *       guaranteed to be a stable sort.
 * </ul>
 * @lucene.internal
 */
final class MergedIterator<T extends Comparable<T>> implements Iterator<T> {
  private T current;
  private final TermMergeQueue<T> queue; 
  private final SubIterator<T>[] top;
  private int numTop;
  
  @SuppressWarnings({"unchecked","rawtypes"})
  public MergedIterator(Iterator<T>... iterators) {
    queue = new TermMergeQueue<T>(iterators.length);
    top = new SubIterator[iterators.length];
    int index = 0;
    for (Iterator<T> iterator : iterators) {
      if (iterator.hasNext()) {
        SubIterator<T> sub = new SubIterator<T>();
        sub.current = iterator.next();
        sub.iterator = iterator;
        sub.index = index++;
        queue.add(sub);
      }
    }
  }
  
  @Override
  public boolean hasNext() {
    if (queue.size() > 0) {
      return true;
    }
    
    for (int i = 0; i < numTop; i++) {
      if (top[i].iterator.hasNext()) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public T next() {
    // restore queue
    pushTop();
    
    // gather equal top elements
    if (queue.size() > 0) {
      pullTop();
    } else {
      current = null;
    }
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
  
  private void pullTop() {
    // extract all subs from the queue that have the same top element
    assert numTop == 0;
    while (true) {
      top[numTop++] = queue.pop();
      if (queue.size() == 0
          || !(queue.top()).current.equals(top[0].current)) {
        break;
      }
    }
    current = top[0].current;
  }
  
  private void pushTop() {
    // call next() on each top, and put back into queue
    for (int i = 0; i < numTop; i++) {
      if (top[i].iterator.hasNext()) {
        top[i].current = top[i].iterator.next();
        queue.add(top[i]);
      } else {
        // no more elements
        top[i].current = null;
      }
    }
    numTop = 0;
  }
  
  private static class SubIterator<I extends Comparable<I>> {
    Iterator<I> iterator;
    I current;
    int index;
  }
  
  private static class TermMergeQueue<C extends Comparable<C>> extends PriorityQueue<SubIterator<C>> {
    TermMergeQueue(int size) {
      super(size);
    }
    
    @Override
    protected boolean lessThan(SubIterator<C> a, SubIterator<C> b) {
      final int cmp = a.current.compareTo(b.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        return a.index < b.index;
      }
    }
  }
}
