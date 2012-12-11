package org.apache.lucene.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * An {@link Iterator} implementation that filters elements with a boolean predicate.
 * @see #predicateFunction
 */
public abstract class FilterIterator<T> implements Iterator<T> {
  
  private final Iterator<T> iterator;
  private T next = null;
  private boolean nextIsSet = false;
  
  /** returns true, if this element should be returned by {@link #next()}. */
  protected abstract boolean predicateFunction(T object);
  
  public FilterIterator(Iterator<T> baseIterator) {
    this.iterator = baseIterator;
  }
  
  @Override
  public final boolean hasNext() {
    return nextIsSet || setNext();
  }
  
  @Override
  public final T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    assert nextIsSet;
    try {
      return next;
    } finally {
      nextIsSet = false;
      next = null;
    }
  }
  
  @Override
  public final void remove() {
    throw new UnsupportedOperationException();
  }
  
  private boolean setNext() {
    while (iterator.hasNext()) {
      final T object = iterator.next();
      if (predicateFunction(object)) {
        next = object;
        nextIsSet = true;
        return true;
      }
    }
    return false;
  }
}
