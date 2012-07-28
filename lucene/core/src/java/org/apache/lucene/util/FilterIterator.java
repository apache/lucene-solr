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

public abstract class FilterIterator<T> implements Iterator<T> {
  
  private Iterator<T> iterator;
  private T next = null;
  private boolean nextIsSet = false;
  
  protected abstract boolean predicateFunction(T field);
  
  public FilterIterator(Iterator<T> baseIterator) {
    this.iterator = baseIterator;
  }
  
  public boolean hasNext() {
    if (nextIsSet) {
      return true;
    } else {
      return setNext();
    }
  }
  
  public T next() {
    if (!nextIsSet) {
      if (!setNext()) {
        throw new NoSuchElementException();
      }
    }
    nextIsSet = false;
    return next;
  }
  
  public void remove() {
    throw new UnsupportedOperationException();
  }
  
  private boolean setNext() {
    while (iterator.hasNext()) {
      T object = iterator.next();
      if (predicateFunction(object)) {
        next = object;
        nextIsSet = true;
        return true;
      }
    }
    return false;
  }
}
