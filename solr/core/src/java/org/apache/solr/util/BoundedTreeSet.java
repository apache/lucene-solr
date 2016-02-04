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
package org.apache.solr.util;

import java.util.*;

/**
 * A TreeSet that ensures it never grows beyond a max size.  
 * <code>last()</code> is removed if the <code>size()</code> 
 * get's bigger then <code>getMaxSize()</code>
 */
public class BoundedTreeSet<E> extends TreeSet<E> {
  private int maxSize = Integer.MAX_VALUE;
  public BoundedTreeSet(int maxSize) {
    super();
    this.setMaxSize(maxSize);
  }
  public BoundedTreeSet(int maxSize, Collection<? extends E> c) {
    super(c);
    this.setMaxSize(maxSize);
  }
  public BoundedTreeSet(int maxSize, Comparator<? super E> c) {
    super(c);
    this.setMaxSize(maxSize);
  }
  public BoundedTreeSet(int maxSize, SortedSet<E> s) {
    super(s);
    this.setMaxSize(maxSize);
  }
  public int getMaxSize() {
    return maxSize;
  }
  public void setMaxSize(int max) {
    maxSize = max;
    adjust();
  }
  private void adjust() {
    while (maxSize < size()) {
      remove(last());
    }
  }
  @Override
  public boolean add(E item) {
    boolean out = super.add(item);
    adjust();
    return out;
  }
  @Override
  public boolean addAll(Collection<? extends E> c) {
    boolean out = super.addAll(c);
    adjust();
    return out;
  }
}
