package org.apache.lucene.util;

/**
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

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

/**
 * A Set implementation that wraps an actual Map based
 * implementation.
 * 
 * @lucene.internal
 */
public final class MapBackedSet<E> extends AbstractSet<E> implements Serializable {

  private static final long serialVersionUID = -6761513279741915432L;

  private final Map<E, Boolean> map;

  /**
   * Creates a new instance which wraps the specified {@code map}.
   */
  public MapBackedSet(Map<E, Boolean> map) {
    this.map = map;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  @Override
  public boolean add(E o) {
    return map.put(o, Boolean.TRUE) == null;
  }

  @Override
  public boolean remove(Object o) {
    return map.remove(o) != null;
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }
}
