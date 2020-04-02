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

package org.apache.solr.util.numeric;

import java.util.function.IntConsumer;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntIntProcedure;

public class IntIntHashMap implements IntIntMap {
  private com.carrotsearch.hppc.IntIntHashMap hashMap;
  private int emptyValue;

  public IntIntHashMap(int initialSize, int emptyValue) {
    this.hashMap = new com.carrotsearch.hppc.IntIntHashMap();
    this.emptyValue = emptyValue;
  }

  @Override
  public final void set(int key, int value) {
    this.hashMap.put(key, value);
  }

  @Override
  public final int get(int key) {
    return this.hashMap.getOrDefault(key, emptyValue);
  }

  @Override
  public void forEachValue(IntConsumer consumer) {
    for (IntCursor ord : hashMap.values()) {
      consumer.accept(ord.value);
    }
  }

  @Override
  public void remove(int key) {
    this.hashMap.remove(key);
  }

  @Override
  public int size() {
    return hashMap.size();
  }
}
