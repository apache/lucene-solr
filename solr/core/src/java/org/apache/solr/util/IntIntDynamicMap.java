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

import java.util.Arrays;
import java.util.function.IntConsumer;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntIntProcedure;
import org.apache.lucene.util.ArrayUtil;

public class IntIntDynamicMap implements DynamicMap {
  private int maxSize;
  private IntIntHashMap hashMap;
  private int[] keyValues;
  private int emptyValue;
  private int threshold;

  /**
   * Create map with expected max value of key.
   * Although the map will automatically do resizing to be able to hold key {@code >= expectedKeyMax}.
   * But putting key much larger than {@code expectedKeyMax} is discourage since it can leads to use LOT OF memory.
   */
  public IntIntDynamicMap(int expectedKeyMax, int emptyValue) {
    this.threshold = threshold(expectedKeyMax);
    this.maxSize = expectedKeyMax;
    this.emptyValue = emptyValue;
    if (useArrayBased(expectedKeyMax)) {
      upgradeToArray();
    } else {
      this.hashMap = new IntIntHashMap(mapExpectedElements(expectedKeyMax));
    }
  }

  private void upgradeToArray() {
    keyValues = new int[maxSize];
    if (emptyValue != 0) {
      Arrays.fill(keyValues, emptyValue);
    }
    if (hashMap != null) {
      hashMap.forEach((IntIntProcedure) (key, value) -> keyValues[key] = value);
      hashMap = null;
    }
  }

  private void growBuffer(int minSize) {
    assert keyValues != null;
    int size = keyValues.length;
    keyValues = ArrayUtil.grow(keyValues, minSize);
    if (emptyValue != 0) {
      for (int i = size; i < keyValues.length; i++) {
        keyValues[i] = emptyValue;
      }
    }
  }

  public void put(int key, int value) {
    if (keyValues != null) {
      if (key >= keyValues.length) {
        growBuffer(key + 1);
      }
      keyValues[key] = value;
    } else {
      this.maxSize = Math.max(key + 1, maxSize);
      this.hashMap.put(key, value);
      if (this.hashMap.size() >= threshold) {
        upgradeToArray();
      }
    }
  }

  public int get(int key) {
    if (keyValues != null) {
      if (key >= keyValues.length) {
        return emptyValue;
      }
      return keyValues[key];
    } else {
      return this.hashMap.getOrDefault(key, emptyValue);
    }
  }

  public void forEachValue(IntConsumer consumer) {
    if (keyValues != null) {
      for (int val : keyValues) {
        if (val != emptyValue) consumer.accept(val);
      }
    } else {
      for (IntCursor ord : hashMap.values()) {
        consumer.accept(ord.value);
      }
    }
  }

  public void remove(int key) {
    if (keyValues != null) {
      if (key < keyValues.length)
        keyValues[key] = emptyValue;
    } else {
      hashMap.remove(key);
    }
  }

}
