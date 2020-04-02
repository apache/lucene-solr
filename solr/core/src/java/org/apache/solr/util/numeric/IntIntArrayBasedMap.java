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

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.IntConsumer;

import org.apache.lucene.util.ArrayUtil;

public class IntIntArrayBasedMap implements IntIntMap {

  private int size;
  private int[] keyValues;
  private int emptyValue;

  public IntIntArrayBasedMap(int initialSize, int emptyValue) {
    this.size = initialSize;
    this.keyValues = new int[initialSize];
    this.emptyValue = emptyValue;
    if (emptyValue != 0) {
      Arrays.fill(keyValues, emptyValue);
    }
  }

  @Override
  public void set(int key, int value) {
    if (key >= size) {
      keyValues = ArrayUtil.grow(keyValues);
      if (emptyValue != 0) {
        for (int i = size; i < keyValues.length; i++) {
          keyValues[i] = emptyValue;
        }
      }
      size = keyValues.length;
    }
    keyValues[key] = value;
  }

  @Override
  public int get(int key) {
    if (key >= size) {
      return emptyValue;
    }
    return keyValues[key];
  }

  @Override
  public void forEachValue(IntConsumer consumer) {
    for (int val: keyValues) {
      if (val != emptyValue) {
        consumer.accept(val);
      }
    }
  }

  @Override
  public void remove(int key) {
    if (key < size) keyValues[key] = emptyValue;
  }

  @Override
  public int size() {
    return keyValues.length;
  }
}
