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

package org.apache.solr.util;

import java.util.Arrays;

/** A native int set where one value is reserved to mean "EMPTY" */
public class SentinelIntSet {
  public int[] keys;
  public int count;
  public final int emptyVal;
  public int rehashCount;   // the count at which a rehash should be done

  public SentinelIntSet(int size, int emptyVal) {
    this.emptyVal = emptyVal;
    int tsize = Math.max(org.apache.lucene.util.BitUtil.nextHighestPowerOfTwo(size), 1);
    rehashCount = tsize - (tsize>>2);
    if (size >= rehashCount) {  // should be able to hold "size" w/o rehashing
      tsize <<= 1;
      rehashCount = tsize - (tsize>>2);
    }
    keys = new int[tsize];
    if (emptyVal != 0)
      clear();
  }

  public void clear() {
    Arrays.fill(keys, emptyVal);
    count = 0;
  }

  public int hash(int key) {
    return key;
  }

  public int size() { return count; }

  /** returns the slot for this key */
  public int getSlot(int key) {
    assert key != emptyVal;
    int h = hash(key);
    int s = h & (keys.length-1);
    if (keys[s] == key || keys[s]== emptyVal) return s;

    int increment = (h>>7)|1;
    do {
      s = (s + increment) & (keys.length-1);
    } while (keys[s] != key && keys[s] != emptyVal);
    return s;
  }

  /** returns the slot for this key, or -slot-1 if not found */
  public int find(int key) {
    assert key != emptyVal;
    int h = hash(key);
    int s = h & (keys.length-1);
    if (keys[s] == key) return s;
    if (keys[s] == emptyVal) return -s-1;

    int increment = (h>>7)|1;
    for(;;) {
      s = (s + increment) & (keys.length-1);
      if (keys[s] == key) return s;
      if (keys[s] == emptyVal) return -s-1;
    }
  }


  public boolean exists(int key) {
    return find(key) >= 0;
  }


  public int put(int key) {
    int s = find(key);
    if (s < 0) {
      if (count >= rehashCount) {
        rehash();
        s = getSlot(key);
      } else {
        s = -s-1;
      }
      count++;
      keys[s] = key;
      putKey(key, s);
    } else {
      overwriteKey(key, s);
    }
    return s;
  }


  protected void putKey(int key, int slot) {}
  protected void overwriteKey(int key, int slot) {}

  protected void startRehash(int newSize) {}
  protected void moveKey(int key, int oldSlot, int newSlot) {}
  protected void endRehash() {}

  public void rehash() {
    int newSize = keys.length << 1;
    startRehash(newSize);
    int[] oldKeys = keys;
    keys = new int[newSize];
    if (emptyVal != 0) Arrays.fill(keys, emptyVal);

    for (int i=0; i<oldKeys.length; i++) {
      int key = oldKeys[i];
      if (key == emptyVal) continue;
      int newSlot = getSlot(key);
      keys[newSlot] = key;
      moveKey(key, i, newSlot);
    }
    endRehash();
    rehashCount = newSize - (newSize>>2);

  }

}
