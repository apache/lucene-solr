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
package org.apache.lucene.util.automaton;

import java.util.TreeMap;
import java.util.Map;
import org.apache.lucene.util.ArrayUtil;

// Just holds a set of int[] states, plus a corresponding
// int[] count per state.  Used by Operations.determinize
final class SortedIntSet extends IntSet {
  int[] values;
  int[] counts;
  int upto;
  private int hashCode;

  // If we hold more than this many states, we switch from
  // O(N^2) linear ops to O(N log(N)) TreeMap
  private final static int TREE_MAP_CUTOVER = 30;

  private final Map<Integer,Integer> map = new TreeMap<>();

  private boolean useTreeMap;

  SortedIntSet(int capacity) {
    values = new int[capacity];
    counts = new int[capacity];
  }

  // Adds this state to the set
  public void incr(int num) {
    if (useTreeMap) {
      map.merge(num, 1, Integer::sum);
      return;
    }

    if (upto == values.length) {
      values = ArrayUtil.grow(values, 1+upto);
      counts = ArrayUtil.grow(counts, 1+upto);
    }

    for(int i=0;i<upto;i++) {
      if (values[i] == num) {
        counts[i]++;
        return;
      } else if (num < values[i]) {
        // insert here
        int j = upto-1;
        while (j >= i) {
          values[1+j] = values[j];
          counts[1+j] = counts[j];
          j--;
        }
        values[i] = num;
        counts[i] = 1;
        upto++;
        return;
      }
    }

    // append
    values[upto] = num;
    counts[upto] = 1;
    upto++;

    if (upto == TREE_MAP_CUTOVER) {
      useTreeMap = true;
      for(int i=0;i<upto;i++) {
        map.put(values[i], counts[i]);
      }
    }
  }

  // Removes this state from the set, if count decrs to 0
  public void decr(int num) {

    if (useTreeMap) {
      final int count = map.get(num);
      if (count == 1) {
        map.remove(num);
      } else {
        map.put(num, count-1);
      }
      // Fall back to simple arrays once we touch zero again
      if (map.size() == 0) {
        useTreeMap = false;
        upto = 0;
      }
      return;
    }

    for(int i=0;i<upto;i++) {
      if (values[i] == num) {
        counts[i]--;
        if (counts[i] == 0) {
          final int limit = upto-1;
          while(i < limit) {
            values[i] = values[i+1];
            counts[i] = counts[i+1];
            i++;
          }
          upto = limit;
        }
        return;
      }
    }
    assert false;
  }

  void computeHash() {
    if (useTreeMap) {
      if (map.size() > values.length) {
        final int size = ArrayUtil.oversize(map.size(), Integer.BYTES);
        values = new int[size];
        counts = new int[size];
      }
      hashCode = map.size();
      upto = 0;
      for(int state : map.keySet()) {
        hashCode = 683*hashCode + state;
        values[upto++] = state;
      }
    } else {
      hashCode = upto;
      for(int i=0;i<upto;i++) {
        hashCode = 683*hashCode + values[i];
      }
    }
  }

  /**
   * Create a snapshot of this int set associated with a given state. The snapshot will not retain any frequency
   * information about the elements of this set, only existence.
   * <p>
   * It is the caller's responsibility to ensure that the hashCode and data are up to date via the {@link #computeHash()} method before calling this method.
   * @param state the state to associate with the frozen set.
   * @return A new FrozenIntSet with the same values as this set.
   */
  FrozenIntSet freeze(int state) {
    final int[] c = ArrayUtil.copyOfSubArray(values, 0, upto);
    return new FrozenIntSet(c, hashCode, state);
  }

  @Override
  int[] getArray() {
      return values;
  }

  @Override
  int size() {
    return upto;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append('[');
    for(int i=0;i<upto;i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(values[i]).append(':').append(counts[i]);
    }
    sb.append(']');
    return sb.toString();
  }

}
  
