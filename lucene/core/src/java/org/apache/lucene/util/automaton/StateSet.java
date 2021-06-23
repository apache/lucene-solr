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

import java.util.Arrays;
import org.apache.lucene.util.hppc.BitMixer;
import org.apache.lucene.util.hppc.IntIntHashMap;

/**
 * A thin wrapper of {@link IntIntHashMap} Maps from state in integer representation to its
 * reference count Whenever the count of a state is 0, that state will be removed from the set
 */
final class StateSet extends IntSet {

  private final IntIntHashMap inner;
  private long hashCode;
  private boolean hashUpdated = true;
  private boolean arrayUpdated = true;
  private int[] arrayCache = new int[0];

  StateSet(int capacity) {
    inner = new IntIntHashMap(capacity);
  }

  /**
   * Add the state into this set, if it is already there, increase its reference count by 1
   *
   * @param state an integer representing this state
   */
  void incr(int state) {
    if (inner.addTo(state, 1) == 1) {
      keyChanged();
    }
  }

  /**
   * Decrease the reference count of the state, if the count down to 0, remove the state from this
   * set
   *
   * @param state an integer representing this state
   */
  void decr(int state) {
    assert inner.containsKey(state);
    int keyIndex = inner.indexOf(state);
    int count = inner.indexGet(keyIndex) - 1;
    if (count == 0) {
      inner.indexRemove(keyIndex);
      keyChanged();
    } else {
      inner.indexReplace(keyIndex, count);
    }
  }

  /**
   * Create a snapshot of this int set associated with a given state. The snapshot will not retain
   * any frequency information about the elements of this set, only existence.
   *
   * @param state the state to associate with the frozen set.
   * @return A new FrozenIntSet with the same values as this set.
   */
  FrozenIntSet freeze(int state) {
    return new FrozenIntSet(getArray(), longHashCode(), state);
  }

  private void keyChanged() {
    hashUpdated = false;
    arrayUpdated = false;
  }

  @Override
  int[] getArray() {
    if (arrayUpdated) {
      return arrayCache;
    }
    arrayCache = new int[inner.size()];
    int i = 0;
    for (IntIntHashMap.IntCursor cursor : inner.keys()) {
      arrayCache[i++] = cursor.value;
    }
    // we need to sort this array since "equals" method depend on this
    Arrays.sort(arrayCache);
    arrayUpdated = true;
    return arrayCache;
  }

  @Override
  int size() {
    return inner.size();
  }

  @Override
  long longHashCode() {
    if (hashUpdated) {
      return hashCode;
    }
    hashCode = inner.size();
    for (IntIntHashMap.IntCursor cursor : inner.keys()) {
      hashCode += BitMixer.mix(cursor.value);
    }
    hashUpdated = true;
    return hashCode;
  }
}
