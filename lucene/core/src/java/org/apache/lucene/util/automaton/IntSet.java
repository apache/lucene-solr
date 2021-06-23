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

abstract class IntSet {
  /**
   * Return an array representation of this int set's values. Values are valid for indices [0,
   * {@link #size()}). If this is a mutable int set, then changes to the set are not guaranteed to
   * be visible in this array.
   *
   * @return an array containing the values for this set, guaranteed to be at least {@link #size()}
   *     elements
   */
  abstract int[] getArray();

  /**
   * Guaranteed to be less than or equal to the length of the array returned by {@link #getArray()}.
   *
   * @return The number of values in this set.
   */
  abstract int size();

  abstract long longHashCode();

  @Override
  public int hashCode() {
    return Long.hashCode(longHashCode());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof IntSet)) return false;
    IntSet that = (IntSet) o;
    return longHashCode() == that.longHashCode()
        && org.apache.lucene.util.FutureArrays.equals(getArray(), 0, size(), that.getArray(), 0, that.size());
  }
}
