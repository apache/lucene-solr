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
package org.apache.lucene.util;

/**
 * An implementation of a selection algorithm, ie. computing the k-th greatest value from a
 * collection.
 */
public abstract class Selector {

  /**
   * Reorder elements so that the element at position {@code k} is the same as if all elements were
   * sorted and all other elements are partitioned around it: {@code [from, k)} only contains
   * elements that are less than or equal to {@code k} and {@code (k, to)} only contains elements
   * that are greater than or equal to {@code k}.
   */
  public abstract void select(int from, int to, int k);

  void checkArgs(int from, int to, int k) {
    if (k < from) {
      throw new IllegalArgumentException("k must be >= from");
    }
    if (k >= to) {
      throw new IllegalArgumentException("k must be < to");
    }
  }

  /** Swap values at slots <code>i</code> and <code>j</code>. */
  protected abstract void swap(int i, int j);
}
