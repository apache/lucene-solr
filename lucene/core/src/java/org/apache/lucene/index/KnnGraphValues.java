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

package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.IntsRef;

/**
 * Access to per-document friends lists in a (hierarchical) knn search graph.
 */
public abstract class KnnGraphValues extends DocIdSetIterator {

  /** Sole constructor */
  protected KnnGraphValues() {}

  /**
   * Returns the top level of the entire graph.
   * @return top level
   */
  public abstract int getTopLevel();

  /**
   * Returns the enter points of the graph.
   * @return enter points
   */
  public abstract int[] getEnterPoints();

  /**
   * Returns the max level (height) for the current document ID.
   * It is illegal to call this method after {@link #advanceExact(int)}
   * returned {@code false}.
   * @return max level
   */
  public abstract int getMaxLevel();

  /**
   * Returns the friend list (doc ID list) at the graph {@code level}.
   * Each doc ID in the friend list is connected to the current document ID at this {@code level}.
   * {@code level} must be a valid level, ie. &ge; 0 and &le; {@link #getMaxLevel()}.
   * It is illegal to call this method after {@link #advanceExact(int)}
   * returned {@code false}.
   * @param level the graph level
   * @return friend list
   */
  public abstract IntsRef getFriends(int level);

  /** Move the pointer to exactly {@code target} and return whether
   *  {@code target} has friends lists.
   *  {@code target} must be a valid doc ID, ie. &ge; 0 and &lt; {@code maxDoc}.
   *  After this method returns, {@link #docID()} retuns {@code target}. */
  public abstract boolean advanceExact(int target) throws IOException;

  /** Empty graph value */
  public static KnnGraphValues EMPTY = new KnnGraphValues() {
    @Override
    public int getTopLevel() {
      return -1;
    }

    @Override
    public int[] getEnterPoints() {
      return new int[0];
    }

    @Override
    public int getMaxLevel() {
      return -1;
    }

    @Override
    public IntsRef getFriends(int level) {
      return new IntsRef();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return false;
    }

    @Override
    public int docID() {
      return -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return -1;
    }

    @Override
    public int advance(int target) throws IOException {
      return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0;
    }
  };
}
