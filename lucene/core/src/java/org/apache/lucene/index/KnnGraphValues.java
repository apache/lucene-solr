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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Access to per-document friends lists in a (hierarchical) knn search graph.
 * TODO: replace with SortedNumericDocValues??
 */
public abstract class KnnGraphValues {

  /** Sole constructor */
  protected KnnGraphValues() {}

  /** Move the pointer to exactly {@code target}, the id of a node in the graph.
   *  After this method returns, call {@link #nextArc()} to return successive (ordered) connected node ordinals.
   * @param target must be a valid node in the graph, ie. &ge; 0 and &lt; {@link VectorValues#size()}.
   */
  public abstract void seek(int target) throws IOException;

  /**
   * Iterates over the neighbor list. It is illegal to call this method after it returns
   * NO_MORE_DOCS without calling {@link #seek(int)}, which resets the iterator.
   * @return a node ordinal in the graph, or NO_MORE_DOCS if the iteration is complete.
   */
  public abstract int nextArc() throws IOException ;

  /** Empty graph value */
  public static KnnGraphValues EMPTY = new KnnGraphValues() {

    @Override
    public int nextArc() {
      return NO_MORE_DOCS;
    }

    @Override
    public void seek(int target) {
    }

  };

}
