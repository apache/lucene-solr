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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.index.VectorValues;

/** An abstract neighbor document of a specific query vector; a query can be the vector value of a document to be indexed
 * or an arbitrary query.  */
public abstract class Neighbor implements Comparable<Neighbor> {

  /** Returns document id of this neighbor */
  public abstract int docId();

  /** Returns distance between the query and this neighbor doc; It is illegal if {@link #isDeferred()} = true
   * and {@link #prepareQuery(float[], VectorValues, VectorValues.DistanceFunction)} is not called before calling
   * {@link #distance()} */
  public abstract float distance();

  /** Returns true if distance calculation is deferred */
  public abstract boolean isDeferred();

  /** Set attributes for distance calculation. This must be called before {@link #distance()} when
   * {@link #isDeferred()} = true */
  public void prepareQuery(float[] query, VectorValues vectorValues, VectorValues.DistanceFunction distFunc) {
  }

  public String toString() {
    return "(" + docId() + ", " + distance() + ")";
  }
}
