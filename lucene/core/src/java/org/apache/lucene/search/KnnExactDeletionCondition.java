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

package org.apache.lucene.search;

import java.io.IOException;

/**
 * A query for deleting the exact values of the specified querying vector, used for
 * {@link org.apache.lucene.index.IndexWriter#deleteDocuments(Query...)}.
 */
public class KnnExactDeletionCondition extends KnnGraphQuery {
  /**
   * Creates a delete query for knn graph.
   * Note: only one in-set vector could be deleted.
   *
   * @param field            field name
   * @param queryVector      query vector. must has same number of dimensions to the indexed vectors
   * @param maxDelNumPerSeg  at most maxDelNumPerSeg docs will be deleted from each segment.
   */
  public KnnExactDeletionCondition(String field, float[] queryVector, int maxDelNumPerSeg) {
    super(field, queryVector, maxDelNumPerSeg);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    KnnScoreWeight weight = new KnnExactDeletionFilter(this, boost, scoreMode, field, queryVector, ef);
    weight.setVisitedCounter(visitedCounter);
    return weight;
  }
}
