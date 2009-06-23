package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

/**
 * A wrapper class for the deprecated {@link Weight}.
 * Please re-implement any custom Weight classes as {@link
 * QueryWeight} instead.
 * 
 * @deprecated will be removed in 3.0
 */
public class QueryWeightWrapper extends QueryWeight {

  private Weight weight;
  
  public QueryWeightWrapper(Weight weight) {
    this.weight = weight;
  }
  
  public Explanation explain(IndexReader reader, int doc) throws IOException {
    return weight.explain(reader, doc);
  }

  public Query getQuery() {
    return weight.getQuery();
  }

  public float getValue() {
    return weight.getValue();
  }

  public void normalize(float norm) {
    weight.normalize(norm);
  }

  public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer)
      throws IOException {
    return weight.scorer(reader);
  }

  public float sumOfSquaredWeights() throws IOException {
    return weight.sumOfSquaredWeights();
  }

  public Scorer scorer(IndexReader reader) throws IOException {
    return weight.scorer(reader);
  }

}
