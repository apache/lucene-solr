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
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HNSWGraphReader;

/**
 * Approximate nearest neighbor search query for high dimensional vector values.
 */
public class KnnGraphQuery extends Query implements Accountable {

  public static final Integer DEFAULT_EF = 50;

  private final String field;
  private final float[] queryVector;
  private final int ef;

  private long bytesUsed = 0L;

  /**
   * Creates an nearest neighbor search query with default {@code ef} parameter (={@link #DEFAULT_EF}).
   * @param field field name
   * @param queryVector query vector. must has same number of dimensions to the indexed vectors.
   */
  public KnnGraphQuery(String field, float[] queryVector) {
    this(field, queryVector, DEFAULT_EF);
    this.bytesUsed = RamUsageEstimator.shallowSizeOfInstance(getClass());
  }

  /**
   * Creates an nearest neighbor search query.
   * @param field field name
   * @param queryVector query vector. must has same number of dimensions to the indexed vectors.
   * @param ef number of per-segment candidates to be scored/collected. the collector does not return results exceeding {@code ef}.
   *           increasing this value leads higher recall at the expense of the search speed.
   */
  public KnnGraphQuery(String field, float[] queryVector, int ef) {
    this.field = field;
    this.queryVector = queryVector;
    this.ef = ef;
    this.bytesUsed = RamUsageEstimator.shallowSizeOfInstance(getClass());
  }

  /**
   * Creates an nearest neighbor search query; this also loads per-segment kNN graphs
   * before executing queries so that the latency of the initial search will be reduced.
   * @param field field name
   * @param queryVector query vector. must has same number of dimensions to the indexed vectors.
   * @param ef number of per-segment candidates to be scored/collected. the collector does not return results exceeding {@code ef}.
   *           increasing this value leads higher recall at the expense of the search speed.
   * @param reader index reader
   * @throws IOException
   */
  public KnnGraphQuery(String field, float[] queryVector, int ef, IndexReader reader) throws IOException {
    this.field = field;
    this.queryVector = queryVector;
    this.ef = ef;
    if (reader != null) {
      this.bytesUsed = HNSWGraphReader.reloadGraph(field, reader);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new KnnScoreWeight(this, boost, scoreMode, field, queryVector, ef);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(KnnGraphQuery other) {
    return Objects.equals(field, other.field) && Arrays.equals(queryVector, other.queryVector);
  }

  @Override
  public int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + Arrays.hashCode(queryVector);
    return hash;
  }

  @Override
  public String toString(String field) {
    // TODO: FIXME
    return null;
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed;
  }
}
