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
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.codecs.VectorValues;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;

public class VectorDistanceQuery extends Query {

  private final String field;
  private final float[] queryVector;
  private final int numCentroids;

  public VectorDistanceQuery(String field, float[] queryVector, int numCentroids) {
    this.field = field;
    this.queryVector = queryVector;
    this.numCentroids = numCentroids;
    if (this.numCentroids <= 1) {
      throw new IllegalArgumentException("The numCentroids parameter must be > 1.");
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new VectorDistanceWeight(this, boost, field, queryVector, numCentroids);
  }

  @Override
  public String toString(String field) {
    return String.format(Locale.ROOT, "VectorDistanceQuery{field=%s;fromQuery=%s;numCentroids=%d}",
        field, Arrays.toString(queryVector), numCentroids);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
        equalsTo(getClass().cast(other));
  }

  @Override
  public int hashCode() {
    return classHash() + Objects.hash(field, numCentroids, queryVector);
  }

  private boolean equalsTo(VectorDistanceQuery other) {
    return Objects.equals(field, other.field) &&
        Arrays.equals(queryVector, other.queryVector) &&
        Objects.equals(numCentroids, other.numCentroids);
  }

  private static class VectorDistanceWeight extends Weight {
    private final float boost;

    private final String field;
    private final float[] queryVector;
    private final int numProbes;

    VectorDistanceWeight(Query query, float boost, String field,
                         float[] queryVector, int numProbes) {
      super(query);
      this.boost = boost;

      this.field = field;
      this.queryVector = queryVector;
      this.numProbes = numProbes;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      VectorValues vectorValues = context.reader().getVectorValues(field);
      DocIdSetIterator candidates = vectorValues.getNearestVectors(queryVector, numProbes);
      BinaryDocValues docValues = vectorValues.getValues();

      return new Scorer(this) {
        @Override
        public DocIdSetIterator iterator() {
          return candidates;
        }

        @Override
        public float getMaxScore(int upTo) {
          return boost;
        }

        @Override
        public float score() throws IOException {
          docValues.advance(docID());
          BytesRef encodedVector = docValues.binaryValue();
          double dist = VectorValues.l2norm(queryVector, encodedVector);
          return (float) (boost / (1.0 + dist));
        }

        @Override
        public int docID() {
          return candidates.docID();
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }
}
