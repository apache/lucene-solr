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

public class ExactVectorDistanceQuery extends Query {

  private final String field;
  private final float[] queryVector;

  public ExactVectorDistanceQuery(String field, float[] queryVector) {
    this.field = field;
    this.queryVector = queryVector;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ExactVectorDistanceWeight(this, boost, field, queryVector);
  }

  @Override
  public String toString(String field) {
    return String.format(Locale.ROOT, "VectorDistanceQuery{field=%s;fromQuery=%s}",
        field, Arrays.toString(queryVector));
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
    return classHash() + Objects.hash(field, queryVector);
  }

  private boolean equalsTo(ExactVectorDistanceQuery other) {
    return Objects.equals(field, other.field) &&
        Arrays.equals(queryVector, other.queryVector);
  }

  private static class ExactVectorDistanceWeight extends Weight {
    private final float boost;

    private final String field;
    private final float[] queryVector;

    ExactVectorDistanceWeight(Query query, float boost, String field, float[] queryVector) {
      super(query);
      this.boost = boost;

      this.field = field;
      this.queryVector = queryVector;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      VectorValues vectorValues = context.reader().getVectorValues(field);
      BinaryDocValues docValues = vectorValues.getValues();

      return new Scorer(this) {
        @Override
        public DocIdSetIterator iterator() {
          return docValues;
        }

        @Override
        public float getMaxScore(int upTo) {
          return boost;
        }

        @Override
        public float score() throws IOException {
          BytesRef encodedVector = docValues.binaryValue();
          double dist = VectorValues.l2norm(queryVector, encodedVector);
          return (float) (boost / (1.0 + dist));
        }

        @Override
        public int docID() {
          return docValues.docID();
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }
}
