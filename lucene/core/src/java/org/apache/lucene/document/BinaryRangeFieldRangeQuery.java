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

package org.apache.lucene.document;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

abstract class BinaryRangeFieldRangeQuery extends Query {
  private final String field;
  private byte[] queryPackedValue;
  private final int numBytesPerDimension;
  private final int numDims;
  private final RangeFieldQuery.QueryType queryType;

  BinaryRangeFieldRangeQuery(String field, byte[] queryPackedValue, int numBytesPerDimension, int numDims,
                             RangeFieldQuery.QueryType queryType) {
    this.field = field;
    this.queryPackedValue = queryPackedValue;
    this.numBytesPerDimension = numBytesPerDimension;
    this.numDims = numDims;

    if (!(queryType == RangeFieldQuery.QueryType.INTERSECTS)) {
      throw new UnsupportedOperationException("INTERSECTS is the only query type supported for this field type right now");
    }

    this.queryType = queryType;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    BinaryRangeFieldRangeQuery that = (BinaryRangeFieldRangeQuery) obj;
    return Objects.equals(field, that.field)
        && Arrays.equals(queryPackedValue, that.queryPackedValue);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Arrays.hashCode(queryPackedValue);
    return h;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return super.rewrite(reader);
  }

  private BinaryRangeDocValues getValues(LeafReader reader, String field) throws IOException {
    BinaryDocValues binaryDocValues = reader.getBinaryDocValues(field);

    return new BinaryRangeDocValues(binaryDocValues, numDims, numBytesPerDimension);
  }


  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        BinaryRangeDocValues values = getValues(context.reader(), field);
        if (values == null) {
          return null;
        }

        final TwoPhaseIterator iterator;
        iterator = new TwoPhaseIterator(values) {
          @Override
          public boolean matches() {
            return queryType.matches(queryPackedValue, values.getPackedValue(), numDims, numBytesPerDimension);
          }

          @Override
          public float matchCost() {
            return queryPackedValue.length;
          }
        };

        return new ConstantScoreScorer(this, score(), scoreMode, iterator);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }

    };
  }
}
