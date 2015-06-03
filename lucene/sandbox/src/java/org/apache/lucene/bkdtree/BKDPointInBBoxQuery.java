package org.apache.lucene.bkdtree;

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

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

/** Finds all previously indexed points that fall within the specified boundings box.
 *
 *  <p>The field must be indexed with {@link BKDTreeDocValuesFormat}, and {@link BKDPointField} added per document.
 *
 *  <p><b>NOTE</b>: for fastest performance, this allocates FixedBitSet(maxDoc) for each segment.  The score of each hit is the query boost.
 *
 * @lucene.experimental */

public class BKDPointInBBoxQuery extends Query {
  final String field;
  final double minLat;
  final double maxLat;
  final double minLon;
  final double maxLon;

  /** Matches all points &gt;= minLon, minLat (inclusive) and &lt; maxLon, maxLat (exclusive). */ 
  public BKDPointInBBoxQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    this.field = field;
    if (BKDTreeWriter.validLat(minLat) == false) {
      throw new IllegalArgumentException("minLat=" + minLat + " is not a valid latitude");
    }
    if (BKDTreeWriter.validLat(maxLat) == false) {
      throw new IllegalArgumentException("maxLat=" + maxLat + " is not a valid latitude");
    }
    if (BKDTreeWriter.validLon(minLon) == false) {
      throw new IllegalArgumentException("minLon=" + minLon + " is not a valid longitude");
    }
    if (BKDTreeWriter.validLon(maxLon) == false) {
      throw new IllegalArgumentException("maxLon=" + maxLon + " is not a valid longitude");
    }
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    return new Weight(this) {
      private float queryNorm;
      private float queryWeight;

      @Override
      public void extractTerms(Set<Term> terms) {
      }

      @Override
      public float getValueForNormalization() throws IOException {
        queryWeight = getBoost();
        return queryWeight * queryWeight;
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {
        queryNorm = norm * topLevelBoost;
        queryWeight *= queryNorm;
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        final Scorer s = scorer(context, context.reader().getLiveDocs());
        final boolean exists = s != null && s.advance(doc) == doc;

        if (exists) {
          return Explanation.match(queryWeight, BKDPointInBBoxQuery.this.toString() + ", product of:",
              Explanation.match(getBoost(), "boost"), Explanation.match(queryNorm, "queryNorm"));
        } else {
          return Explanation.noMatch(BKDPointInBBoxQuery.this.toString() + " doesn't match id " + doc);
        }
      }

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        LeafReader reader = context.reader();
        SortedNumericDocValues sdv = reader.getSortedNumericDocValues(field);
        if (sdv == null) {
          // No docs in this segment had this field
          return null;
        }

        if (sdv instanceof BKDTreeSortedNumericDocValues == false) {
          throw new IllegalStateException("field \"" + field + "\" was not indexed with BKDTreeDocValuesFormat: got: " + sdv);
        }
        BKDTreeSortedNumericDocValues treeDV = (BKDTreeSortedNumericDocValues) sdv;
        BKDTreeReader tree = treeDV.getBKDTreeReader();

        DocIdSet result = tree.intersect(acceptDocs, minLat, maxLat, minLon, maxLon, treeDV.delegate);

        final DocIdSetIterator disi = result.iterator();

        return new Scorer(this) {

          @Override
          public float score() throws IOException {
            return queryWeight;
          }

          @Override
          public int freq() throws IOException {
            return 1;
          }

          @Override
          public int docID() {
            return disi.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return disi.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return disi.advance(target);
          }

          @Override
          public long cost() {
            return disi.cost();
          }
        };
      }
    };
  }
  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash += Double.hashCode(minLat)^0x14fa55fb;
    hash += Double.hashCode(maxLat)^0x733fa5fe;
    hash += Double.hashCode(minLon)^0x14fa55fb;
    hash += Double.hashCode(maxLon)^0x733fa5fe;
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other) && other instanceof BKDPointInBBoxQuery) {
      final BKDPointInBBoxQuery q = (BKDPointInBBoxQuery) other;
      return field.equals(q.field) &&
        minLat == q.minLat &&
        maxLat == q.maxLat &&
        minLon == q.minLon &&
        maxLon == q.maxLon;
    }

    return false;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append("field=");
      sb.append(this.field);
      sb.append(':');
    }

    return sb.append(" Lower Left: [")
        .append(minLon)
        .append(',')
        .append(minLat)
        .append(']')
        .append(" Upper Right: [")
        .append(maxLon)
        .append(',')
        .append(maxLat)
        .append("]")
        .append(ToStringUtils.boost(getBoost()))
        .toString();
  }
}
