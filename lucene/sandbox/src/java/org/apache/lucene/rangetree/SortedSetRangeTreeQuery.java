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
package org.apache.lucene.rangetree;

import org.apache.lucene.document.SortedSetDocValuesField; // javadocs
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/** Finds all previously indexed values that fall within the specified {@link BytesRef} range.
 *
 * <p>The field must be indexed with {@link RangeTreeDocValuesFormat}, and {@link SortedSetDocValuesField} added per document.
 *
 * @lucene.experimental */

public class SortedSetRangeTreeQuery extends Query {
  final String field;
  final BytesRef minValue;
  final BytesRef maxValue;
  final boolean minInclusive;
  final boolean maxInclusive;

  /** Matches all values in the specified {@link BytesRef} range. */ 
  public SortedSetRangeTreeQuery(String field, BytesRef minValue, boolean minInclusive, BytesRef maxValue, boolean maxInclusive) {
    this.field = field;
    this.minInclusive = minInclusive;
    this.minValue = minValue;
    this.maxInclusive = maxInclusive;
    this.maxValue = maxValue;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        final SortedSetDocValues ssdv = reader.getSortedSetDocValues(field);
        if (ssdv == null) {
          // No docs in this segment had this field
          return null;
        }

        if (ssdv instanceof RangeTreeSortedSetDocValues == false) {
          throw new IllegalStateException("field \"" + field + "\" was not indexed with RangeTreeDocValuesFormat: got: " + ssdv);
        }
        RangeTreeSortedSetDocValues treeDV = (RangeTreeSortedSetDocValues) ssdv;
        RangeTreeReader tree = treeDV.getRangeTreeReader();

        /*
        for(int i=0;i<treeDV.getValueCount();i++) {
          System.out.println("  ord " + i + " -> " + treeDV.lookupOrd(i));
        }
        */

        // lower
        final long minOrdIncl;
        if (minValue == null) {
          minOrdIncl = 0;
        } else {
          long ord = ssdv.lookupTerm(minValue);
          if (ord >= 0) {
            // Exact match
            if (minInclusive) {
              minOrdIncl = ord;
            } else {
              minOrdIncl = ord+1;
            }
          } else {
            minOrdIncl = -ord-1;
          }
        }

        // upper
        final long maxOrdIncl;
        if (maxValue == null) {
          maxOrdIncl = Long.MAX_VALUE;
        } else {
          long ord = ssdv.lookupTerm(maxValue);
          if (ord >= 0) {
            // Exact match
            if (maxInclusive) {
              maxOrdIncl = ord;
            } else {
              maxOrdIncl = ord-1;
            }
          } else {
            maxOrdIncl = -ord-2;
          }
        }

        if (maxOrdIncl < minOrdIncl) {  
          // This can happen when the requested range lies entirely between 2 adjacent ords:
          return null;
        }

        //System.out.println(reader + ": ORD: " + minOrdIncl + "-" + maxOrdIncl + "; " + minValue + " - " + maxValue);
        
        // Just a "view" of only the ords from the SSDV, as an SNDV.  Maybe we
        // have this view implemented somewhere else already?  It's not so bad that
        // we are inefficient here (making 2 passes over the ords): this is only
        // used in at most 2 leaf cells (the boundary cells).
        SortedNumericDocValues ords = new SortedNumericDocValues() {

            private long[] ords = new long[2];
            private int count;

            @Override
            public void setDocument(int doc) {
              ssdv.setDocument(doc);
              long ord;
              count = 0;
              while ((ord = ssdv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                if (count == ords.length) {
                  ords = ArrayUtil.grow(ords, count+1);
                }
                ords[count++] = ord;
              }
            }

            @Override
            public int count() {
              return count;
            }

            @Override
            public long valueAt(int index) {
              return ords[index];
            }
          };

        DocIdSet result = tree.intersect(minOrdIncl, maxOrdIncl, ords, context.reader().maxDoc());

        final DocIdSetIterator disi = result.iterator();

        return new ConstantScoreScorer(this, score(), disi);
      }
    };
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    if (minValue != null) hash += minValue.hashCode()^0x14fa55fb;
    if (maxValue != null) hash += maxValue.hashCode()^0x733fa5fe;
    return hash +
      (Boolean.valueOf(minInclusive).hashCode()^0x14fa55fb)+
      (Boolean.valueOf(maxInclusive).hashCode()^0x733fa5fe);
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other)) {
      final SortedSetRangeTreeQuery q = (SortedSetRangeTreeQuery) other;
      return (
        (q.minValue == null ? minValue == null : q.minValue.equals(minValue)) &&
        (q.maxValue == null ? maxValue == null : q.maxValue.equals(maxValue)) &&
        minInclusive == q.minInclusive &&
        maxInclusive == q.maxInclusive
      );
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

    return sb.append(minInclusive ? '[' : '{')
      .append((minValue == null) ? "*" : minValue.toString())
      .append(" TO ")
      .append((maxValue == null) ? "*" : maxValue.toString())
      .append(maxInclusive ? ']' : '}')
      .append(ToStringUtils.boost(getBoost()))
      .toString();
  }
}
