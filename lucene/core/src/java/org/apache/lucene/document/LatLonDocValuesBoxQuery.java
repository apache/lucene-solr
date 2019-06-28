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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/** Distance query for {@link LatLonDocValuesField}. */
final class LatLonDocValuesBoxQuery extends Query {

  private final String field;
  private final int minLatitude, maxLatitude, minLongitude, maxLongitude;
  private final boolean crossesDateline;

  LatLonDocValuesBoxQuery(String field, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
    GeoUtils.checkLatitude(minLatitude);
    GeoUtils.checkLatitude(maxLatitude);
    GeoUtils.checkLongitude(minLongitude);
    GeoUtils.checkLongitude(maxLongitude);
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    this.field = field;
    this.crossesDateline = minLongitude > maxLongitude; // make sure to compute this before rounding
    this.minLatitude = GeoEncodingUtils.encodeLatitudeCeil(minLatitude);
    this.maxLatitude = GeoEncodingUtils.encodeLatitude(maxLatitude);
    this.minLongitude = GeoEncodingUtils.encodeLongitudeCeil(minLongitude);
    this.maxLongitude = GeoEncodingUtils.encodeLongitude(maxLongitude);
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (!this.field.equals(field)) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("box(minLat=").append(GeoEncodingUtils.decodeLatitude(minLatitude));
    sb.append(", maxLat=").append(GeoEncodingUtils.decodeLatitude(maxLatitude));
    sb.append(", minLon=").append(GeoEncodingUtils.decodeLongitude(minLongitude));
    sb.append(", maxLon=").append(GeoEncodingUtils.decodeLongitude(maxLongitude));
    return sb.append(")").toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    LatLonDocValuesBoxQuery other = (LatLonDocValuesBoxQuery) obj;
    return field.equals(other.field) &&
        crossesDateline == other.crossesDateline &&
        minLatitude == other.minLatitude &&
        maxLatitude == other.maxLatitude &&
        minLongitude == other.minLongitude &&
        maxLongitude == other.maxLongitude;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Boolean.hashCode(crossesDateline);
    h = 31 * h + Integer.hashCode(minLatitude);
    h = 31 * h + Integer.hashCode(maxLatitude);
    h = 31 * h + Integer.hashCode(minLongitude);
    h = 31 * h + Integer.hashCode(maxLongitude);
    return h;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final SortedNumericDocValues values = context.reader().getSortedNumericDocValues(field);
        if (values == null) {
          return null;
        }

        final TwoPhaseIterator iterator = new TwoPhaseIterator(values) {
          @Override
          public boolean matches() throws IOException {
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
              final long value = values.nextValue();
              final int lat = (int) (value >>> 32);
              if (lat < minLatitude || lat > maxLatitude) {
                // not within latitude range
                continue;
              }

              final int lon = (int) (value & 0xFFFFFFFF);
              if (crossesDateline) {
                if (lon > maxLongitude && lon < minLongitude) {
                  // not within longitude range
                  continue;
                }
              } else {
                if (lon < minLongitude || lon > maxLongitude) {
                  // not within longitude range
                  continue;
                }
              }

              return true;
            }
            return false;
          }

          @Override
          public float matchCost() {
            return 5; // 5 comparisons
          }
        };
        return new ConstantScoreScorer(this, boost, scoreMode, iterator);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }

    };
  }

}
