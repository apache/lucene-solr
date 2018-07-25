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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/** Distance query for {@link LatLonDocValuesField}. */
final class LatLonDocValuesDistanceQuery extends Query {

  private final String field;
  private final double latitude, longitude;
  private final double radiusMeters;

  LatLonDocValuesDistanceQuery(String field, double latitude, double longitude, double radiusMeters) {
    if (Double.isFinite(radiusMeters) == false || radiusMeters < 0) {
      throw new IllegalArgumentException("radiusMeters: '" + radiusMeters + "' is invalid");
    }
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    this.field = field;
    this.latitude = latitude;
    this.longitude = longitude;
    this.radiusMeters = radiusMeters;
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (!this.field.equals(field)) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(latitude);
    sb.append(",");
    sb.append(longitude);
    sb.append(" +/- ");
    sb.append(radiusMeters);
    sb.append(" meters");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    LatLonDocValuesDistanceQuery other = (LatLonDocValuesDistanceQuery) obj;
    return field.equals(other.field) &&
        Double.doubleToLongBits(latitude) == Double.doubleToLongBits(other.latitude) &&
        Double.doubleToLongBits(longitude) == Double.doubleToLongBits(other.longitude) &&
        Double.doubleToLongBits(radiusMeters) == Double.doubleToLongBits(other.radiusMeters);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Double.hashCode(latitude);
    h = 31 * h + Double.hashCode(longitude);
    h = 31 * h + Double.hashCode(radiusMeters);
    return h;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {

      private final GeoEncodingUtils.DistancePredicate distancePredicate = GeoEncodingUtils.createDistancePredicate(latitude, longitude, radiusMeters);

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
              final int lon = (int) (value & 0xFFFFFFFF);
              if (distancePredicate.test(lat, lon)) {
                return true;
              }
            }
            return false;
          }

          @Override
          public float matchCost() {
            return 100f; // TODO: what should it be?
          }

        };
        return new ConstantScoreScorer(this, boost, iterator);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }

    };
  }

}
