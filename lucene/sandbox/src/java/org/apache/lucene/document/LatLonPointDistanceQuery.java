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
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * Distance query for {@link LatLonPoint}.
 */
final class LatLonPointDistanceQuery extends Query {
  final String field;
  final double latitude;
  final double longitude;
  final double radiusMeters;

  public LatLonPointDistanceQuery(String field, double latitude, double longitude, double radiusMeters) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (Double.isFinite(radiusMeters) == false || radiusMeters < 0) {
      throw new IllegalArgumentException("radiusMeters: '" + radiusMeters + "' is invalid");
    }
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    this.field = field;
    this.latitude = latitude;
    this.longitude = longitude;
    this.radiusMeters = radiusMeters;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    Rectangle box = Rectangle.fromPointDistance(latitude, longitude, radiusMeters);
    // create bounding box(es) for the distance range
    // these are pre-encoded with LatLonPoint's encoding
    final byte minLat[] = new byte[Integer.BYTES];
    final byte maxLat[] = new byte[Integer.BYTES];
    final byte minLon[] = new byte[Integer.BYTES];
    final byte maxLon[] = new byte[Integer.BYTES];
    // second set of longitude ranges to check (for cross-dateline case)
    final byte minLon2[] = new byte[Integer.BYTES];

    NumericUtils.intToSortableBytes(encodeLatitude(box.minLat), minLat, 0);
    NumericUtils.intToSortableBytes(encodeLatitude(box.maxLat), maxLat, 0);

    // crosses dateline: split
    if (box.crossesDateline()) {
      // box1
      NumericUtils.intToSortableBytes(Integer.MIN_VALUE, minLon, 0);
      NumericUtils.intToSortableBytes(encodeLongitude(box.maxLon), maxLon, 0);
      // box2
      NumericUtils.intToSortableBytes(encodeLongitude(box.minLon), minLon2, 0);
    } else {
      NumericUtils.intToSortableBytes(encodeLongitude(box.minLon), minLon, 0);
      NumericUtils.intToSortableBytes(encodeLongitude(box.maxLon), maxLon, 0);
      // disable box2
      NumericUtils.intToSortableBytes(Integer.MAX_VALUE, minLon2, 0);
    }

    // compute exact sort key: avoid any asin() computations
    final double sortKey = GeoUtils.distanceQuerySortKey(radiusMeters);

    final double axisLat = Rectangle.axisLat(latitude, radiusMeters);

    return new ConstantScoreWeight(this) {

      final GeoEncodingUtils.DistancePredicate distancePredicate = GeoEncodingUtils.createDistancePredicate(latitude, longitude, radiusMeters);

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(false);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues();
        if (values == null) {
          // No docs in this segment had any points fields
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }
        LatLonPoint.checkCompatible(fieldInfo);
        
        // matching docids
        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
        final IntersectVisitor visitor =
                         new IntersectVisitor() {

                           DocIdSetBuilder.BulkAdder adder;

                           @Override
                           public void grow(int count) {
                             adder = result.grow(count);
                           }

                           @Override
                           public void visit(int docID) {
                             adder.add(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             // bounding box check
                             if (StringHelper.compare(Integer.BYTES, packedValue, 0, maxLat, 0) > 0 ||
                                 StringHelper.compare(Integer.BYTES, packedValue, 0, minLat, 0) < 0) {
                               // latitude out of bounding box range
                               return;
                             }

                             if ((StringHelper.compare(Integer.BYTES, packedValue, Integer.BYTES, maxLon, 0) > 0 ||
                                  StringHelper.compare(Integer.BYTES, packedValue, Integer.BYTES, minLon, 0) < 0)
                                 && StringHelper.compare(Integer.BYTES, packedValue, Integer.BYTES, minLon2, 0) < 0) {
                               // longitude out of bounding box range
                               return;
                             }

                             int docLatitude = NumericUtils.sortableBytesToInt(packedValue, 0);
                             int docLongitude = NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES);
                             if (distancePredicate.test(docLatitude, docLongitude)) {
                               adder.add(docID);
                             }
                           }
                           
                           // algorithm: we create a bounding box (two bounding boxes if we cross the dateline).
                           // 1. check our bounding box(es) first. if the subtree is entirely outside of those, bail.
                           // 2. check if the subtree is disjoint. it may cross the bounding box but not intersect with circle
                           // 3. see if the subtree is fully contained. if the subtree is enormous along the x axis, wrapping half way around the world, etc: then this can't work, just go to step 4.
                           // 4. recurse naively (subtrees crossing over circle edge)
                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                             if (StringHelper.compare(Integer.BYTES, minPackedValue, 0, maxLat, 0) > 0 ||
                                 StringHelper.compare(Integer.BYTES, maxPackedValue, 0, minLat, 0) < 0) {
                               // latitude out of bounding box range
                               return Relation.CELL_OUTSIDE_QUERY;
                             }

                             if ((StringHelper.compare(Integer.BYTES, minPackedValue, Integer.BYTES, maxLon, 0) > 0 ||
                                  StringHelper.compare(Integer.BYTES, maxPackedValue, Integer.BYTES, minLon, 0) < 0)
                                 && StringHelper.compare(Integer.BYTES, maxPackedValue, Integer.BYTES, minLon2, 0) < 0) {
                               // longitude out of bounding box range
                               return Relation.CELL_OUTSIDE_QUERY;
                             }

                             double latMin = decodeLatitude(minPackedValue, 0);
                             double lonMin = decodeLongitude(minPackedValue, Integer.BYTES);
                             double latMax = decodeLatitude(maxPackedValue, 0);
                             double lonMax = decodeLongitude(maxPackedValue, Integer.BYTES);

                             return GeoUtils.relate(latMin, latMax, lonMin, lonMax, latitude, longitude, sortKey, axisLat);
                           }
                         };
        final Weight weight = this;
        return new ScorerSupplier() {

          long cost = -1;

          @Override
          public Scorer get(boolean randomAccess) throws IOException {
            values.intersect(field, visitor);
            return new ConstantScoreScorer(weight, score(), result.build().iterator());
          }

          @Override
          public long cost() {
            if (cost == -1) {
              cost = values.estimatePointCount(field, visitor);
            }
            assert cost >= 0;
            return cost;
          }
        };

      }
    };
  }

  public String getField() {
    return field;
  }

  public double getLatitude() {
    return latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public double getRadiusMeters() {
    return radiusMeters;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + field.hashCode();
    long temp;
    temp = Double.doubleToLongBits(latitude);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(longitude);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(radiusMeters);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(LatLonPointDistanceQuery other) {
    return field.equals(other.field) &&
           Double.doubleToLongBits(latitude) == Double.doubleToLongBits(other.latitude) &&
           Double.doubleToLongBits(longitude) == Double.doubleToLongBits(other.longitude) &&
           Double.doubleToLongBits(radiusMeters) == Double.doubleToLongBits(other.radiusMeters);
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
}
