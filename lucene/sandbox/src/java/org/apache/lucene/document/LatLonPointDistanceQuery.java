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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoUtils;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.StringHelper;

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
      throw new IllegalArgumentException("field cannot be null");
    }
    if (Double.isFinite(radiusMeters) == false || radiusMeters < 0) {
      throw new IllegalArgumentException("radiusMeters: '" + radiusMeters + "' is invalid");
    }
    if (GeoUtils.isValidLat(latitude) == false) {
      throw new IllegalArgumentException("latitude: '" + latitude + "' is invalid");
    }
    if (GeoUtils.isValidLon(longitude) == false) {
      throw new IllegalArgumentException("longitude: '" + longitude + "' is invalid");
    }
    this.field = field;
    this.latitude = latitude;
    this.longitude = longitude;
    this.radiusMeters = radiusMeters;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    GeoRect box = GeoUtils.circleToBBox(latitude, longitude, radiusMeters);
    // create bounding box(es) for the distance range
    // these are pre-encoded with LatLonPoint's encoding
    final byte minLat[] = new byte[Integer.BYTES];
    final byte maxLat[] = new byte[Integer.BYTES];
    final byte minLon[] = new byte[Integer.BYTES];
    final byte maxLon[] = new byte[Integer.BYTES];
    // second set of longitude ranges to check (for cross-dateline case)
    final byte minLon2[] = new byte[Integer.BYTES];

    NumericUtils.intToSortableBytes(LatLonPoint.encodeLatitude(box.minLat), minLat, 0);
    NumericUtils.intToSortableBytes(LatLonPoint.encodeLatitude(box.maxLat), maxLat, 0);

    // crosses dateline: split
    if (box.crossesDateline()) {
      // box1
      NumericUtils.intToSortableBytes(Integer.MIN_VALUE, minLon, 0);
      NumericUtils.intToSortableBytes(LatLonPoint.encodeLongitude(box.maxLon), maxLon, 0);
      // box2
      NumericUtils.intToSortableBytes(LatLonPoint.encodeLongitude(box.minLon), minLon2, 0);
    } else {
      NumericUtils.intToSortableBytes(LatLonPoint.encodeLongitude(box.minLon), minLon, 0);
      NumericUtils.intToSortableBytes(LatLonPoint.encodeLongitude(box.maxLon), maxLon, 0);
      // disable box2
      NumericUtils.intToSortableBytes(Integer.MAX_VALUE, minLon2, 0);
    }

    // compute a maximum partial haversin: unless our box is crazy, we can use this bound
    // to reject edge cases faster in matches()
    final double minPartialDistance;
    if (box.maxLon - longitude < 90 && longitude - box.minLon < 90) {
      minPartialDistance = Math.max(SloppyMath.haversinSortKey(latitude, longitude, latitude, box.maxLon),
                                    SloppyMath.haversinSortKey(latitude, longitude, box.maxLat, longitude));
    } else {
      minPartialDistance = Double.POSITIVE_INFINITY;
    }

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
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
        
        // approximation (postfiltering has not yet been applied)
        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());
        // subset of documents that need no postfiltering, this is purely an optimization
        final BitSet preApproved;
        // dumb heuristic: if the field is really sparse, use a sparse impl
        if (values.getDocCount(field) * 100L < reader.maxDoc()) {
          preApproved = new SparseFixedBitSet(reader.maxDoc());
        } else {
          preApproved = new FixedBitSet(reader.maxDoc());
        }
        values.intersect(field,
                         new IntersectVisitor() {
                           @Override
                           public void grow(int count) {
                             result.grow(count);
                           }

                           @Override
                           public void visit(int docID) {
                             result.add(docID);
                             preApproved.set(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             // we bounds check individual values, as subtrees may cross, but we are being sent the values anyway:
                             // this reduces the amount of docvalues fetches (improves approximation)

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

                             result.add(docID);
                           }
                           
                           // algorithm: we create a bounding box (two bounding boxes if we cross the dateline).
                           // 1. check our bounding box(es) first. if the subtree is entirely outside of those, bail.
                           // 2. see if the subtree is fully contained. if the subtree is enormous along the x axis, wrapping half way around the world, etc: then this can't work, just go to step 3.
                           // 3. recurse naively.
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

                             double latMin = LatLonPoint.decodeLatitude(minPackedValue, 0);
                             double lonMin = LatLonPoint.decodeLongitude(minPackedValue, Integer.BYTES);
                             double latMax = LatLonPoint.decodeLatitude(maxPackedValue, 0);
                             double lonMax = LatLonPoint.decodeLongitude(maxPackedValue, Integer.BYTES);

                             if (lonMax - longitude < 90 && longitude - lonMin < 90 &&
                                 SloppyMath.haversinMeters(latitude, longitude, latMin, lonMin) <= radiusMeters &&
                                 SloppyMath.haversinMeters(latitude, longitude, latMin, lonMax) <= radiusMeters &&
                                 SloppyMath.haversinMeters(latitude, longitude, latMax, lonMin) <= radiusMeters &&
                                 SloppyMath.haversinMeters(latitude, longitude, latMax, lonMax) <= radiusMeters) {
                               // we are fully enclosed, collect everything within this subtree
                               return Relation.CELL_INSIDE_QUERY;
                             } else {
                               // recurse: its inside our bounding box(es), but not fully, or it wraps around.
                               return Relation.CELL_CROSSES_QUERY;
                             }
                           }
                         });

        DocIdSet set = result.build();
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }

        // return two-phase iterator using docvalues to postfilter candidates
        SortedNumericDocValues docValues = DocValues.getSortedNumeric(reader, field);
        TwoPhaseIterator iterator = new TwoPhaseIterator(disi) {
          @Override
          public boolean matches() throws IOException {
            int docId = disi.docID();
            if (preApproved.get(docId)) {
              return true;
            } else {
              docValues.setDocument(docId);
              int count = docValues.count();
              for (int i = 0; i < count; i++) {
                long encoded = docValues.valueAt(i);
                double docLatitude = LatLonPoint.decodeLatitude((int)(encoded >> 32));
                double docLongitude = LatLonPoint.decodeLongitude((int)(encoded & 0xFFFFFFFF));

                // first check the partial distance, if its more than that, it can't be <= radiusMeters
                double h1 = SloppyMath.haversinSortKey(latitude, longitude, docLatitude, docLongitude);
                if (h1 > minPartialDistance) {
                  continue;
                }

                // fully confirm with part 2:
                if (SloppyMath.haversinMeters(h1) <= radiusMeters) {
                  return true;
                }
              }
              return false;
            }
          }

          @Override
          public float matchCost() {
            return 20; // TODO: make this fancier
          }
        };
        return new ConstantScoreScorer(this, score(), iterator);
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
    int result = super.hashCode();
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
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    LatLonPointDistanceQuery other = (LatLonPointDistanceQuery) obj;
    if (!field.equals(other.field)) return false;
    if (Double.doubleToLongBits(latitude) != Double.doubleToLongBits(other.latitude)) return false;
    if (Double.doubleToLongBits(longitude) != Double.doubleToLongBits(other.longitude)) return false;
    if (Double.doubleToLongBits(radiusMeters) != Double.doubleToLongBits(other.radiusMeters)) return false;
    return true;
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
