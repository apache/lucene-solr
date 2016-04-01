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
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.Polygon;

/** Finds all previously indexed points that fall within the specified polygons.
 *
 *  <p>The field must be indexed with using {@link org.apache.lucene.document.LatLonPoint} added per document.
 *
 *  @lucene.experimental */

final class LatLonPointInPolygonQuery extends Query {
  final String field;
  final Polygon[] polygons;

  /** The lats/lons must be clockwise or counter-clockwise. */
  public LatLonPointInPolygonQuery(String field, Polygon[] polygons) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (polygons == null) {
      throw new IllegalArgumentException("polygons must not be null");
    }
    if (polygons.length == 0) {
      throw new IllegalArgumentException("polygons must not be empty");
    }
    for (int i = 0; i < polygons.length; i++) {
      if (polygons[i] == null) {
        throw new IllegalArgumentException("polygon[" + i + "] must not be null");
      }
    }
    this.field = field;
    this.polygons = polygons.clone();
    // TODO: we could also compute the maximal inner bounding box, to make relations faster to compute?
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:
    
    // bounding box over all polygons, this can speed up tree intersection/cheaply improve approximation for complex multi-polygons
    // these are pre-encoded with LatLonPoint's encoding
    final GeoRect box = Polygon.getBoundingBox(polygons);
    final byte minLat[] = new byte[Integer.BYTES];
    final byte maxLat[] = new byte[Integer.BYTES];
    final byte minLon[] = new byte[Integer.BYTES];
    final byte maxLon[] = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(LatLonPoint.encodeLatitude(box.minLat), minLat, 0);
    NumericUtils.intToSortableBytes(LatLonPoint.encodeLatitude(box.maxLat), maxLat, 0);
    NumericUtils.intToSortableBytes(LatLonPoint.encodeLongitude(box.minLon), minLon, 0);
    NumericUtils.intToSortableBytes(LatLonPoint.encodeLongitude(box.maxLon), maxLon, 0);

    // TODO: make this fancier, but currently linear with number of vertices
    float cumulativeCost = 0;
    for (Polygon polygon : polygons) {
      cumulativeCost += 20 * (polygon.getPolyLats().length + polygon.getHoles().length);
    }
    final float matchCost = cumulativeCost;

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
                           public void visit(int docID) {
                             result.add(docID);
                             preApproved.set(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             // we bounds check individual values, as subtrees may cross, but we are being sent the values anyway:
                             // this reduces the amount of docvalues fetches (improves approximation)
                             if (StringHelper.compare(Integer.BYTES, packedValue, 0, maxLat, 0) > 0 ||
                                 StringHelper.compare(Integer.BYTES, packedValue, 0, minLat, 0) < 0 ||
                                 StringHelper.compare(Integer.BYTES, packedValue, Integer.BYTES, maxLon, 0) > 0 ||
                                 StringHelper.compare(Integer.BYTES, packedValue, Integer.BYTES, minLon, 0) < 0) {
                               // outside of global bounding box range
                               return;
                             }
                             result.add(docID);
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                             if (StringHelper.compare(Integer.BYTES, minPackedValue, 0, maxLat, 0) > 0 ||
                                 StringHelper.compare(Integer.BYTES, maxPackedValue, 0, minLat, 0) < 0 ||
                                 StringHelper.compare(Integer.BYTES, minPackedValue, Integer.BYTES, maxLon, 0) > 0 ||
                                 StringHelper.compare(Integer.BYTES, maxPackedValue, Integer.BYTES, minLon, 0) < 0) {
                               // outside of global bounding box range
                               return Relation.CELL_OUTSIDE_QUERY;
                             }
                             
                             double cellMinLat = LatLonPoint.decodeLatitude(minPackedValue, 0);
                             double cellMinLon = LatLonPoint.decodeLongitude(minPackedValue, Integer.BYTES);
                             double cellMaxLat = LatLonPoint.decodeLatitude(maxPackedValue, 0);
                             double cellMaxLon = LatLonPoint.decodeLongitude(maxPackedValue, Integer.BYTES);

                             if (Polygon.contains(polygons, cellMinLat, cellMaxLat, cellMinLon, cellMaxLon)) {
                               return Relation.CELL_INSIDE_QUERY;
                             } else if (Polygon.crosses(polygons, cellMinLat, cellMaxLat, cellMinLon, cellMaxLon)) {
                               return Relation.CELL_CROSSES_QUERY;
                             } else {
                               return Relation.CELL_OUTSIDE_QUERY;
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
                if (Polygon.contains(polygons, docLatitude, docLongitude)) {
                  return true;
                }
              }
              return false;
            }
          }

          @Override
          public float matchCost() {
            return matchCost;
          }
        };
        return new ConstantScoreScorer(this, score(), iterator);
      }
    };
  }

  /** Returns the query field */
  public String getField() {
    return field;
  }

  /** Returns a copy of the internal polygon array */
  public Polygon[] getPolygons() {
    return polygons.clone();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + field.hashCode();
    result = prime * result + Arrays.hashCode(polygons);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    LatLonPointInPolygonQuery other = (LatLonPointInPolygonQuery) obj;
    if (!field.equals(other.field)) return false;
    if (!Arrays.equals(polygons, other.polygons)) return false;
    return true;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(Arrays.toString(polygons));
    return sb.toString();
  }
}
