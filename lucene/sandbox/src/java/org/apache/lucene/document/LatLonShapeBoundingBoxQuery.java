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

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * Finds all previously indexed shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields(String, Polygon)} added per document.
 *
 *  @lucene.experimental
 **/
class LatLonShapeBoundingBoxQuery extends Query {
  final String field;
  final byte[] bbox;
  final int minX;
  final int maxX;
  final int minY;
  final int maxY;

  public LatLonShapeBoundingBoxQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    if (minLon > maxLon) {
      throw new IllegalArgumentException("dateline crossing bounding box queries are not supported for [" + field + "]");
    }
    this.field = field;
    this.bbox = new byte[4 * LatLonPoint.BYTES];
    this.minX = encodeLongitudeCeil(minLon);
    this.maxX = encodeLongitude(maxLon);
    this.minY = encodeLatitudeCeil(minLat);
    this.maxY = encodeLatitude(maxLat);
    NumericUtils.intToSortableBytes(this.minY, this.bbox, 0);
    NumericUtils.intToSortableBytes(this.minX, this.bbox, LatLonPoint.BYTES);
    NumericUtils.intToSortableBytes(this.maxY, this.bbox, 2 * LatLonPoint.BYTES);
    NumericUtils.intToSortableBytes(this.maxX, this.bbox, 3 * LatLonPoint.BYTES);
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {

      private boolean edgeIntersectsQuery(double ax, double ay, double bx, double by) {
        // top
        if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0 &&
            orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
          return true;
        }

        // right
        if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0 &&
            orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
          return true;
        }

        // bottom
        if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0 &&
            orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
          return true;
        }

        // left
        if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0 &&
            orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
          return true;
        }
        return false;
      }

      private boolean queryContains(byte[] t, int point) {
        final int yIdx = 2 * LatLonPoint.BYTES * point;
        final int xIdx = yIdx + LatLonPoint.BYTES;

        if (FutureArrays.compareUnsigned(t, yIdx, xIdx, bbox, 0, LatLonPoint.BYTES) < 0 ||                     //minY
            FutureArrays.compareUnsigned(t, yIdx, xIdx, bbox, 2 * LatLonPoint.BYTES, 3 * LatLonPoint.BYTES) > 0 ||  //maxY
            FutureArrays.compareUnsigned(t, xIdx, xIdx + LatLonPoint.BYTES, bbox, LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) < 0 || // minX
            FutureArrays.compareUnsigned(t, xIdx, xIdx + LatLonPoint.BYTES, bbox, 3 * LatLonPoint.BYTES, bbox.length) > 0) {
          return false;
        }
        return true;
      }

      private boolean queryIntersects(int ax, int ay, int bx, int by, int cx, int cy) {
        // check each edge of the triangle against the query
        if (edgeIntersectsQuery(ax, ay, bx, by) ||
            edgeIntersectsQuery(bx, by, cx, cy) ||
            edgeIntersectsQuery(cx, cy, ax, ay)) {
          return true;
        }
        return false;
      }

      private boolean queryCrossesTriangle(byte[] t) {
        // 1. query contains any triangle points
        if (queryContains(t, 0) || queryContains(t, 1) || queryContains(t, 2)) {
          return true;
        }

        int aY = NumericUtils.sortableBytesToInt(t, 0);
        int aX = NumericUtils.sortableBytesToInt(t, LatLonPoint.BYTES);
        int bY = NumericUtils.sortableBytesToInt(t, 2 * LatLonPoint.BYTES);
        int bX = NumericUtils.sortableBytesToInt(t, 3 * LatLonPoint.BYTES);
        int cY = NumericUtils.sortableBytesToInt(t, 4 * LatLonPoint.BYTES);
        int cX = NumericUtils.sortableBytesToInt(t, 5 * LatLonPoint.BYTES);

        int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
        int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
        int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
        int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

        // 2. check bounding boxes are disjoint
        if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
          return false;
        }

        // 3. check triangle contains any query points
        if (Tessellator.pointInTriangle(minX, minY, aX, aY, bX, bY, cX, cY)) {
          return true;
        } else if (Tessellator.pointInTriangle(maxX, minY, aX, aY, bX, bY, cX, cY)) {
          return true;
        } else if (Tessellator.pointInTriangle(maxX, maxY, aX, aY, bX, bY, cX, cY)) {
          return true;
        } else if (Tessellator.pointInTriangle(minX, maxY, aX, aY, bX, bY, cX, cY)) {
          return true;
        }


        // 4. last ditch effort: check crossings
        if (queryIntersects(aX, aY, bX, bY, cX, cY)) {
          return true;
        }
        return false;
      }

      private Relation relateRangeToQuery(byte[] minTriangle, byte[] maxTriangle) {
        // compute bounding box
        int minXOfs = 0;
        int minYOfs = 0;
        int maxXOfs = 0;
        int maxYOfs = 0;
        for (int d = 1; d < 3; ++d) {
          // check minX
          int aOfs = (minXOfs * 2 * LatLonPoint.BYTES) + LatLonPoint.BYTES;
          int bOfs = (d * 2 * LatLonPoint.BYTES) + LatLonPoint.BYTES;
          if (FutureArrays.compareUnsigned(minTriangle, bOfs, bOfs + LatLonPoint.BYTES, minTriangle, aOfs, aOfs + LatLonPoint.BYTES) < 0) {
            minXOfs = d;
          }
          // check maxX
          aOfs = (maxXOfs * 2 * LatLonPoint.BYTES) + LatLonPoint.BYTES;
          if (FutureArrays.compareUnsigned(maxTriangle, bOfs, bOfs + LatLonPoint.BYTES, maxTriangle, aOfs, aOfs + LatLonPoint.BYTES) > 0) {
            maxXOfs = d;
          }
          // check minY
          aOfs = minYOfs * 2 * LatLonPoint.BYTES;
          bOfs = d * 2 * LatLonPoint.BYTES;
          if (FutureArrays.compareUnsigned(minTriangle, bOfs, bOfs + LatLonPoint.BYTES, minTriangle, aOfs, aOfs + LatLonPoint.BYTES) < 0) {
            minYOfs = d;
          }
          // check maxY
          aOfs = maxYOfs * 2 * LatLonPoint.BYTES;
          if (FutureArrays.compareUnsigned(maxTriangle, bOfs, bOfs + LatLonPoint.BYTES, maxTriangle, aOfs, aOfs + LatLonPoint.BYTES) > 0) {
            maxYOfs = d;
          }
        }
        minXOfs = (minXOfs * 2 * LatLonPoint.BYTES) + LatLonPoint.BYTES;
        maxXOfs = (maxXOfs * 2 * LatLonPoint.BYTES) + LatLonPoint.BYTES;
        minYOfs *= 2 * LatLonPoint.BYTES;
        maxYOfs *= 2 * LatLonPoint.BYTES;

        // check bounding box (DISJOINT)
        if (FutureArrays.compareUnsigned(minTriangle, minXOfs, minXOfs + LatLonPoint.BYTES, bbox, 3 * LatLonPoint.BYTES, 4 * LatLonPoint.BYTES) > 0 ||
            FutureArrays.compareUnsigned(maxTriangle, maxXOfs, maxXOfs + LatLonPoint.BYTES, bbox, LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) < 0 ||
            FutureArrays.compareUnsigned(minTriangle, minYOfs, minYOfs + LatLonPoint.BYTES, bbox, 2 * LatLonPoint.BYTES, 3 * LatLonPoint.BYTES) > 0 ||
            FutureArrays.compareUnsigned(maxTriangle, maxYOfs, maxYOfs + LatLonPoint.BYTES, bbox, 0, LatLonPoint.BYTES) < 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        }

        if (FutureArrays.compareUnsigned(minTriangle, minXOfs, minXOfs + LatLonPoint.BYTES, bbox, LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) > 0 &&
            FutureArrays.compareUnsigned(maxTriangle, maxXOfs, maxXOfs + LatLonPoint.BYTES, bbox, 3 * LatLonPoint.BYTES, 4 * LatLonPoint.BYTES) < 0 &&
            FutureArrays.compareUnsigned(minTriangle, minYOfs, minYOfs + LatLonPoint.BYTES, bbox, 0, LatLonPoint.BYTES) > 0 &&
            FutureArrays.compareUnsigned(maxTriangle, maxYOfs, maxYOfs + LatLonPoint.BYTES, bbox, 2 * LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) < 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        return Relation.CELL_CROSSES_QUERY;
      }

      private IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
        return new IntersectVisitor() {

          DocIdSetBuilder.BulkAdder adder;

          @Override
          public void grow(int count) {
            adder = result.grow(count);
          }

          @Override
          public void visit(int docID) throws IOException {
            adder.add(docID);
          }

          @Override
          public void visit(int docID, byte[] t) throws IOException {
            if (queryCrossesTriangle(t)) {
              adder.add(docID);
            }
          }

          @Override
          public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
            return relateRangeToQuery(minTriangle, maxTriangle);
          }
        };
      }

      /**
       * Create a visitor that clears documents that do NOT match the bounding box query.
       */
      private IntersectVisitor getInverseIntersectVisitor(FixedBitSet result, int[] cost) {
        return new IntersectVisitor() {

          @Override
          public void visit(int docID) {
            result.clear(docID);
            cost[0]--;
          }

          @Override
          public void visit(int docID, byte[] packedTriangle) {
            if (queryCrossesTriangle(packedTriangle) == false) {
              result.clear(docID);
              cost[0]--;
            }
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            Relation r = relateRangeToQuery(minPackedValue, maxPackedValue);
            if (r == Relation.CELL_OUTSIDE_QUERY) {
              return Relation.CELL_INSIDE_QUERY;
            } else if (r == Relation.CELL_INSIDE_QUERY) {
              return Relation.CELL_OUTSIDE_QUERY;
            }
            return r;
          }
        };
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues(field);
        if (values == null) {
          return null;
        }

        boolean allDocsMatch = true;
        if (values.getDocCount() != reader.maxDoc() ||
            relateRangeToQuery(values.getMinPackedValue(), values.getMaxPackedValue()) != Relation.CELL_INSIDE_QUERY) {
          allDocsMatch = false;
        }

        final Weight weight = this;
        if (allDocsMatch) {
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return new ConstantScoreScorer(weight, score(),
                  DocIdSetIterator.all(reader.maxDoc()));
            }

            @Override
            public long cost() {
              return reader.maxDoc();
            }
          };
        } else {
          return new ScorerSupplier() {
            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
            final IntersectVisitor visitor = getIntersectVisitor(result);
            long cost = -1;

            @Override
            public Scorer get(long leadCost) throws IOException {
              if (values.getDocCount() == reader.maxDoc()
                  && values.getDocCount() == values.size()
                  && cost() > reader.maxDoc() / 2) {
                // If all docs have exactly one value and the cost is greater
                // than half the leaf size then maybe we can make things faster
                // by computing the set of documents that do NOT match the query
                final FixedBitSet result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc());
                int[] cost = new int[]{reader.maxDoc()};
                values.intersect(getInverseIntersectVisitor(result, cost));
                final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
                return new ConstantScoreScorer(weight, score(), iterator);
              }

              values.intersect(visitor);
              DocIdSetIterator iterator = result.build().iterator();
              return new ConstantScoreScorer(weight, score(), iterator);
            }

            @Override
            public long cost() {
              if (cost == -1) {
                // Computing the cost may be expensive, so only do it if necessary
                cost = values.estimatePointCount(visitor);
                assert cost >= 0;
              }
              return cost;
            }
          };
        }
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  public String getField() {
    return field;
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(LatLonShapeBoundingBoxQuery o) {
    return Objects.equals(field, o.field) &&
        Arrays.equals(bbox, o.bbox);
  }

  @Override
  public int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + Arrays.hashCode(bbox);
    return hash;
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
    sb.append("Rectangle(lat=");
    sb.append(decodeLatitude(minY));
    sb.append(" TO ");
    sb.append(decodeLatitude(maxY));
    sb.append(" lon=");
    sb.append(decodeLongitude(minX));
    sb.append(" TO ");
    sb.append(decodeLongitude(maxX));
    sb.append(")");
    return sb.toString();
  }
}
