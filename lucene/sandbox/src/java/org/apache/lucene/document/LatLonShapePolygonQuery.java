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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.FieldInfo;
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

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

/**
 * Finds all previously indexed shapes that intersect the specified arbitrary.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields(String, Polygon)} added per document.
 *
 *  @lucene.experimental
 **/
public class LatLonShapePolygonQuery extends Query {
  final String field;
  final Polygon[] polygons;


  public LatLonShapePolygonQuery(String field, Polygon... polygons) {
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
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final Rectangle box = Rectangle.fromPolygon(polygons);
    final byte minLat[] = new byte[Integer.BYTES];
    final byte maxLat[] = new byte[Integer.BYTES];
    final byte minLon[] = new byte[Integer.BYTES];
    final byte maxLon[] = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(encodeLatitudeCeil(box.minLat), minLat, 0);
    NumericUtils.intToSortableBytes(encodeLatitude(box.maxLat), maxLat, 0);
    NumericUtils.intToSortableBytes(encodeLongitudeCeil(box.minLon), minLon, 0);
    NumericUtils.intToSortableBytes(encodeLongitude(box.maxLon), maxLon, 0);

    final Polygon2D polygon = Polygon2D.create(polygons);

    return new ConstantScoreWeight(this, boost) {

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

        double minLat = GeoEncodingUtils.decodeLatitude(minTriangle, minYOfs);
        double minLon = GeoEncodingUtils.decodeLongitude(minTriangle, minXOfs);
        double maxLat = GeoEncodingUtils.decodeLatitude(maxTriangle, maxYOfs);
        double maxLon = GeoEncodingUtils.decodeLongitude(maxTriangle, maxXOfs);

        // check internal node against query
        return polygon.relate(minLat, maxLat, minLon, maxLon);
      }

      private boolean queryCrossesTriangle(byte[] t) {
        double ay = GeoEncodingUtils.decodeLatitude(t, 0);
        double ax = GeoEncodingUtils.decodeLongitude(t, LatLonPoint.BYTES);
        double by = GeoEncodingUtils.decodeLatitude(t, 2 * LatLonPoint.BYTES);
        double bx = GeoEncodingUtils.decodeLongitude(t, 3 * LatLonPoint.BYTES);
        double cy = GeoEncodingUtils.decodeLatitude(t, 4 * LatLonPoint.BYTES);
        double cx = GeoEncodingUtils.decodeLongitude(t, 5 * LatLonPoint.BYTES);
        return polygon.relateTriangle(ax, ay, bx, by, cx, cy) != Relation.CELL_OUTSIDE_QUERY;
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
       * Create a visitor that clears documents that do NOT match the polygon query.
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
          // No docs in this segment had any points fields
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
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
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("Polygon(" + polygons[0].toGeoJSON() + ")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(LatLonShapePolygonQuery o) {
    return Objects.equals(field, o.field) && Arrays.equals(polygons, o.polygons);
  }

  @Override
  public int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + Arrays.hashCode(polygons);
    return hash;
  }
}