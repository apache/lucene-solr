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

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
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
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** Finds all previously indexed points that fall within the specified geometries.
 *
 *  <p>The field must be indexed with using {@link LatLonPoint} added per document.
 *
 *  @lucene.experimental */

final class LatLonPointInGeometryQuery extends Query {
  final String field;
  final LatLonGeometry[] geometries;

  LatLonPointInGeometryQuery(String field, LatLonGeometry[] geometries) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (geometries == null) {
      throw new IllegalArgumentException("geometries must not be null");
    }
    if (geometries.length == 0) {
      throw new IllegalArgumentException("geometries must not be empty");
    }
    for (int i = 0; i < geometries.length; i++) {
      if (geometries[i] == null) {
        throw new IllegalArgumentException("geometries[" + i + "] must not be null");
      }
      if (geometries[i] instanceof Line) {
        throw new IllegalArgumentException("LatLonPointInGeometryQuery does not support queries with line geometries");
      }
    }
    this.field = field;
    this.geometries = geometries.clone();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  private IntersectVisitor getIntersectVisitor(DocIdSetBuilder result, Component2D tree, GeoEncodingUtils.Component2DPredicate component2DPredicate,
                                               byte[] minLat, byte[] maxLat, byte[] minLon, byte[] maxLon) {
    return new IntersectVisitor() {
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
            if (component2DPredicate.test(NumericUtils.sortableBytesToInt(packedValue, 0),
                NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES))) {
              visit(docID);
            }
          }

          @Override
          public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (component2DPredicate.test(NumericUtils.sortableBytesToInt(packedValue, 0),
                NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES))) {
              int docID;
              while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                visit(docID);
              }
            }
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            if (Arrays.compareUnsigned(minPackedValue, 0, Integer.BYTES, maxLat, 0, Integer.BYTES) > 0 ||
                Arrays.compareUnsigned(maxPackedValue, 0, Integer.BYTES, minLat, 0, Integer.BYTES) < 0 ||
                Arrays.compareUnsigned(minPackedValue, Integer.BYTES, Integer.BYTES + Integer.BYTES, maxLon, 0, Integer.BYTES) > 0 ||
                Arrays.compareUnsigned(maxPackedValue, Integer.BYTES, Integer.BYTES + Integer.BYTES, minLon, 0, Integer.BYTES) < 0) {
              // outside of global bounding box range
              return Relation.CELL_OUTSIDE_QUERY;
            }

            double cellMinLat = decodeLatitude(minPackedValue, 0);
            double cellMinLon = decodeLongitude(minPackedValue, Integer.BYTES);
            double cellMaxLat = decodeLatitude(maxPackedValue, 0);
            double cellMaxLon = decodeLongitude(maxPackedValue, Integer.BYTES);

            return tree.relate(cellMinLon, cellMaxLon, cellMinLat, cellMaxLat);
          }
        };
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

    final Component2D tree = LatLonGeometry.create(geometries);
    final GeoEncodingUtils.Component2DPredicate component2DPredicate = GeoEncodingUtils.createComponentPredicate(tree);
    // bounding box over all geometries, this can speed up tree intersection/cheaply improve approximation for complex multi-geometries
    final byte minLat[] = new byte[Integer.BYTES];
    final byte maxLat[] = new byte[Integer.BYTES];
    final byte minLon[] = new byte[Integer.BYTES];
    final byte maxLon[] = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(encodeLatitude(tree.getMinY()), minLat, 0);
    NumericUtils.intToSortableBytes(encodeLatitude(tree.getMaxY()), maxLat, 0);
    NumericUtils.intToSortableBytes(encodeLongitude(tree.getMinX()), minLon, 0);
    NumericUtils.intToSortableBytes(encodeLongitude(tree.getMaxX()), maxLon, 0);

    return new ConstantScoreWeight(this, boost) {

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
        LatLonPoint.checkCompatible(fieldInfo);
        final Weight weight = this;

        return new ScorerSupplier() {

          long cost = -1;
          DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
          final IntersectVisitor visitor = getIntersectVisitor(result, tree, component2DPredicate, minLat, maxLat, minLon, maxLon);

          @Override
          public Scorer get(long leadCost) throws IOException {
            values.intersect(visitor);
            return new ConstantScoreScorer(weight, score(), scoreMode, result.build().iterator());
          }

          @Override
          public long cost() {
            if (cost == -1) {
               // Computing the cost may be expensive, so only do it if necessary
              cost = values.estimateDocCount(visitor);
              assert cost >= 0;
            }
            return cost;
          }
        };
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

  /** Returns the query field */
  public String getField() {
    return field;
  }

  /** Returns a copy of the internal geometry array */
  public LatLonGeometry[] getGeometries() {
    return geometries.clone();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + field.hashCode();
    result = prime * result + Arrays.hashCode(geometries);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(LatLonPointInGeometryQuery other) {
    return field.equals(other.field) &&
           Arrays.equals(geometries, other.geometries);
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
    sb.append(Arrays.toString(geometries));
    return sb.toString();
  }
}
