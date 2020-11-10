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
package org.apache.lucene.spatial3d;

import java.io.IOException;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.XYZBounds;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/** Finds all previously indexed points that fall within the specified polygon.
 *
 * <p>The field must be indexed using {@link Geo3DPoint}.
 *
 * @lucene.experimental */

final class PointInGeo3DShapeQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(PointInGeo3DShapeQuery.class);

  final String field;
  final GeoShape shape;
  final XYZBounds shapeBounds;

  /** The lats/lons must be clockwise or counter-clockwise. */
  public PointInGeo3DShapeQuery(String field, GeoShape shape) {
    this.field = field;
    this.shape = shape;
    this.shapeBounds = new XYZBounds();
    shape.getBounds(shapeBounds);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues(field);
        if (values == null) {
          return null;
        }

        /*
        XYZBounds bounds = new XYZBounds();
        shape.getBounds(bounds);

        final double planetMax = planetModel.getMaximumMagnitude();
        if (planetMax != treeDV.planetMax) {
          throw new IllegalStateException(planetModel + " is not the same one used during indexing: planetMax=" + planetMax + " vs indexing planetMax=" + treeDV.planetMax);
        }
        */

        /*
        GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(planetModel,
                                                      bounds.getMinimumX(),
                                                      bounds.getMaximumX(),
                                                      bounds.getMinimumY(),
                                                      bounds.getMaximumY(),
                                                      bounds.getMinimumZ(),
                                                      bounds.getMaximumZ());

        assert xyzSolid.getRelationship(shape) == GeoArea.WITHIN || xyzSolid.getRelationship(shape) == GeoArea.OVERLAPS: "expected WITHIN (1) or OVERLAPS (2) but got " + xyzSolid.getRelationship(shape) + "; shape="+shape+"; XYZSolid="+xyzSolid;
        */

        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);

        values.intersect(new PointInShapeIntersectVisitor(result, shape, shapeBounds));

        return new ConstantScoreScorer(this, score(), scoreMode, result.build().iterator());
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

  public GeoShape getShape() {
    return shape;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(PointInGeo3DShapeQuery other) {
    return field.equals(other.field) &&
           shape.equals(other.shape);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + field.hashCode();
    result = 31 * result + shape.hashCode();
    return result;
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
    sb.append(" Shape: ");
    sb.append(shape);
    return sb.toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(field) +
        RamUsageEstimator.sizeOfObject(shape) +
        RamUsageEstimator.sizeOfObject(shapeBounds);
  }
}
