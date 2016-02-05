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
package org.apache.lucene.geo3d;

import java.io.IOException;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;

/** Finds all previously indexed points that fall within the specified polygon.
 *
 * <p>The field must be indexed using {@link Geo3DPoint}.
 *
 * @lucene.experimental */

public class PointInGeo3DShapeQuery extends Query {
  final String field;
  final PlanetModel planetModel;
  final GeoShape shape;

  /** The lats/lons must be clockwise or counter-clockwise. */
  public PointInGeo3DShapeQuery(PlanetModel planetModel, String field, GeoShape shape) {
    this.field = field;
    this.planetModel = planetModel;
    this.shape = shape;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues();
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

        double planetMax = planetModel.getMaximumMagnitude();

        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());

        int[] hitCount = new int[1];
        values.intersect(field,
                         new IntersectVisitor() {

                           @Override
                           public void visit(int docID) {
                             result.add(docID);
                             hitCount[0]++;
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             assert packedValue.length == 12;
                             double x = Geo3DUtil.decodeValueCenter(planetMax, NumericUtils.bytesToInt(packedValue, 0));
                             double y = Geo3DUtil.decodeValueCenter(planetMax, NumericUtils.bytesToInt(packedValue, 1));
                             double z = Geo3DUtil.decodeValueCenter(planetMax, NumericUtils.bytesToInt(packedValue, 2));
                             if (shape.isWithin(x, y, z)) {
                               result.add(docID);
                               hitCount[0]++;
                             }
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                             // Because the dimensional format operates in quantized (64 bit -> 32 bit) space, and the cell bounds
                             // here are inclusive, we need to extend the bounds to the largest un-quantized values that
                             // could quantize into these bounds.  The encoding (Geo3DUtil.encodeValue) does
                             // a Math.round from double to long, so e.g. 1.4 -> 1, and -1.4 -> -1:
                             double xMin = Geo3DUtil.decodeValueMin(planetMax, NumericUtils.bytesToInt(minPackedValue, 0));
                             double xMax = Geo3DUtil.decodeValueMax(planetMax, NumericUtils.bytesToInt(maxPackedValue, 0));
                             double yMin = Geo3DUtil.decodeValueMin(planetMax, NumericUtils.bytesToInt(minPackedValue, 1));
                             double yMax = Geo3DUtil.decodeValueMax(planetMax, NumericUtils.bytesToInt(maxPackedValue, 1));
                             double zMin = Geo3DUtil.decodeValueMin(planetMax, NumericUtils.bytesToInt(minPackedValue, 2));
                             double zMax = Geo3DUtil.decodeValueMax(planetMax, NumericUtils.bytesToInt(maxPackedValue, 2));

                             //System.out.println("  compare: x=" + cellXMin + "-" + cellXMax + " y=" + cellYMin + "-" + cellYMax + " z=" + cellZMin + "-" + cellZMax);
                             assert xMin <= xMax;
                             assert yMin <= yMax;
                             assert zMin <= zMax;

                             GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(planetModel, xMin, xMax, yMin, yMax, zMin, zMax);

                             switch(xyzSolid.getRelationship(shape)) {
                             case GeoArea.CONTAINS:
                               // Shape fully contains the cell
                               //System.out.println("    inside");
                               return Relation.CELL_INSIDE_QUERY;
                             case GeoArea.OVERLAPS:
                               // They do overlap but neither contains the other:
                               //System.out.println("    crosses1");
                               return Relation.CELL_CROSSES_QUERY;
                             case GeoArea.WITHIN:
                               // Cell fully contains the shape:
                               //System.out.println("    crosses2");
                               // return Relation.SHAPE_INSIDE_CELL;
                               return Relation.CELL_CROSSES_QUERY;
                             case GeoArea.DISJOINT:
                               // They do not overlap at all
                               //System.out.println("    outside");
                               return Relation.CELL_OUTSIDE_QUERY;
                             default:
                               assert false;
                               return Relation.CELL_CROSSES_QUERY;
                             }
                           }
                         });

        // NOTE: hitCount[0] will be over-estimate in multi-valued case
        return new ConstantScoreScorer(this, score(), result.build(hitCount[0]).iterator());
      }
    };
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    PointInGeo3DShapeQuery that = (PointInGeo3DShapeQuery) o;

    return planetModel.equals(that.planetModel) && shape.equals(that.shape);
  }

  @Override
  public final int hashCode() {
    int result = super.hashCode();
    result = 31 * result + planetModel.hashCode();
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
    sb.append("PlanetModel: ");
    sb.append(planetModel);
    sb.append(" Shape: ");
    sb.append(shape);
    return sb.toString();
  }
}
