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
package org.apache.lucene.bkdtree3d;

import org.apache.lucene.geo3d.GeoArea;
import org.apache.lucene.geo3d.GeoAreaFactory;
import org.apache.lucene.geo3d.GeoShape;
import org.apache.lucene.geo3d.PlanetModel;
import org.apache.lucene.geo3d.XYZBounds;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/** Finds all previously indexed points that fall within the specified polygon.
 *
 *  <p>The field must be indexed with {@link Geo3DDocValuesFormat}, and {@link Geo3DPointField} added per document.
 *
 *  <p>Because this implementation cannot intersect each cell with the polygon, it will be costly especially for large polygons, as every
 *   possible point must be checked.
 *
 *  <p><b>NOTE</b>: for fastest performance, this allocates FixedBitSet(maxDoc) for each segment.  The score of each hit is the query boost.
 *
 * @lucene.experimental
 *
 * @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
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
        BinaryDocValues bdv = reader.getBinaryDocValues(field);
        if (bdv == null) {
          // No docs in this segment had this field
          return null;
        }

        if (bdv instanceof Geo3DBinaryDocValues == false) {
          throw new IllegalStateException("field \"" + field + "\" was not indexed with Geo3DBinaryDocValuesFormat: got: " + bdv);
        }
        final Geo3DBinaryDocValues treeDV = (Geo3DBinaryDocValues) bdv;
        BKD3DTreeReader tree = treeDV.getBKD3DTreeReader();

        XYZBounds bounds = new XYZBounds();
        shape.getBounds(bounds);

        final double planetMax = planetModel.getMaximumMagnitude();
        if (planetMax != treeDV.planetMax) {
          throw new IllegalStateException(planetModel + " is not the same one used during indexing: planetMax=" + planetMax + " vs indexing planetMax=" + treeDV.planetMax);
        }

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

        DocIdSet result = tree.intersect(Geo3DDocValuesFormat.encodeValueLenient(planetMax, bounds.getMinimumX()),
                                         Geo3DDocValuesFormat.encodeValueLenient(planetMax, bounds.getMaximumX()),
                                         Geo3DDocValuesFormat.encodeValueLenient(planetMax, bounds.getMinimumY()),
                                         Geo3DDocValuesFormat.encodeValueLenient(planetMax, bounds.getMaximumY()),
                                         Geo3DDocValuesFormat.encodeValueLenient(planetMax, bounds.getMinimumZ()),
                                         Geo3DDocValuesFormat.encodeValueLenient(planetMax, bounds.getMaximumZ()),
                                         new BKD3DTreeReader.ValueFilter() {
                                           @Override
                                           public boolean accept(int docID) {
                                             //System.out.println("  accept? docID=" + docID);
                                             BytesRef bytes = treeDV.get(docID);
                                             if (bytes == null) {
                                               //System.out.println("    false (null)");
                                               return false;
                                             }

                                             assert bytes.length == 12;
                                             double x = Geo3DDocValuesFormat.decodeValueCenter(treeDV.planetMax, Geo3DDocValuesFormat.readInt(bytes.bytes, bytes.offset));
                                             double y = Geo3DDocValuesFormat.decodeValueCenter(treeDV.planetMax, Geo3DDocValuesFormat.readInt(bytes.bytes, bytes.offset+4));
                                             double z = Geo3DDocValuesFormat.decodeValueCenter(treeDV.planetMax, Geo3DDocValuesFormat.readInt(bytes.bytes, bytes.offset+8));
                                             // System.out.println("  accept docID=" + docID + " point: x=" + x + " y=" + y + " z=" + z);

                                             // True if x,y,z is within shape
                                             //System.out.println("    x=" + x + " y=" + y + " z=" + z);
                                             //System.out.println("    ret: " + shape.isWithin(x, y, z));

                                             return shape.isWithin(x, y, z);
                                           }

                                           @Override
                                           public BKD3DTreeReader.Relation compare(int cellXMinEnc, int cellXMaxEnc, int cellYMinEnc, int cellYMaxEnc, int cellZMinEnc, int cellZMaxEnc) {
                                             assert cellXMinEnc <= cellXMaxEnc;
                                             assert cellYMinEnc <= cellYMaxEnc;
                                             assert cellZMinEnc <= cellZMaxEnc;

                                             // Because the BKD tree operates in quantized (64 bit -> 32 bit) space, and the cell bounds
                                             // here are inclusive, we need to extend the bounds to the largest un-quantized values that
                                             // could quantize into these bounds.  The encoding (Geo3DDocValuesFormat.encodeValue) does
                                             // a Math.round from double to long, so e.g. 1.4 -> 1, and -1.4 -> -1:
                                             double cellXMin = Geo3DDocValuesFormat.decodeValueMin(treeDV.planetMax, cellXMinEnc);
                                             double cellXMax = Geo3DDocValuesFormat.decodeValueMax(treeDV.planetMax, cellXMaxEnc);
                                             double cellYMin = Geo3DDocValuesFormat.decodeValueMin(treeDV.planetMax, cellYMinEnc);
                                             double cellYMax = Geo3DDocValuesFormat.decodeValueMax(treeDV.planetMax, cellYMaxEnc);
                                             double cellZMin = Geo3DDocValuesFormat.decodeValueMin(treeDV.planetMax, cellZMinEnc);
                                             double cellZMax = Geo3DDocValuesFormat.decodeValueMax(treeDV.planetMax, cellZMaxEnc);
                                             //System.out.println("  compare: x=" + cellXMin + "-" + cellXMax + " y=" + cellYMin + "-" + cellYMax + " z=" + cellZMin + "-" + cellZMax);

                                             GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(planetModel, cellXMin, cellXMax, cellYMin, cellYMax, cellZMin, cellZMax);

                                             switch(xyzSolid.getRelationship(shape)) {
                                             case GeoArea.CONTAINS:
                                               // Shape fully contains the cell
                                               //System.out.println("    inside");
                                               return BKD3DTreeReader.Relation.CELL_INSIDE_SHAPE;
                                             case GeoArea.OVERLAPS:
                                               // They do overlap but neither contains the other:
                                               //System.out.println("    crosses1");
                                               return BKD3DTreeReader.Relation.SHAPE_CROSSES_CELL;
                                             case GeoArea.WITHIN:
                                               // Cell fully contains the shape:
                                               //System.out.println("    crosses2");
                                               return BKD3DTreeReader.Relation.SHAPE_INSIDE_CELL;
                                             case GeoArea.DISJOINT:
                                               // They do not overlap at all
                                               //System.out.println("    outside");
                                               return BKD3DTreeReader.Relation.SHAPE_OUTSIDE_CELL;
                                             default:
                                               assert false;
                                               return BKD3DTreeReader.Relation.SHAPE_CROSSES_CELL;
                                             }
                                           }
                                         });

        final DocIdSetIterator disi = result.iterator();

        return new ConstantScoreScorer(this, score(), disi);
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
    sb.append(ToStringUtils.boost(getBoost()));
    return sb.toString();
  }
}
