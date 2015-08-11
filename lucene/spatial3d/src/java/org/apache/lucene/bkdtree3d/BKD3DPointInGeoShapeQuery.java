package org.apache.lucene.bkdtree3d;

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

import org.apache.lucene.geo3d.GeoShape;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

// nocommit rename Geo3DPointInShapeQuery?

/** Finds all previously indexed points that fall within the specified polygon.
 *
 *  <p>The field must be indexed with {@link BKD3DTreeDocValuesFormat}, and {@link BKD3DPointField} added per document.
 *
 *  <p>Because this implementation cannot intersect each cell with the polygon, it will be costly especially for large polygons, as every
 *   possible point must be checked.
 *
 *  <p><b>NOTE</b>: for fastest performance, this allocates FixedBitSet(maxDoc) for each segment.  The score of each hit is the query boost.
 *
 * @lucene.experimental */

public class BKD3DPointInGeoShapeQuery extends Query {
  final String field;
  final GeoShape shape;
  final int minX;
  final int maxX;
  final int minY;
  final int maxY;
  final int minZ;
  final int maxZ;

  /** The lats/lons must be clockwise or counter-clockwise. */
  public BKD3DPointInGeoShapeQuery(String field, GeoShape shape) {
    this.field = field;
    this.shape = shape;
    // nocommit need to set x/y/zMin/Max
    minX = 0;
    maxX = 0;
    minY = 0;
    maxY = 0;
    minZ = 0;
    maxZ = 0;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    // TODO: except that the polygon verify is costly!  The approximation should be all docs in all overlapping cells, and matches() should
    // then check the polygon

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        BinaryDocValues bdv = reader.getBinaryDocValues(field);
        if (bdv == null) {
          // No docs in this segment had this field
          return null;
        }

        if (bdv instanceof BKD3DTreeBinaryDocValues == false) {
          throw new IllegalStateException("field \"" + field + "\" was not indexed with BKD3DTreeBinaryDocValuesFormat: got: " + bdv);
        }
        BKD3DTreeBinaryDocValues treeDV = (BKD3DTreeBinaryDocValues) bdv;
        BKD3DTreeReader tree = treeDV.getBKD3DTreeReader();
        
        // TODO: make this more efficient: as we recurse the BKD tree we should check whether the
        // bbox we are recursing into intersects our shape; Apache SIS may have (non-GPL!) code to do this?
        DocIdSet result = tree.intersect(minX, maxX, minY, maxY, minZ, maxZ,
                                         new BKD3DTreeReader.ValueFilter() {
                                           @Override
                                           public boolean accept(int docID) {
                                             BytesRef bytes = treeDV.get(docID);
                                             if (bytes == null) {
                                               return false;
                                             }

                                             assert bytes.length == 12;
                                             double x = BKD3DTreeDocValuesFormat.decodeValue(BKD3DTreeDocValuesFormat.readInt(bytes.bytes, bytes.offset));
                                             double y = BKD3DTreeDocValuesFormat.decodeValue(BKD3DTreeDocValuesFormat.readInt(bytes.bytes, bytes.offset+4));
                                             double z = BKD3DTreeDocValuesFormat.decodeValue(BKD3DTreeDocValuesFormat.readInt(bytes.bytes, bytes.offset+8));
                                             //return GeoUtils.pointInPolygon(polyLons, polyLats, lat, lon);
                                             // nocommit fixme!
                                             return true;
                                           }

                                           @Override
                                           public BKD3DTreeReader.Relation compare(int xMin, int xMax, int yMin, int yMax, int zMin, int zMax) {
                                             // nocommit fixme!
                                             return BKD3DTreeReader.Relation.INSIDE;
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

    BKD3DPointInGeoShapeQuery that = (BKD3DPointInGeoShapeQuery) o;

    return shape.equals(that.shape);
  }

  @Override
  public final int hashCode() {
    int result = super.hashCode();
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
    sb.append(ToStringUtils.boost(getBoost()));
    return sb.toString();
  }
}
