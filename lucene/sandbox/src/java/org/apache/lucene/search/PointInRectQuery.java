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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.spatial.util.GeoUtils;

/** Finds all previously indexed points that fall within the specified boundings box.
 *
 *  <p>The field must be indexed with using {@link org.apache.lucene.document.LatLonPoint} added per document.
 *
 *  @lucene.experimental */

public class PointInRectQuery extends Query {
  final String field;
  final double minLat;
  final double maxLat;
  final double minLon;
  final double maxLon;

  /** Matches all points &gt;= minLon, minLat (inclusive) and &lt; maxLon, maxLat (exclusive). */ 
  public PointInRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    this.field = field;
    if (GeoUtils.isValidLat(minLat) == false) {
      throw new IllegalArgumentException("minLat=" + minLat + " is not a valid latitude");
    }
    if (GeoUtils.isValidLat(maxLat) == false) {
      throw new IllegalArgumentException("maxLat=" + maxLat + " is not a valid latitude");
    }
    if (GeoUtils.isValidLon(minLon) == false) {
      throw new IllegalArgumentException("minLon=" + minLon + " is not a valid longitude");
    }
    if (GeoUtils.isValidLon(maxLon) == false) {
      throw new IllegalArgumentException("maxLon=" + maxLon + " is not a valid longitude");
    }
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
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
          // No docs in this segment had any points fields
          return null;
        }

        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());
        int[] hitCount = new int[1];
        values.intersect(field,
                         new IntersectVisitor() {
                           @Override
                           public void grow(int count) {
                             result.grow(count);
                           }

                           @Override
                           public void visit(int docID) {
                             hitCount[0]++;
                             result.add(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             assert packedValue.length == 8;
                             double lat = LatLonPoint.decodeLat(NumericUtils.bytesToInt(packedValue, 0));
                             double lon = LatLonPoint.decodeLon(NumericUtils.bytesToInt(packedValue, 1));
                             if (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon) {
                               hitCount[0]++;
                               result.add(docID);
                             }
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                             double cellMinLat = LatLonPoint.decodeLat(NumericUtils.bytesToInt(minPackedValue, 0));
                             double cellMinLon = LatLonPoint.decodeLon(NumericUtils.bytesToInt(minPackedValue, 1));
                             double cellMaxLat = LatLonPoint.decodeLat(NumericUtils.bytesToInt(maxPackedValue, 0));
                             double cellMaxLon = LatLonPoint.decodeLon(NumericUtils.bytesToInt(maxPackedValue, 1));

                             if (minLat <= cellMinLat && maxLat >= cellMaxLat && minLon <= cellMinLon && maxLon >= cellMaxLon) {
                               return Relation.CELL_INSIDE_QUERY;
                             }

                             if (cellMaxLat < minLat || cellMinLat > maxLat || cellMaxLon < minLon || cellMinLon > maxLon) {
                               return Relation.CELL_OUTSIDE_QUERY;
                             }

                             return Relation.CELL_CROSSES_QUERY;
                           }
                         });

        // NOTE: hitCount[0] will be over-estimate in multi-valued case
        return new ConstantScoreScorer(this, score(), result.build(hitCount[0]).iterator());
      }
    };
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // Crosses date line: we just rewrite into OR of two bboxes:
    if (maxLon < minLon) {

      // Disable coord here because a multi-valued doc could match both rects and get unfairly boosted:
      BooleanQuery.Builder q = new BooleanQuery.Builder();
      q.setDisableCoord(true);

      // E.g.: maxLon = -179, minLon = 179
      PointInRectQuery left = new PointInRectQuery(field, minLat, maxLat, GeoUtils.MIN_LON_INCL, maxLon);
      q.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));
      PointInRectQuery right = new PointInRectQuery(field, minLat, maxLat, minLon, GeoUtils.MAX_LON_INCL);
      q.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
      return new ConstantScoreQuery(q.build());
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash += Double.hashCode(minLat)^0x14fa55fb;
    hash += Double.hashCode(maxLat)^0x733fa5fe;
    hash += Double.hashCode(minLon)^0x14fa55fb;
    hash += Double.hashCode(maxLon)^0x733fa5fe;
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other) && other instanceof PointInRectQuery) {
      final PointInRectQuery q = (PointInRectQuery) other;
      return field.equals(q.field) &&
        minLat == q.minLat &&
        maxLat == q.maxLat &&
        minLon == q.minLon &&
        maxLon == q.maxLon;
    }

    return false;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append("field=");
      sb.append(this.field);
      sb.append(':');
    }

    return sb.append(" Lower Left: [")
        .append(minLon)
        .append(',')
        .append(minLat)
        .append(']')
        .append(" Upper Right: [")
        .append(maxLon)
        .append(',')
        .append(maxLat)
        .append("]")
        .toString();
  }
}
