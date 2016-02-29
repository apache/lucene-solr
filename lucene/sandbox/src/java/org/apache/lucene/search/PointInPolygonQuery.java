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
import java.util.Arrays;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;

/** Finds all previously indexed points that fall within the specified polygon.
 *
 *  <p>The field must be indexed with using {@link org.apache.lucene.document.LatLonPoint} added per document.
 *
 *  @lucene.experimental */

public class PointInPolygonQuery extends Query {
  final String field;
  final double minLat;
  final double maxLat;
  final double minLon;
  final double maxLon;
  final double[] polyLats;
  final double[] polyLons;

  /** The lats/lons must be clockwise or counter-clockwise. */
  public PointInPolygonQuery(String field, double[] polyLats, double[] polyLons) {
    this.field = field;
    if (polyLats.length != polyLons.length) {
      throw new IllegalArgumentException("polyLats and polyLons must be equal length");
    }
    if (polyLats.length < 4) {
      throw new IllegalArgumentException("at least 4 polygon points required");
    }
    if (polyLats[0] != polyLats[polyLats.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLats[0]=" + polyLats[0] + " polyLats[" + (polyLats.length-1) + "]=" + polyLats[polyLats.length-1]);
    }
    if (polyLons[0] != polyLons[polyLons.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLons[0]=" + polyLons[0] + " polyLons[" + (polyLons.length-1) + "]=" + polyLons[polyLons.length-1]);
    }

    this.polyLats = polyLats;
    this.polyLons = polyLons;

    // TODO: we could also compute the maximal inner bounding box, to make relations faster to compute?

    double minLon = Double.POSITIVE_INFINITY;
    double minLat = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    for(int i=0;i<polyLats.length;i++) {
      double lat = polyLats[i];
      if (GeoUtils.isValidLat(lat) == false) {
        throw new IllegalArgumentException("polyLats[" + i + "]=" + lat + " is not a valid latitude");
      }
      minLat = Math.min(minLat, lat);
      maxLat = Math.max(maxLat, lat);
      double lon = polyLons[i];
      if (GeoUtils.isValidLon(lon) == false) {
        throw new IllegalArgumentException("polyLons[" + i + "]=" + lat + " is not a valid longitude");
      }
      minLon = Math.min(minLon, lon);
      maxLon = Math.max(maxLon, lon);
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

    // TODO: except that the polygon verify is costly!  The approximation should be all docs in all overlapping cells, and matches() should
    // then check the polygon

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
        values.intersect(field,
                         new IntersectVisitor() {
                           @Override
                           public void visit(int docID) {
                             result.add(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             assert packedValue.length == 8;
                             double lat = LatLonPoint.decodeLat(NumericUtils.bytesToInt(packedValue, 0));
                             double lon = LatLonPoint.decodeLon(NumericUtils.bytesToInt(packedValue, Integer.BYTES));
                             if (GeoRelationUtils.pointInPolygon(polyLons, polyLats, lat, lon)) {
                               result.add(docID);
                             }
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                             double cellMinLat = LatLonPoint.decodeLat(NumericUtils.bytesToInt(minPackedValue, 0));
                             double cellMinLon = LatLonPoint.decodeLon(NumericUtils.bytesToInt(minPackedValue, Integer.BYTES));
                             double cellMaxLat = LatLonPoint.decodeLat(NumericUtils.bytesToInt(maxPackedValue, 0));
                             double cellMaxLon = LatLonPoint.decodeLon(NumericUtils.bytesToInt(maxPackedValue, Integer.BYTES));

                             if (cellMinLat <= minLat && cellMaxLat >= maxLat && cellMinLon <= minLon && cellMaxLon >= maxLon) {
                               // Cell fully encloses the query
                               return Relation.CELL_CROSSES_QUERY;
                             } else  if (GeoRelationUtils.rectWithinPolyPrecise(cellMinLon, cellMinLat, cellMaxLon, cellMaxLat,
                                                                 polyLons, polyLats,
                                                                 minLon, minLat, maxLon, maxLat)) {
                               return Relation.CELL_INSIDE_QUERY;
                             } else if (GeoRelationUtils.rectCrossesPolyPrecise(cellMinLon, cellMinLat, cellMaxLon, cellMaxLat,
                                                                 polyLons, polyLats,
                                                                 minLon, minLat, maxLon, maxLat)) {
                               return Relation.CELL_CROSSES_QUERY;
                             } else {
                               return Relation.CELL_OUTSIDE_QUERY;
                             }
                           }
                         });

        return new ConstantScoreScorer(this, score(), result.build().iterator());
      }
    };
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    PointInPolygonQuery that = (PointInPolygonQuery) o;

    if (Arrays.equals(polyLons, that.polyLons) == false) {
      return false;
    }
    if (Arrays.equals(polyLats, that.polyLats) == false) {
      return false;
    }

    return true;
  }

  @Override
  public final int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(polyLons);
    result = 31 * result + Arrays.hashCode(polyLats);
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
    sb.append(" Points: ");
    for (int i=0; i<polyLons.length; ++i) {
      sb.append("[")
        .append(polyLons[i])
        .append(", ")
        .append(polyLats[i])
        .append("] ");
    }
    return sb.toString();
  }
}
