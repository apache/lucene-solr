package org.apache.lucene.bkdtree;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.GeoProjectionUtils;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.SloppyMath;

/** Finds all previously indexed points that fall within the specified distance from a center point.
 *
 * <p>The field must be indexed with {@link BKDTreeDocValuesFormat}, and {@link BKDPointField} added per document.
 *
 * @lucene.experimental */

public class BKDDistanceQuery extends Query {
  final String field;
  final double centerLat;
  final double centerLon;
  final double radiusMeters;
  final double minLon, maxLon;
  final double minLat, maxLat;

  public BKDDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    this.field = field;
    if (BKDTreeWriter.validLat(centerLat) == false) {
      throw new IllegalArgumentException("centerLat=" + centerLat + " is not a valid latitude");
    }
    if (BKDTreeWriter.validLon(centerLon) == false) {
      throw new IllegalArgumentException("centerLon=" + centerLon + " is not a valid longitude");
    }
    if (radiusMeters <= 0.0) {
      throw new IllegalArgumentException("radiusMeters=" + radiusMeters + " is not a valid radius");
    }
    this.centerLat = centerLat;
    this.centerLon = centerLon;
    this.radiusMeters = radiusMeters;

    GeoRect bbox = GeoUtils.circleToBBox(centerLon, centerLat, radiusMeters);
    minLon = bbox.minLon;
    minLat = bbox.minLat;
    maxLon = bbox.maxLon;
    maxLat = bbox.maxLat;

    //System.out.println("distance query bbox: lon=" + minLon + " TO " + maxLon + "; lat=" + minLat + " TO " + maxLat);
    assert minLat <= maxLat: "minLat=" + minLat + " maxLat=" + maxLat;
  }

  /** Used by rewrite, when circle crosses the date line */
  private BKDDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters,
                           double minLat, double maxLat, double minLon, double maxLon) {
    this.field = field;
    assert BKDTreeWriter.validLat(centerLat);
    assert BKDTreeWriter.validLon(centerLon);
    assert radiusMeters > 0.0;

    this.centerLat = centerLat;
    this.centerLon = centerLon;
    this.radiusMeters = radiusMeters;
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLon = minLon;
    this.maxLon = maxLon;
    //System.out.println("  rewrite bbox: lat=" + minLat + " TO " + maxLat + " lon=" + minLon + " TO " + maxLon);
  }    

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        SortedNumericDocValues sdv = reader.getSortedNumericDocValues(field);
        if (sdv == null) {
          // No docs in this segment had this field
          return null;
        }

        if (sdv instanceof BKDTreeSortedNumericDocValues == false) {
          throw new IllegalStateException("field \"" + field + "\" was not indexed with BKDTreeDocValuesFormat: got: " + sdv);
        }
        BKDTreeSortedNumericDocValues treeDV = (BKDTreeSortedNumericDocValues) sdv;
        BKDTreeReader tree = treeDV.getBKDTreeReader();

        DocIdSet result = tree.intersect(minLat, maxLat, minLon, maxLon,
                                         new BKDTreeReader.LatLonFilter() {

                                           @Override
                                           public boolean accept(double lat, double lon) {
                                             double distanceMeters = SloppyMath.haversin(centerLat, centerLon, lat, lon) * 1000.0;
                                             boolean result = distanceMeters <= radiusMeters;
                                             //System.out.println("accept? centerLat=" + centerLat + " centerLon=" + centerLon + " lat=" + lat + " lon=" + lon + " distanceMeters=" + distanceMeters + " vs " + radiusMeters + " result=" + result);
                                             return result;
                                           }

                                           @Override
                                           public BKDTreeReader.Relation compare(double cellLatMin, double cellLatMax, double cellLonMin, double cellLonMax) {
                                             //System.out.println("compare lat=" + cellLatMin + " TO " + cellLatMax + "; lon=" + cellLonMin + " TO " + cellLonMax);
                                             if (GeoUtils.rectWithinCircle(cellLonMin, cellLatMin, cellLonMax, cellLatMax, centerLon, centerLat, radiusMeters)) {
                                               // nocommit hacky workaround:
                                               if (cellLonMax - cellLonMin < 100 && cellLatMax - cellLatMin < 50) {
                                                 //System.out.println("  CELL_INSIDE_SHAPE");
                                                 return BKDTreeReader.Relation.CELL_INSIDE_SHAPE;
                                               } else {
                                                 //System.out.println("  HACK: SHAPE_CROSSES_CELL");
                                                 return BKDTreeReader.Relation.SHAPE_CROSSES_CELL;
                                               }
                                             } else if (GeoUtils.rectCrossesCircle(cellLonMin, cellLatMin, cellLonMax, cellLatMax, centerLon, centerLat, radiusMeters)) {
                                               //System.out.println("  SHAPE_CROSSES_CELL");
                                               return BKDTreeReader.Relation.SHAPE_CROSSES_CELL;
                                             } else {
                                               // nocommit hacky workaround:
                                               if (cellLonMax - cellLonMin < 100 && cellLatMax - cellLatMin < 50) {
                                                 //System.out.println("  SHAPE_OUTSIDE_CELL");
                                                 return BKDTreeReader.Relation.SHAPE_OUTSIDE_CELL;
                                               } else {
                                                 //System.out.println("  HACK: SHAPE_CROSSES_CELL");
                                                 return BKDTreeReader.Relation.SHAPE_CROSSES_CELL;
                                               }
                                             }
                                           }
                                         }, treeDV.delegate);

        return new ConstantScoreScorer(this, score(), result.iterator());
      }
    };
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // nocommit re-enable, using docsWithField?
    if (false && radiusMeters >= GeoProjectionUtils.SEMIMINOR_AXIS) {
      return new MatchAllDocsQuery();
    }

    if (maxLon < minLon) {
      //System.out.println("BKD crosses dateline");
      // Crosses date line: we just rewrite into OR of two bboxes:

      // Disable coord here because a multi-valued doc could match both circles and get unfairly boosted:
      BooleanQuery.Builder q = new BooleanQuery.Builder();
      q.setDisableCoord(true);

      // E.g.: maxLon = -179, minLon = 179
      q.add(new BooleanClause(new BKDDistanceQuery(field, centerLat, centerLon, radiusMeters, minLat, maxLat, BKDTreeWriter.MIN_LON_INCL, maxLon),
                              BooleanClause.Occur.SHOULD));
      q.add(new BooleanClause(new BKDDistanceQuery(field, centerLat, centerLon, radiusMeters, minLat, maxLat, minLon, BKDTreeWriter.MAX_LON_INCL),
                              BooleanClause.Occur.SHOULD));
      return q.build();
    } else {
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BKDDistanceQuery)) return false;
    if (!super.equals(o)) return false;

    BKDDistanceQuery that = (BKDDistanceQuery) o;

    if (Double.compare(that.centerLat, centerLat) != 0) return false;
    if (Double.compare(that.centerLon, centerLon) != 0) return false;
    if (Double.compare(that.radiusMeters, radiusMeters) != 0) return false;
    if (Double.compare(that.minLon, minLon) != 0) return false;
    if (Double.compare(that.maxLon, maxLon) != 0) return false;
    if (Double.compare(that.minLat, minLat) != 0) return false;
    if (Double.compare(that.maxLat, maxLat) != 0) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(centerLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(centerLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(radiusMeters);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString(String field) {
    // nocommit get crossesDateLine into this
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!this.field.equals(field)) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    return sb.append( " Center: [")
        .append(centerLon)
        .append(',')
        .append(centerLat)
        .append(']')
        .append(" Distance: ")
        .append(radiusMeters)
        .append(" meters")
        .append("]")
        .toString();
  }

  public double getCenterLon() {
    return this.centerLon;
  }

  public double getCenterLat() {
    return this.centerLat;
  }

  public double getRadiusMeters() {
    return this.radiusMeters;
  }
}
