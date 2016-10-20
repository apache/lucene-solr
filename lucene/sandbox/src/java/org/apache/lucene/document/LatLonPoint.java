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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.lucene60.Lucene60PointsFormat;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.BKDReader;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

/** 
 * An indexed location field.
 * <p>
 * Finding all documents within a range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for common operations:
 * <ul>
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching points within a bounding box.
 *   <li>{@link #newDistanceQuery newDistanceQuery()} for matching points within a specified distance.
 *   <li>{@link #newPolygonQuery newPolygonQuery()} for matching points within an arbitrary polygon.
 *   <li>{@link #nearest nearest()} for finding the k-nearest neighbors by distance.
 * </ul>
 * <p>
 * If you also need per-document operations such as sort by distance, add a separate {@link LatLonDocValuesField} instance.
 * If you also need to store the value, you should add a separate {@link StoredField} instance.
 * <p>
 * <b>WARNING</b>: Values are indexed with some loss of precision from the
 * original {@code double} values (4.190951585769653E-8 for the latitude component
 * and 8.381903171539307E-8 for longitude).
 * @see PointValues
 * @see LatLonDocValuesField
 */
// TODO ^^^ that is very sandy and hurts the API, usage, and tests tremendously, because what the user passes
// to the field is not actually what gets indexed. Float would be 1E-5 error vs 1E-7, but it might be
// a better tradeoff? then it would be completely transparent to the user and lucene would be "lossless".
public class LatLonPoint extends Field {

  /**
   * Type for an indexed LatLonPoint
   * <p>
   * Each point stores two dimensions with 4 bytes per dimension.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(2, Integer.BYTES);
    TYPE.freeze();
  }
  
  /**
   * Change the values of this field
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if latitude or longitude are out of bounds
   */
  public void setLocationValue(double latitude, double longitude) {
    final byte[] bytes;

    if (fieldsData == null) {
      bytes = new byte[8];
      fieldsData = new BytesRef(bytes);
    } else {
      bytes = ((BytesRef) fieldsData).bytes;
    }

    int latitudeEncoded = encodeLatitude(latitude);
    int longitudeEncoded = encodeLongitude(longitude);
    NumericUtils.intToSortableBytes(latitudeEncoded, bytes, 0);
    NumericUtils.intToSortableBytes(longitudeEncoded, bytes, Integer.BYTES);
  }

  /** 
   * Creates a new LatLonPoint with the specified latitude and longitude
   * @param name field name
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public LatLonPoint(String name, double latitude, double longitude) {
    super(name, TYPE);
    setLocationValue(latitude, longitude);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    byte bytes[] = ((BytesRef) fieldsData).bytes;
    result.append(decodeLatitude(bytes, 0));
    result.append(',');
    result.append(decodeLongitude(bytes, Integer.BYTES));

    result.append('>');
    return result.toString();
  }
  
  /** sugar encodes a single point as a byte array */
  private static byte[] encode(double latitude, double longitude) {
    byte[] bytes = new byte[2 * Integer.BYTES];
    NumericUtils.intToSortableBytes(encodeLatitude(latitude), bytes, 0);
    NumericUtils.intToSortableBytes(encodeLongitude(longitude), bytes, Integer.BYTES);
    return bytes;
  }
  
  /** sugar encodes a single point as a byte array, rounding values up */
  private static byte[] encodeCeil(double latitude, double longitude) {
    byte[] bytes = new byte[2 * Integer.BYTES];
    NumericUtils.intToSortableBytes(encodeLatitudeCeil(latitude), bytes, 0);
    NumericUtils.intToSortableBytes(encodeLongitudeCeil(longitude), bytes, Integer.BYTES);
    return bytes;
  }

  /** helper: checks a fieldinfo and throws exception if its definitely not a LatLonPoint */
  static void checkCompatible(FieldInfo fieldInfo) {
    // point/dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
    if (fieldInfo.getPointDimensionCount() != 0 && fieldInfo.getPointDimensionCount() != TYPE.pointDimensionCount()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with numDims=" + fieldInfo.getPointDimensionCount() + 
                                         " but this point type has numDims=" + TYPE.pointDimensionCount() + 
                                         ", is the field really a LatLonPoint?");
    }
    if (fieldInfo.getPointNumBytes() != 0 && fieldInfo.getPointNumBytes() != TYPE.pointNumBytes()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with bytesPerDim=" + fieldInfo.getPointNumBytes() + 
                                         " but this point type has bytesPerDim=" + TYPE.pointNumBytes() + 
                                         ", is the field really a LatLonPoint?");
    }
  }

  // static methods for generating queries

  /**
   * Create a query for matching a bounding box.
   * <p>
   * The box may cross over the dateline.
   * @param field field name. must not be null.
   * @param minLatitude latitude lower bound: must be within standard +/-90 coordinate bounds.
   * @param maxLatitude latitude upper bound: must be within standard +/-90 coordinate bounds.
   * @param minLongitude longitude lower bound: must be within standard +/-180 coordinate bounds.
   * @param maxLongitude longitude upper bound: must be within standard +/-180 coordinate bounds.
   * @return query matching points within this box
   * @throws IllegalArgumentException if {@code field} is null, or the box has invalid coordinates.
   */
  public static Query newBoxQuery(String field, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
    // exact double values of lat=90.0D and lon=180.0D must be treated special as they are not represented in the encoding
    // and should not drag in extra bogus junk! TODO: should encodeCeil just throw ArithmeticException to be less trappy here?
    if (minLatitude == 90.0) {
      // range cannot match as 90.0 can never exist
      return new MatchNoDocsQuery("LatLonPoint.newBoxQuery with minLatitude=90.0");
    }
    if (minLongitude == 180.0) {
      if (maxLongitude == 180.0) {
        // range cannot match as 180.0 can never exist
        return new MatchNoDocsQuery("LatLonPoint.newBoxQuery with minLongitude=maxLongitude=180.0");
      } else if (maxLongitude < minLongitude) {
        // encodeCeil() with dateline wrapping!
        minLongitude = -180.0;
      }
    }
    byte[] lower = encodeCeil(minLatitude, minLongitude);
    byte[] upper = encode(maxLatitude, maxLongitude);
    // Crosses date line: we just rewrite into OR of two bboxes, with longitude as an open range:
    if (maxLongitude < minLongitude) {
      // Disable coord here because a multi-valued doc could match both rects and get unfairly boosted:
      BooleanQuery.Builder q = new BooleanQuery.Builder();

      // E.g.: maxLon = -179, minLon = 179
      byte[] leftOpen = lower.clone();
      // leave longitude open
      NumericUtils.intToSortableBytes(Integer.MIN_VALUE, leftOpen, Integer.BYTES);
      Query left = newBoxInternal(field, leftOpen, upper);
      q.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));

      byte[] rightOpen = upper.clone();
      // leave longitude open
      NumericUtils.intToSortableBytes(Integer.MAX_VALUE, rightOpen, Integer.BYTES);
      Query right = newBoxInternal(field, lower, rightOpen);
      q.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
      return new ConstantScoreQuery(q.build());
    } else {
      return newBoxInternal(field, lower, upper);
    }
  }
  
  private static Query newBoxInternal(String field, byte[] min, byte[] max) {
    return new PointRangeQuery(field, min, max, 2) {
      @Override
      protected String toString(int dimension, byte[] value) {
        if (dimension == 0) {
          return Double.toString(decodeLatitude(value, 0));
        } else if (dimension == 1) {
          return Double.toString(decodeLongitude(value, 0));
        } else {
          throw new AssertionError();
        }
      }
    };
  }
  
  /**
   * Create a query for matching points within the specified distance of the supplied location.
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or radius is invalid.
   */
  public static Query newDistanceQuery(String field, double latitude, double longitude, double radiusMeters) {
    return new LatLonPointDistanceQuery(field, latitude, longitude, radiusMeters);
  }
  
  /** 
   * Create a query for matching one or more polygons.
   * @param field field name. must not be null.
   * @param polygons array of polygons. must not be null or empty
   * @return query matching points within this polygon
   * @throws IllegalArgumentException if {@code field} is null, {@code polygons} is null or empty
   * @see Polygon
   */
  public static Query newPolygonQuery(String field, Polygon... polygons) {
    return new LatLonPointInPolygonQuery(field, polygons);
  }

  /**
   * Finds the {@code n} nearest indexed points to the provided point, according to Haversine distance.
   * <p>
   * This is functionally equivalent to running {@link MatchAllDocsQuery} with a {@link LatLonDocValuesField#newDistanceSort},
   * but is far more efficient since it takes advantage of properties the indexed BKD tree.  Currently this
   * only works with {@link Lucene60PointsFormat} (used by the default codec).  Multi-valued fields are
   * currently not de-duplicated, so if a document had multiple instances of the specified field that
   * make it into the top n, that document will appear more than once.
   * <p>
   * Documents are ordered by ascending distance from the location. The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * 
   * @param searcher IndexSearcher to find nearest points from.
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param n the number of nearest neighbors to retrieve.
   * @return TopFieldDocs containing documents ordered by distance, where the field value for each {@link FieldDoc} is the distance in meters
   * @throws IllegalArgumentException if the underlying PointValues is not a {@code Lucene60PointsReader} (this is a current limitation), or
   *         if {@code field} or {@code searcher} is null, or if {@code latitude}, {@code longitude} or {@code n} are out-of-bounds
   * @throws IOException if an IOException occurs while finding the points.
   */
  // TODO: what about multi-valued documents? what happens?
  public static TopFieldDocs nearest(IndexSearcher searcher, String field, double latitude, double longitude, int n) throws IOException {
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    if (n < 1) {
      throw new IllegalArgumentException("n must be at least 1; got " + n);
    }
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (searcher == null) {
      throw new IllegalArgumentException("searcher must not be null");
    }
    List<BKDReader> readers = new ArrayList<>();
    List<Integer> docBases = new ArrayList<>();
    List<Bits> liveDocs = new ArrayList<>();
    int totalHits = 0;
    for(LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
      PointValues points = leaf.reader().getPointValues(field);
      if (points != null) {
        if (points instanceof BKDReader == false) {
          throw new IllegalArgumentException("can only run on Lucene60PointsReader points implementation, but got " + points);
        }
        totalHits += points.getDocCount();
        BKDReader reader = (BKDReader) points;
        if (reader != null) {
          readers.add(reader);
          docBases.add(leaf.docBase);
          liveDocs.add(leaf.reader().getLiveDocs());
        }
      }
    }

    NearestNeighbor.NearestHit[] hits = NearestNeighbor.nearest(latitude, longitude, readers, liveDocs, docBases, n);

    // Convert to TopFieldDocs:
    ScoreDoc[] scoreDocs = new ScoreDoc[hits.length];
    for(int i=0;i<hits.length;i++) {
      NearestNeighbor.NearestHit hit = hits[i];
      scoreDocs[i] = new FieldDoc(hit.docID, 0.0f, new Object[] {Double.valueOf(hit.distanceMeters)});
    }
    return new TopFieldDocs(totalHits, scoreDocs, null, 0.0f);
  }
}
