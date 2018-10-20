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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.Tessellator.Triangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * An indexed shape utility class.
 * <p>
 * {@link Polygon}'s are decomposed into a triangular mesh using the {@link Tessellator} utility class
 * Each {@link Triangle} is encoded and indexed as a multi-value field.
 * <p>
 * Finding all shapes that intersect a range (e.g., bounding box) at search time is efficient.
 * <p>
 * This class defines static factory methods for common operations:
 * <ul>
 *   <li>{@link #createIndexableFields(String, Polygon)} for matching polygons that intersect a bounding box.
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching polygons that intersect a bounding box.
 * </ul>

 * <b>WARNING</b>: Like {@link LatLonPoint}, vertex values are indexed with some loss of precision from the
 * original {@code double} values (4.190951585769653E-8 for the latitude component
 * and 8.381903171539307E-8 for longitude).
 * @see PointValues
 * @see LatLonDocValuesField
 *
 * @lucene.experimental
 */
public class LatLonShape {
  public static final int BYTES = 2 * LatLonPoint.BYTES;

  protected static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(7, 4, BYTES);
    TYPE.freeze();
  }

  // no instance:
  private LatLonShape() {
  }

  /** create indexable fields for polygon geometry */
  public static Field[] createIndexableFields(String fieldName, Polygon polygon) {
    // the lionshare of the indexing is done by the tessellator
    List<Triangle> tessellation = Tessellator.tessellate(polygon);
    List<LatLonTriangle> fields = new ArrayList<>();
    for (Triangle t : tessellation) {
      fields.add(new LatLonTriangle(fieldName, t));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create indexable fields for line geometry */
  public static Field[] createIndexableFields(String fieldName, Line line) {
    int numPoints = line.numPoints();
    List<LatLonTriangle> fields = new ArrayList<>(numPoints - 1);

    // create "flat" triangles
    double aLat, bLat, aLon, bLon, temp;
    double size;
    for (int i = 0, j = 1; j < numPoints; ++i, ++j) {
      aLat = line.getLat(i);
      aLon = line.getLon(i);
      bLat = line.getLat(j);
      bLon = line.getLon(j);
      if (aLat > bLat) {
        temp = aLat;
        aLat = bLat;
        bLat = temp;
        temp = aLon;
        aLon = bLon;
        bLon = temp;
      } else if (aLat == bLat) {
        if (aLon > bLon) {
          temp = aLat;
          aLat = bLat;
          bLat = temp;
          temp = aLon;
          aLon = bLon;
          bLon = temp;
        }
      }
      size = StrictMath.sqrt(StrictMath.pow(aLat - bLat, 2d) + StrictMath.pow(aLon - bLon, 2d));
      fields.add(new LatLonTriangle(fieldName, aLat, aLon, bLat, bLon, aLat, aLon, size));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create indexable fields for point geometry */
  public static Field[] createIndexableFields(String fieldName, double lat, double lon) {
    return new Field[] {new LatLonTriangle(fieldName, lat, lon, lat, lon, lat, lon, 0d)};
  }

  /** create a query to find all polygons that intersect a defined bounding box
   *  note: does not currently support dateline crossing boxes
   * todo split dateline crossing boxes into two queries like {@link LatLonPoint#newBoxQuery}
   **/
  public static Query newBoxQuery(String field, QueryRelation queryRelation, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
    return new LatLonShapeBoundingBoxQuery(field, queryRelation, minLatitude, maxLatitude, minLongitude, maxLongitude);
  }

  public static Query newPolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    return new LatLonShapePolygonQuery(field, queryRelation, polygons);
  }

  /** polygons are decomposed into tessellated triangles using {@link org.apache.lucene.geo.Tessellator}
   * these triangles are encoded and inserted as separate indexed POINT fields
   */
  private static class LatLonTriangle extends Field {

    LatLonTriangle(String name, double aLat, double aLon, double bLat, double bLon, double cLat, double cLon, double size) {
      super(name, TYPE);
      setTriangleValue(encodeLongitude(aLon), encodeLatitude(aLat), encodeLongitude(bLon), encodeLatitude(bLat), encodeLongitude(cLon), encodeLatitude(cLat));
    }

    LatLonTriangle(String name, Triangle t) {
      super(name, TYPE);
      setTriangleValue(t.getEncodedX(0), t.getEncodedY(0), t.getEncodedX(1), t.getEncodedY(1), t.getEncodedX(2), t.getEncodedY(2));
    }

    public void setTriangleValue(int aX, int aY, int bX, int bY, int cX, int cY) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef) fieldsData).bytes;
      }

      int minX = StrictMath.min(aX, StrictMath.min(bX, cX));
      int minY = StrictMath.min(aY, StrictMath.min(bY, cY));
      int maxX = StrictMath.max(aX, StrictMath.max(bX, cX));
      int maxY = StrictMath.max(aY, StrictMath.max(bY, cY));

      encodeTriangle(bytes, minY, minX, maxY, maxX, aX, aY, bX, bY, cX, cY);
    }

    private void encodeTriangle(byte[] bytes, int minY, int minX, int maxY, int maxX, int aX, int aY, int bX, int bY, int cX, int cY) {
      encodeTriangleBoxVal(minY, bytes, 0);
      encodeTriangleBoxVal(minX, bytes, BYTES);
      encodeTriangleBoxVal(maxY, bytes, 2 * BYTES);
      encodeTriangleBoxVal(maxX, bytes, 3 * BYTES);

      long a = (((long)aX) << 32) | (((long)aY) & 0x00000000FFFFFFFFL);
      long b = (((long)bX) << 32) | (((long)bY) & 0x00000000FFFFFFFFL);
      long c = (((long)cX) << 32) | (((long)cY) & 0x00000000FFFFFFFFL);
      NumericUtils.longToSortableBytes(a, bytes, 4 * BYTES);
      NumericUtils.longToSortableBytes(b, bytes, 5 * BYTES);
      NumericUtils.longToSortableBytes(c, bytes, 6 * BYTES);
    }
  }

  public static void encodeTriangleBoxVal(int encodedVal, byte[] bytes, int offset) {
    long val = (long)(encodedVal ^ 0x80000000);
    val &= 0x00000000FFFFFFFFL;
    val ^= 0x8000000000000000L;
    NumericUtils.longToSortableBytes(val, bytes, offset);
  }

  public static int decodeTriangleBoxVal(byte[] encoded, int offset) {
    long val = NumericUtils.sortableBytesToLong(encoded, offset);
    int result = (int)(val & 0x00000000FFFFFFFF);
    return result ^ 0x80000000;
  }

  /** Query Relation Types **/
  public enum QueryRelation {
    INTERSECTS, WITHIN, DISJOINT
  }
}
