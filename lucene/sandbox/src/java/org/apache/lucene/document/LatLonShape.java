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

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.Tessellator.Triangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

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
  public static final int BYTES = LatLonPoint.BYTES;

  protected static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(6, BYTES);
    TYPE.freeze();
  }

  // no instance:
  private LatLonShape() {
  }

  /** the lionshare of the indexing is done by the tessellator */
  public static Field[] createIndexableFields(String fieldName, Polygon polygon) {
    List<Triangle> tessellation = Tessellator.tessellate(polygon);
    List<LatLonTriangle> fields = new ArrayList<>();
    for (int i = 0; i < tessellation.size(); ++i) {
      fields.add(new LatLonTriangle(fieldName, tessellation.get(i)));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create a query to find all polygons that intersect a defined bounding box
   *  note: does not currently support dateline crossing boxes
   * todo split dateline crossing boxes into two queries like {@link LatLonPoint#newBoxQuery}
   **/
  public static Query newBoxQuery(String field, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
    return new LatLonShapeBoundingBoxQuery(field, minLatitude, maxLatitude, minLongitude, maxLongitude);
  }

  /** polygons are decomposed into tessellated triangles using {@link org.apache.lucene.geo.Tessellator}
   * these triangles are encoded and inserted as separate indexed POINT fields
   */
  private static class LatLonTriangle extends Field {

    public LatLonTriangle(String name, Triangle t) {
      super(name, TYPE);
      setTriangleValue(t.getEncodedX(0), t.getEncodedY(0),
                       t.getEncodedX(1), t.getEncodedY(1),
                       t.getEncodedX(2), t.getEncodedY(2));
    }

    public void setTriangleValue(int aX, int aY, int bX, int bY, int cX, int cY) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[24];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef) fieldsData).bytes;
      }

      NumericUtils.intToSortableBytes(aY, bytes, 0);
      NumericUtils.intToSortableBytes(aX, bytes, BYTES);
      NumericUtils.intToSortableBytes(bY, bytes, BYTES * 2);
      NumericUtils.intToSortableBytes(bX, bytes, BYTES * 3);
      NumericUtils.intToSortableBytes(cY, bytes, BYTES * 4);
      NumericUtils.intToSortableBytes(cX, bytes, BYTES * 5);
    }
  }
}
