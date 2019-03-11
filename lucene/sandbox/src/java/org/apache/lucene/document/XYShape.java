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

import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.Tessellator.Triangle;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class XYShape {
  static final int INT_BYTES = Integer.BYTES;
  static final int LONG_BYTES = Long.BYTES;

  protected static final FieldType INT_TYPE = new FieldType();
  protected static final FieldType LONG_TYPE = new FieldType();
  static {
    INT_TYPE.setDimensions(7, 4, INT_BYTES);
    LONG_TYPE.setDimensions(7, 4, LONG_BYTES);
    INT_TYPE.freeze();
    LONG_TYPE.freeze();
  }

  private XYShape() {
  }

  public static Field[] createIndexableFields(String fieldName, XYPolygon polygon) {
    if (polygon.getEncodingType() == XYShapeType.LONG) {
      throw new IllegalArgumentException("LONG type not yet supported");
    }
    List<Triangle> tessellation = Tessellator.tessellate(polygon);
    List<XYTriangle> fields = new ArrayList<>(tessellation.size());
    for (Triangle t : tessellation) {
      fields.add(new XYTriangle(fieldName, t));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create a query to find all polygons that intersect a defined bounding box
   **/
  public static Query newBoxQuery(String field, QueryRelation queryRelation, double minX, double maxX, double minY, double maxY) {
    return new XYShapeBoundingBoxQuery(field, queryRelation, minX, maxX, minY, maxY);
  }

  private static class XYTriangle extends Field {
    XYTriangle(String name, int ax, int ay, int bx, int by, int cx, int cy) {
      super(name, INT_TYPE);
      setTriangleValue(ax, ay, bx, by, cx, cy);
    }

    XYTriangle(String name, Triangle t) {
      super(name, INT_TYPE);
      setTriangleValue(t.getEncodedX(0), t.getEncodedY(0), t.getEncodedX(1),
          t.getEncodedY(1), t.getEncodedX(2), t.getEncodedY(2));
    }

    public void setTriangleValue(int ax, int ay, int bx, int by, int cx, int cy) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * INT_BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef)fieldsData).bytes;
      }
      LatLonShape.encodeTriangle(bytes, ay, ax, by, bx, cy, cx);
    }
  }

  /** Query Relation Types **/
  public enum QueryRelation {
    INTERSECTS, WITHIN, DISJOINT
  }

  public enum XYShapeType {
    INTEGER {
      @Override
      public FieldType fieldType() {
        return INT_TYPE;
      }
    },
    LONG {
      @Override
      public FieldType fieldType() {
        return LONG_TYPE;
      }
    };

    abstract public FieldType fieldType();
  }
}
