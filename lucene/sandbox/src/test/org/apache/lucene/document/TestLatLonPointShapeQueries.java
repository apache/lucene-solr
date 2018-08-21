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

import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.index.PointValues.Relation;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** random bounding box and polygon query tests for random generated {@code latitude, longitude} points */
public class TestLatLonPointShapeQueries extends BaseLatLonShapeTestCase {

  protected final PointValidator VALIDATOR = new PointValidator();

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POINT;
  }

  @Override
  protected Field[] createIndexableFields(String field, Object point) {
    Point p = (Point)point;
    return LatLonShape.createIndexableFields(field, p.lat, p.lon);
  }

  @Override
  protected Validator getValidator() {
    return VALIDATOR;
  }

  protected class PointValidator implements Validator {
    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Point p = (Point)shape;
      double lat = decodeLatitude(encodeLatitude(p.lat));
      double lon = decodeLongitude(encodeLongitude(p.lon));
      return (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) == false;
    }

    @Override
    public boolean testPolygonQuery(Polygon2D poly2d, Object shape) {
      Point p = (Point) shape;
      double lat = decodeLatitude(encodeLatitude(p.lat));
      double lon = decodeLongitude(encodeLongitude(p.lon));
      // for consistency w/ the query we test the point as a triangle
      return poly2d.relateTriangle(lon, lat, lon, lat, lon, lat) != Relation.CELL_OUTSIDE_QUERY;
    }
  }

  @Override
  public void testRandomTiny() throws Exception {
  }
}
