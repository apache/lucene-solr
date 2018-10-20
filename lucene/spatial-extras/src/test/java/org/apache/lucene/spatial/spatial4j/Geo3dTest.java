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

package org.apache.lucene.spatial.spatial4j;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;

public class Geo3dTest extends LuceneTestCase {

  @Test
  public void testWKT() throws Exception {
    Geo3dSpatialContextFactory factory = new Geo3dSpatialContextFactory();
    SpatialContext ctx = factory.newSpatialContext();
    String wkt = "POLYGON ((20.0 -60.4, 20.1 -60.4, 20.1 -60.3, 20.0  -60.3,20.0 -60.4))";
    Shape s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "POINT (30 10)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "LINESTRING (30 10, 10 30, 40 40)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "ENVELOPE(1, 2, 4, 3)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    wkt = "BUFFER(POINT(-10 30), 5.2)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dShape<?>);
    //wkt = "BUFFER(LINESTRING(1 2, 3 4), 0.5)";
    //s = ctx.getFormats().getWktReader().read(wkt);
    //assertTrue(s instanceof  Geo3dShape<?>);
  }

  @Test
  public void testPolygonWithCoplanarPoints() {
    Geo3dSpatialContextFactory factory = new Geo3dSpatialContextFactory();
    SpatialContext ctx = factory.newSpatialContext();

    final String polygon = "POLYGON ((-180 90, -180 -90, 180 -90, 180 90,-180 -90))";
    expectThrows(InvalidShapeException.class, () -> ctx.getFormats().getWktReader().read(polygon));

    final String polygonWithHole = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 20 30, 20 30))";
    expectThrows(InvalidShapeException.class, () -> ctx.getFormats().getWktReader().read(polygonWithHole));

    final String geometryCollection = "GEOMETRYCOLLECTION(POINT(4 6), LINESTRING(4 6,7 10), POLYGON ((-180 90, -180 -90, 180 -90, 180 90,-180 -90)))";
    expectThrows(InvalidShapeException.class, () -> ctx.getFormats().getWktReader().read(geometryCollection));

    final String multiPolygon = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)), ((180 90, 90 90, 180 90)))";
    expectThrows(InvalidShapeException.class, () -> ctx.getFormats().getWktReader().read(multiPolygon));

  }
}
