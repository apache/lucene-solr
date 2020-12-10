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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.BaseXYPointTestCase;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPolygon;

public class TestXYPointQueries extends BaseXYPointTestCase  {

  @Override
  protected void addPointToDoc(String field, Document doc, float x, float y) {
    doc.add(new XYPointField(field, x, y));
  }

  @Override
  protected Query newRectQuery(String field, float minX, float maxX, float minY, float maxY) {
    return XYPointField.newBoxQuery(field, minX, maxX, minY, maxY);
  }

  @Override
  protected Query newDistanceQuery(String field, float centerX, float centerY, float radius) {
    return XYPointField.newDistanceQuery(field, centerX, centerY, radius);
  }

  @Override
  protected Query newPolygonQuery(String field, XYPolygon... polygons) {
    return XYPointField.newPolygonQuery(field, polygons);
  }

  @Override
  protected Query newGeometryQuery(String field, XYGeometry... geometries) {
    return XYPointField.newGeometryQuery(field, geometries);
  }
}
