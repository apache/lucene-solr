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
package org.apache.lucene.geo;

/**
 * 2D cartesian polygon implementation represented as a balanced interval tree of edges.
 *
 * @lucene.internal
 */
public class XYPolygon2D extends Polygon2D {

  protected XYPolygon2D(XYPolygon polygon, Component2D holes) {
    super(polygon.minX, polygon.maxX, polygon.minY, polygon.maxY, polygon.getPolyX(), polygon.getPolyY(), holes);
  }

  /** Builds a Polygon2D from multipolygon */
  public static Component2D create(XYPolygon... polygons) {
    XYPolygon2D components[] = new XYPolygon2D[polygons.length];
    for (int i = 0; i < components.length; i++) {
      XYPolygon gon = polygons[i];
      XYPolygon gonHoles[] = gon.getHoles();
      Component2D holes = null;
      if (gonHoles.length > 0) {
        holes = create(gonHoles);
      }
      components[i] = new XYPolygon2D(gon, holes);
    }
    return ComponentTree.create(components);
  }
}
