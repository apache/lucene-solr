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

package org.apache.lucene.spatial3d.geom;

/**
 * Class which constructs a GeoPolygon representing S2 google pixel.
 *
 * @lucene.experimental
 */
public class GeoS2ShapeFactory {

  private GeoS2ShapeFactory() {
  }

  /**
   * Creates a convex polygon with 4 planes by providing 4 points in CCW.
   * This is a very fast shape and there are no checks that the points currently define
   * a convex shape.
   *
   * @param planetModel The planet model
   * @param point1 the first point.
   * @param point2 the second point.
   * @param point3 the third point.
   * @param point4 the four point.
   * @return the generated shape.
   */
  public static GeoPolygon makeGeoS2Shape(final PlanetModel planetModel,
                                          final GeoPoint point1,
                                          final GeoPoint point2,
                                          final GeoPoint point3,
                                          final GeoPoint point4) {
    return new GeoS2Shape(planetModel, point1, point2, point3, point4);
  }
}

