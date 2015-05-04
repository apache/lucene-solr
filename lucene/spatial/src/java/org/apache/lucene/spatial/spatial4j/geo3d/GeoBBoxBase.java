package org.apache.lucene.spatial.spatial4j.geo3d;

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

/**
 * All bounding box shapes can derive from this base class, which furnishes
 * some common code
 *
 * @lucene.internal
 */
public abstract class GeoBBoxBase implements GeoBBox {

  protected final static GeoPoint NORTH_POLE = new GeoPoint(0.0, 0.0, 1.0);
  protected final static GeoPoint SOUTH_POLE = new GeoPoint(0.0, 0.0, -1.0);

  @Override
  public abstract boolean isWithin(final Vector point);

  protected final static int ALL_INSIDE = 0;
  protected final static int SOME_INSIDE = 1;
  protected final static int NONE_INSIDE = 2;

  protected int isShapeInsideBBox(final GeoShape path) {
    final GeoPoint[] pathPoints = path.getEdgePoints();
    boolean foundOutside = false;
    boolean foundInside = false;
    for (GeoPoint p : pathPoints) {
      if (isWithin(p)) {
        foundInside = true;
      } else {
        foundOutside = true;
      }
    }
    if (!foundInside && !foundOutside)
      return NONE_INSIDE;
    if (foundInside && !foundOutside)
      return ALL_INSIDE;
    if (foundOutside && !foundInside)
      return NONE_INSIDE;
    return SOME_INSIDE;
  }
}

