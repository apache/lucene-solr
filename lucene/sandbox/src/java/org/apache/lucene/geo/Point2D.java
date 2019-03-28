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

import org.apache.lucene.index.PointValues;

public class Point2D implements Component {

  private final double lat;
  private final double lon;
  private final Rectangle box;

  public Point2D(double lat, double lon) {
    this.lat = lat;
    this.lon = lon;
    this.box = new Rectangle(lat, lat, lon, lon);
  }

  @Override
  public PointValues.Relation relate(double minY, double maxY, double minX, double maxX) {
    if (minY >=  lat && maxY <= lat && minX >= lon && maxX <= lon) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public PointValues.Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    if (ComponentTree.pointInTriangle(lon, lat, ax, ay, bx, by, cx, cy)) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public boolean contains(double lat, double lon) {
    return lat == lat && lon == lon;
  }

  @Override
  public Rectangle getBoundingBox() {
    return box;
  }
}
