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

import java.util.Objects;

import org.apache.lucene.index.PointValues.Relation;

/**
 * 2D polygon implementation represented as a balanced interval tree of edges.
 * <p>
 * Loosely based on the algorithm described in <a href="http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf">
 * http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf</a>.
 *
 * @lucene.internal
 */
public final class Polygon2D implements Component {

  /** tree of holes, or null */
  private final Polygon polygon;
  private final ComponentTree holes;
  private final EdgeTree edge;
  private final Rectangle box;

  private Polygon2D(Polygon polygon, ComponentTree holes) {
    this.polygon = polygon;
    this.holes = holes;
    this.edge = EdgeTree.createTree(polygon.getPolyLats(), polygon.getPolyLons());
    this.box = new Rectangle(polygon.minLat, polygon.maxLat, polygon.minLon, polygon.maxLon);
  }

  @Override
  public boolean contains(double latitude, double longitude) {
    if (edge.contains(latitude, longitude)) {
      if (holes != null && holes.contains(latitude, longitude)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public Rectangle getBoundingBox() {
    return box;
  }

  @Override
  public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
    // check any holes
    if (holes != null) {
      Relation holeRelation = holes.relate(minLat, maxLat, minLon, maxLon);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    // check each corner: if < 4 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfCorners(minLat, maxLat, minLon, maxLon);
    if (numCorners == 4) {
      if (edge.crosses(minLat, maxLat, minLon, maxLon)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    }  else if (numCorners == 0) {
      if (minLat >= edge.lat1 && maxLat <= edge.lat1 && minLon >= edge.lon2 && maxLon <= edge.lon2) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (edge.crosses(minLat, maxLat, minLon, maxLon)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // check any holes
    if (holes != null) {
      Relation holeRelation = holes.relateTriangle(ax, ay, bx, by, cx, cy);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    // check each corner: if < 3 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfTriangleCorners(ax, ay, bx, by, cx, cy);
    if (numCorners == 3) {
      if (edge.crossesTriangle(ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (ComponentTree.pointInTriangle(edge.lon1, edge.lat1, ax, ay, bx, by, cx, cy) == true) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (edge.crossesTriangle(ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  private int numberOfTriangleCorners(double ax, double ay, double bx, double by, double cx, double cy) {
    int containsCount = 0;
    if (edge.contains(ay, ax)) {
      containsCount++;
    }
    if (edge.contains(by, bx)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (edge.contains(cy, cx)) {
      containsCount++;
    }
    return containsCount;
  }

  @Override
  public String toString() {
    return "Polygon2D{" +
        "polygon=" + polygon +
        '}';
  }

  // returns 0, 4, or something in between
  private int numberOfCorners(double minLat, double maxLat, double minLon, double maxLon) {
    int containsCount = 0;
    if (edge.contains(minLat, minLon)) {
      containsCount++;
    }
    if (edge.contains(minLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (edge.contains(maxLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 2) {
      return containsCount;
    }
    if (edge.contains(maxLat, minLon)) {
      containsCount++;
    }
    return containsCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Polygon2D polygon2D = (Polygon2D) o;
    return Objects.equals(polygon, polygon2D.polygon);
  }

  @Override
  public int hashCode() {
    return Objects.hash(polygon);
  }

  /** Builds a Component from polygon */
  public static Component createComponent(Polygon polygon) {
    Polygon gonHoles[] = polygon.getHoles();
    ComponentTree holes = null;
    if (gonHoles.length > 0) {
      holes = create(gonHoles);
    }
    return new Polygon2D(polygon, holes);
  }

  /** Builds a Component tree from multipolygon */
  public static ComponentTree create(Polygon... polygons) {
    Component components[] = new Component[polygons.length];
    for (int i = 0; i < components.length; i++) {
      components[i] = createComponent(polygons[i]);
    }
    return ComponentTree.create(components);
  }
}
