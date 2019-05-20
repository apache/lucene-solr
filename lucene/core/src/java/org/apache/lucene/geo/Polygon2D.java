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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.PointValues.Relation;

/**
 * 2D polygon implementation represented as a balanced interval tree of edges.
 * <p>
 * Loosely based on the algorithm described in <a href="http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf">
 * http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf</a>.
 * @lucene.internal
 */
public final class Polygon2D implements Component {
  private final Polygon polygon;
  private final EdgeTree tree;
  private final Rectangle box;
  /** Holes component or null */
  private final Component holes;
  /** keeps track if points lies on polygon boundary */
  private final AtomicBoolean containsBoundary = new AtomicBoolean(false);

  private Polygon2D(Polygon polygon, Component holes) {
    this.polygon = polygon;
    this.holes = holes;
    this.tree = EdgeTree.createTree(polygon.getPolyLats(), polygon.getPolyLons());
    this.box = new Rectangle(polygon.minLat, polygon.maxLat, polygon.minLon, polygon.maxLon);
  }

  /**
   * <p>
   * See <a href="https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html">
   * https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html</a> for more information.
   */
  @Override
  public boolean contains(double latitude, double longitude) {
    if (Rectangle.containsPoint(latitude, longitude, box.minLat, box.maxLat, box.minLon, box.maxLon)) {
      containsBoundary.set(false);
      if (tree.contains(latitude, longitude, containsBoundary)) {
        if (holes != null && holes.contains(latitude, longitude)) {
          return false;
        }
        return true;
      }
    }
    return false;
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
      if (tree.crossesBox(minLat, maxLat, minLon, maxLon, false)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    }  else if (numCorners == 0) {
      if (Rectangle.containsPoint(tree.lat1, tree.lon1, minLat, maxLat, minLon, maxLon) ||
          tree.crossesBox(minLat, maxLat, minLon, maxLon, false)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    if (holes != null) {
      Relation holeRelation = holes.relateTriangle(ax, ay, bx, by, cx, cy);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    if (ax == bx && bx == cx && ay == by && by == cy)  {
      // indexed "triangle" is a point: shortcut by checking contains
      return contains(ay, ax) ? Relation.CELL_INSIDE_QUERY : Relation.CELL_OUTSIDE_QUERY;
    } else if ((ax == cx && ay == cy) || (bx == cx && by == cy)) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(ax, ay, bx, by);
    }
    // indexed "triangle" is a triangle:
    return relateIndexedTriangle(ax, ay, bx, by, cx, cy);
  }

  /** relates an indexed line segment (a "flat triangle") with the polygon */
  private Relation relateIndexedLineSegment(double a2x, double a2y, double b2x, double b2y) {
    // check endpoints of the line segment
    int numCorners = 0;
    if (contains(a2y, a2x)) {
      ++numCorners;
    }
    if (contains(b2y, b2x)) {
      ++numCorners;
    }

    if (numCorners == 2) {
      if (tree.crossesLine(a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (tree.crossesLine(a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** relates an indexed triangle with the polygon */
  private Relation relateIndexedTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // check each corner: if < 3 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfTriangleCorners(ax, ay, bx, by, cx, cy);
    if (numCorners == 3) {
      if (tree.crossesTriangle(ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (Component.pointInTriangle(tree.lon1, tree.lat1, ax, ay, bx, by, cx, cy) ||
          tree.crossesTriangle(ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  private int numberOfTriangleCorners(double ax, double ay, double bx, double by, double cx, double cy) {
    int containsCount = 0;
    if (contains(ay, ax)) {
      containsCount++;
    }
    if (contains(by, bx)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (contains(cy, cx)) {
      containsCount++;
    }
    return containsCount;
  }

  // returns 0, 4, or something in between
  private int numberOfCorners(double minLat, double maxLat, double minLon, double maxLon) {
    int containsCount = 0;
    if (contains(minLat, minLon)) {
      containsCount++;
    }
    if (contains(minLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (contains(maxLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 2) {
      return containsCount;
    }
    if (contains(maxLat, minLon)) {
      containsCount++;
    }
    return containsCount;
  }

  @Override
  public Rectangle getBoundingBox() {
    return box;
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

  @Override
  public String toString() {
    return "Polygon2D{" +
        "polygon=" + polygon +
        '}';
  }

  /** Builds a Component from polygon */
  private static Component createComponent(Polygon polygon) {
    Polygon gonHoles[] = polygon.getHoles();
    Component holes = null;
    if (gonHoles.length > 0) {
      holes = create(gonHoles);
    }
    return new Polygon2D(polygon, holes);
  }

  /** Builds a Component tree from multipolygon */
  public static Component create(Polygon... polygons) {
    Component components[] = new Component[polygons.length];
    for (int i = 0; i < components.length; i++) {
      components[i] = createComponent(polygons[i]);
    }
    return ComponentTree.create(components);
  }
}
