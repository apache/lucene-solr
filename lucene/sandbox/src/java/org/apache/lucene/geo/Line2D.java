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

import org.apache.lucene.index.PointValues.Relation;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D line implementation represented as a balanced interval tree of edges.
 * <p>
 * Line {@code Line2D} Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #relate relate()} are {@code O(n)}, but for most practical lines are much faster than brute force.
 * @lucene.internal
 */
public final class Line2D extends EdgeTree {

  private Line2D(Line line) {
    super(line.minLat, line.maxLat, line.minLon, line.maxLon, line.getLats(), line.getLons());
  }

  /** create a Line2D edge tree from provided array of Linestrings */
  public static Line2D create(Line... lines) {
    Line2D components[] = new Line2D[lines.length];
    for (int i = 0; i < components.length; ++i) {
      components[i] = new Line2D(lines[i]);
    }
    return (Line2D)createTree(components, 0, components.length - 1, false);
  }

  @Override
  protected Relation componentRelate(double minLat, double maxLat, double minLon, double maxLon) {
    if (tree.crossesBox(minLat, maxLat, minLon, maxLon, true)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  protected Relation componentRelateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    if (ax == bx && bx == cx && ay == by && by == cy) {
      // indexed "triangle" is a point: check if point lies on any line segment
      if (isPointOnLine(tree, ax, ay)) {
        return Relation.CELL_INSIDE_QUERY;
      }
    } else if (ax == cx && ay == cy) {
      // indexed "triangle" is a line:
      if (tree.crossesLine(ax, ay, bx, by)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (pointInTriangle(tree.lon1, tree.lat1, ax, ay, bx, by, cx, cy) == true ||
        tree.crossesTriangle(ax, ay, bx, by, cx, cy)) {
      // indexed "triangle" is a triangle:
      return Relation.CELL_CROSSES_QUERY;
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** returns true if the provided x, y point lies on the line */
  private boolean isPointOnLine(Edge tree, double x, double y) {
    if (y <= tree.max) {
      double minY = StrictMath.min(tree.lat1, tree.lat2);
      double maxY = StrictMath.max(tree.lat1, tree.lat2);
      double minX = StrictMath.min(tree.lon1, tree.lon2);
      double maxX = StrictMath.max(tree.lon1, tree.lon2);
      if (Rectangle.containsPoint(y, x, minY, maxY, minX, maxX) &&
          orient(tree.lon1, tree.lat1, tree.lon2, tree.lat2, x, y) == 0) {
        return true;
      }
      if (tree.left != null && isPointOnLine(tree.left, x, y)) {
        return true;
      }
      if (tree.right != null && maxY >= tree.low && isPointOnLine(tree.right, x, y)) {
        return true;
      }
    }
    return false;
  }
}