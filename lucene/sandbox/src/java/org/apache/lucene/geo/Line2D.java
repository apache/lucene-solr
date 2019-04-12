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
 * 2D line implementation represented as a balanced interval tree of edges.
 * <p>
 * Line {@code Line2D} Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #relate relate()} are {@code O(n)}, but for most practical lines are much faster than brute force.
 * @lucene.internal
 */
public final class Line2D implements Component {

  private final Line line;
  private final EdgeTree tree;
  private final Rectangle box;

  private Line2D(Line line) {
    this.line = line;
    tree = EdgeTree.createTree(line.getLats(), line.getLons());
    box = new Rectangle(line.minLat, line.maxLat, line.minLon, line.maxLon);
  }

  @Override
  public boolean contains(double latitude, double longitude) {
    if (Rectangle.containsPoint(latitude, longitude, box.minLat, box.maxLat, box.minLon, box.maxLon)) {
      if (tree.pointInEdge(latitude, longitude)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
    if (tree.crossesBox(minLat, maxLat, minLon, maxLon, true)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    if (ax == bx && bx == cx && ay == by && by == cy) {
      // indexed "triangle" is a point: check if point lies on any line segment
      if (tree.pointInEdge(ay, ax)) {
        return Relation.CELL_INSIDE_QUERY;
      }
    } else if ((ax == cx && ay == cy) || (bx == cx && by == cy)) {
      // indexed "triangle" is a line:
      if (tree.crossesLine(ax, ay, bx, by)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (Component.pointInTriangle(tree.lon1, tree.lat1, ax, ay, bx, by, cx, cy) == true ||
        tree.crossesTriangle(ax, ay, bx, by, cx, cy)) {
      // indexed "triangle" is a triangle:
      return Relation.CELL_CROSSES_QUERY;
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public Rectangle getBoundingBox() {
    return box;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Line2D line2D = (Line2D) o;
    return Objects.equals(line, line2D.line);
  }

  @Override
  public int hashCode() {
    return Objects.hash(line);
  }

  @Override
  public String toString() {
    return "Line2D{" +
        "line=" + line +
        '}';
  }

  /** Builds a Component from polygon */
  private static Component createComponent(Line line) {
    return new Line2D(line);
  }

  /** create a Line2D edge tree from provided array of Linestrings */
  public static ComponentTree create(Line... lines) {
    Component[] components = new Component[lines.length];
    for (int i = 0; i < components.length; ++i) {
      components[i] = createComponent(lines[i]);
    }
    return ComponentTree.create(components);
  }
}