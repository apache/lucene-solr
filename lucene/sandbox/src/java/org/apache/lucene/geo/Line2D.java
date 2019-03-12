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
    if (tree.crosses(minLat, maxLat, minLon, maxLon)) {
      return Relation.CELL_CROSSES_QUERY;
    }

    return Relation.CELL_OUTSIDE_QUERY;
  }

  protected Relation componentRelateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    if (tree.crossesTriangle(ax, ay, bx, by, cx, cy)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    //check if line is inside triangle
    if (pointInTriangle(tree.lon1, tree.lat1, ax, ay, bx, by, cx, cy)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  protected WithinRelation componentRelateWithinTriangle(double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
    //short cut, lines and points cannot contain a lines??
    if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)) {
      return WithinRelation.DISJOINT;
    }


    WithinRelation relation = WithinRelation.DISJOINT;
    //if any of the edges intersects an edge belonging to the shape then it cannot be within.
    if (tree.crossesLine(ax, ay, bx, by)) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (tree.crossesLine(bx, by, cx, cy)) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (tree.crossesLine(cx, cy, ax, ay)) {
      if (ca == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    //if any of the edges crosses and edge that does not belong to the shape
    // then it is a candidate for within
    if (relation == WithinRelation.CANDIDATE) {
      return WithinRelation.CANDIDATE;
    }

    double minLat = StrictMath.min(StrictMath.min(ay, by), cy);
    double minLon = StrictMath.min(StrictMath.min(ax, bx), cx);
    double maxLat = StrictMath.max(StrictMath.max(ay, by), cy);
    double maxLon = StrictMath.max(StrictMath.max(ax, bx), cx);

    //check that triangle bounding box not inside shape bounding box
    if (minLon > this.minLon || maxLon < this.maxLon || minLat > this.minLat || maxLat < this.maxLat) {
      return WithinRelation.DISJOINT;
    }

    //Check if shape is within the triangle
    if (pointInTriangle(tree.lon1, tree.lat1, ax, ay, bx, by, cx, cy) == true) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
  }
}