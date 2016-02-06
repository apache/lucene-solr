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
package org.apache.lucene.geo3d;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Class which constructs a GeoMembershipShape representing an arbitrary polygon.
 *
 * @lucene.experimental
 */
public class GeoPolygonFactory {
  private GeoPolygonFactory() {
  }

  /**
   * Create a GeoMembershipShape of the right kind given the specified bounds.
   *
   * @param pointList        is a list of the GeoPoints to build an arbitrary polygon out of.
   * @param convexPointIndex is the index of a single convex point whose conformation with
   *                         its neighbors determines inside/outside for the entire polygon.
   * @return a GeoPolygon corresponding to what was specified.
   */
  public static GeoPolygon makeGeoPolygon(final PlanetModel planetModel, final List<GeoPoint> pointList, final int convexPointIndex) {
    // The basic operation uses a set of points, two points determining one particular edge, and a sided plane
    // describing membership.
    return buildPolygonShape(planetModel, pointList, convexPointIndex, getLegalIndex(convexPointIndex + 1, pointList.size()),
        new SidedPlane(pointList.get(getLegalIndex(convexPointIndex - 1, pointList.size())),
            pointList.get(convexPointIndex), pointList.get(getLegalIndex(convexPointIndex + 1, pointList.size()))),
        false);
  }

  /** Build a GeoMembershipShape given points, starting edge, and whether starting edge is internal or not.
   * @param pointsList        is a list of the GeoPoints to build an arbitrary polygon out of.
   * @param startPointIndex is one of the points constituting the starting edge.
   * @param endPointIndex is another of the points constituting the starting edge.
   * @param startingEdge is the plane describing the starting edge.
   * @param isInternalEdge is true if the specified edge is an internal one.
   * @return a GeoMembershipShape corresponding to what was specified.
   */
  public static GeoPolygon buildPolygonShape(final PlanetModel planetModel, final List<GeoPoint> pointsList, final int startPointIndex, final int endPointIndex, final SidedPlane startingEdge, final boolean isInternalEdge) {
    // Algorithm as follows:
    // Start with sided edge.  Go through all points in some order.  For each new point, determine if the point is within all edges considered so far.
    // If not, put it into a list of points for recursion.  If it is within, add new edge and keep going.
    // Once we detect a point that is within, if there are points put aside for recursion, then call recursively.

    // Current composite.  This is what we'll actually be returning.
    final GeoCompositePolygon rval = new GeoCompositePolygon();

    final List<GeoPoint> recursionList = new ArrayList<GeoPoint>();
    final List<GeoPoint> currentList = new ArrayList<GeoPoint>();
    final BitSet internalEdgeList = new BitSet();
    final List<SidedPlane> currentPlanes = new ArrayList<SidedPlane>();

    // Initialize the current list and current planes
    currentList.add(pointsList.get(startPointIndex));
    currentList.add(pointsList.get(endPointIndex));
    internalEdgeList.set(currentPlanes.size(), isInternalEdge);
    currentPlanes.add(startingEdge);

    // Now, scan all remaining points, in order.  We'll use an index and just add to it.
    for (int i = 0; i < pointsList.size() - 2; i++) {
      GeoPoint newPoint = pointsList.get(getLegalIndex(i + endPointIndex + 1, pointsList.size()));
      if (isWithin(newPoint, currentPlanes)) {
        // Construct a sided plane based on the last two points, and the previous point
        SidedPlane newBoundary = new SidedPlane(currentList.get(currentList.size() - 2), newPoint, currentList.get(currentList.size() - 1));
        // Construct a sided plane based on the return trip
        SidedPlane returnBoundary = new SidedPlane(currentList.get(currentList.size() - 1), currentList.get(0), newPoint);
        // Verify that none of the points beyond the new point in the list are inside the polygon we'd
        // be creating if we stopped making the current polygon right now.
        boolean pointInside = false;
        for (int j = i + 1; j < pointsList.size() - 2; j++) {
          GeoPoint checkPoint = pointsList.get(getLegalIndex(j + endPointIndex + 1, pointsList.size()));
          boolean isInside = true;
          if (isInside && !newBoundary.isWithin(checkPoint))
            isInside = false;
          if (isInside && !returnBoundary.isWithin(checkPoint))
            isInside = false;
          if (isInside) {
            for (SidedPlane plane : currentPlanes) {
              if (!plane.isWithin(checkPoint)) {
                isInside = false;
                break;
              }
            }
          }
          if (isInside) {
            pointInside = true;
            break;
          }
        }
        if (!pointInside) {
          // Any excluded points?
          boolean isInternalBoundary = recursionList.size() > 0;
          if (isInternalBoundary) {
            // Handle exclusion
            recursionList.add(newPoint);
            recursionList.add(currentList.get(currentList.size() - 1));
            if (recursionList.size() == pointsList.size()) {
              // We are trying to recurse with a list the same size as the one we started with.
              // Clearly, the polygon cannot be constructed
              throw new IllegalArgumentException("Polygon is illegal; cannot be decomposed into convex parts");
            }
            // We want the other side for the recursion
            SidedPlane otherSideNewBoundary = new SidedPlane(newBoundary);
            rval.addShape(buildPolygonShape(planetModel, recursionList, recursionList.size() - 2, recursionList.size() - 1, otherSideNewBoundary, true));
            recursionList.clear();
          }
          currentList.add(newPoint);
          internalEdgeList.set(currentPlanes.size(), isInternalBoundary);
          currentPlanes.add(newBoundary);
        } else {
          recursionList.add(newPoint);
        }
      } else {
        recursionList.add(newPoint);
      }
    }

    boolean returnEdgeInternalBoundary = recursionList.size() > 0;
    if (returnEdgeInternalBoundary) {
      // The last step back to the start point had a recursion, so take care of that before we complete our work
      recursionList.add(currentList.get(0));
      recursionList.add(currentList.get(currentList.size() - 1));
      if (recursionList.size() == pointsList.size()) {
        // We are trying to recurse with a list the same size as the one we started with.
        // Clearly, the polygon cannot be constructed
        throw new IllegalArgumentException("Polygon is illegal; cannot be decomposed into convex parts");
      }
      // Construct a sided plane based on these two points, and the previous point
      SidedPlane newBoundary = new SidedPlane(currentList.get(currentList.size() - 2), currentList.get(0), currentList.get(currentList.size() - 1));
      // We want the other side for the recursion
      SidedPlane otherSideNewBoundary = new SidedPlane(newBoundary);
      rval.addShape(buildPolygonShape(planetModel, recursionList, recursionList.size() - 2, recursionList.size() - 1, otherSideNewBoundary, true));
      recursionList.clear();
    }
    // Now, add in the current shape.
    rval.addShape(new GeoConvexPolygon(planetModel, currentList, internalEdgeList, returnEdgeInternalBoundary));
    //System.out.println("Done creating polygon");
    return rval;
  }

  /** Check if a point is within a described list of planes.
   *@param newPoint is the point. 
   *@param currentPlanes is the list of planes.
   *@return true if within.
   */
  protected static boolean isWithin(final GeoPoint newPoint, final List<SidedPlane> currentPlanes) {
    for (SidedPlane p : currentPlanes) {
      if (!p.isWithin(newPoint))
        return false;
    }
    return true;
  }

  /** Convert raw point index into valid array position.
   *@param index is the array index.
   *@param size is the array size.
   *@return an updated index.
   */
  protected static int getLegalIndex(int index, int size) {
    while (index < 0) {
      index += size;
    }
    while (index >= size) {
      index -= size;
    }
    return index;
  }

}
