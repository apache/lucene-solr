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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * GeoConvexPolygon objects are generic building blocks of more complex structures.
 * The only restrictions on these objects are: (1) they must be convex; (2) they must have
 * a maximum extent no larger than PI.  Violating either one of these limits will
 * cause the logic to fail.
 */
public class GeoConvexPolygon extends GeoBaseExtendedShape implements GeoMembershipShape {
  protected final List<GeoPoint> points;
  protected final BitSet isInternalEdges;

  protected SidedPlane[] edges = null;
  protected boolean[] internalEdges = null;
  protected GeoPoint[][] notableEdgePoints = null;

  protected GeoPoint[] edgePoints = null;

  protected double fullDistance = 0.0;

  /**
   * Create a convex polygon from a list of points.  The first point must be on the
   * external edge.
   */
  public GeoConvexPolygon(final List<GeoPoint> pointList) {
    this.points = pointList;
    this.isInternalEdges = null;
    donePoints(false);
  }

  /**
   * Create a convex polygon from a list of points, keeping track of which boundaries
   * are internal.  This is used when creating a polygon as a building block for another shape.
   */
  public GeoConvexPolygon(final List<GeoPoint> pointList, final BitSet internalEdgeFlags, final boolean returnEdgeInternal) {
    this.points = pointList;
    this.isInternalEdges = internalEdgeFlags;
    donePoints(returnEdgeInternal);
  }

  /**
   * Create a convex polygon, with a starting latitude and longitude.
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
   */
  public GeoConvexPolygon(final double startLatitude, final double startLongitude) {
    points = new ArrayList<GeoPoint>();
    isInternalEdges = new BitSet();
    // Argument checking
    if (startLatitude > Math.PI * 0.5 || startLatitude < -Math.PI * 0.5)
      throw new IllegalArgumentException("Latitude out of range");
    if (startLongitude < -Math.PI || startLongitude > Math.PI)
      throw new IllegalArgumentException("Longitude out of range");

    final GeoPoint p = new GeoPoint(startLatitude, startLongitude);
    points.add(p);
  }

  /**
   * Add a point to the polygon.
   * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
   *
   * @param latitude       is the latitude of the next point.
   * @param longitude      is the longitude of the next point.
   * @param isInternalEdge is true if the edge just added should be considered "internal", and not
   *                       intersected as part of the intersects() operation.
   */
  public void addPoint(final double latitude, final double longitude, final boolean isInternalEdge) {
    // Argument checking
    if (latitude > Math.PI * 0.5 || latitude < -Math.PI * 0.5)
      throw new IllegalArgumentException("Latitude out of range");
    if (longitude < -Math.PI || longitude > Math.PI)
      throw new IllegalArgumentException("Longitude out of range");

    final GeoPoint p = new GeoPoint(latitude, longitude);
    isInternalEdges.set(points.size(), isInternalEdge);
    points.add(p);
  }

  /**
   * Finish the polygon, by connecting the last added point with the starting point.
   */
  public void donePoints(final boolean isInternalReturnEdge) {
    // If fewer than 3 points, can't do it.
    if (points.size() < 3)
      throw new IllegalArgumentException("Polygon needs at least three points.");
    // Time to construct the planes.  If the polygon is truly convex, then any adjacent point
    // to a segment can provide an interior measurement.
    edges = new SidedPlane[points.size()];
    notableEdgePoints = new GeoPoint[points.size()][];
    internalEdges = new boolean[points.size()];
    for (int i = 0; i < points.size(); i++) {
      final GeoPoint start = points.get(i);
      final boolean isInternalEdge = (isInternalEdges != null ? (i == isInternalEdges.size() ? isInternalReturnEdge : isInternalEdges.get(i)) : false);
      final GeoPoint end = points.get(legalIndex(i + 1));
      final double distance = start.arcDistance(end);
      if (distance > fullDistance)
        fullDistance = distance;
      final GeoPoint check = points.get(legalIndex(i + 2));
      final SidedPlane sp = new SidedPlane(check, start, end);
      //System.out.println("Created edge "+sp+" using start="+start+" end="+end+" check="+check);
      edges[i] = sp;
      notableEdgePoints[i] = new GeoPoint[]{start, end};
      internalEdges[i] = isInternalEdge;
    }
    createCenterPoint();
  }

  protected void createCenterPoint() {
    // In order to naively confirm that the polygon is convex, I would need to
    // check every edge, and verify that every point (other than the edge endpoints)
    // is within the edge's sided plane.  This is an order n^2 operation.  That's still
    // not wrong, though, because everything else about polygons has a similar cost.
    for (int edgeIndex = 0; edgeIndex < edges.length; edgeIndex++) {
      final SidedPlane edge = edges[edgeIndex];
      for (int pointIndex = 0; pointIndex < points.size(); pointIndex++) {
        if (pointIndex != edgeIndex && pointIndex != legalIndex(edgeIndex + 1)) {
          if (!edge.isWithin(points.get(pointIndex)))
            throw new IllegalArgumentException("Polygon is not convex: Point " + points.get(pointIndex) + " Edge " + edge);
        }
      }
    }
    edgePoints = new GeoPoint[]{points.get(0)};
  }

  protected int legalIndex(int index) {
    while (index >= points.size())
      index -= points.size();
    return index;
  }

  @Override
  public boolean isWithin(final Vector point) {
    for (final SidedPlane edge : edges) {
      if (!edge.isWithin(point))
        return false;
    }
    return true;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    for (final SidedPlane edge : edges) {
      if (!edge.isWithin(x, y, z))
        return false;
    }
    return true;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    //System.err.println("Checking for polygon intersection with plane "+p+"...");
    for (int edgeIndex = 0; edgeIndex < edges.length; edgeIndex++) {
      final SidedPlane edge = edges[edgeIndex];
      final GeoPoint[] points = this.notableEdgePoints[edgeIndex];
      if (!internalEdges[edgeIndex]) {
        //System.err.println(" non-internal edge "+edge);
        // Edges flagged as 'internal only' are excluded from the matching
        // Construct boundaries
        final Membership[] membershipBounds = new Membership[edges.length - 1];
        int count = 0;
        for (int otherIndex = 0; otherIndex < edges.length; otherIndex++) {
          if (otherIndex != edgeIndex) {
            membershipBounds[count++] = edges[otherIndex];
          }
        }
        if (edge.intersects(p, notablePoints, points, bounds, membershipBounds)) {
          //System.err.println(" intersects!");
          return true;
        }
      }
    }
    //System.err.println(" no intersection");
    return false;
  }

  /**
   * Compute longitude/latitude bounds for the shape.
   *
   * @param bounds is the optional input bounds object.  If this is null,
   *               a bounds object will be created.  Otherwise, the input object will be modified.
   * @return a Bounds object describing the shape's bounds.  If the bounds cannot
   * be computed, then return a Bounds object with noLongitudeBound,
   * noTopLatitudeBound, and noBottomLatitudeBound.
   */
  @Override
  public Bounds getBounds(Bounds bounds) {
    bounds = super.getBounds(bounds);

    // Add all the points
    for (final GeoPoint point : points) {
      bounds.addPoint(point);
    }

    // Add planes with membership.
    for (int edgeIndex = 0; edgeIndex < edges.length; edgeIndex++) {
      final SidedPlane edge = edges[edgeIndex];
      // Construct boundaries
      final Membership[] membershipBounds = new Membership[edges.length - 1];
      int count = 0;
      for (int otherIndex = 0; otherIndex < edges.length; otherIndex++) {
        if (otherIndex != edgeIndex) {
          membershipBounds[count++] = edges[otherIndex];
        }
      }
      edge.recordBounds(bounds, membershipBounds);
    }

    if (fullDistance >= Math.PI) {
      // We can't reliably assume that bounds did its longitude calculation right, so we force it to be unbounded.
      bounds.noLongitudeBound();
    }
    return bounds;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoConvexPolygon))
      return false;
    GeoConvexPolygon other = (GeoConvexPolygon) o;
    if (other.points.size() != points.size())
      return false;

    for (int i = 0; i < points.size(); i++) {
      if (!other.points.get(i).equals(points.get(i)))
        return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return points.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder edgeString = new StringBuilder("{");
    for (int i = 0; i < edges.length; i++) {
      edgeString.append(edges[i]).append(" internal? ").append(internalEdges[i]).append("; ");
    }
    edgeString.append("}");
    return "GeoConvexPolygon: {points=" + points + " edges=" + edgeString + "}";
  }
}
  
