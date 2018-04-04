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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GeoShape representing a path across the surface of the globe,
 * with a specified half-width.  Path is described by a series of points.
 * Distances are measured from the starting point along the path, and then at right
 * angles to the path.
 *
 * @lucene.internal
 */
class GeoDegeneratePath extends GeoBasePath {

  /** The original list of path points */
  protected final List<GeoPoint> points = new ArrayList<GeoPoint>();
  
  /** A list of SegmentEndpoints */
  protected List<SegmentEndpoint> endPoints;
  /** A list of PathSegments */
  protected List<PathSegment> segments;

  /** A point on the edge */
  protected GeoPoint[] edgePoints;

  /** Set to true if path has been completely constructed */
  protected boolean isDone = false;
  
  /** Constructor.
   *@param planetModel is the planet model.
   *@param pathPoints are the points in the path.
   */
  public GeoDegeneratePath(final PlanetModel planetModel, final GeoPoint[] pathPoints) {
    this(planetModel);
    Collections.addAll(points, pathPoints);
    done();
  }

  /** Piece-wise constructor.  Use in conjunction with addPoint() and done().
   *@param planetModel is the planet model.
   */
  public GeoDegeneratePath(final PlanetModel planetModel) {
    super(planetModel);
  }

  /** Add a point to the path.
   *@param lat is the latitude of the point.
   *@param lon is the longitude of the point.
   */
  public void addPoint(final double lat, final double lon) {
    if (isDone)
      throw new IllegalStateException("Can't call addPoint() if done() already called");
    points.add(new GeoPoint(planetModel, lat, lon));
  }
  
  /** Complete the path.
   */
  public void done() {
    if (isDone)
      throw new IllegalStateException("Can't call done() twice");
    if (points.size() == 0)
      throw new IllegalArgumentException("Path must have at least one point");
    isDone = true;

    endPoints = new ArrayList<>(points.size());
    segments = new ArrayList<>(points.size());
    
    // First, build all segments.  We'll then go back and build corresponding segment endpoints.
    GeoPoint lastPoint = null;
    for (final GeoPoint end : points) {
      if (lastPoint != null) {
        final Plane normalizedConnectingPlane = new Plane(lastPoint, end);
        if (normalizedConnectingPlane == null) {
          continue;
        }
        segments.add(new PathSegment(planetModel, lastPoint, end, normalizedConnectingPlane));
      }
      lastPoint = end;
    }
    
    if (segments.size() == 0) {
      // Simple circle
      final GeoPoint point = points.get(0);
      
      final SegmentEndpoint onlyEndpoint = new SegmentEndpoint(point);
      endPoints.add(onlyEndpoint);
      this.edgePoints = new GeoPoint[]{point};
      return;
    }
    
    // Create segment endpoints.  Use an appropriate constructor for the start and end of the path.
    for (int i = 0; i < segments.size(); i++) {
      final PathSegment currentSegment = segments.get(i);
      
      if (i == 0) {
        // Starting endpoint
        final SegmentEndpoint startEndpoint = new SegmentEndpoint(currentSegment.start,
          currentSegment.startCutoffPlane);
        endPoints.add(startEndpoint);
        this.edgePoints = new GeoPoint[]{currentSegment.start};
        continue;
      }
      
      endPoints.add(new SegmentEndpoint(currentSegment.start,
        segments.get(i-1).endCutoffPlane,
        currentSegment.startCutoffPlane));
    }
    // Do final endpoint
    final PathSegment lastSegment = segments.get(segments.size()-1);
    endPoints.add(new SegmentEndpoint(lastSegment.end,
      lastSegment.endCutoffPlane));

  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoDegeneratePath(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, 
      SerializableObject.readPointArray(planetModel, inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writePointArray(outputStream, points);
  }

  @Override
  public double computePathCenterDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    // Walk along path and keep track of the closest distance we find
    double closestDistance = Double.POSITIVE_INFINITY;
    // Segments first
    for (PathSegment segment : segments) {
      final double segmentDistance = segment.pathCenterDistance(planetModel, distanceStyle, x, y, z);
      if (segmentDistance < closestDistance) {
        closestDistance = segmentDistance;
      }
    }
    // Now, endpoints
    for (SegmentEndpoint endpoint : endPoints) {
      final double endpointDistance = endpoint.pathCenterDistance(distanceStyle, x, y, z);
      if (endpointDistance < closestDistance) {
        closestDistance = endpointDistance;
      }
    }
    return closestDistance;
  }

  @Override
  public double computeNearestDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    double currentDistance = 0.0;
    double minPathCenterDistance = Double.POSITIVE_INFINITY;
    double bestDistance = Double.POSITIVE_INFINITY;
    int segmentIndex = 0;
    
    for (SegmentEndpoint endpoint : endPoints) {
      final double endpointPathCenterDistance = endpoint.pathCenterDistance(distanceStyle, x, y, z);
      if (endpointPathCenterDistance < minPathCenterDistance) {
        // Use this endpoint
        minPathCenterDistance = endpointPathCenterDistance;
        bestDistance = currentDistance;
      }
      // Look at the following segment, if any
      if (segmentIndex < segments.size()) {
        final PathSegment segment = segments.get(segmentIndex++);
        final double segmentPathCenterDistance = segment.pathCenterDistance(planetModel, distanceStyle, x, y, z);
        if (segmentPathCenterDistance < minPathCenterDistance) {
          minPathCenterDistance = segmentPathCenterDistance;
          bestDistance = distanceStyle.aggregateDistances(currentDistance, segment.nearestPathDistance(planetModel, distanceStyle, x, y, z));
        }
        currentDistance = distanceStyle.aggregateDistances(currentDistance, segment.fullPathDistance(distanceStyle));
      }
    }
    return bestDistance;
  }

  @Override
  protected double distance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    // Algorithm:
    // (1) If the point is within any of the segments along the path, return that value.
    // (2) If the point is within any of the segment end circles along the path, return that value.
    double currentDistance = 0.0;
    for (PathSegment segment : segments) {
      double distance = segment.pathDistance(planetModel, distanceStyle, x,y,z);
      if (distance != Double.POSITIVE_INFINITY)
        return distanceStyle.fromAggregationForm(distanceStyle.aggregateDistances(currentDistance, distance));
      currentDistance = distanceStyle.aggregateDistances(currentDistance, segment.fullPathDistance(distanceStyle));
    }

    int segmentIndex = 0;
    currentDistance = 0.0;
    for (SegmentEndpoint endpoint : endPoints) {
      double distance = endpoint.pathDistance(distanceStyle, x, y, z);
      if (distance != Double.POSITIVE_INFINITY)
        return distanceStyle.fromAggregationForm(distanceStyle.aggregateDistances(currentDistance, distance));
      if (segmentIndex < segments.size())
        currentDistance = distanceStyle.aggregateDistances(currentDistance, segments.get(segmentIndex++).fullPathDistance(distanceStyle));
    }

    return Double.POSITIVE_INFINITY;
  }

  @Override
  protected double deltaDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    // Since this is always called when a point is within the degenerate path, delta distance is always zero by definition.
    return 0.0;
  }
  
  @Override
  protected void distanceBounds(final Bounds bounds, final DistanceStyle distanceStyle, final double distanceValue) {
    // TBD: Compute actual bounds based on distance
    getBounds(bounds);
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    double minDistance = Double.POSITIVE_INFINITY;
    for (final SegmentEndpoint endpoint : endPoints) {
      final double newDistance = endpoint.outsideDistance(distanceStyle, x,y,z);
      if (newDistance < minDistance)
        minDistance = newDistance;
    }
    for (final PathSegment segment : segments) {
      final double newDistance = segment.outsideDistance(planetModel, distanceStyle, x, y, z);
      if (newDistance < minDistance)
        minDistance = newDistance;
    }
    return minDistance;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    for (SegmentEndpoint pathPoint : endPoints) {
      if (pathPoint.isWithin(x, y, z)) {
        return true;
      }
    }
    for (PathSegment pathSegment : segments) {
      if (pathSegment.isWithin(x, y, z)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane plane, final GeoPoint[] notablePoints, final Membership... bounds) {
    // We look for an intersection with any of the exterior edges of the path.
    // We also have to look for intersections with the cones described by the endpoints.
    // Return "true" if any such intersections are found.

    // For plane intersections, the basic idea is to come up with an equation of the line that is
    // the intersection (if any).  Then, find the intersections with the unit sphere (if any).  If
    // any of the intersection points are within the bounds, then we've detected an intersection.
    // Well, sort of.  We can detect intersections also due to overlap of segments with each other.
    // But that's an edge case and we won't be optimizing for it.
    //System.err.println(" Looking for intersection of plane "+plane+" with path "+this);
    
    // Since the endpoints are included in the path segments, we only need to do this if there are
    // no path segments
    if (endPoints.size() == 1) {
      return endPoints.get(0).intersects(planetModel, plane, notablePoints, bounds);
    }

    for (final PathSegment pathSegment : segments) {
      if (pathSegment.intersects(planetModel, plane, notablePoints, bounds)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean intersects(GeoShape geoShape) {
    // Since the endpoints are included in the path segments, we only need to do this if there are
    // no path segments
    if (endPoints.size() == 1) {
      return endPoints.get(0).intersects(geoShape);
    }

    for (final PathSegment pathSegment : segments) {
      if (pathSegment.intersects(geoShape)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    // For building bounds, order matters.  We want to traverse
    // never more than 180 degrees longitude at a pop or we risk having the
    // bounds object get itself inverted.  So do the edges first.
    for (PathSegment pathSegment : segments) {
      pathSegment.getBounds(planetModel, bounds);
    }
    if (endPoints.size() == 1) {
      endPoints.get(0).getBounds(planetModel, bounds);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoDegeneratePath))
      return false;
    GeoDegeneratePath p = (GeoDegeneratePath) o;
    if (!super.equals(p))
      return false;
    return points.equals(p.points);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + points.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoDegeneratePath: {planetmodel=" + planetModel+", points={" + points + "}}";
  }

  /**
   * This is precalculated data for segment endpoint.  Since the path is degenerate, there are several different cases:
   * (1) The path consists of a single endpoint.  In this case, the degenerate path consists of this one point.
   * (2) This is the end of a path.  There is a bounding plane passed in which describes the part of the world that is considered
   *    to belong to this endpoint.
   * (3) Intersection.  There are two cutoff planes, one for each end of the intersection.
   */
  private static class SegmentEndpoint {
    /** The center point of the endpoint */
    public final GeoPoint point;
    /** Pertinent cutoff planes from adjoining segments */
    public final Membership[] cutoffPlanes;
    /** Notable points for this segment endpoint */
    public final GeoPoint[] notablePoints;
    /** No notable points from the circle itself */
    public final static GeoPoint[] circlePoints = new GeoPoint[0];
    /** Null membership */
    public final static Membership[] NO_MEMBERSHIP = new Membership[0];
    
    /** Constructor for case (1).
     *@param point is the center point.
     */
    public SegmentEndpoint(final GeoPoint point) {
      this.point = point;
      this.cutoffPlanes = NO_MEMBERSHIP;
      this.notablePoints = circlePoints;
    }
    
    /** Constructor for case (2).
     * Generate an endpoint, given a single cutoff plane plus upper and lower edge points.
     *@param point is the center point.
     *@param cutoffPlane is the plane from the adjoining path segment marking the boundary between this endpoint and that segment.
     */
    public SegmentEndpoint(final GeoPoint point, final SidedPlane cutoffPlane) {
      this.point = point;
      this.cutoffPlanes = new Membership[]{new SidedPlane(cutoffPlane)};
      this.notablePoints = new GeoPoint[]{point};
    }

    /** Constructor for case (3).
     * Generate an endpoint, given two cutoff planes.
     *@param point is the center.
     *@param cutoffPlane1 is one adjoining path segment cutoff plane.
     *@param cutoffPlane2 is another adjoining path segment cutoff plane.
     */
    public SegmentEndpoint(final GeoPoint point,
      final SidedPlane cutoffPlane1, final SidedPlane cutoffPlane2) {
      this.point = point;
      this.cutoffPlanes = new Membership[]{new SidedPlane(cutoffPlane1), new SidedPlane(cutoffPlane2)};
      this.notablePoints = new GeoPoint[]{point};
    }
    
    /** Check if point is within this endpoint.
     *@param point is the point.
     *@return true of within.
     */
    public boolean isWithin(final Vector point) {
      return this.point.isIdentical(point.x, point.y, point.z);
    }

    /** Check if point is within this endpoint.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return true of within.
     */
    public boolean isWithin(final double x, final double y, final double z) {
      return this.point.isIdentical(x, y, z);
    }

    /** Compute interior path distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, in aggregation form.
     */
    public double pathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.POSITIVE_INFINITY;
      return distanceStyle.toAggregationForm(distanceStyle.computeDistance(this.point, x, y, z));
    }

    /** Compute nearest path distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric (always value zero), in aggregation form, or POSITIVE_INFINITY
     * if the point is not within the bounds of the endpoint.
     */
    public double nearestPathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return Double.POSITIVE_INFINITY;
        }
      }
      return distanceStyle.toAggregationForm(0.0);
    }

    /** Compute path center distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, or POSITIVE_INFINITY
     * if the point is not within the bounds of the endpoint.
     */
    public double pathCenterDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return Double.POSITIVE_INFINITY;
        }
      }
      return distanceStyle.computeDistance(this.point, x, y, z);
    }

    /** Compute external distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric.
     */
    public double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      return distanceStyle.computeDistance(this.point, x, y, z);
    }

    /** Determine if this endpoint intersects a specified plane.
     *@param planetModel is the planet model.
     *@param p is the plane.
     *@param notablePoints are the points associated with the plane.
     *@param bounds are any bounds which the intersection must lie within.
     *@return true if there is a matching intersection.
     */
    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
    // If not on the plane, no intersection
    if (!p.evaluateIsZero(point))
      return false;

    for (Membership m : bounds) {
      if (!m.isWithin(point))
        return false;
    }
    return true;
    }

    /** Determine if this endpoint intersects a GeoShape.
     *@param geoShape is the GeoShape.
     *@return true if there is shape intersect this endpoint.
     */
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.isWithin(point);
    }

    /** Get the bounds for a segment endpoint.
     *@param planetModel is the planet model.
     *@param bounds are the bounds to be modified.
     */
    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      bounds.addPoint(point);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SegmentEndpoint))
        return false;
      SegmentEndpoint other = (SegmentEndpoint) o;
      return point.equals(other.point);
    }

    @Override
    public int hashCode() {
      return point.hashCode();
    }

    @Override
    public String toString() {
      return point.toString();
    }
  }

  /**
   * This is the pre-calculated data for a path segment.
   */
  private static class PathSegment {
    /** Starting point of the segment */
    public final GeoPoint start;
    /** End point of the segment */
    public final GeoPoint end;
    /** Place to keep any complete segment distances we've calculated so far */
    public final Map<DistanceStyle,Double> fullDistanceCache = new HashMap<DistanceStyle,Double>();
    /** Normalized plane connecting the two points and going through world center */
    public final Plane normalizedConnectingPlane;
    /** Plane going through the center and start point, marking the start edge of the segment */
    public final SidedPlane startCutoffPlane;
    /** Plane going through the center and end point, marking the end edge of the segment */
    public final SidedPlane endCutoffPlane;
    /** Notable points for the connecting plane */
    public final GeoPoint[] connectingPlanePoints;

    /** Construct a path segment.
     *@param planetModel is the planet model.
     *@param start is the starting point.
     *@param end is the ending point.
     *@param normalizedConnectingPlane is the connecting plane.
     */
    public PathSegment(final PlanetModel planetModel, final GeoPoint start, final GeoPoint end,
      final Plane normalizedConnectingPlane) {
      this.start = start;
      this.end = end;
      this.normalizedConnectingPlane = normalizedConnectingPlane;
      
      // Cutoff planes use opposite endpoints as correct side examples
      startCutoffPlane = new SidedPlane(end, normalizedConnectingPlane, start);
      endCutoffPlane = new SidedPlane(start, normalizedConnectingPlane, end);
      connectingPlanePoints = new GeoPoint[]{start, end};
    }

    /** Compute the full distance along this path segment.
     *@param distanceStyle is the distance style.
     *@return the distance metric, in aggregation form.
     */
    public double fullPathDistance(final DistanceStyle distanceStyle) {
      synchronized (fullDistanceCache) {
        Double dist = fullDistanceCache.get(distanceStyle);
        if (dist == null) {
          dist = new Double(distanceStyle.toAggregationForm(distanceStyle.computeDistance(start, end.x, end.y, end.z)));
          fullDistanceCache.put(distanceStyle, dist);
        }
        return dist.doubleValue();
      }
    }
  
    /** Check if point is within this segment.
     *@param point is the point.
     *@return true of within.
     */
    public boolean isWithin(final Vector point) {
      return startCutoffPlane.isWithin(point) &&
          endCutoffPlane.isWithin(point) &&
          normalizedConnectingPlane.evaluateIsZero(point);
    }

    /** Check if point is within this segment.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return true of within.
     */
    public boolean isWithin(final double x, final double y, final double z) {
      return startCutoffPlane.isWithin(x, y, z) &&
          endCutoffPlane.isWithin(x, y, z) &&
          normalizedConnectingPlane.evaluateIsZero(x, y, z);
    }

    /** Compute path center distance.
     *@param planetModel is the planet model.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, or Double.POSITIVE_INFINITY if outside this segment
     */
    public double pathCenterDistance(final PlanetModel planetModel, final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // First, if this point is outside the endplanes of the segment, return POSITIVE_INFINITY.
      if (!startCutoffPlane.isWithin(x, y, z) || !endCutoffPlane.isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      // (1) Compute normalizedPerpPlane.  If degenerate, then there is no such plane, which means that the point given
      // is insufficient to distinguish between a family of such planes.  This can happen only if the point is one of the
      // "poles", imagining the normalized plane to be the "equator".  In that case, the distance returned should be zero.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION)
        return distanceStyle.computeDistance(start, x, y, z);
      final double normFactor = 1.0/magnitude;
      final Plane normalizedPerpPlane = new Plane(perpX * normFactor, perpY * normFactor, perpZ * normFactor, 0.0);
      
      final GeoPoint[] intersectionPoints = normalizedConnectingPlane.findIntersections(planetModel, normalizedPerpPlane);
      GeoPoint thePoint;
      if (intersectionPoints.length == 0)
        throw new RuntimeException("Can't find world intersection for point x="+x+" y="+y+" z="+z);
      else if (intersectionPoints.length == 1)
        thePoint = intersectionPoints[0];
      else {
        if (startCutoffPlane.isWithin(intersectionPoints[0]) && endCutoffPlane.isWithin(intersectionPoints[0]))
          thePoint = intersectionPoints[0];
        else if (startCutoffPlane.isWithin(intersectionPoints[1]) && endCutoffPlane.isWithin(intersectionPoints[1]))
          thePoint = intersectionPoints[1];
        else
          throw new RuntimeException("Can't find world intersection for point x="+x+" y="+y+" z="+z);
      }
      return distanceStyle.computeDistance(thePoint, x, y, z);
    }
    
    /** Compute nearest path distance.
     *@param planetModel is the planet model.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, in aggregation form, or Double.POSITIVE_INFINITY if outside this segment
     */
    public double nearestPathDistance(final PlanetModel planetModel, final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      // First, if this point is outside the endplanes of the segment, return POSITIVE_INFINITY.
      if (!startCutoffPlane.isWithin(x, y, z) || !endCutoffPlane.isWithin(x, y, z)) {
        return Double.POSITIVE_INFINITY;
      }
      // (1) Compute normalizedPerpPlane.  If degenerate, then there is no such plane, which means that the point given
      // is insufficient to distinguish between a family of such planes.  This can happen only if the point is one of the
      // "poles", imagining the normalized plane to be the "equator".  In that case, the distance returned should be zero.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION)
        return distanceStyle.toAggregationForm(0.0);
      final double normFactor = 1.0/magnitude;
      final Plane normalizedPerpPlane = new Plane(perpX * normFactor, perpY * normFactor, perpZ * normFactor, 0.0);
      
      final GeoPoint[] intersectionPoints = normalizedConnectingPlane.findIntersections(planetModel, normalizedPerpPlane);
      GeoPoint thePoint;
      if (intersectionPoints.length == 0)
        throw new RuntimeException("Can't find world intersection for point x="+x+" y="+y+" z="+z);
      else if (intersectionPoints.length == 1)
        thePoint = intersectionPoints[0];
      else {
        if (startCutoffPlane.isWithin(intersectionPoints[0]) && endCutoffPlane.isWithin(intersectionPoints[0]))
          thePoint = intersectionPoints[0];
        else if (startCutoffPlane.isWithin(intersectionPoints[1]) && endCutoffPlane.isWithin(intersectionPoints[1]))
          thePoint = intersectionPoints[1];
        else
          throw new RuntimeException("Can't find world intersection for point x="+x+" y="+y+" z="+z);
      }
      return distanceStyle.toAggregationForm(distanceStyle.computeDistance(start, thePoint.x, thePoint.y, thePoint.z));
    }
      

    /** Compute interior path distance.
     *@param planetModel is the planet model.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, in aggregation form.
     */
    public double pathDistance(final PlanetModel planetModel, final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.POSITIVE_INFINITY;

      // (1) Compute normalizedPerpPlane.  If degenerate, then return point distance from start to point.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION)
        return distanceStyle.toAggregationForm(distanceStyle.computeDistance(start, x,y,z));
      final double normFactor = 1.0/magnitude;
      final Plane normalizedPerpPlane = new Plane(perpX * normFactor, perpY * normFactor, perpZ * normFactor, 0.0);
      
      // Old computation: too expensive, because it calculates the intersection point twice.
      //return distanceStyle.computeDistance(planetModel, normalizedConnectingPlane, x, y, z, startCutoffPlane, endCutoffPlane) +
      //  distanceStyle.computeDistance(planetModel, normalizedPerpPlane, start.x, start.y, start.z, upperConnectingPlane, lowerConnectingPlane);

      final GeoPoint[] intersectionPoints = normalizedConnectingPlane.findIntersections(planetModel, normalizedPerpPlane);
      GeoPoint thePoint;
      if (intersectionPoints.length == 0)
        throw new RuntimeException("Can't find world intersection for point x="+x+" y="+y+" z="+z);
      else if (intersectionPoints.length == 1)
        thePoint = intersectionPoints[0];
      else {
        if (startCutoffPlane.isWithin(intersectionPoints[0]) && endCutoffPlane.isWithin(intersectionPoints[0]))
          thePoint = intersectionPoints[0];
        else if (startCutoffPlane.isWithin(intersectionPoints[1]) && endCutoffPlane.isWithin(intersectionPoints[1]))
          thePoint = intersectionPoints[1];
        else
          throw new RuntimeException("Can't find world intersection for point x="+x+" y="+y+" z="+z);
      }
      return distanceStyle.aggregateDistances(distanceStyle.toAggregationForm(distanceStyle.computeDistance(thePoint, x, y, z)),
        distanceStyle.toAggregationForm(distanceStyle.computeDistance(start, thePoint.x, thePoint.y, thePoint.z)));
    }

    /** Compute external distance.
     *@param planetModel is the planet model.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric.
     */
    public double outsideDistance(final PlanetModel planetModel, final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      final double distance = distanceStyle.computeDistance(planetModel, normalizedConnectingPlane, x,y,z, startCutoffPlane, endCutoffPlane);
      final double startDistance = distanceStyle.computeDistance(start, x,y,z);
      final double endDistance = distanceStyle.computeDistance(end, x,y,z);
      return Math.min(
        Math.min(startDistance, endDistance),
        distance);
    }

    /** Determine if this endpoint intersects a specified plane.
     *@param planetModel is the planet model.
     *@param p is the plane.
     *@param notablePoints are the points associated with the plane.
     *@param bounds are any bounds which the intersection must lie within.
     *@return true if there is a matching intersection.
     */
    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      return normalizedConnectingPlane.intersects(planetModel, p, connectingPlanePoints, notablePoints, bounds, startCutoffPlane, endCutoffPlane);
    }

    /** Determine if this endpoint intersects a specified GeoShape.
     *@param geoShape is the GeoShape.
     *@return true if there GeoShape intersects this endpoint.
     */
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(normalizedConnectingPlane, connectingPlanePoints, startCutoffPlane, endCutoffPlane);
    }

    /** Get the bounds for a segment endpoint.
     *@param planetModel is the planet model.
     *@param bounds are the bounds to be modified.
     */
    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      // We need to do all bounding planes as well as corner points
      bounds.addPoint(start).addPoint(end)
        .addPlane(planetModel, normalizedConnectingPlane, startCutoffPlane, endCutoffPlane);
    }

  }

}
