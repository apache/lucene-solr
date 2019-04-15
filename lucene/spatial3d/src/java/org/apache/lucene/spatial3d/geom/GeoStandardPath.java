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
class GeoStandardPath extends GeoBasePath {
  /** The cutoff angle (width) */
  protected final double cutoffAngle;

  /** Sine of cutoff angle */
  protected final double sinAngle;
  /** Cosine of cutoff angle */
  protected final double cosAngle;

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
   *@param maxCutoffAngle is the width of the path, measured as an angle.
   *@param pathPoints are the points in the path.
   */
  public GeoStandardPath(final PlanetModel planetModel, final double maxCutoffAngle, final GeoPoint[] pathPoints) {
    this(planetModel, maxCutoffAngle);
    Collections.addAll(points, pathPoints);
    done();
  }

  /** Piece-wise constructor.  Use in conjunction with addPoint() and done().
   *@param planetModel is the planet model.
   *@param maxCutoffAngle is the width of the path, measured as an angle.
   */
  public GeoStandardPath(final PlanetModel planetModel, final double maxCutoffAngle) {
    super(planetModel);
    if (maxCutoffAngle <= 0.0 || maxCutoffAngle > Math.PI * 0.5)
      throw new IllegalArgumentException("Cutoff angle out of bounds");
    this.cutoffAngle = maxCutoffAngle;
    this.cosAngle = Math.cos(maxCutoffAngle);
    this.sinAngle = Math.sin(maxCutoffAngle);
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
    // Compute an offset to use for all segments.  This will be based on the minimum magnitude of
    // the entire ellipsoid.
    final double cutoffOffset = this.sinAngle * planetModel.getMinimumMagnitude();
    
    // First, build all segments.  We'll then go back and build corresponding segment endpoints.
    GeoPoint lastPoint = null;
    for (final GeoPoint end : points) {
      if (lastPoint != null) {
        final Plane normalizedConnectingPlane = new Plane(lastPoint, end);
        if (normalizedConnectingPlane == null) {
          continue;
        }
        segments.add(new PathSegment(planetModel, lastPoint, end, normalizedConnectingPlane, cutoffOffset));
      }
      lastPoint = end;
    }
    
    if (segments.size() == 0) {
      // Simple circle
      double lat = points.get(0).getLatitude();
      double lon = points.get(0).getLongitude();
      // Compute two points on the circle, with the right angle from the center.  We'll use these
      // to obtain the perpendicular plane to the circle.
      double upperLat = lat + cutoffAngle;
      double upperLon = lon;
      if (upperLat > Math.PI * 0.5) {
        upperLon += Math.PI;
        if (upperLon > Math.PI)
          upperLon -= 2.0 * Math.PI;
        upperLat = Math.PI - upperLat;
      }
      double lowerLat = lat - cutoffAngle;
      double lowerLon = lon;
      if (lowerLat < -Math.PI * 0.5) {
        lowerLon += Math.PI;
        if (lowerLon > Math.PI)
          lowerLon -= 2.0 * Math.PI;
        lowerLat = -Math.PI - lowerLat;
      }
      final GeoPoint upperPoint = new GeoPoint(planetModel, upperLat, upperLon);
      final GeoPoint lowerPoint = new GeoPoint(planetModel, lowerLat, lowerLon);
      final GeoPoint point = points.get(0);
      
      // Construct normal plane
      final Plane normalPlane = Plane.constructNormalizedZPlane(upperPoint, lowerPoint, point);

      final CircleSegmentEndpoint onlyEndpoint = new CircleSegmentEndpoint(point, normalPlane, upperPoint, lowerPoint);
      endPoints.add(onlyEndpoint);
      this.edgePoints = new GeoPoint[]{onlyEndpoint.circlePlane.getSampleIntersectionPoint(planetModel, normalPlane)};
      return;
    }
    
    // Create segment endpoints.  Use an appropriate constructor for the start and end of the path.
    for (int i = 0; i < segments.size(); i++) {
      final PathSegment currentSegment = segments.get(i);
      
      if (i == 0) {
        // Starting endpoint
        final SegmentEndpoint startEndpoint = new CutoffSingleCircleSegmentEndpoint(currentSegment.start, 
          currentSegment.startCutoffPlane, currentSegment.ULHC, currentSegment.LLHC);
        endPoints.add(startEndpoint);
        this.edgePoints = new GeoPoint[]{currentSegment.ULHC};
        continue;
      }
      
      // General intersection case
      final PathSegment prevSegment = segments.get(i-1);
      if (prevSegment.endCutoffPlane.isWithin(currentSegment.ULHC) && prevSegment.endCutoffPlane.isWithin(currentSegment.LLHC) &&
        currentSegment.startCutoffPlane.isWithin(prevSegment.URHC) && currentSegment.startCutoffPlane.isWithin(prevSegment.LRHC)) {
        // The planes are identical.  We wouldn't need a circle at all except for the possibility of
        // backing up, which is hard to detect here.
        final SegmentEndpoint midEndpoint = new CutoffSingleCircleSegmentEndpoint(currentSegment.start, 
          prevSegment.endCutoffPlane, currentSegment.startCutoffPlane, currentSegment.ULHC, currentSegment.LLHC);
        //don't need a circle at all.  Special constructor...
        endPoints.add(midEndpoint);
      } else {
        endPoints.add(new CutoffDualCircleSegmentEndpoint(currentSegment.start,
          prevSegment.endCutoffPlane, currentSegment.startCutoffPlane,
          prevSegment.URHC, prevSegment.LRHC,
          currentSegment.ULHC, currentSegment.LLHC));
      }
    }
    // Do final endpoint
    final PathSegment lastSegment = segments.get(segments.size()-1);
    endPoints.add(new CutoffSingleCircleSegmentEndpoint(lastSegment.end,
      lastSegment.endCutoffPlane, lastSegment.URHC, lastSegment.LRHC));

  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoStandardPath(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, 
      SerializableObject.readDouble(inputStream),
      SerializableObject.readPointArray(planetModel, inputStream));
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeDouble(outputStream, cutoffAngle);
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
    
    for (final SegmentEndpoint endpoint : endPoints) {
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
    // The algorithm loops over the whole path to get the shortest distance
    double bestDistance = Double.POSITIVE_INFINITY;
    
    double currentDistance = 0.0;
    for (final PathSegment segment : segments) {
      double distance = segment.pathDistance(planetModel, distanceStyle, x,y,z);
      if (distance != Double.POSITIVE_INFINITY) {
        final double thisDistance = distanceStyle.fromAggregationForm(distanceStyle.aggregateDistances(currentDistance, distance));
        if (thisDistance < bestDistance) {
          bestDistance = thisDistance;
        }
      }
      currentDistance = distanceStyle.aggregateDistances(currentDistance, segment.fullPathDistance(distanceStyle));
    }

    int segmentIndex = 0;
    currentDistance = 0.0;
    for (final SegmentEndpoint endpoint : endPoints) {
      double distance = endpoint.pathDistance(distanceStyle, x, y, z);
      if (distance != Double.POSITIVE_INFINITY) {
        final double thisDistance = distanceStyle.fromAggregationForm(distanceStyle.aggregateDistances(currentDistance, distance));
        if (thisDistance < bestDistance) {
          bestDistance = thisDistance;
        }
      }
      if (segmentIndex < segments.size())
        currentDistance = distanceStyle.aggregateDistances(currentDistance, segments.get(segmentIndex++).fullPathDistance(distanceStyle));
    }

    return bestDistance;
  }

  @Override
  protected double deltaDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    // Algorithm:
    // (1) If the point is within any of the segments along the path, return that value.
    // (2) If the point is within any of the segment end circles along the path, return that value.
    // Finds best distance
    double bestDistance = Double.POSITIVE_INFINITY;
    
    for (final PathSegment segment : segments) {
      final double distance = segment.pathDeltaDistance(planetModel, distanceStyle, x, y, z);
      if (distance != Double.POSITIVE_INFINITY) {
        final double thisDistance = distanceStyle.fromAggregationForm(distance);
        if (thisDistance < bestDistance) {
          bestDistance = thisDistance;
        }
      }
    }

    for (final SegmentEndpoint endpoint : endPoints) {
      final double distance = endpoint.pathDeltaDistance(distanceStyle, x, y, z);
      if (distance != Double.POSITIVE_INFINITY) {
        final double thisDistance = distanceStyle.fromAggregationForm(distance);
        if (thisDistance < bestDistance) {
          bestDistance = thisDistance;
        }
      }
    }

    return bestDistance;
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
    for (final SegmentEndpoint pathPoint : endPoints) {
      if (pathPoint.intersects(planetModel, plane, notablePoints, bounds)) {
        return true;
      }
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
    for (final SegmentEndpoint pathPoint : endPoints) {
      if (pathPoint.intersects(geoShape)) {
        return true;
      }
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
    for (SegmentEndpoint pathPoint : endPoints) {
      pathPoint.getBounds(planetModel, bounds);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoStandardPath))
      return false;
    GeoStandardPath p = (GeoStandardPath) o;
    if (!super.equals(p))
      return false;
    if (cutoffAngle != p.cutoffAngle)
      return false;
    return points.equals(p.points);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp = Double.doubleToLongBits(cutoffAngle);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + points.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "GeoStandardPath: {planetmodel=" + planetModel+", width=" + cutoffAngle + "(" + cutoffAngle * 180.0 / Math.PI + "), points={" + points + "}}";
  }

  /**
   * Internal interface describing segment endpoint implementations.
   * There are several different such implementations, each corresponding to a different geometric conformation.
   * Note well: This is not necessarily a circle.  There are four cases:
   * (1) The path consists of a single endpoint.  In this case, we build a simple circle with the proper cutoff offset.
   * (2) This is the end of a path.  The circle plane must be constructed to go through two supplied points and be perpendicular to a connecting plane.
   * (2.5) Intersection, but the path on both sides is linear.  We generate a circle, but we use the cutoff planes to limit its influence in the straight line case.
   * (3) This is an intersection in a path.  We are supplied FOUR planes.  If there are intersections within bounds for both upper and lower, then
   *    we generate no circle at all.  If there is one intersection only, then we generate a plane that includes that intersection, as well as the remaining
   *    cutoff plane/edge plane points.
   */
  private interface SegmentEndpoint {
    
    /** Check if point is within this endpoint.
     *@param point is the point.
     *@return true of within.
     */
    boolean isWithin(final Vector point);

    /** Check if point is within this endpoint.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return true of within.
     */
    boolean isWithin(final double x, final double y, final double z);
    
    /** Compute delta path distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, in aggregation form.
     */
    double pathDeltaDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /** Compute interior path distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, in aggregation form.
     */
    double pathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /** Compute nearest path distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric (always value zero), in aggregation form, or POSITIVE_INFINITY
     * if the point is not within the bounds of the endpoint.
     */
    double nearestPathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /** Compute path center distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, or POSITIVE_INFINITY
     * if the point is not within the bounds of the endpoint.
     */
    double pathCenterDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /** Compute external distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric.
     */
    double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z);

    /** Determine if this endpoint intersects a specified plane.
     *@param planetModel is the planet model.
     *@param p is the plane.
     *@param notablePoints are the points associated with the plane.
     *@param bounds are any bounds which the intersection must lie within.
     *@return true if there is a matching intersection.
     */
    boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds);

    /** Determine if this endpoint intersects a GeoShape.
     *@param geoShape is the GeoShape.
     *@return true if there is shape intersect this endpoint.
     */
    boolean intersects(final GeoShape geoShape);

    /** Get the bounds for a segment endpoint.
     *@param planetModel is the planet model.
     *@param bounds are the bounds to be modified.
     */
    void getBounds(final PlanetModel planetModel, Bounds bounds);
  }

  /**
   * Base implementation of SegmentEndpoint
   */
  private static class BaseSegmentEndpoint implements SegmentEndpoint {
    /** The center point of the endpoint */
    protected final GeoPoint point;
    /** Null membership */
    protected final static Membership[] NO_MEMBERSHIP = new Membership[0];
    
    public BaseSegmentEndpoint(final GeoPoint point) {
      this.point = point;
    }

    @Override
    public boolean isWithin(final Vector point) {
      return false;
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return false;
    }

    @Override
    public double pathDeltaDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.POSITIVE_INFINITY;
      final double theDistance = distanceStyle.toAggregationForm(distanceStyle.computeDistance(this.point, x, y, z));
      return distanceStyle.aggregateDistances(theDistance, theDistance);
    }

    @Override
    public double pathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.POSITIVE_INFINITY;
      return distanceStyle.toAggregationForm(distanceStyle.computeDistance(this.point, x, y, z));
    }

    @Override
    public double nearestPathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      return distanceStyle.toAggregationForm(0.0);
    }

    @Override
    public double pathCenterDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      return distanceStyle.computeDistance(this.point, x, y, z);
    }

    @Override
    public double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      return distanceStyle.computeDistance(this.point, x, y, z);
    }

    @Override
    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      return false;
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return false;
    }

    @Override
    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      bounds.addPoint(point);
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof BaseSegmentEndpoint))
        return false;
      final BaseSegmentEndpoint other = (BaseSegmentEndpoint) o;
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
   * Simplest possible implementation of segment endpoint: a single point.
   */
  private static class DegenerateSegmentEndpoint extends BaseSegmentEndpoint {

    public DegenerateSegmentEndpoint(final GeoPoint point) {
      super(point);
    }

  }
  
  /**
   * Endpoint that's a simple circle.
   */
  private static class CircleSegmentEndpoint extends BaseSegmentEndpoint {
    /** A plane describing the circle */
    protected final SidedPlane circlePlane;
    /** No notable points from the circle itself */
    protected final static GeoPoint[] circlePoints = new GeoPoint[0];

    /** Constructor for case (1).
     * Generate a simple circle cutoff plane.
     *@param point is the center point.
     *@param upperPoint is a point that must be on the circle plane.
     *@param lowerPoint is another point that must be on the circle plane.
     */
    public CircleSegmentEndpoint(final GeoPoint point, final Plane normalPlane, final GeoPoint upperPoint, final GeoPoint lowerPoint) {
      super(point);
      // Construct a sided plane that goes through the two points and whose normal is in the normalPlane.
      this.circlePlane = SidedPlane.constructNormalizedPerpendicularSidedPlane(point, normalPlane, upperPoint, lowerPoint);
    }

    /** Constructor for case (3).  Called by superclass only.
     *@param point is the center point.
     *@param circlePlane is the circle plane.
     */
    protected CircleSegmentEndpoint(final GeoPoint point, final SidedPlane circlePlane) {
      super(point);
      this.circlePlane = circlePlane;
    }
    
    @Override
    public boolean isWithin(final Vector point) {
      return circlePlane.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      return circlePlane.isWithin(x, y, z);
    }

    @Override
    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      return circlePlane.intersects(planetModel, p, notablePoints, circlePoints, bounds);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(circlePlane, circlePoints, NO_MEMBERSHIP);
    }

    @Override
    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      super.getBounds(planetModel, bounds);
      bounds.addPlane(planetModel, circlePlane);
    }
    
  }
  
  /**
   * Endpoint that's a single circle with cutoff(s).
   */
  private static class CutoffSingleCircleSegmentEndpoint extends CircleSegmentEndpoint {
    
    /** Pertinent cutoff plane from adjoining segments */
    protected final Membership[] cutoffPlanes;
    /** Notable points for this segment endpoint */
    private final GeoPoint[] notablePoints;

    /** Constructor for case (2).
     * Generate an endpoint, given a single cutoff plane plus upper and lower edge points.
     *@param point is the center point.
     *@param cutoffPlane is the plane from the adjoining path segment marking the boundary between this endpoint and that segment.
     *@param topEdgePoint is a point on the cutoffPlane that should be also on the circle plane.
     *@param bottomEdgePoint is another point on the cutoffPlane that should be also on the circle plane.
     */
    public CutoffSingleCircleSegmentEndpoint(final GeoPoint point,
      final SidedPlane cutoffPlane, final GeoPoint topEdgePoint, final GeoPoint bottomEdgePoint) {
      super(point, cutoffPlane, topEdgePoint, bottomEdgePoint);
      this.cutoffPlanes = new Membership[]{new SidedPlane(cutoffPlane)};
      this.notablePoints = new GeoPoint[]{topEdgePoint, bottomEdgePoint};
    }

    /** Constructor for case (2.5).
     * Generate an endpoint, given two cutoff planes plus upper and lower edge points.
     *@param point is the center.
     *@param cutoffPlane1 is one adjoining path segment cutoff plane.
     *@param cutoffPlane2 is another adjoining path segment cutoff plane.
     *@param topEdgePoint is a point on the cutoffPlane that should be also on the circle plane.
     *@param bottomEdgePoint is another point on the cutoffPlane that should be also on the circle plane.
     */
    public CutoffSingleCircleSegmentEndpoint(final GeoPoint point,
      final SidedPlane cutoffPlane1, final SidedPlane cutoffPlane2, final GeoPoint topEdgePoint, final GeoPoint bottomEdgePoint) {
      super(point, cutoffPlane1, topEdgePoint, bottomEdgePoint);
      this.cutoffPlanes = new Membership[]{new SidedPlane(cutoffPlane1), new SidedPlane(cutoffPlane2)};
      this.notablePoints = new GeoPoint[]{topEdgePoint, bottomEdgePoint};
    }

    @Override
    public boolean isWithin(final Vector point) {
      if (!super.isWithin(point)) {
        return false;
      }
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(point)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      if (!super.isWithin(x, y, z)) {
        return false;
      }
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public double nearestPathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return Double.POSITIVE_INFINITY;
        }
      }
      return super.nearestPathDistance(distanceStyle, x, y, z);
    }

    @Override
    public double pathCenterDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return Double.POSITIVE_INFINITY;
        }
      }
      return super.pathCenterDistance(distanceStyle, x, y, z);
    }

    @Override
    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      return circlePlane.intersects(planetModel, p, notablePoints, this.notablePoints, bounds, this.cutoffPlanes);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(circlePlane, this.notablePoints, this.cutoffPlanes);
    }

  }
  
  /**
   * Endpoint that's a dual circle with cutoff(s).
   * This SegmentEndpoint is used when we have two adjoining segments that are not colinear, and when we are on a non-spherical world.
   * (1) We construct two circles.  Each circle uses the two segment endpoints for one of the two segments, plus the one segment endpoint
   *   that is on the other side of the segment's cutoff plane.
   * (2) isWithin() is computed using both circles, using just the portion that is within both segments' cutoff planes.  If either matches, the point is included.
   * (3) intersects() is computed using both circles, with similar cutoffs.
   * (4) bounds() uses both circles too.
   *
   */
  private static class CutoffDualCircleSegmentEndpoint extends BaseSegmentEndpoint {
   
    /** First circle */
    protected final SidedPlane circlePlane1;
    /** Second circle */
    protected final SidedPlane circlePlane2;
    /** Notable points for first circle */
    protected final GeoPoint[] notablePoints1;
    /** Notable points for second circle */
    protected final GeoPoint[] notablePoints2;
    /** Both cutoff planes are included here */
    protected final Membership[] cutoffPlanes;
    
    public CutoffDualCircleSegmentEndpoint(final GeoPoint point,
      final SidedPlane prevCutoffPlane, final SidedPlane nextCutoffPlane,
      final GeoPoint prevURHC, final GeoPoint prevLRHC,
      final GeoPoint currentULHC, final GeoPoint currentLLHC) {
      // Initialize superclass
      super(point);
      // First plane consists of prev endpoints plus one of the current endpoints (the one past the end of the prev segment)
      if (!prevCutoffPlane.isWithin(currentULHC)) {
        circlePlane1 = SidedPlane.constructNormalizedThreePointSidedPlane(point, prevURHC, prevLRHC, currentULHC);
        notablePoints1 = new GeoPoint[]{prevURHC, prevLRHC, currentULHC};
      } else if (!prevCutoffPlane.isWithin(currentLLHC)) {
        circlePlane1 = SidedPlane.constructNormalizedThreePointSidedPlane(point, prevURHC, prevLRHC, currentLLHC);
        notablePoints1 = new GeoPoint[]{prevURHC, prevLRHC, currentLLHC};
      } else {
        throw new IllegalArgumentException("Constructing CutoffDualCircleSegmentEndpoint with colinear segments");
      }
      // Second plane consists of current endpoints plus one of the prev endpoints (the one past the end of the current segment)
      if (!nextCutoffPlane.isWithin(prevURHC)) {
        circlePlane2 = SidedPlane.constructNormalizedThreePointSidedPlane(point, currentULHC, currentLLHC, prevURHC);
        notablePoints2 = new GeoPoint[]{currentULHC, currentLLHC, prevURHC};
      } else if (!nextCutoffPlane.isWithin(prevLRHC)) {
        circlePlane2 = SidedPlane.constructNormalizedThreePointSidedPlane(point, currentULHC, currentLLHC, prevLRHC);
        notablePoints2 = new GeoPoint[]{currentULHC, currentLLHC, prevLRHC};
      } else {
        throw new IllegalArgumentException("Constructing CutoffDualCircleSegmentEndpoint with colinear segments");
      }        
      this.cutoffPlanes = new Membership[]{new SidedPlane(prevCutoffPlane), new SidedPlane(nextCutoffPlane)};
    }

    @Override
    public boolean isWithin(final Vector point) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(point)) {
          return false;
        }
      }
      return circlePlane1.isWithin(point) || circlePlane2.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return false;
        }
      }
      return circlePlane1.isWithin(x, y, z) || circlePlane2.isWithin(x, y, z);
    }

    @Override
    public double nearestPathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return Double.POSITIVE_INFINITY;
        }
      }
      return super.nearestPathDistance(distanceStyle, x, y, z);
    }

    @Override
    public double pathCenterDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return Double.POSITIVE_INFINITY;
        }
      }
      return super.pathCenterDistance(distanceStyle, x, y, z);
    }

    @Override
    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      return circlePlane1.intersects(planetModel, p, notablePoints, this.notablePoints1, bounds, this.cutoffPlanes) ||
        circlePlane2.intersects(planetModel, p, notablePoints, this.notablePoints2, bounds, this.cutoffPlanes);
    }

    @Override
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(circlePlane1, this.notablePoints1, this.cutoffPlanes) ||
       geoShape.intersects(circlePlane2, this.notablePoints2, this.cutoffPlanes);
    }

    @Override
    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      super.getBounds(planetModel, bounds);
      bounds.addPlane(planetModel, circlePlane1);
      bounds.addPlane(planetModel, circlePlane2);
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
    /** Cutoff plane parallel to connecting plane representing one side of the path segment */
    public final SidedPlane upperConnectingPlane;
    /** Cutoff plane parallel to connecting plane representing the other side of the path segment */
    public final SidedPlane lowerConnectingPlane;
    /** Plane going through the center and start point, marking the start edge of the segment */
    public final SidedPlane startCutoffPlane;
    /** Plane going through the center and end point, marking the end edge of the segment */
    public final SidedPlane endCutoffPlane;
    /** Upper right hand corner of segment */
    public final GeoPoint URHC;
    /** Lower right hand corner of segment */
    public final GeoPoint LRHC;
    /** Upper left hand corner of segment */
    public final GeoPoint ULHC;
    /** Lower left hand corner of segment */
    public final GeoPoint LLHC;
    /** Notable points for the upper connecting plane */
    public final GeoPoint[] upperConnectingPlanePoints;
    /** Notable points for the lower connecting plane */
    public final GeoPoint[] lowerConnectingPlanePoints;
    /** Notable points for the start cutoff plane */
    public final GeoPoint[] startCutoffPlanePoints;
    /** Notable points for the end cutoff plane */
    public final GeoPoint[] endCutoffPlanePoints;

    /** Construct a path segment.
     *@param planetModel is the planet model.
     *@param start is the starting point.
     *@param end is the ending point.
     *@param normalizedConnectingPlane is the connecting plane.
     *@param planeBoundingOffset is the linear offset from the connecting plane to either side.
     */
    public PathSegment(final PlanetModel planetModel, final GeoPoint start, final GeoPoint end,
      final Plane normalizedConnectingPlane, final double planeBoundingOffset) {
      this.start = start;
      this.end = end;
      this.normalizedConnectingPlane = normalizedConnectingPlane;
      
      // Either start or end should be on the correct side
      upperConnectingPlane = new SidedPlane(start, normalizedConnectingPlane, -planeBoundingOffset);
      lowerConnectingPlane = new SidedPlane(start, normalizedConnectingPlane, planeBoundingOffset);
      // Cutoff planes use opposite endpoints as correct side examples
      startCutoffPlane = new SidedPlane(end, normalizedConnectingPlane, start);
      endCutoffPlane = new SidedPlane(start, normalizedConnectingPlane, end);
      final Membership[] upperSide = new Membership[]{upperConnectingPlane};
      final Membership[] lowerSide = new Membership[]{lowerConnectingPlane};
      final Membership[] startSide = new Membership[]{startCutoffPlane};
      final Membership[] endSide = new Membership[]{endCutoffPlane};
      GeoPoint[] points;
      points = upperConnectingPlane.findIntersections(planetModel, startCutoffPlane, lowerSide, endSide);
      if (points.length == 0) {
        throw new IllegalArgumentException("Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.ULHC = points[0];
      points = upperConnectingPlane.findIntersections(planetModel, endCutoffPlane, lowerSide, startSide);
      if (points.length == 0) {
        throw new IllegalArgumentException("Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.URHC = points[0];
      points = lowerConnectingPlane.findIntersections(planetModel, startCutoffPlane, upperSide, endSide);
      if (points.length == 0) {
        throw new IllegalArgumentException("Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.LLHC = points[0];
      points = lowerConnectingPlane.findIntersections(planetModel, endCutoffPlane, upperSide, startSide);
      if (points.length == 0) {
        throw new IllegalArgumentException("Some segment boundary points are off the ellipsoid; path too wide");
      }
      if (points.length > 1) {
        throw new IllegalArgumentException("Ambiguous boundary points; path too short");
      }
      this.LRHC = points[0];
      upperConnectingPlanePoints = new GeoPoint[]{ULHC, URHC};
      lowerConnectingPlanePoints = new GeoPoint[]{LLHC, LRHC};
      startCutoffPlanePoints = new GeoPoint[]{ULHC, LLHC};
      endCutoffPlanePoints = new GeoPoint[]{URHC, LRHC};
    }

    /** Compute the full distance along this path segment.
     *@param distanceStyle is the distance style.
     *@return the distance metric, in aggregation form.
     */
    public double fullPathDistance(final DistanceStyle distanceStyle) {
      synchronized (fullDistanceCache) {
        Double dist = fullDistanceCache.get(distanceStyle);
        if (dist == null) {
          dist = distanceStyle.toAggregationForm(distanceStyle.computeDistance(start, end.x, end.y, end.z));
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
          upperConnectingPlane.isWithin(point) &&
          lowerConnectingPlane.isWithin(point);
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
          upperConnectingPlane.isWithin(x, y, z) &&
          lowerConnectingPlane.isWithin(x, y, z);
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
      
    /** Compute delta path distance.
     *@param planetModel is the planet model.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric, in aggregation form, or Double.POSITIVE_INFINITY if outside the segment.
     */
    public double pathDeltaDistance(final PlanetModel planetModel, final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.POSITIVE_INFINITY;
      // (1) Compute normalizedPerpPlane.  If degenerate, then return point distance from start to point.
      // Want no allocations or expensive operations!  so we do this the hard way
      final double perpX = normalizedConnectingPlane.y * z - normalizedConnectingPlane.z * y;
      final double perpY = normalizedConnectingPlane.z * x - normalizedConnectingPlane.x * z;
      final double perpZ = normalizedConnectingPlane.x * y - normalizedConnectingPlane.y * x;
      final double magnitude = Math.sqrt(perpX * perpX + perpY * perpY + perpZ * perpZ);
      if (Math.abs(magnitude) < Vector.MINIMUM_RESOLUTION) {
        final double theDistance = distanceStyle.computeDistance(start, x,y,z);
        return distanceStyle.aggregateDistances(theDistance, theDistance);
      }
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
      final double theDistance = distanceStyle.toAggregationForm(distanceStyle.computeDistance(thePoint, x, y, z));
      return distanceStyle.aggregateDistances(theDistance, theDistance);
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
      final double upperDistance = distanceStyle.computeDistance(planetModel, upperConnectingPlane, x,y,z, lowerConnectingPlane, startCutoffPlane, endCutoffPlane);
      final double lowerDistance = distanceStyle.computeDistance(planetModel, lowerConnectingPlane, x,y,z, upperConnectingPlane, startCutoffPlane, endCutoffPlane);
      final double startDistance = distanceStyle.computeDistance(planetModel, startCutoffPlane, x,y,z, endCutoffPlane, lowerConnectingPlane, upperConnectingPlane);
      final double endDistance = distanceStyle.computeDistance(planetModel, endCutoffPlane, x,y,z, startCutoffPlane, lowerConnectingPlane, upperConnectingPlane);
      final double ULHCDistance = distanceStyle.computeDistance(ULHC, x,y,z);
      final double URHCDistance = distanceStyle.computeDistance(URHC, x,y,z);
      final double LLHCDistance = distanceStyle.computeDistance(LLHC, x,y,z);
      final double LRHCDistance = distanceStyle.computeDistance(LRHC, x,y,z);
      return Math.min(
        Math.min(
          Math.min(upperDistance,lowerDistance),
          Math.min(startDistance,endDistance)),
        Math.min(
          Math.min(ULHCDistance, URHCDistance),
          Math.min(LLHCDistance, LRHCDistance)));
    }

    /** Determine if this endpoint intersects a specified plane.
     *@param planetModel is the planet model.
     *@param p is the plane.
     *@param notablePoints are the points associated with the plane.
     *@param bounds are any bounds which the intersection must lie within.
     *@return true if there is a matching intersection.
     */
    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      return upperConnectingPlane.intersects(planetModel, p, notablePoints, upperConnectingPlanePoints, bounds, lowerConnectingPlane, startCutoffPlane, endCutoffPlane) ||
          lowerConnectingPlane.intersects(planetModel, p, notablePoints, lowerConnectingPlanePoints, bounds, upperConnectingPlane, startCutoffPlane, endCutoffPlane);
          /* ||
          // These two are necessary because our segment endpoints are not necessarily good fits to their adjoining segments.  The checks should really be part of the segment endpoint, however
          startCutoffPlane.intersects(planetModel, p, notablePoints, startCutoffPlanePoints, bounds, endCutoffPlane, upperConnectingPlane, lowerConnectingPlane) ||
          endCutoffPlane.intersects(planetModel, p, notablePoints, endCutoffPlanePoints, bounds, startCutoffPlane, upperConnectingPlane, lowerConnectingPlane);
              */
    }

    /** Determine if this endpoint intersects a specified GeoShape.
     *@param geoShape is the GeoShape.
     *@return true if there GeoShape intersects this endpoint.
     */
    public boolean intersects(final GeoShape geoShape) {
      return geoShape.intersects(upperConnectingPlane, upperConnectingPlanePoints, lowerConnectingPlane, startCutoffPlane, endCutoffPlane) ||
          geoShape.intersects(lowerConnectingPlane, lowerConnectingPlanePoints, upperConnectingPlane, startCutoffPlane, endCutoffPlane);
          /*||
          // These two are necessary because our segment endpoints are not necessarily good fits to their adjoining segments.  The checks should really be part of the segment endpoint, however
          geoShape.intersects(startCutoffPlane, startCutoffPlanePoints, endCutoffPlane, upperConnectingPlane, lowerConnectingPlane) ||
          geoShape.intersects(endCutoffPlane, endCutoffPlanePoints, startCutoffPlane, upperConnectingPlane, lowerConnectingPlane);
              */
    }

    /** Get the bounds for a segment endpoint.
     *@param planetModel is the planet model.
     *@param bounds are the bounds to be modified.
     */
    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      // We need to do all bounding planes as well as corner points
      bounds.addPoint(start).addPoint(end)
        .addPoint(ULHC).addPoint(URHC).addPoint(LRHC).addPoint(LLHC)
        .addPlane(planetModel, upperConnectingPlane, lowerConnectingPlane, startCutoffPlane, endCutoffPlane)
        .addPlane(planetModel, lowerConnectingPlane, upperConnectingPlane, startCutoffPlane, endCutoffPlane)
        .addPlane(planetModel, startCutoffPlane, endCutoffPlane, upperConnectingPlane, lowerConnectingPlane)
        .addPlane(planetModel, endCutoffPlane, startCutoffPlane, upperConnectingPlane, lowerConnectingPlane)
        .addIntersection(planetModel, upperConnectingPlane, startCutoffPlane, lowerConnectingPlane, endCutoffPlane)
        .addIntersection(planetModel, startCutoffPlane, lowerConnectingPlane, endCutoffPlane, upperConnectingPlane)
        .addIntersection(planetModel, lowerConnectingPlane, endCutoffPlane, upperConnectingPlane, startCutoffPlane)
        .addIntersection(planetModel, endCutoffPlane, upperConnectingPlane, startCutoffPlane, lowerConnectingPlane);
    }

  }

}
