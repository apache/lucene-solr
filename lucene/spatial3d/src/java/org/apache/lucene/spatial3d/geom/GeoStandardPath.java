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

      final SegmentEndpoint onlyEndpoint = new SegmentEndpoint(point, normalPlane, upperPoint, lowerPoint);
      endPoints.add(onlyEndpoint);
      this.edgePoints = new GeoPoint[]{onlyEndpoint.circlePlane.getSampleIntersectionPoint(planetModel, normalPlane)};
      return;
    }
    
    // Create segment endpoints.  Use an appropriate constructor for the start and end of the path.
    for (int i = 0; i < segments.size(); i++) {
      final PathSegment currentSegment = segments.get(i);
      
      if (i == 0) {
        // Starting endpoint
        final SegmentEndpoint startEndpoint = new SegmentEndpoint(currentSegment.start, 
          currentSegment.startCutoffPlane, currentSegment.ULHC, currentSegment.LLHC);
        endPoints.add(startEndpoint);
        this.edgePoints = new GeoPoint[]{currentSegment.ULHC};
        continue;
      }
      
      // General intersection case
      final PathSegment prevSegment = segments.get(i-1);
      // We construct four separate planes, and evaluate which one includes all interior points with least overlap
      final SidedPlane candidate1 = SidedPlane.constructNormalizedThreePointSidedPlane(currentSegment.start, prevSegment.URHC, currentSegment.ULHC, currentSegment.LLHC);
      final SidedPlane candidate2 = SidedPlane.constructNormalizedThreePointSidedPlane(currentSegment.start, currentSegment.ULHC, currentSegment.LLHC, prevSegment.LRHC);
      final SidedPlane candidate3 = SidedPlane.constructNormalizedThreePointSidedPlane(currentSegment.start, currentSegment.LLHC, prevSegment.LRHC, prevSegment.URHC);
      final SidedPlane candidate4 = SidedPlane.constructNormalizedThreePointSidedPlane(currentSegment.start, prevSegment.LRHC, prevSegment.URHC, currentSegment.ULHC);

      if (candidate1 == null && candidate2 == null && candidate3 == null && candidate4 == null) {
        // The planes are identical.  We wouldn't need a circle at all except for the possibility of
        // backing up, which is hard to detect here.
        final SegmentEndpoint midEndpoint = new SegmentEndpoint(currentSegment.start, 
          prevSegment.endCutoffPlane, currentSegment.startCutoffPlane, currentSegment.ULHC, currentSegment.LLHC);
        //don't need a circle at all.  Special constructor...
        endPoints.add(midEndpoint);
      } else {
        endPoints.add(new SegmentEndpoint(currentSegment.start,
          prevSegment.endCutoffPlane, currentSegment.startCutoffPlane,
          prevSegment.URHC, prevSegment.LRHC,
          currentSegment.ULHC, currentSegment.LLHC,
          candidate1, candidate2, candidate3, candidate4));
      }
    }
    // Do final endpoint
    final PathSegment lastSegment = segments.get(segments.size()-1);
    endPoints.add(new SegmentEndpoint(lastSegment.end,
      lastSegment.endCutoffPlane, lastSegment.URHC, lastSegment.LRHC));

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
        return currentDistance + distance;
      currentDistance += segment.fullPathDistance(distanceStyle);
    }

    int segmentIndex = 0;
    currentDistance = 0.0;
    for (SegmentEndpoint endpoint : endPoints) {
      double distance = endpoint.pathDistance(distanceStyle, x, y, z);
      if (distance != Double.POSITIVE_INFINITY)
        return currentDistance + distance;
      if (segmentIndex < segments.size())
        currentDistance += segments.get(segmentIndex++).fullPathDistance(distanceStyle);
    }

    return Double.POSITIVE_INFINITY;
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
   * This is precalculated data for segment endpoint.
   * Note well: This is not necessarily a circle.  There are four cases:
   * (1) The path consists of a single endpoint.  In this case, we build a simple circle with the proper cutoff offset.
   * (2) This is the end of a path.  The circle plane must be constructed to go through two supplied points and be perpendicular to a connecting plane.
   * (2.5) Intersection, but the path on both sides is linear.  We generate a circle, but we use the cutoff planes to limit its influence in the straight line case.
   * (3) This is an intersection in a path.  We are supplied FOUR planes.  If there are intersections within bounds for both upper and lower, then
   *    we generate no circle at all.  If there is one intersection only, then we generate a plane that includes that intersection, as well as the remaining
   *    cutoff plane/edge plane points.
   */
  private static class SegmentEndpoint {
    /** The center point of the endpoint */
    public final GeoPoint point;
    /** A plane describing the circle */
    public final SidedPlane circlePlane;
    /** Pertinent cutoff planes from adjoining segments */
    public final Membership[] cutoffPlanes;
    /** Notable points for this segment endpoint */
    public final GeoPoint[] notablePoints;
    /** No notable points from the circle itself */
    public final static GeoPoint[] circlePoints = new GeoPoint[0];
    /** Null membership */
    public final static Membership[] NO_MEMBERSHIP = new Membership[0];
    
    /** Base case.  Does nothing at all.
     */
    public SegmentEndpoint(final GeoPoint point) {
      this.point = point;
      this.circlePlane = null;
      this.cutoffPlanes = null;
      this.notablePoints = null;
    }
    
    /** Constructor for case (1).
     * Generate a simple circle cutoff plane.
     *@param point is the center point.
     *@param upperPoint is a point that must be on the circle plane.
     *@param lowerPoint is another point that must be on the circle plane.
     */
    public SegmentEndpoint(final GeoPoint point, final Plane normalPlane, final GeoPoint upperPoint, final GeoPoint lowerPoint) {
      this.point = point;
      // Construct a sided plane that goes through the two points and whose normal is in the normalPlane.
      this.circlePlane = SidedPlane.constructNormalizedPerpendicularSidedPlane(point, normalPlane, upperPoint, lowerPoint);
      this.cutoffPlanes = NO_MEMBERSHIP;
      this.notablePoints = circlePoints;
    }
    
    /** Constructor for case (2).
     * Generate an endpoint, given a single cutoff plane plus upper and lower edge points.
     *@param point is the center point.
     *@param cutoffPlane is the plane from the adjoining path segment marking the boundary between this endpoint and that segment.
     *@param topEdgePoint is a point on the cutoffPlane that should be also on the circle plane.
     *@param bottomEdgePoint is another point on the cutoffPlane that should be also on the circle plane.
     */
    public SegmentEndpoint(final GeoPoint point,
      final SidedPlane cutoffPlane, final GeoPoint topEdgePoint, final GeoPoint bottomEdgePoint) {
      this.point = point;
      this.cutoffPlanes = new Membership[]{new SidedPlane(cutoffPlane)};
      this.notablePoints = new GeoPoint[]{topEdgePoint, bottomEdgePoint};
      // To construct the plane, we now just need D, which is simply the negative of the evaluation of the circle normal vector at one of the points.
      this.circlePlane = SidedPlane.constructNormalizedPerpendicularSidedPlane(point, cutoffPlane, topEdgePoint, bottomEdgePoint);
    }

    /** Constructor for case (2.5).
     * Generate an endpoint, given two cutoff planes plus upper and lower edge points.
     *@param point is the center.
     *@param cutoffPlane1 is one adjoining path segment cutoff plane.
     *@param cutoffPlane2 is another adjoining path segment cutoff plane.
     *@param topEdgePoint is a point on the cutoffPlane that should be also on the circle plane.
     *@param bottomEdgePoint is another point on the cutoffPlane that should be also on the circle plane.
     */
    public SegmentEndpoint(final GeoPoint point,
      final SidedPlane cutoffPlane1, final SidedPlane cutoffPlane2, final GeoPoint topEdgePoint, final GeoPoint bottomEdgePoint) {
      this.point = point;
      this.cutoffPlanes = new Membership[]{new SidedPlane(cutoffPlane1), new SidedPlane(cutoffPlane2)};
      this.notablePoints = new GeoPoint[]{topEdgePoint, bottomEdgePoint};
      // To construct the plane, we now just need D, which is simply the negative of the evaluation of the circle normal vector at one of the points.
      this.circlePlane = SidedPlane.constructNormalizedPerpendicularSidedPlane(point, cutoffPlane1, topEdgePoint, bottomEdgePoint);
    }
    
    /** Constructor for case (3).
     * Generate an endpoint for an intersection, given four points.
     *@param point is the center.
     *@param prevCutoffPlane is the previous adjoining segment cutoff plane.
     *@param nextCutoffPlane is the next path segment cutoff plane.
     *@param notCand2Point is a point NOT on candidate2.
     *@param notCand1Point is a point NOT on candidate1.
     *@param notCand3Point is a point NOT on candidate3.
     *@param notCand4Point is a point NOT on candidate4.
     *@param candidate1 one of four candidate circle planes.
     *@param candidate2 one of four candidate circle planes.
     *@param candidate3 one of four candidate circle planes.
     *@param candidate4 one of four candidate circle planes.
     */
    public SegmentEndpoint(final GeoPoint point,
      final SidedPlane prevCutoffPlane, final SidedPlane nextCutoffPlane,
      final GeoPoint notCand2Point, final GeoPoint notCand1Point,
      final GeoPoint notCand3Point, final GeoPoint notCand4Point,
      final SidedPlane candidate1, final SidedPlane candidate2, final SidedPlane candidate3, final SidedPlane candidate4) {
      // Note: What we really need is a single plane that goes through all four points.
      // Since that's not possible in the ellipsoid case (because three points determine a plane, not four), we
      // need an approximation that at least creates a boundary that has no interruptions.
      // There are three obvious choices for the third point: either (a) one of the two remaining points, or (b) the top or bottom edge
      // intersection point.  (a) has no guarantee of continuity, while (b) is capable of producing something very far from a circle if
      // the angle between segments is acute.
      // The solution is to look for the side (top or bottom) that has an intersection within the shape.  We use the two points from
      // the opposite side to determine the plane, AND we pick the third to be either of the two points on the intersecting side
      // PROVIDED that the other point is within the final circle we come up with.
      this.point = point;
      
      // We construct four separate planes, and evaluate which one includes all interior points with least overlap
      // (Constructed beforehand because we need them for degeneracy check)

      final boolean cand1IsOtherWithin = candidate1!=null?candidate1.isWithin(notCand1Point):false;
      final boolean cand2IsOtherWithin = candidate2!=null?candidate2.isWithin(notCand2Point):false;
      final boolean cand3IsOtherWithin = candidate3!=null?candidate3.isWithin(notCand3Point):false;
      final boolean cand4IsOtherWithin = candidate4!=null?candidate4.isWithin(notCand4Point):false;
      
      if (cand1IsOtherWithin && cand2IsOtherWithin && cand3IsOtherWithin && cand4IsOtherWithin) {
        // The only way we should see both within is if all four points are coplanar.  In that case, we default to the simplest treatment.
        this.circlePlane = candidate1;  // doesn't matter which
        this.notablePoints = new GeoPoint[]{notCand2Point, notCand3Point, notCand1Point, notCand4Point};
        this.cutoffPlanes = new Membership[]{new SidedPlane(prevCutoffPlane), new SidedPlane(nextCutoffPlane)};
      } else if (cand1IsOtherWithin) {
        // Use candidate1, and DON'T include prevCutoffPlane in the cutoff planes list
        this.circlePlane = candidate1;
        this.notablePoints = new GeoPoint[]{notCand2Point, notCand3Point, notCand4Point};
        this.cutoffPlanes = new Membership[]{new SidedPlane(nextCutoffPlane)};
      } else if (cand2IsOtherWithin) {
        // Use candidate2
        this.circlePlane = candidate2;
        this.notablePoints = new GeoPoint[]{notCand3Point, notCand4Point, notCand1Point};
        this.cutoffPlanes = new Membership[]{new SidedPlane(nextCutoffPlane)};
      } else if (cand3IsOtherWithin) {
        this.circlePlane = candidate3;
        this.notablePoints = new GeoPoint[]{notCand4Point, notCand1Point, notCand2Point};
        this.cutoffPlanes = new Membership[]{new SidedPlane(prevCutoffPlane)};
      } else if (cand4IsOtherWithin) {
        this.circlePlane = candidate4;
        this.notablePoints = new GeoPoint[]{notCand1Point, notCand2Point, notCand3Point};
        this.cutoffPlanes = new Membership[]{new SidedPlane(prevCutoffPlane)};
      } else {
        // dunno what happened
        throw new RuntimeException("Couldn't come up with a plane through three points that included the fourth");
      }
    }

    /** Check if point is within this endpoint.
     *@param point is the point.
     *@return true of within.
     */
    public boolean isWithin(final Vector point) {
      if (circlePlane == null)
        return false;
      if (!circlePlane.isWithin(point))
        return false;
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(point)) {
          return false;
        }
      }
      return true;
    }

    /** Check if point is within this endpoint.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return true of within.
     */
    public boolean isWithin(final double x, final double y, final double z) {
      if (circlePlane == null)
        return false;
      if (!circlePlane.isWithin(x, y, z))
        return false;
      for (final Membership m : cutoffPlanes) {
        if (!m.isWithin(x,y,z)) {
          return false;
        }
      }
      return true;
    }

    /** Compute interior path distance.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric.
     */
    public double pathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.POSITIVE_INFINITY;
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
      //System.err.println("  looking for intersection between plane "+p+" and circle "+circlePlane+" on proper side of "+cutoffPlanes+" within "+bounds);
      if (circlePlane == null)
        return false;
      return circlePlane.intersects(planetModel, p, notablePoints, this.notablePoints, bounds, this.cutoffPlanes);
    }

    /** Get the bounds for a segment endpoint.
     *@param planetModel is the planet model.
     *@param bounds are the bounds to be modified.
     */
    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      bounds.addPoint(point);
      if (circlePlane == null)
        return;
      bounds.addPlane(planetModel, circlePlane);
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
     *@return the distance metric.
     */
    public double fullPathDistance(final DistanceStyle distanceStyle) {
      synchronized (fullDistanceCache) {
        Double dist = fullDistanceCache.get(distanceStyle);
        if (dist == null) {
          dist = new Double(distanceStyle.computeDistance(start, end.x, end.y, end.z));
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

    /** Compute interior path distance.
     *@param planetModel is the planet model.
     *@param distanceStyle is the distance style.
     *@param x is the point x.
     *@param y is the point y.
     *@param z is the point z.
     *@return the distance metric.
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
        return distanceStyle.computeDistance(start, x,y,z);
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
      return distanceStyle.computeDistance(thePoint, x, y, z) + distanceStyle.computeDistance(start, thePoint.x, thePoint.y, thePoint.z);
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
