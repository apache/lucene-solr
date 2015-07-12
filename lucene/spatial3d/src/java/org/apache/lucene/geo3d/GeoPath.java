package org.apache.lucene.geo3d;

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
 * @lucene.experimental
 */
public class GeoPath extends GeoBaseDistanceShape {
  
  public final double cutoffAngle;

  public final double sinAngle; // sine of cutoffAngle
  public final double cosAngle; // cosine of cutoffAngle

  public final List<GeoPoint> points = new ArrayList<GeoPoint>();
  
  public List<SegmentEndpoint> endPoints;
  public List<PathSegment> segments;

  public GeoPoint[] edgePoints;

  public boolean isDone = false;
  
  public GeoPath(final PlanetModel planetModel, final double maxCutoffAngle, final GeoPoint[] pathPoints) {
    this(planetModel, maxCutoffAngle);
    Collections.addAll(points, pathPoints);
    done();
  }
  
  public GeoPath(final PlanetModel planetModel, final double maxCutoffAngle) {
    super(planetModel);
    if (maxCutoffAngle <= 0.0 || maxCutoffAngle > Math.PI * 0.5)
      throw new IllegalArgumentException("Cutoff angle out of bounds");
    this.cutoffAngle = maxCutoffAngle;
    this.cosAngle = Math.cos(maxCutoffAngle);
    this.sinAngle = Math.sin(maxCutoffAngle);
  }

  public void addPoint(double lat, double lon) {
    if (isDone)
      throw new IllegalStateException("Can't call addPoint() if done() already called");
    points.add(new GeoPoint(planetModel, lat, lon));
  }
  
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

      final SegmentEndpoint onlyEndpoint = new SegmentEndpoint(points.get(0), upperPoint, lowerPoint);
      endPoints.add(onlyEndpoint);
      this.edgePoints = new GeoPoint[]{upperPoint};
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
      if (distance != Double.MAX_VALUE)
        return currentDistance + distance;
      currentDistance += segment.fullPathDistance(distanceStyle);
    }

    int segmentIndex = 0;
    currentDistance = 0.0;
    for (SegmentEndpoint endpoint : endPoints) {
      double distance = endpoint.pathDistance(distanceStyle, x, y, z);
      if (distance != Double.MAX_VALUE)
        return currentDistance + distance;
      if (segmentIndex < segments.size())
        currentDistance += segments.get(segmentIndex++).fullPathDistance(distanceStyle);
    }

    return Double.MAX_VALUE;
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    double minDistance = Double.MAX_VALUE;
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
      if (pathPoint.isWithin(x, y, z))
        return true;
    }
    for (PathSegment pathSegment : segments) {
      if (pathSegment.isWithin(x, y, z))
        return true;
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
  public Bounds getBounds(Bounds bounds) {
    bounds = super.getBounds(bounds);
    // For building bounds, order matters.  We want to traverse
    // never more than 180 degrees longitude at a pop or we risk having the
    // bounds object get itself inverted.  So do the edges first.
    for (PathSegment pathSegment : segments) {
      pathSegment.getBounds(planetModel, bounds);
    }
    for (SegmentEndpoint pathPoint : endPoints) {
      pathPoint.getBounds(planetModel, bounds);
    }
    return bounds;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoPath))
      return false;
    GeoPath p = (GeoPath) o;
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
    return "GeoPath: {planetmodel=" + planetModel+", width=" + cutoffAngle + "(" + cutoffAngle * 180.0 / Math.PI + "), points={" + points + "}}";
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
  public static class SegmentEndpoint {
    public final GeoPoint point;
    public final SidedPlane circlePlane;
    public final Membership[] cutoffPlanes;
    public final GeoPoint[] notablePoints;

    public final static GeoPoint[] circlePoints = new GeoPoint[0];

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
     */
    public SegmentEndpoint(final GeoPoint point, final GeoPoint upperPoint, final GeoPoint lowerPoint) {
      this.point = point;
      // Construct normal plane
      final Plane normalPlane = Plane.constructNormalizedVerticalPlane(upperPoint, lowerPoint, point);
      // Construct a sided plane that goes through the two points and whose normal is in the normalPlane.
      this.circlePlane = SidedPlane.constructNormalizedPerpendicularSidedPlane(point, normalPlane, upperPoint, lowerPoint);
      this.cutoffPlanes = new Membership[0];
      this.notablePoints = new GeoPoint[0];
    }
    
    /** Constructor for case (2).
     * Generate an endpoint, given a single cutoff plane plus upper and lower edge points.
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

    public boolean isWithin(final Vector point) {
      if (circlePlane == null)
        return false;
      return circlePlane.isWithin(point);
    }

    public boolean isWithin(final double x, final double y, final double z) {
      if (circlePlane == null)
        return false;
      return circlePlane.isWithin(x, y, z);
    }

    public double pathDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.MAX_VALUE;
      return distanceStyle.computeDistance(this.point, x, y, z);
    }

    public double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      return distanceStyle.computeDistance(this.point, x, y, z);
    }

    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      //System.err.println("  looking for intersection between plane "+p+" and circle "+circlePlane+" on proper side of "+cutoffPlanes+" within "+bounds);
      if (circlePlane == null)
        return false;
      return circlePlane.intersects(planetModel, p, notablePoints, this.notablePoints, bounds, this.cutoffPlanes);
    }

    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      bounds.addPoint(point);
      if (circlePlane == null)
        return;
      circlePlane.recordBounds(planetModel, bounds);
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
  public static class PathSegment {
    public final GeoPoint start;
    public final GeoPoint end;
    public final Map<DistanceStyle,Double> fullDistanceCache = new HashMap<DistanceStyle,Double>();
    public final Plane normalizedConnectingPlane;
    public final SidedPlane upperConnectingPlane;
    public final SidedPlane lowerConnectingPlane;
    public final SidedPlane startCutoffPlane;
    public final SidedPlane endCutoffPlane;
    public final GeoPoint URHC;
    public final GeoPoint LRHC;
    public final GeoPoint ULHC;
    public final GeoPoint LLHC;
    public final GeoPoint[] upperConnectingPlanePoints;
    public final GeoPoint[] lowerConnectingPlanePoints;
    public final GeoPoint[] startCutoffPlanePoints;
    public final GeoPoint[] endCutoffPlanePoints;
    public final double planeBoundingOffset;

    public PathSegment(final PlanetModel planetModel, final GeoPoint start, final GeoPoint end,
      final Plane normalizedConnectingPlane, final double planeBoundingOffset) {
      this.start = start;
      this.end = end;
      this.normalizedConnectingPlane = normalizedConnectingPlane;
      this.planeBoundingOffset = planeBoundingOffset;

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
      this.ULHC = points[0];
      points = upperConnectingPlane.findIntersections(planetModel, endCutoffPlane, lowerSide, startSide);
      if (points.length == 0) {
        throw new IllegalArgumentException("Some segment boundary points are off the ellipsoid; path too wide");
      }
      this.URHC = points[0];
      points = lowerConnectingPlane.findIntersections(planetModel, startCutoffPlane, upperSide, endSide);
      if (points.length == 0) {
        throw new IllegalArgumentException("Some segment boundary points are off the ellipsoid; path too wide");
      }
      this.LLHC = points[0];
      points = lowerConnectingPlane.findIntersections(planetModel, endCutoffPlane, upperSide, startSide);
      if (points.length == 0) {
        throw new IllegalArgumentException("Some segment boundary points are off the ellipsoid; path too wide");
      }
      this.LRHC = points[0];
      upperConnectingPlanePoints = new GeoPoint[]{ULHC, URHC};
      lowerConnectingPlanePoints = new GeoPoint[]{LLHC, LRHC};
      startCutoffPlanePoints = new GeoPoint[]{ULHC, LLHC};
      endCutoffPlanePoints = new GeoPoint[]{URHC, LRHC};
    }

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
    
    public boolean isWithin(final Vector point) {
      //System.err.println(" assessing whether point "+point+" is within path segment "+this);
      //System.err.println("  within "+startCutoffPlane+": "+startCutoffPlane.isWithin(point));
      //System.err.println("  within "+endCutoffPlane+": "+endCutoffPlane.isWithin(point));
      //System.err.println("  within "+upperConnectingPlane+": "+upperConnectingPlane.isWithin(point));
      //System.err.println("  within "+lowerConnectingPlane+": "+lowerConnectingPlane.isWithin(point));

      return startCutoffPlane.isWithin(point) &&
          endCutoffPlane.isWithin(point) &&
          upperConnectingPlane.isWithin(point) &&
          lowerConnectingPlane.isWithin(point);
    }

    public boolean isWithin(final double x, final double y, final double z) {
      return startCutoffPlane.isWithin(x, y, z) &&
          endCutoffPlane.isWithin(x, y, z) &&
          upperConnectingPlane.isWithin(x, y, z) &&
          lowerConnectingPlane.isWithin(x, y, z);
    }

    public double pathDistance(final PlanetModel planetModel, final DistanceStyle distanceStyle, final double x, final double y, final double z) {
      if (!isWithin(x,y,z))
        return Double.MAX_VALUE;

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

    public boolean intersects(final PlanetModel planetModel, final Plane p, final GeoPoint[] notablePoints, final Membership[] bounds) {
      return upperConnectingPlane.intersects(planetModel, p, notablePoints, upperConnectingPlanePoints, bounds, lowerConnectingPlane, startCutoffPlane, endCutoffPlane) ||
          lowerConnectingPlane.intersects(planetModel, p, notablePoints, lowerConnectingPlanePoints, bounds, upperConnectingPlane, startCutoffPlane, endCutoffPlane);
    }

    public void getBounds(final PlanetModel planetModel, Bounds bounds) {
      // We need to do all bounding planes as well as corner points
      bounds.addPoint(start).addPoint(end);
      upperConnectingPlane.recordBounds(planetModel, startCutoffPlane, bounds, lowerConnectingPlane, endCutoffPlane);
      startCutoffPlane.recordBounds(planetModel, lowerConnectingPlane, bounds, endCutoffPlane, upperConnectingPlane);
      lowerConnectingPlane.recordBounds(planetModel, endCutoffPlane, bounds, upperConnectingPlane, startCutoffPlane);
      endCutoffPlane.recordBounds(planetModel, upperConnectingPlane, bounds, startCutoffPlane, lowerConnectingPlane);
      upperConnectingPlane.recordBounds(planetModel, bounds, lowerConnectingPlane, startCutoffPlane, endCutoffPlane);
      lowerConnectingPlane.recordBounds(planetModel, bounds, upperConnectingPlane, startCutoffPlane, endCutoffPlane);
      startCutoffPlane.recordBounds(planetModel, bounds, endCutoffPlane, upperConnectingPlane, lowerConnectingPlane);
      endCutoffPlane.recordBounds(planetModel, bounds, startCutoffPlane, upperConnectingPlane, lowerConnectingPlane);
    }

  }

}
