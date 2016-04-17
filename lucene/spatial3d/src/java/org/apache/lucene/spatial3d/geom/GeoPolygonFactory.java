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
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

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
  public static GeoPolygon makeGeoPolygon(final PlanetModel planetModel,
    final List<GeoPoint> pointList,
    final int convexPointIndex) {
    return makeGeoPolygon(planetModel, pointList, convexPointIndex, null);
  }
  
  /**
   * Create a GeoMembershipShape of the right kind given the specified bounds.
   *
   * @param pointList        is a list of the GeoPoints to build an arbitrary polygon out of.
   * @param convexPointIndex is the index of a single convex point whose conformation with
   *                         its neighbors determines inside/outside for the entire polygon.
   * @param holes is a list of polygons representing "holes" in the outside polygon.  Null == none.
   * @return a GeoPolygon corresponding to what was specified.
   */
  public static GeoPolygon makeGeoPolygon(final PlanetModel planetModel,
    final List<GeoPoint> pointList,
    final int convexPointIndex,
    final List<GeoPolygon> holes) {
    // The basic operation uses a set of points, two points determining one particular edge, and a sided plane
    // describing membership.
    //System.out.println("Initial point list = "+pointList+"; convexPointIndex = "+convexPointIndex+"; holes = "+holes);
    final GeoCompositePolygon rval = new GeoCompositePolygon();
    if (buildPolygonShape(rval,
        planetModel, pointList, new BitSet(),
        convexPointIndex, getLegalIndex(convexPointIndex + 1, pointList.size()),
        new SidedPlane(pointList.get(getLegalIndex(convexPointIndex - 1, pointList.size())),
            pointList.get(convexPointIndex), pointList.get(getLegalIndex(convexPointIndex + 1, pointList.size()))),
        holes,
        null) == false) {
      return null;
    }
    return rval;
  }

  /** Create a GeoPolygon using the specified points and holes, using order to determine 
   * siding of the polygon.  Much like ESRI, this method uses clockwise to indicate the space
   * on the same side of the shape as being inside, and counter-clockwise to indicate the
   * space on the opposite side as being inside.
   * @param pointList is a list of the GeoPoints to build an arbitrary polygon out of.  If points go
   *  clockwise from a given pole, then that pole should be within the polygon.  If points go
   *  counter-clockwise, then that pole should be outside the polygon.
   * @return a GeoPolygon corresponding to what was specified.
   */
  public static GeoPolygon makeGeoPolygon(final PlanetModel planetModel,
    final List<GeoPoint> pointList) {
    return makeGeoPolygon(planetModel, pointList, null);
  }
  
  /** Create a GeoPolygon using the specified points and holes, using order to determine 
   * siding of the polygon.  Much like ESRI, this method uses clockwise to indicate the space
   * on the same side of the shape as being inside, and counter-clockwise to indicate the
   * space on the opposite side as being inside.
   * @param pointList is a list of the GeoPoints to build an arbitrary polygon out of.  If points go
   *  clockwise from a given pole, then that pole should be within the polygon.  If points go
   *  counter-clockwise, then that pole should be outside the polygon.
   * @param holes is a list of polygons representing "holes" in the outside polygon.  Null == none.
   * @return a GeoPolygon corresponding to what was specified.
   */
  public static GeoPolygon makeGeoPolygon(final PlanetModel planetModel,
    final List<GeoPoint> pointList,
    final List<GeoPolygon> holes) {
    // Create a random number generator.  Effectively this furnishes us with a repeatable sequence
    // of points to use for poles.
    final Random generator = new Random(1234);
    //int counter = 0;
    while (true) {
      //counter++;
      // Pick the next random pole
      final GeoPoint pole = pickPole(generator, planetModel, pointList);
      // Is it inside or outside?
      final Boolean isPoleInside = isInsidePolygon(pole, pointList);
      if (isPoleInside != null) {
        // Legal pole
        //System.out.println("Took "+counter+" iterations to find pole");
        //System.out.println("Pole = "+pole+"; isInside="+isPoleInside+"; pointList = "+pointList);
        return makeGeoPolygon(planetModel, pointList, holes, pole, isPoleInside);
      }
      // If pole choice was illegal, try another one
    }
  }
    
  /**
   * Create a GeoPolygon using the specified points and holes and a test point.
   *
   * @param pointList        is a list of the GeoPoints to build an arbitrary polygon out of.
   * @param holes is a list of polygons representing "holes" in the outside polygon.  Null == none.
   * @param testPoint is a test point that is either known to be within the polygon area, or not.
   * @param testPointInside is true if the test point is within the area, false otherwise.
   * @return a GeoPolygon corresponding to what was specified.
   */
  public static GeoPolygon makeGeoPolygon(final PlanetModel planetModel,
    final List<GeoPoint> pointList,
    final List<GeoPolygon> holes,
    final GeoPoint testPoint, 
    final boolean testPointInside) {
    // First, exercise a sanity filter on the provided pointList, and remove identical points, linear points, and backtracks
    final List<GeoPoint> filteredPointList = filterPoints(pointList);
    // We will be trying twice to find the right GeoPolygon, using alternate siding choices for the first polygon
    // side.  While this looks like it might be 2x as expensive as it could be, there's really no other choice I can
    // find.
    final SidedPlane initialPlane = new SidedPlane(testPoint, filteredPointList.get(0), filteredPointList.get(1));
    // We don't know if this is the correct siding choice.  We will only know as we build the complex polygon.
    // So we need to be prepared to try both possibilities.
    GeoCompositePolygon rval = new GeoCompositePolygon();
    if (buildPolygonShape(rval, planetModel, filteredPointList, new BitSet(), 0, 1, initialPlane, holes, testPoint) == false) {
      // The testPoint was within the shape.  Was that intended?
      if (testPointInside) {
        // Yes: build it for real
        rval = new GeoCompositePolygon();
        buildPolygonShape(rval, planetModel, filteredPointList, new BitSet(), 0, 1, initialPlane, holes, null);
        return rval;
      }
      // No: do the complement and return that.
      rval = new GeoCompositePolygon();
      buildPolygonShape(rval, planetModel, filteredPointList, new BitSet(), 0, 1, new SidedPlane(initialPlane), holes, null);
      return rval;
    } else {
      // The testPoint was outside the shape.  Was that intended?
      if (!testPointInside) {
        // Yes: return what we just built
        return rval;
      }
      // No: return the complement
      rval = new GeoCompositePolygon();
      buildPolygonShape(rval, planetModel, filteredPointList, new BitSet(), 0, 1, new SidedPlane(initialPlane), holes, null);
      return rval;
    }
  }

  /** Filter duplicate points and coplanar points.
   */
  static List<GeoPoint> filterPoints(final List<GeoPoint> input) {
    
    final List<GeoPoint> noIdenticalPoints = new ArrayList<>(input.size());
    
    // Backtrack to find something different from the first point
    int startIndex = -1;
    final GeoPoint comparePoint = input.get(0);
    for (int i = 0; i < input.size()-1; i++) {
      final GeoPoint thePoint = input.get(getLegalIndex(- i - 1, input.size()));
      if (!thePoint.isNumericallyIdentical(comparePoint)) {
        startIndex = getLegalIndex(-i, input.size());
        break;
      }
    }
    if (startIndex == -1) {
      throw new IllegalArgumentException("polygon is degenerate: all points are identical");
    }
    
    // Now we can start the process of walking around, removing duplicate points.
    int currentIndex = startIndex;
    while (true) {
      final GeoPoint currentPoint = input.get(currentIndex);
      noIdenticalPoints.add(currentPoint);
      while (true) {
        currentIndex = getLegalIndex(currentIndex + 1, input.size());
        if (currentIndex == startIndex) {
          break;
        }
        final GeoPoint nextNonIdenticalPoint = input.get(currentIndex);
        if (!nextNonIdenticalPoint.isNumericallyIdentical(currentPoint)) {
          break;
        }
      }
      if (currentIndex == startIndex) {
        break;
      }
    }
    
    if (noIdenticalPoints.size() < 3) {
      throw new IllegalArgumentException("polygon has fewer than three non-identical points");
    }
    
    // Next step: remove coplanar points and backtracks.  For this, we use a strategy that is similar but we assess whether the points
    // are on the same plane, taking the first and last points on the same plane only.
    
    final List<GeoPoint> nonCoplanarPoints = new ArrayList<>(noIdenticalPoints.size());
    
    int startPlaneIndex = -1;
    final Plane comparePlane = new Plane(noIdenticalPoints.get(0), noIdenticalPoints.get(1));
    for (int i = 0; i < noIdenticalPoints.size()-1; i++) {
      final GeoPoint thePoint = noIdenticalPoints.get(getLegalIndex(- i - 1, noIdenticalPoints.size()));
      if (!comparePlane.evaluateIsZero(thePoint)) {
        startPlaneIndex = getLegalIndex(-i, noIdenticalPoints.size());
        break;
      }
    }
    if (startPlaneIndex == -1) {
      throw new IllegalArgumentException("polygon is degenerate: all points are coplanar");
    }

    // Now we can start the process of walking around, removing duplicate points.
    int currentPlaneIndex = startPlaneIndex;
    while (true) {
      final GeoPoint currentPoint = noIdenticalPoints.get(currentPlaneIndex);
      nonCoplanarPoints.add(currentPoint);
      int nextPlaneIndex = getLegalIndex(currentPlaneIndex + 1, noIdenticalPoints.size());
      if (nextPlaneIndex == startPlaneIndex) {
        break;
      }
      final Plane testPlane = new Plane(currentPoint, noIdenticalPoints.get(nextPlaneIndex));
      while (true) {
        currentPlaneIndex = nextPlaneIndex;
        if (currentPlaneIndex == startPlaneIndex) {
          break;
        }
        // Check if the next point is off plane
        nextPlaneIndex = getLegalIndex(currentPlaneIndex + 1, noIdenticalPoints.size());
        final GeoPoint nextNonCoplanarPoint = noIdenticalPoints.get(nextPlaneIndex);
        if (!testPlane.evaluateIsZero(nextNonCoplanarPoint)) {
          // We will want to add the point at currentPlaneIndex to the list (last on of the series)
          break;
        }
      }
      if (currentPlaneIndex == startPlaneIndex) {
        break;
      }
    }
    
    return nonCoplanarPoints;
  }
  
  /** The maximum distance from the close point to the trial pole: 2 degrees */
  private final static double MAX_POLE_DISTANCE = Math.PI * 2.0 / 180.0;
  
  /** Pick a random pole that has a good chance of being inside the polygon described by the points.
   * @param generator is the random number generator to use.
   * @param planetModel is the planet model to use.
   * @param points is the list of points available.
   * @return the randomly-determined pole selection.
   */
  private static GeoPoint pickPole(final Random generator, final PlanetModel planetModel, final List<GeoPoint> points) {
    final int pointIndex = generator.nextInt(points.size());
    final GeoPoint closePoint = points.get(pointIndex);
    // We pick a random angle and random arc distance, then generate a point based on closePoint
    final double angle = generator.nextDouble() * Math.PI * 2.0 - Math.PI;
    final double arcDistance = MAX_POLE_DISTANCE - generator.nextDouble() * MAX_POLE_DISTANCE;
    // We come up with a unit circle (x,y,z) coordinate given the random angle and arc distance.  The point is centered around the positive x axis.
    final double x = Math.cos(arcDistance);
    final double sinArcDistance = Math.sin(arcDistance);
    final double y = Math.cos(angle) * sinArcDistance;
    final double z = Math.sin(angle) * sinArcDistance;
    // Now, use closePoint for a rotation pole
    final double sinLatitude = Math.sin(closePoint.getLatitude());
    final double cosLatitude = Math.cos(closePoint.getLatitude());
    final double sinLongitude = Math.sin(closePoint.getLongitude());
    final double cosLongitude = Math.cos(closePoint.getLongitude());
    // This transformation should take the point (1,0,0) and transform it to the closepoint's actual (x,y,z) coordinates.
    // Coordinate rotation formula:
    // x1 = x0 cos T - y0 sin T
    // y1 = x0 sin T + y0 cos T
    // We're in essence undoing the following transformation (from GeoPolygonFactory):
    // x1 = x0 cos az + y0 sin az
    // y1 = - x0 sin az + y0 cos az
    // z1 = z0
    // x2 = x1 cos al + z1 sin al
    // y2 = y1
    // z2 = - x1 sin al + z1 cos al
    // So, we reverse the order of the transformations, AND we transform backwards.
    // Transforming backwards means using these identities: sin(-angle) = -sin(angle), cos(-angle) = cos(angle)
    // So:
    // x1 = x0 cos al - z0 sin al
    // y1 = y0
    // z1 = x0 sin al + z0 cos al
    // x2 = x1 cos az - y1 sin az
    // y2 = x1 sin az + y1 cos az
    // z2 = z1
    final double x1 = x * cosLatitude - z * sinLatitude;
    final double y1 = y;
    final double z1 = x * sinLatitude + z * cosLatitude;
    final double x2 = x1 * cosLongitude - y1 * sinLongitude;
    final double y2 = x1 * sinLongitude + y1 * cosLongitude;
    final double z2 = z1;
    // Finally, scale to put the point on the surface
    return planetModel.createSurfacePoint(x2, y2, z2);
  }
  
  /** For a specified point and a list of poly points, determine based on point order whether the
   * point should be considered in or out of the polygon.
   * @param point is the point to check.
   * @param polyPoints is the list of points comprising the polygon.
   * @return null if the point is illegal, otherwise false if the point is inside and true if the point is outside
   * of the polygon.
   */
  private static Boolean isInsidePolygon(final GeoPoint point, final List<GeoPoint> polyPoints) {
    // First, compute sine and cosine of pole point latitude and longitude
    final double latitude = point.getLatitude();
    final double longitude = point.getLongitude();
    final double sinLatitude = Math.sin(latitude);
    final double cosLatitude = Math.cos(latitude);
    final double sinLongitude = Math.sin(longitude);
    final double cosLongitude = Math.cos(longitude);
    
    // Now, compute the incremental arc distance around the points of the polygon
    double arcDistance = 0.0;
    Double prevAngle = null;
    //System.out.println("Computing angles:");
    for (final GeoPoint polyPoint : polyPoints) {
      final Double angle = computeAngle(polyPoint, sinLatitude, cosLatitude, sinLongitude, cosLongitude);
      if (angle == null) {
        return null;
      }
      //System.out.println("Computed angle: "+angle);
      if (prevAngle != null) {
        // Figure out delta between prevAngle and current angle, and add it to arcDistance
        double angleDelta = angle - prevAngle;
        if (angleDelta < -Math.PI) {
          angleDelta += Math.PI * 2.0;
        }
        if (angleDelta > Math.PI) {
          angleDelta -= Math.PI * 2.0;
        }
        if (Math.abs(angleDelta - Math.PI) < Vector.MINIMUM_RESOLUTION) {
          return null;
        }
        //System.out.println(" angle delta = "+angleDelta);
        arcDistance += angleDelta;
        //System.out.println(" For point "+polyPoint+" angle is "+angle+"; delta is "+angleDelta+"; arcDistance is "+arcDistance);
      }
      prevAngle = angle;
    }
    if (prevAngle != null) {
      final Double lastAngle = computeAngle(polyPoints.get(0), sinLatitude, cosLatitude, sinLongitude, cosLongitude);
      if (lastAngle == null) {
        return null;
      }
      //System.out.println("Computed last angle: "+lastAngle);
      // Figure out delta and add it
      double angleDelta = lastAngle - prevAngle;
      if (angleDelta < -Math.PI) {
        angleDelta += Math.PI * 2.0;
      }
      if (angleDelta > Math.PI) {
        angleDelta -= Math.PI * 2.0;
      }
      if (Math.abs(angleDelta - Math.PI) < Vector.MINIMUM_RESOLUTION) {
        return null;
      }
      //System.out.println(" angle delta = "+angleDelta);
      arcDistance += angleDelta;
      //System.out.println(" For point "+polyPoints.get(0)+" angle is "+lastAngle+"; delta is "+angleDelta+"; arcDistance is "+arcDistance);
    }

    // Clockwise == inside == negative
    //System.out.println("Arcdistance = "+arcDistance);
    if (Math.abs(arcDistance) < Vector.MINIMUM_RESOLUTION) {
      // No idea what direction, so try another pole.
      return null;
    }
    return arcDistance > 0.0;
  }
  
  /** Compute the angle for a point given rotation information.
    * @param point is the point to assess
    * @param sinLatitude the sine of the latitude
    * @param cosLatitude the cosine of the latitude
    * @param sinLongitude the sine of the longitude
    * @param cosLongitude the cosine of the longitude
    * @return the angle of rotation, or null if not computable
    */
  private static Double computeAngle(final GeoPoint point,
    final double sinLatitude,
    final double cosLatitude,
    final double sinLongitude,
    final double cosLongitude) {
    // Coordinate rotation formula:
    // x1 = x0 cos T - y0 sin T
    // y1 = x0 sin T + y0 cos T
    // We need to rotate the point in question into the coordinate frame specified by
    // the lat and lon trig functions.
    // To do this we need to do two rotations on it.  First rotation is in x/y.  Second rotation is in x/z.
    // And we rotate in the negative direction.
    // So:
    // x1 = x0 cos az + y0 sin az
    // y1 = - x0 sin az + y0 cos az
    // z1 = z0
    // x2 = x1 cos al + z1 sin al
    // y2 = y1
    // z2 = - x1 sin al + z1 cos al
      
    final double x1 = point.x * cosLongitude + point.y * sinLongitude;
    final double y1 = - point.x * sinLongitude + point.y * cosLongitude;
    final double z1 = point.z;
      
    // final double x2 = x1 * cosLatitude + z1 * sinLatitude;
    final double y2 = y1;
    final double z2 = - x1 * sinLatitude + z1 * cosLatitude;
    
    // Now we should be looking down the X axis; the original point has rotated coordinates (N, 0, 0).
    // So we can just compute the angle using y2 and z2.  (If Math.sqrt(y2*y2 + z2 * z2) is 0.0, then the point is on the pole and we need another one).
    if (Math.sqrt(y2*y2 + z2*z2) < Vector.MINIMUM_RESOLUTION) {
      return null;
    }
    
    return Math.atan2(z2, y2);
  }

  /** Build a GeoPolygon out of one concave part and multiple convex parts given points, starting edge, and whether starting edge is internal or not.
   * @param rval is the composite polygon to add to.
   * @param planetModel is the planet model.
   * @param pointsList is a list of the GeoPoints to build an arbitrary polygon out of.
   * @param internalEdges specifies which edges are internal.
   * @param startPointIndex is the first of the points, constituting the starting edge.
   * @param startingEdge is the plane describing the starting edge.
   * @param holes is the list of holes in the polygon, or null if none.
   * @param testPoint is an (optional) test point, which will be used to determine if we are generating
   *  a shape with the proper sidedness.  It is passed in only when the test point is supposed to be outside
   *  of the generated polygon.  In this case, if the generated polygon is found to contain the point, the
   *  method exits early with a null return value.
   *  This only makes sense in the context of evaluating both possible choices and using logic to determine
   *  which result to use.  If the test point is supposed to be within the shape, then it must be outside of the
   *  complement shape.  If the test point is supposed to be outside the shape, then it must be outside of the
   *  original shape.  Either way, we can figure out the right thing to use.
   * @return false if what was specified
   *  was inconsistent with what we generated.  Specifically, if we specify an exterior point that is
   *  found in the interior of the shape we create here we return false, which is a signal that we chose
   *  our initial plane sidedness backwards.
   */
  public static boolean buildPolygonShape(
    final GeoCompositePolygon rval,
    final PlanetModel planetModel,
    final List<GeoPoint> pointsList,
    final BitSet internalEdges,
    final int startPointIndex,
    final int endPointIndex,
    final SidedPlane startingEdge,
    final List<GeoPolygon> holes,
    final GeoPoint testPoint) {

    // It could be the case that we need a concave polygon.  So we need to try and look for that case
    // as part of the general code for constructing complex polygons.

    // Note that there can be only one concave polygon.
              
    // The code here must keep track of two lists of sided planes.  The first list contains the planes consistent with
    // a concave polygon.  This list will grow and shrink.  The second list is built starting at the current edge that
    // was last consistent with the concave polygon, and contains all edges consistent with a convex polygon.
    // When that sequence of edges is done, then an internal edge is created and the identified points are converted to a
    // convex polygon.  That internal edge is used to extend the list of edges in the concave polygon edge list.

    // The edge buffer.
    final EdgeBuffer edgeBuffer = new EdgeBuffer(pointsList, internalEdges, startPointIndex, endPointIndex, startingEdge);

    /*
    // Verify that the polygon does not self-intersect
    // Now, look for non-adjacent edges that cross.
    System.err.println("Looking for intersections...");
    System.err.println("Starting edge is: "+startingEdge);
    final Iterator<Edge> edgeIterator = edgeBuffer.iterator();
    while (edgeIterator.hasNext()) {
      final Edge edge = edgeIterator.next();
      final Set<Edge> excludedEdges = new HashSet<>();
      excludedEdges.add(edge);
      Edge oneBoundary = edgeBuffer.getPrevious(edge);
      while (oneBoundary.plane.isNumericallyIdentical(edge.plane)) {
        excludedEdges.add(oneBoundary);
        oneBoundary = edgeBuffer.getPrevious(oneBoundary);
      }
      excludedEdges.add(oneBoundary);
      Edge otherBoundary = edgeBuffer.getNext(edge);
      while (otherBoundary.plane.isNumericallyIdentical(edge.plane)) {
        excludedEdges.add(otherBoundary);
        otherBoundary = edgeBuffer.getNext(otherBoundary);
      }
      excludedEdges.add(otherBoundary);

      // Now go through all other edges and rule out any intersections
      final Iterator<Edge> compareIterator = edgeBuffer.iterator();
      while (compareIterator.hasNext()) {
        final Edge compareEdge = compareIterator.next();
        if (!excludedEdges.contains(compareEdge)) {
          // Found an edge we can compare with!
          //System.err.println("Found a compare edge...");
          boolean nonOverlapping = true;
          // We need the other boundaries though.
          Edge oneCompareBoundary = edgeBuffer.getPrevious(compareEdge);
          while (oneCompareBoundary.plane.isNumericallyIdentical(compareEdge.plane)) {
            if (excludedEdges.contains(oneCompareBoundary)) {
              //System.err.println(" excluded because oneCompareBoundary found to be in set");
              nonOverlapping = false;
              break;
            }
            oneCompareBoundary = edgeBuffer.getPrevious(oneCompareBoundary);
          }
          Edge otherCompareBoundary = edgeBuffer.getNext(compareEdge);
          while (otherCompareBoundary.plane.isNumericallyIdentical(compareEdge.plane)) {
            if (excludedEdges.contains(otherCompareBoundary)) {
              //System.err.println(" excluded because otherCompareBoundary found to be in set");
              nonOverlapping = false;
              break;
            }
            otherCompareBoundary = edgeBuffer.getNext(otherCompareBoundary);
          }
          if (nonOverlapping) {
            //System.err.println("Preparing to call findIntersections...");
            // Finally do an intersection test
            if (edge.plane.findIntersections(planetModel, compareEdge.plane, oneBoundary.plane, otherBoundary.plane, oneCompareBoundary.plane, otherCompareBoundary.plane).length > 0) {
              throw new IllegalArgumentException("polygon has intersecting edges");
            }
          }
        }
      }
    }
    */
    
    // Starting state:
    // The stopping point
    Edge stoppingPoint = edgeBuffer.pickOne();
    // The current edge
    Edge currentEdge = stoppingPoint;
    
    // Progressively look for convex sections.  If we find one, we emit it and replace it.
    // Keep going until we have been around once and nothing needed to change, and then
    // do the concave polygon, if necessary.
    while (true) {

      if (currentEdge == null) {
        // We're done!
        break;
      }
      
      // Find convexity around the current edge, if any
      final Boolean foundIt = findConvexPolygon(planetModel, currentEdge, rval, edgeBuffer, holes, testPoint);
      if (foundIt == null) {
        return false;
      }
      
      if (foundIt) {
        // New start point
        stoppingPoint = edgeBuffer.pickOne();
        currentEdge = stoppingPoint;
        // back around
        continue;
      }
      
      // Otherwise, go on to the next
      currentEdge = edgeBuffer.getNext(currentEdge);
      if (currentEdge == stoppingPoint) {
        break;
      }
    }
    
    // Look for any reason that the concave polygon cannot be created.
    // This test is really the converse of the one for a convex polygon.
    // Points on the edge of a convex polygon MUST be inside all the other
    // edges.  For a concave polygon, this check is still the same, except we have
    // to look at the reverse sided planes, not the forward ones.
    
    // If we find a point that is outside of the complementary edges, it means that
    // the point is in fact able to form a convex polygon with the edge it is
    // offending. 
    
    // If what is left has any plane/point pair that is on the wrong side, we have to split using one of the plane endpoints and the 
    // point in question.  This is best structured as a recursion, if detected.
    final Iterator<Edge> checkIterator = edgeBuffer.iterator();
    while (checkIterator.hasNext()) {
      final Edge checkEdge = checkIterator.next();
      final SidedPlane flippedPlane = new SidedPlane(checkEdge.plane);
      // Now walk around again looking for points that fail
      final Iterator<Edge> confirmIterator = edgeBuffer.iterator();
      while (confirmIterator.hasNext()) {
        final Edge confirmEdge = confirmIterator.next();
        if (confirmEdge == checkEdge) {
          continue;
        }
        // Look for a point that is on the wrong side of the check edge.  This means that we can't build the polygon.
        final GeoPoint thePoint;
        if (checkEdge.startPoint != confirmEdge.startPoint && checkEdge.endPoint != confirmEdge.startPoint && !flippedPlane.isWithin(confirmEdge.startPoint)) {
          thePoint = confirmEdge.startPoint;
        } else if (checkEdge.startPoint != confirmEdge.endPoint && checkEdge.endPoint != confirmEdge.endPoint && !flippedPlane.isWithin(confirmEdge.endPoint)) {
          thePoint = confirmEdge.endPoint;
        } else {
          thePoint = null;
        }
        if (thePoint != null) {
          // thePoint is on the wrong side of the complementary plane.  That means we cannot build a concave polygon, because the complement would not
          // be a legal convex polygon.
          // But we can take advantage of the fact that the distance between the edge and thePoint is less than 180 degrees, and so we can split the
          // would-be concave polygon into three segments.  The first segment includes the edge and thePoint, and uses the sense of the edge to determine the sense
          // of the polygon.
          
          // This should be the only problematic part of the polygon.
          // We know that thePoint is on the "wrong" side of the edge -- that is, it's on the side that the
          // edge is pointing at.
          final List<GeoPoint> thirdPartPoints = new ArrayList<>();
          final BitSet thirdPartInternal = new BitSet();
          thirdPartPoints.add(checkEdge.startPoint);
          thirdPartInternal.set(0, checkEdge.isInternal);
          thirdPartPoints.add(checkEdge.endPoint);
          thirdPartInternal.set(1, true);
          thirdPartPoints.add(thePoint);
          thirdPartInternal.set(2, true);
          //System.out.println("Doing convex part...");
          if (buildPolygonShape(rval,
            planetModel,
            thirdPartPoints,
            thirdPartInternal, 
            0,
            1,
            checkEdge.plane,
            holes,
            testPoint) == false) {
            return false;
          }
          //System.out.println("...done convex part.");

          // ??? check if we get the sense right
          
          // The part preceding the bad edge, back to thePoint, needs to be recursively
          // processed.  So, assemble what we need, which is basically a list of edges.
          Edge loopEdge = edgeBuffer.getPrevious(checkEdge);
          final List<GeoPoint> firstPartPoints = new ArrayList<>();
          final BitSet firstPartInternal = new BitSet();
          int i = 0;
          while (true) {
            firstPartPoints.add(loopEdge.endPoint);
            if (loopEdge.endPoint == thePoint) {
              break;
            }
            firstPartInternal.set(i++, loopEdge.isInternal);
            loopEdge = edgeBuffer.getPrevious(loopEdge);
          }
          firstPartInternal.set(i, true);
          //System.out.println("Doing first part...");
          if (buildPolygonShape(rval,
            planetModel,
            firstPartPoints,
            firstPartInternal, 
            firstPartPoints.size()-1,
            0,
            new SidedPlane(checkEdge.endPoint, false, checkEdge.startPoint, thePoint),
            holes,
            testPoint) == false) {
            return false;
          }
          //System.out.println("...done first part.");
          
          final List<GeoPoint> secondPartPoints = new ArrayList<>();
          final BitSet secondPartInternal = new BitSet();
          loopEdge = edgeBuffer.getNext(checkEdge);
          i = 0;
          while (true) {
            secondPartPoints.add(loopEdge.startPoint);
            if (loopEdge.startPoint == thePoint) {
              break;
            }
            secondPartInternal.set(i++, loopEdge.isInternal);
            loopEdge = edgeBuffer.getNext(loopEdge);
          }
          secondPartInternal.set(i, true);
          //System.out.println("Doing second part...");
          if (buildPolygonShape(rval,
            planetModel,
            secondPartPoints,
            secondPartInternal, 
            secondPartPoints.size()-1,
            0,
            new SidedPlane(checkEdge.startPoint, false, checkEdge.endPoint, thePoint),
            holes,
            testPoint) == false) {
            return false;
          }
          //System.out.println("... done second part");
          
          return true;
        }
      }
    }
    
    // No violations found: we know it's a legal concave polygon.
    
    // If there's anything left in the edge buffer, convert to concave polygon.
    if (makeConcavePolygon(planetModel, rval, edgeBuffer, holes, testPoint) == false) {
      return false;
    }
    
    return true;
  }
  
  /** Look for a concave polygon in the remainder of the edgebuffer.
   * By this point, if there are any edges in the edgebuffer, they represent a concave polygon.
   * @param planetModel is the planet model.
   * @param rval is the composite polygon we're building.
   * @param edgeBuffer is the edge buffer.
   * @param holes is the optional list of holes.
   * @param testPoint is the optional test point.
   * @return true unless the testPoint caused failure.
   */
  private static boolean makeConcavePolygon(final PlanetModel planetModel,
    final GeoCompositePolygon rval,
    final EdgeBuffer edgeBuffer,
    final List<GeoPolygon> holes,
    final GeoPoint testPoint) {
    if (edgeBuffer.size() == 0) {
      return true;
    }
    
    // If there are less than three edges, something got messed up somehow.  Don't know how this
    // can happen but check.
    if (edgeBuffer.size() < 3) {
      throw new IllegalStateException("Ending edge buffer had only "+edgeBuffer.size()+" edges");
    }
    
    // Create the list of points
    final List<GeoPoint> points = new ArrayList<GeoPoint>(edgeBuffer.size());
    final BitSet internalEdges = new BitSet(edgeBuffer.size()-1);

    //System.out.println("Concave polygon points:");
    Edge edge = edgeBuffer.pickOne();
    boolean isInternal = false;
    for (int i = 0; i < edgeBuffer.size(); i++) {
      //System.out.println(" "+edge.plane+": "+edge.startPoint+"->"+edge.endPoint+"; previous? "+(edge.plane.isWithin(edgeBuffer.getPrevious(edge).startPoint)?"in":"out")+" next? "+(edge.plane.isWithin(edgeBuffer.getNext(edge).endPoint)?"in":"out"));
      points.add(edge.startPoint);
      if (i < edgeBuffer.size() - 1) {
        internalEdges.set(i, edge.isInternal);
      } else {
        isInternal = edge.isInternal;
      }
      edge = edgeBuffer.getNext(edge);
    }
    
    if (testPoint != null && holes != null && holes.size() > 0) {
      // No holes, for test
      final GeoPolygon testPolygon = new GeoConcavePolygon(planetModel, points, null, internalEdges, isInternal);
      if (testPolygon.isWithin(testPoint)) {
        return false;
      }
    }
    
    final GeoPolygon realPolygon = new GeoConcavePolygon(planetModel, points, holes, internalEdges, isInternal);
    if (testPoint != null && (holes == null || holes.size() == 0)) {
      if (realPolygon.isWithin(testPoint)) {
        return false;
      }
    }
    
    rval.addShape(realPolygon);
    return true;
  }
  
  /** Look for a convex polygon at the specified edge.  If we find it, create one and adjust the edge buffer.
   * @param planetModel is the planet model.
   * @param currentEdge is the current edge to use starting the search.
   * @param rval is the composite polygon to build.
   * @param edgeBuffer is the edge buffer.
   * @param holes is the optional list of holes.
   * @param testPoint is the optional test point.
   * @return null if the testPoint is within any polygon detected, otherwise true if a convex polygon was created.
   */
  private static Boolean findConvexPolygon(final PlanetModel planetModel,
    final Edge currentEdge,
    final GeoCompositePolygon rval,
    final EdgeBuffer edgeBuffer,
    final List<GeoPolygon> holes,
    final GeoPoint testPoint) {
    
    //System.out.println("Looking at edge "+currentEdge+" with startpoint "+currentEdge.startPoint+" endpoint "+currentEdge.endPoint);
      
    // Initialize the structure.
    // We don't keep track of order here; we just care about membership.
    // The only exception is the head and tail pointers.
    final Set<Edge> includedEdges = new HashSet<>();
    includedEdges.add(currentEdge);
    Edge firstEdge = currentEdge;
    Edge lastEdge = currentEdge;
    
    // First, walk towards the end until we need to stop
    while (true) {
      if (firstEdge.startPoint == lastEdge.endPoint) {
        break;
      }
      final Edge newLastEdge = edgeBuffer.getNext(lastEdge);
      if (isWithin(newLastEdge.endPoint, includedEdges)) {
        //System.out.println(" maybe can extend to next edge");
        // Found a candidate for extension.  But do some other checks first.  Basically, we need to know if we construct a polygon
        // here will overlap with other remaining points?
        final SidedPlane returnBoundary;
        if (firstEdge.startPoint != newLastEdge.endPoint) {
          returnBoundary = new SidedPlane(firstEdge.endPoint, firstEdge.startPoint, newLastEdge.endPoint);
        } else {
          returnBoundary = null;
        }
        // The complete set of sided planes for the tentative new polygon include the ones in includedEdges, plus the one from newLastEdge,
        // plus the new tentative return boundary.  We have to make sure there are no points from elsewhere within the tentative convex polygon.
        boolean foundPointInside = false;
        final Iterator<Edge> edgeIterator = edgeBuffer.iterator();
        while (edgeIterator.hasNext()) {
          final Edge edge = edgeIterator.next();
          if (!includedEdges.contains(edge) && edge != newLastEdge) {
            // This edge has a point to check
            if (edge.startPoint != newLastEdge.endPoint) {
              // look at edge.startPoint
              if (isWithin(edge.startPoint, includedEdges, newLastEdge, returnBoundary)) {
                //System.out.println("  nope; point within found: "+edge.startPoint);
                foundPointInside = true;
                break;
              }
            }
            if (edge.endPoint != firstEdge.startPoint) {
              // look at edge.endPoint
              if (isWithin(edge.endPoint, includedEdges, newLastEdge, returnBoundary)) {
                //System.out.println("  nope; point within found: "+edge.endPoint);
                foundPointInside = true;
                break;
              }
            }
          }
        }
        
        if (!foundPointInside) {
          //System.out.println("  extending!");
          // Extend the polygon by the new last edge
          includedEdges.add(newLastEdge);
          lastEdge = newLastEdge;
          // continue extending in this direction
          continue;
        }
      }
      // We can't extend any more in this direction, so break from the loop.
      break;
    }
    
    // Now, walk towards the beginning until we need to stop
    while (true) {
      if (firstEdge.startPoint == lastEdge.endPoint) {
        break;
      }
      final Edge newFirstEdge = edgeBuffer.getPrevious(firstEdge);
      if (isWithin(newFirstEdge.startPoint, includedEdges)) {
        //System.out.println(" maybe can extend to previous edge");
        // Found a candidate for extension.  But do some other checks first.  Basically, we need to know if we construct a polygon
        // here will overlap with other remaining points?
        final SidedPlane returnBoundary;
        if (newFirstEdge.startPoint != lastEdge.endPoint) {
          returnBoundary = new SidedPlane(lastEdge.startPoint, lastEdge.endPoint, newFirstEdge.startPoint);
        } else {
          returnBoundary = null;
        }
        // The complete set of sided planes for the tentative new polygon include the ones in includedEdges, plus the one from newLastEdge,
        // plus the new tentative return boundary.  We have to make sure there are no points from elsewhere within the tentative convex polygon.
        boolean foundPointInside = false;
        final Iterator<Edge> edgeIterator = edgeBuffer.iterator();
        while (edgeIterator.hasNext()) {
          final Edge edge = edgeIterator.next();
          if (!includedEdges.contains(edge) && edge != newFirstEdge) {
            // This edge has a point to check
            if (edge.startPoint != lastEdge.endPoint) {
              // look at edge.startPoint
              if (isWithin(edge.startPoint, includedEdges, newFirstEdge, returnBoundary)) {
                //System.out.println("  nope; point within found: "+edge.startPoint);
                foundPointInside = true;
                break;
              }
            }
            if (edge.endPoint != newFirstEdge.startPoint) {
              // look at edge.endPoint
              if (isWithin(edge.endPoint, includedEdges, newFirstEdge, returnBoundary)) {
                //System.out.println("  nope; point within found: "+edge.endPoint);
                foundPointInside = true;
                break;
              }
            }
          }
        }
        
        if (!foundPointInside) {
          //System.out.println("  extending!");
          // Extend the polygon by the new last edge
          includedEdges.add(newFirstEdge);
          firstEdge = newFirstEdge;
          // continue extending in this direction
          continue;
        }
      }
      // We can't extend any more in this direction, so break from the loop.
      break;
    }

    // Ok, figure out what we've accumulated.  If it is enough for a polygon, build it.
    if (includedEdges.size() < 2) {
      //System.out.println("Done edge "+currentEdge+": no poly found");
      return false;
    }
    
    // It's enough to build a convex polygon
    //System.out.println("Edge "+currentEdge+": Found complex poly");
    
    // Create the point list and edge list, starting with the first edge and going to the last.  The return edge will be between
    // the start point of the first edge and the end point of the last edge.  If the first edge start point is the same as the last edge end point,
    // it's a degenerate case and we want to just clean out the edge buffer entirely.
    
    final List<GeoPoint> points = new ArrayList<GeoPoint>(includedEdges.size()+1);
    final BitSet internalEdges = new BitSet(includedEdges.size());
    final boolean returnIsInternal;
    
    if (firstEdge.startPoint == lastEdge.endPoint) {
      // Degenerate case!!  There is no return edge -- or rather, we already have it.
      if (includedEdges.size() < 3) {
        // This means we found a degenerate cycle of edges.  If we emit a polygon at this point it
        // has no contents, so we've clearly done something wrong, but not sure what.
        throw new IllegalArgumentException("polygon was illegal (degenerate illegal two-edge cyclical polygon encountered in processing)");
      }
      Edge edge = firstEdge;
      points.add(edge.startPoint);
      int i = 0;
      while (true) {
        if (edge == lastEdge) {
          break;
        }
        points.add(edge.endPoint);
        internalEdges.set(i++, edge.isInternal);
        edge = edgeBuffer.getNext(edge);
      }
      returnIsInternal = lastEdge.isInternal;
      edgeBuffer.clear();
    } else {
      // Build the return edge (internal, of course)
      final SidedPlane returnSidedPlane = new SidedPlane(firstEdge.endPoint, false, firstEdge.startPoint, lastEdge.endPoint);
      final Edge returnEdge = new Edge(firstEdge.startPoint, lastEdge.endPoint, returnSidedPlane, true);

      // Build point list and edge list
      final List<Edge> edges = new ArrayList<Edge>(includedEdges.size());
      returnIsInternal = true;

      Edge edge = firstEdge;
      points.add(edge.startPoint);
      int i = 0;
      while (true) {
        points.add(edge.endPoint);
        internalEdges.set(i++, edge.isInternal);
        edges.add(edge);
        if (edge == lastEdge) {
          break;
        }
        edge = edgeBuffer.getNext(edge);
      }
      
      // Modify the edge buffer
      edgeBuffer.replace(edges, returnEdge);
    }
    
    // Now, construct the polygon
    if (testPoint != null && holes != null && holes.size() > 0) {
      // No holes, for test
      final GeoPolygon testPolygon = new GeoConvexPolygon(planetModel, points, null, internalEdges, returnIsInternal);
      if (testPolygon.isWithin(testPoint)) {
        return null;
      }
    }
    
    final GeoPolygon realPolygon = new GeoConvexPolygon(planetModel, points, holes, internalEdges, returnIsInternal);
    if (testPoint != null && (holes == null || holes.size() == 0)) {
      if (realPolygon.isWithin(testPoint)) {
        return null;
      }
    }
    
    rval.addShape(realPolygon);
    return true;
  }
  
  /** Check if a point is within a set of edges.
    * @param point is the point
    * @param edgeSet is the set of edges
    * @param extension is the new edge
    * @param returnBoundary is the return edge
    * @return true if within
    */
  private static boolean isWithin(final GeoPoint point, final Set<Edge> edgeSet, final Edge extension, final SidedPlane returnBoundary) {
    if (!extension.plane.isWithin(point)) {
      return false;
    }
    if (returnBoundary != null && !returnBoundary.isWithin(point)) {
      return false;
    }
    return isWithin(point, edgeSet);
  }
  
  /** Check if a point is within a set of edges.
    * @param point is the point
    * @param edgeSet is the set of edges
    * @return true if within
    */
  private static boolean isWithin(final GeoPoint point, final Set<Edge> edgeSet) {
    for (final Edge edge : edgeSet) {
      if (!edge.plane.isWithin(point)) {
        return false;
      }
    }
    return true;
  }
  
  /** Convert raw point index into valid array position.
   *@param index is the array index.
   *@param size is the array size.
   *@return an updated index.
   */
  private static int getLegalIndex(int index, int size) {
    while (index < 0) {
      index += size;
    }
    while (index >= size) {
      index -= size;
    }
    return index;
  }

  /** Class representing a single (unused) edge.
   */
  private static class Edge {
    /** Plane */
    public final SidedPlane plane;
    /** Start point */
    public final GeoPoint startPoint;
    /** End point */
    public final GeoPoint endPoint;
    /** Internal edge flag */
    public final boolean isInternal;

    /** Constructor.
      * @param startPoint the edge start point
      * @param endPoint the edge end point
      * @param plane the edge plane
      * @param isInternal true if internal edge
      */
    public Edge(final GeoPoint startPoint, final GeoPoint endPoint, final SidedPlane plane, final boolean isInternal) {
      this.startPoint = startPoint;
      this.endPoint = endPoint;
      this.plane = plane;
      this.isInternal = isInternal;
    }
    
    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
    
    @Override
    public boolean equals(final Object o) {
      return o == this;
    }
  }
  
  /** Class representing an iterator over an EdgeBuffer.
   */
  private static class EdgeBufferIterator implements Iterator<Edge> {
    /** Edge buffer */
    protected final EdgeBuffer edgeBuffer;
    /** First edge */
    protected final Edge firstEdge;
    /** Current edge */
    protected Edge currentEdge;
    
    /** Constructor.
      * @param edgeBuffer the edge buffer
      */
    public EdgeBufferIterator(final EdgeBuffer edgeBuffer) {
      this.edgeBuffer = edgeBuffer;
      this.currentEdge = edgeBuffer.pickOne();
      this.firstEdge = currentEdge;
    }
    
    @Override
    public boolean hasNext() {
      return currentEdge != null;
    }
    
    @Override
    public Edge next() {
      final Edge rval = currentEdge;
      if (currentEdge != null) {
        currentEdge = edgeBuffer.getNext(currentEdge);
        if (currentEdge == firstEdge) {
          currentEdge = null;
        }
      }
      return rval;
    }
    
    @Override
    public void remove() {
      throw new RuntimeException("Unsupported operation");
    }
  }
  
  /** Class representing a pool of unused edges, all linked together by vertices.
   */
  private static class EdgeBuffer {
    /** Starting edge */
    protected Edge oneEdge;
    /** Full set of edges */
    protected final Set<Edge> edges = new HashSet<>();
    /** Map to previous edge */
    protected final Map<Edge, Edge> previousEdges = new HashMap<>();
    /** Map to next edge */
    protected final Map<Edge, Edge> nextEdges = new HashMap<>();

    /** Constructor.
      * @param pointList is the list of points.
      * @param internalEdges is the list of edges that are internal (includes return edge)
      * @param startPlaneStartIndex is the index of the startPlane's starting point
      * @param startPlaneEndIndex is the index of the startPlane's ending point
      * @param startPlane is the starting plane
      */
    public EdgeBuffer(final List<GeoPoint> pointList, final BitSet internalEdges, final int startPlaneStartIndex, final int startPlaneEndIndex, final SidedPlane startPlane) {
      /*
      System.out.println("Initial points:");
      for (final GeoPoint p : pointList) {
        System.out.println(" "+p);
      }
      */

      // We need to detect backtracks, and also situations where someone has tried to stitch together multiple segments into one long arc (> 180 degrees).
      // To do this, every time we extend by a coplanar segment, we compute the total arc distance to the new endpoint, as
      // well as a sum of the arc distances we've accumulated as we march forward.  If these two numbers disagree, then
      // we know there has been a backtrack or other anomaly.
      
      // extend the edge, we compute the distance along the 
      final Edge startEdge = new Edge(pointList.get(startPlaneStartIndex), pointList.get(startPlaneEndIndex), startPlane, internalEdges.get(startPlaneStartIndex));
      // Fill in the EdgeBuffer by walking around creating more stuff
      Edge currentEdge = startEdge;
      int startIndex = startPlaneStartIndex;
      int endIndex = startPlaneEndIndex;
      while (true) {
        // Compute the next edge
        startIndex = endIndex;
        endIndex++;
        if (endIndex >= pointList.size()) {
          endIndex -= pointList.size();
        }
        // Get the next point
        final GeoPoint newPoint = pointList.get(endIndex);
        // Build the new edge
        // We have to be sure that the point we use as a check does not lie on the plane.
        // In order to meet that goal, we need to go hunting for a point that meets the criteria.  If we don't
        // find one, we've got a linear "polygon" that we cannot use.
        
        // We need to know the sidedness of the new plane.  The point we're going to be presenting to it has
        // a certain relationship with the sided plane we already have for the current edge.  If the current edge
        // is colinear with the new edge, then we want to maintain the same relationship.  If the new edge
        // is not colinear, then we can use the new point's relationship with the current edge as our guide.
        
        final boolean isNewPointWithin;
        final GeoPoint pointToPresent;
        if (currentEdge.plane.evaluateIsZero(newPoint)) {
          // The new point is colinear with the current edge.  We'll have to look backwards for the first point that isn't.
          int checkPointIndex = -1;
          // Compute the arc distance before we try to extend, so that we note backtracking when we see it
          double accumulatedDistance = newPoint.arcDistance(pointList.get(startIndex));
          final Plane checkPlane = new Plane(pointList.get(startIndex), newPoint);
          for (int i = 0; i < pointList.size(); i++) {
            final int index = getLegalIndex(startIndex - 1 - i, pointList.size());
            if (!checkPlane.evaluateIsZero(pointList.get(index))) {
              checkPointIndex = index;
              break;
            } else {
              accumulatedDistance += pointList.get(getLegalIndex(index+1, pointList.size())).arcDistance(pointList.get(index));
              final double actualDistance = newPoint.arcDistance(pointList.get(index));
              if (Math.abs(actualDistance - accumulatedDistance) >= Vector.MINIMUM_RESOLUTION) {
                throw new IllegalArgumentException("polygon backtracks over itself");
              }
            }
          }
          if (checkPointIndex == -1) {
            throw new IllegalArgumentException("polygon is illegal (linear)");
          }
          pointToPresent = pointList.get(checkPointIndex);
          isNewPointWithin = currentEdge.plane.isWithin(pointToPresent);
        } else {
          isNewPointWithin = currentEdge.plane.isWithin(newPoint);
          pointToPresent = currentEdge.startPoint;
        }

        final SidedPlane newPlane = new SidedPlane(pointToPresent, isNewPointWithin, pointList.get(startIndex), newPoint);
        /*
        System.out.println("For next plane, the following points are in/out:");
        for (final GeoPoint p: pointList) {
          System.out.println(" "+p+" is: "+(newPlane.isWithin(p)?"in":"out"));
        }
        */
        final Edge newEdge = new Edge(pointList.get(startIndex), pointList.get(endIndex), newPlane, internalEdges.get(startIndex));
        
        // Link it in
        previousEdges.put(newEdge, currentEdge);
        nextEdges.put(currentEdge, newEdge);
        edges.add(newEdge);
        currentEdge = newEdge;

        if (currentEdge.endPoint == startEdge.startPoint) {
          // We finish here.  Link the current edge to the start edge, and exit
          previousEdges.put(startEdge, currentEdge);
          nextEdges.put(currentEdge, startEdge);
          edges.add(startEdge);
          break;
        }
      }
      
      oneEdge = startEdge;
      
      // Verify the structure. 
      //verify();
    }

    /*
    protected void verify() {
      if (edges.size() != previousEdges.size() || edges.size() != nextEdges.size()) {
        throw new IllegalStateException("broken structure");
      }
      // Confirm each edge
      for (final Edge e : edges) {
        final Edge previousEdge = getPrevious(e);
        final Edge nextEdge = getNext(e);
        if (e.endPoint != nextEdge.startPoint) {
          throw new IllegalStateException("broken structure");
        }
        if (e.startPoint != previousEdge.endPoint) {
          throw new IllegalStateException("broken structure");
        }
        if (getNext(previousEdge) != e) {
          throw new IllegalStateException("broken structure");
        }
        if (getPrevious(nextEdge) != e) {
          throw new IllegalStateException("broken structure");
        }
      }
      if (oneEdge != null && !edges.contains(oneEdge)) {
        throw new IllegalStateException("broken structure");
      }
      if (oneEdge == null && edges.size() > 0) {
        throw new IllegalStateException("broken structure");
      }
    }
    */
    
    /** Get the previous edge.
      * @param currentEdge is the current edge.
      * @return the previous edge, if found.
      */
    public Edge getPrevious(final Edge currentEdge) {
      return previousEdges.get(currentEdge);
    }
    
    /** Get the next edge.
      * @param currentEdge is the current edge.
      * @return the next edge, if found.
      */
    public Edge getNext(final Edge currentEdge) {
      return nextEdges.get(currentEdge);
    }
    
    /** Replace a list of edges with a new edge.
      * @param removeList is the list of edges to remove.
      * @param newEdge is the edge to add.
      */
    public void replace(final List<Edge> removeList, final Edge newEdge) {
      /*
      System.out.println("Replacing: ");
      for (final Edge e : removeList) {
        System.out.println(" "+e.startPoint+"-->"+e.endPoint);
      }
      System.out.println("...with: "+newEdge.startPoint+"-->"+newEdge.endPoint);
      */
      final Edge previous = previousEdges.get(removeList.get(0));
      final Edge next = nextEdges.get(removeList.get(removeList.size()-1));
      edges.add(newEdge);
      previousEdges.put(newEdge, previous);
      nextEdges.put(previous, newEdge);
      previousEdges.put(next, newEdge);
      nextEdges.put(newEdge, next);
      for (final Edge edge : removeList) {
        if (edge == oneEdge) {
          oneEdge = newEdge;
        }
        edges.remove(edge);
        previousEdges.remove(edge);
        nextEdges.remove(edge);
      }
      //verify();
    }

    /** Clear all edges.
      */
    public void clear() {
      edges.clear();
      previousEdges.clear();
      nextEdges.clear();
      oneEdge = null;
    }
    
    /** Get the size of the edge buffer.
      * @return the size.
      */
    public int size() {
      return edges.size();
    }
    
    /** Get an iterator to iterate over edges.
      * @return the iterator.
      */
    public Iterator<Edge> iterator() {
      return new EdgeBufferIterator(this);
    }
    
    /** Return a first edge.
      * @return the edge.
      */
    public Edge pickOne() {
      return oneEdge;
    }
    
  }
  
}
