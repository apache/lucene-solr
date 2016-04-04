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
    return buildPolygonShape(planetModel, pointList, convexPointIndex, getLegalIndex(convexPointIndex + 1, pointList.size()),
        new SidedPlane(pointList.get(getLegalIndex(convexPointIndex - 1, pointList.size())),
            pointList.get(convexPointIndex), pointList.get(getLegalIndex(convexPointIndex + 1, pointList.size()))),
        false,
        holes,
        null);
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
    while (true) {
      // Pick the next random pole
      final double poleLat = generator.nextDouble() * Math.PI - Math.PI * 0.5;
      final double poleLon = generator.nextDouble() * Math.PI * 2.0 - Math.PI;
      final GeoPoint pole = new GeoPoint(planetModel, poleLat, poleLon);
      // Is it inside or outside?
      final Boolean isPoleInside = isInsidePolygon(pole, pointList);
      if (isPoleInside != null) {
        // Legal pole
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
    // We will be trying twice to find the right GeoPolygon, using alternate siding choices for the first polygon
    // side.  While this looks like it might be 2x as expensive as it could be, there's really no other choice I can
    // find.
    final SidedPlane initialPlane = new SidedPlane(testPoint, pointList.get(0), pointList.get(1));
    // We don't know if this is the correct siding choice.  We will only know as we build the complex polygon.
    // So we need to be prepared to try both possibilities.
    final GeoPolygon trial = buildPolygonShape(planetModel, pointList, 0, 1, initialPlane, false, holes, testPoint);
    if (trial == null) {
      // The testPoint was within the shape.  Was that intended?
      if (testPointInside) {
        // Yes: build it for real
        return buildPolygonShape(planetModel, pointList, 0, 1, initialPlane, false, holes, null);
      }
      // No: do the complement and return that.
      return buildPolygonShape(planetModel, pointList, 0, 1, new SidedPlane(initialPlane), false, holes, null);
    } else {
      // The testPoint was outside the shape.  Was that intended?
      if (!testPointInside) {
        // Yes: return what we just built
        return trial;
      }
      // No: return the complement
      return buildPolygonShape(planetModel, pointList, 0, 1, new SidedPlane(initialPlane), false, holes, null);
    }
  }

  /** For a specified point and a list of poly points, determine based on point order whether the
   * point should be considered in or out of the polygon.
   * @param point is the point to check.
   * @param polyPoints is the list of points comprising the polygon.
   * @return null if the point is illegal, otherwise false if the point is inside and true if the point is outside
   * of the polygon.
   */
  protected static Boolean isInsidePolygon(final GeoPoint point, final List<GeoPoint> polyPoints) {
    // First, compute sine and cosine of pole point latitude and longitude
    final double norm = 1.0 / point.magnitude();
    final double xyDenom = Math.sqrt(point.x * point.x + point.y * point.y);
    final double sinLatitude = point.z * norm;
    final double cosLatitude = xyDenom * norm;
    final double sinLongitude;
    final double cosLongitude;
    if (Math.abs(xyDenom) < Vector.MINIMUM_RESOLUTION) {
      sinLongitude = 0.0;
      cosLongitude = 1.0;
    } else {
      final double xyNorm = 1.0 / xyDenom;
      sinLongitude = point.y * xyNorm;
      cosLongitude = point.x * xyNorm;
    }
    
    // Now, compute the incremental arc distance around the points of the polygon
    double arcDistance = 0.0;
    Double prevAngle = null;
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
    }
    // Clockwise == inside == negative
    //System.out.println("Arcdistance = "+arcDistance);
    if (Math.abs(arcDistance) < Vector.MINIMUM_RESOLUTION) {
      // No idea what direction, so try another pole.
      return null;
    }
    return arcDistance > 0.0;
  }
  
  protected static Double computeAngle(final GeoPoint point,
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
    // So:
    // x1 = x0 cos az - y0 sin az
    // y1 = x0 sin az + y0 cos az
    // z1 = z0
    // x2 = x1 cos al - z1 sin al
    // y2 = y1
    // z2 = x1 sin al + z1 cos al
      
    final double x1 = point.x * cosLongitude - point.y * sinLongitude;
    final double y1 = point.x * sinLongitude + point.y * cosLongitude;
    final double z1 = point.z;
    //final double x2 = x1 * cosLatitude - z1 * sinLatitude;
    final double y2 = y1;
    final double z2 = x1 * sinLatitude + z1 * cosLatitude;
      
    // Now we should be looking down the X axis; the original point has rotated coordinates (N, 0, 0).
    // So we can just compute the angle using y2 and z2.  (If Math.sqrt(y2*y2 + z2 * z2) is 0.0, then the point is on the pole and we need another one).
    if (Math.sqrt(y2*y2 + z2*z2) < Vector.MINIMUM_RESOLUTION) {
      return null;
    }
    
    return Math.atan2(z2, y2);
  }

  /** Build a GeoPolygon out of one concave part and multiple convex parts given points, starting edge, and whether starting edge is internal or not.
   * @param pointsList        is a list of the GeoPoints to build an arbitrary polygon out of.
   * @param startPointIndex is the first of the points, constituting the starting edge.
   * @param startingEdge is the plane describing the starting edge.
   * @param isInternalEdge is true if the specified edge is an internal one.
   * @param holes is the list of holes in the polygon, or null if none.
   * @param testPoint is an (optional) test point, which will be used to determine if we are generating
   *  a shape with the proper sidedness.  It is passed in only when the test point is supposed to be outside
   *  of the generated polygon.  In this case, if the generated polygon is found to contain the point, the
   *  method exits early with a null return value.
   *  This only makes sense in the context of evaluating both possible choices and using logic to determine
   *  which result to use.  If the test point is supposed to be within the shape, then it must be outside of the
   *  complement shape.  If the test point is supposed to be outside the shape, then it must be outside of the
   *  original shape.  Either way, we can figure out the right thing to use.
   * @return a GeoMembershipShape corresponding to what was specified, or null if what was specified
   *  was inconsistent with what we generated.  Specifically, if we specify an exterior point that is
   *  found in the interior of the shape we create here we return null, which is a signal that we chose
   *  our initial plane sidedness backwards.
   */
  public static GeoPolygon buildPolygonShape(
    final PlanetModel planetModel,
    final List<GeoPoint> pointsList,
    final int startPointIndex,
    final int endPointIndex,
    final SidedPlane startingEdge,
    final boolean isInternalEdge,
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
    final EdgeBuffer edgeBuffer = new EdgeBuffer(pointsList, startPointIndex, endPointIndex, startingEdge, isInternalEdge);

    // Current composite.  This is what we'll actually be returning.  This will have a number of convex polygons, and
    // maybe a single concave one too.
    final GeoCompositePolygon rval = new GeoCompositePolygon();

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
        return null;
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
    
    // If there's anything left in the edge buffer, convert to concave polygon.
    if (makeConcavePolygon(planetModel, rval, edgeBuffer, holes, testPoint) == false) {
      return null;
    }
    
    return rval;
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
  protected static boolean makeConcavePolygon(final PlanetModel planetModel,
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

    Edge edge = edgeBuffer.pickOne();
    boolean isInternal = false;
    for (int i = 0; i < edgeBuffer.size(); i++) {
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
  protected static Boolean findConvexPolygon(final PlanetModel planetModel,
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
    
    final List<GeoPoint> points = new ArrayList<GeoPoint>(includedEdges.size());
    final BitSet internalEdges = new BitSet(includedEdges.size()-1);
    final boolean returnIsInternal;
    
    if (firstEdge.startPoint == lastEdge.endPoint) {
      // Degenerate case!!  There is no return edge -- or rather, we already have it.
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
  
  protected static boolean isWithin(final GeoPoint point, final Set<Edge> edgeSet, final Edge extension, final SidedPlane returnBoundary) {
    if (!extension.plane.isWithin(point)) {
      return false;
    }
    if (returnBoundary != null && !returnBoundary.isWithin(point)) {
      return false;
    }
    return isWithin(point, edgeSet);
  }
  
  protected static boolean isWithin(final GeoPoint point, final Set<Edge> edgeSet) {
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
  protected static int getLegalIndex(int index, int size) {
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
  protected static class Edge {
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
  protected static class EdgeBufferIterator implements Iterator<Edge> {
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
  protected static class EdgeBuffer {
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
      * @param startPlaneStartIndex is the index of the startPlane's starting point
      * @param startPlaneEndIndex is the index of the startPlane's ending point
      * @param startPlane is the starting plane
      * @param startPlaneIsInternal signals whether the startPlane is an internal edge
      */
    public EdgeBuffer(final List<GeoPoint> pointList, final int startPlaneStartIndex, final int startPlaneEndIndex, final SidedPlane startPlane, final boolean startPlaneIsInternal) {
      /*
      System.out.println("Initial points:");
      for (final GeoPoint p : pointList) {
        System.out.println(" "+p);
      }
      System.out.println("For start plane, the following points are in/out:");
      for (final GeoPoint p: pointList) {
        System.out.println(" "+p+" is: "+(startPlane.isWithin(p)?"in":"out"));
      }
      */
      
      final Edge startEdge = new Edge(pointList.get(startPlaneStartIndex), pointList.get(startPlaneEndIndex), startPlane, startPlaneIsInternal);
      // Fill in the EdgeBuffer by walking around creating more stuff
      Edge currentEdge = startEdge;
      int startIndex = startPlaneStartIndex;
      int endIndex = startPlaneEndIndex;
      boolean isInternal = startPlaneIsInternal;
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
        final boolean isNewPointWithin = currentEdge.plane.isWithin(newPoint);
        final SidedPlane newPlane = new SidedPlane(currentEdge.startPoint, isNewPointWithin, pointList.get(startIndex), newPoint);
        /*
        System.out.println("For next plane, the following points are in/out:");
        for (final GeoPoint p: pointList) {
          System.out.println(" "+p+" is: "+(newPlane.isWithin(p)?"in":"out"));
        }
        */
        final Edge newEdge = new Edge(pointList.get(startIndex), pointList.get(endIndex), newPlane, false);
        
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
