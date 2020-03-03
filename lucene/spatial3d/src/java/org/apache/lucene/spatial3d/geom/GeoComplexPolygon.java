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

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * GeoComplexPolygon objects are structures designed to handle very large numbers of edges.
 * They perform very well in this case compared to the alternatives, which all have O(N) evaluation
 * and O(N^2) setup times.  Complex polygons have O(N) setup times and best case O(log(N))
 * evaluation times.
 *
 * The tradeoff is that these objects perform object creation when evaluating intersects() and
 * isWithin().
 *
 * @lucene.internal
 */
class GeoComplexPolygon extends GeoBasePolygon {
  
  private final Tree xTree;
  private final Tree yTree;
  private final Tree zTree;
  
  private final List<List<GeoPoint>> pointsList;
  
  private final boolean testPoint1InSet;
  private final GeoPoint testPoint1;

  private final Plane testPoint1FixedYPlane;
  private final Plane testPoint1FixedYAbovePlane;
  private final Plane testPoint1FixedYBelowPlane;
  private final Plane testPoint1FixedXPlane;
  private final Plane testPoint1FixedXAbovePlane;
  private final Plane testPoint1FixedXBelowPlane;
  private final Plane testPoint1FixedZPlane;
  private final Plane testPoint1FixedZAbovePlane;
  private final Plane testPoint1FixedZBelowPlane;

  private final GeoPoint[] edgePoints;
  private final Edge[] shapeStartEdges;
  
  private final static double NEAR_EDGE_CUTOFF = -Vector.MINIMUM_RESOLUTION * 10000.0;
  
  /**
   * Create a complex polygon from multiple lists of points, and a single point which is known to be in or out of
   * set.
   *@param planetModel is the planet model.
   *@param pointsList is the list of lists of edge points.  The edge points describe edges, and have an implied
   *  return boundary, so that N edges require N points.  These points have furthermore been filtered so that
   *  no adjacent points are identical (within the bounds of the definition used by this package).  It is assumed
   *  that no edges intersect, but the structure can contain both outer rings as well as holes.
   *@param testPoint is the point whose in/out of setness is known.
   *@param testPointInSet is true if the test point is considered "within" the polygon.
   */
  public GeoComplexPolygon(final PlanetModel planetModel, final List<List<GeoPoint>> pointsList, final GeoPoint testPoint, final boolean testPointInSet) {
    super(planetModel);
    
    assert planetModel.pointOnSurface(testPoint.x, testPoint.y, testPoint.z) : "Test point is not on the ellipsoid surface";
    
    this.pointsList = pointsList;  // For serialization

    // Construct and index edges
    this.edgePoints = new GeoPoint[pointsList.size()];
    this.shapeStartEdges = new Edge[pointsList.size()];
    final ArrayList<Edge> allEdges = new ArrayList<>();
    int edgePointIndex = 0;
    for (final List<GeoPoint> shapePoints : pointsList) {
      allEdges.ensureCapacity(allEdges.size() + shapePoints.size());
      GeoPoint lastGeoPoint = shapePoints.get(shapePoints.size()-1);
      edgePoints[edgePointIndex] = lastGeoPoint;
      Edge lastEdge = null;
      Edge firstEdge = null;
      for (final GeoPoint thisGeoPoint : shapePoints) {
        assert planetModel.pointOnSurface(thisGeoPoint) : "Polygon edge point must be on surface; "+thisGeoPoint+" is not";
        final Edge edge = new Edge(planetModel, lastGeoPoint, thisGeoPoint);
        if (edge.isWithin(testPoint.x, testPoint.y, testPoint.z)) {
          throw new IllegalArgumentException("Test point is on polygon edge: not allowed");
        }
        allEdges.add(edge);
        // Now, link
        if (firstEdge == null) {
          firstEdge = edge;
        }
        if (lastEdge != null) {
          lastEdge.next = edge;
          edge.previous = lastEdge;
        }
        lastEdge = edge;
        lastGeoPoint = thisGeoPoint;
      }
      firstEdge.previous = lastEdge;
      lastEdge.next = firstEdge;
      shapeStartEdges[edgePointIndex] = firstEdge;
      edgePointIndex++;
    }

    xTree = new XTree(allEdges);
    yTree = new YTree(allEdges);
    zTree = new ZTree(allEdges);

    // Record testPoint1 as-is
    this.testPoint1 = testPoint;

    // Construct fixed planes for testPoint1
    this.testPoint1FixedYPlane = new Plane(0.0, 1.0, 0.0, -testPoint1.y);
    this.testPoint1FixedXPlane = new Plane(1.0, 0.0, 0.0, -testPoint1.x);
    this.testPoint1FixedZPlane = new Plane(0.0, 0.0, 1.0, -testPoint1.z);
    
    Plane testPoint1FixedYAbovePlane = new Plane(testPoint1FixedYPlane, true);
    
    // We compare the plane's Y value (etc), which is -D, with the planet's maximum and minimum Y poles.
    
    if (-testPoint1FixedYAbovePlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumYValue() + testPoint1FixedYAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedYAbovePlane = null;
    }
    this.testPoint1FixedYAbovePlane = testPoint1FixedYAbovePlane;
    
    Plane testPoint1FixedYBelowPlane = new Plane(testPoint1FixedYPlane, false);
    if (-testPoint1FixedYBelowPlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF ||  planetModel.getMinimumYValue() + testPoint1FixedYBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedYBelowPlane = null;
    }
    this.testPoint1FixedYBelowPlane = testPoint1FixedYBelowPlane;
    
    Plane testPoint1FixedXAbovePlane = new Plane(testPoint1FixedXPlane, true);
    if (-testPoint1FixedXAbovePlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() + testPoint1FixedXAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedXAbovePlane = null;
    }
    this.testPoint1FixedXAbovePlane = testPoint1FixedXAbovePlane;
    
    Plane testPoint1FixedXBelowPlane = new Plane(testPoint1FixedXPlane, false);
    if (-testPoint1FixedXBelowPlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() + testPoint1FixedXBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedXBelowPlane = null;
    }
    this.testPoint1FixedXBelowPlane = testPoint1FixedXBelowPlane;
    
    Plane testPoint1FixedZAbovePlane = new Plane(testPoint1FixedZPlane, true);
    if (-testPoint1FixedZAbovePlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF ||planetModel.getMinimumZValue() + testPoint1FixedZAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedZAbovePlane = null;
    }
    this.testPoint1FixedZAbovePlane = testPoint1FixedZAbovePlane;
    
    Plane testPoint1FixedZBelowPlane = new Plane(testPoint1FixedZPlane, false);
    if (-testPoint1FixedZBelowPlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumZValue() + testPoint1FixedZBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedZBelowPlane = null;
    }
    this.testPoint1FixedZBelowPlane = testPoint1FixedZBelowPlane;

    // We know inset/out-of-set for testPoint1 only right now
    this.testPoint1InSet = testPointInSet;
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoComplexPolygon(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    this(planetModel, 
      readPointsList(planetModel, inputStream),
      new GeoPoint(planetModel, inputStream),
      SerializableObject.readBoolean(inputStream));
  }

  private static List<List<GeoPoint>> readPointsList(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
    final int count = SerializableObject.readInt(inputStream);
    final List<List<GeoPoint>> array = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      array.add(java.util.Arrays.asList(SerializableObject.readPointArray(planetModel, inputStream)));
    }
    return array;
  }
  
  @Override
  public void write(final OutputStream outputStream) throws IOException {
    writePointsList(outputStream, pointsList);
    testPoint1.write(outputStream);
    SerializableObject.writeBoolean(outputStream, testPoint1InSet);
  }

  private static void writePointsList(final OutputStream outputStream, final List<List<GeoPoint>> pointsList) throws IOException {
    SerializableObject.writeInt(outputStream, pointsList.size());
    for (final List<GeoPoint> points : pointsList) {
      SerializableObject.writePointArray(outputStream, points);
    }
  }
  
  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    //System.out.println("IsWithin() for ["+x+","+y+","+z+"]");
    return isInSet(x, y, z,
      testPoint1,
      testPoint1InSet,
      testPoint1FixedXPlane, testPoint1FixedXAbovePlane, testPoint1FixedXBelowPlane,
      testPoint1FixedYPlane, testPoint1FixedYAbovePlane, testPoint1FixedYBelowPlane,
      testPoint1FixedZPlane, testPoint1FixedZAbovePlane, testPoint1FixedZBelowPlane);
  }
  
  /** Given a test point, whether it is in set, and the associated planes, figure out if another point
    * is in set or not.
    */
  private boolean isInSet(final double x, final double y, final double z,
    final GeoPoint testPoint,
    final boolean testPointInSet,
    final Plane testPointFixedXPlane, final Plane testPointFixedXAbovePlane, final Plane testPointFixedXBelowPlane,
    final Plane testPointFixedYPlane, final Plane testPointFixedYAbovePlane, final Plane testPointFixedYBelowPlane,
    final Plane testPointFixedZPlane, final Plane testPointFixedZAbovePlane, final Plane testPointFixedZBelowPlane) {

    //System.out.println("\nIsInSet called for ["+x+","+y+","+z+"], testPoint="+testPoint+"; is in set? "+testPointInSet);
    // If we're right on top of the point, we know the answer.
    if (testPoint.isNumericallyIdentical(x, y, z)) {
      return testPointInSet;
    }
    
    // If we're right on top of any of the test planes, we navigate solely on that plane.
    if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && testPointFixedYPlane.evaluateIsZero(x, y, z)) {
      // Use the XZ plane exclusively.
      //System.out.println(" Using XZ plane alone");
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPoint, testPointFixedYPlane, testPointFixedYAbovePlane, testPointFixedYBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the y tree because that's fixed.
      yTree.traverse(crossingEdgeIterator, testPoint.y);
      return crossingEdgeIterator.isOnEdge() || (((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet);
    } else if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && testPointFixedXPlane.evaluateIsZero(x, y, z)) {
      // Use the YZ plane exclusively.
      //System.out.println(" Using YZ plane alone");
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPoint, testPointFixedXPlane, testPointFixedXAbovePlane, testPointFixedXBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the x tree because that's fixed.
      xTree.traverse(crossingEdgeIterator, testPoint.x);
      return crossingEdgeIterator.isOnEdge() || (((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet);
    } else if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && testPointFixedZPlane.evaluateIsZero(x, y, z)) {
      //System.out.println(" Using XY plane alone");
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPoint, testPointFixedZPlane, testPointFixedZAbovePlane, testPointFixedZBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the z tree because that's fixed.
      zTree.traverse(crossingEdgeIterator, testPoint.z);
      return crossingEdgeIterator.isOnEdge() || (((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet);
    } else {
      //System.out.println(" Using two planes");
      // This is the expensive part!!
      // Changing the code below has an enormous impact on the queries per second we see with the benchmark.
      
      // We need to use two planes to get there.  We don't know which two planes will do it but we can figure it out.
      final Plane travelPlaneFixedX = new Plane(1.0, 0.0, 0.0, -x);
      final Plane travelPlaneFixedY = new Plane(0.0, 1.0, 0.0, -y);
      final Plane travelPlaneFixedZ = new Plane(0.0, 0.0, 1.0, -z);

      Plane fixedYAbovePlane = new Plane(travelPlaneFixedY, true);
      if (-fixedYAbovePlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumYValue() + fixedYAbovePlane.D > NEAR_EDGE_CUTOFF) {
          fixedYAbovePlane = null;
      }
      
      Plane fixedYBelowPlane = new Plane(travelPlaneFixedY, false);
      if (-fixedYBelowPlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumYValue() + fixedYBelowPlane.D > NEAR_EDGE_CUTOFF) {
          fixedYBelowPlane = null;
      }
      
      Plane fixedXAbovePlane = new Plane(travelPlaneFixedX, true);
      if (-fixedXAbovePlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() + fixedXAbovePlane.D > NEAR_EDGE_CUTOFF) {
          fixedXAbovePlane = null;
      }
      
      Plane fixedXBelowPlane = new Plane(travelPlaneFixedX, false);
      if (-fixedXBelowPlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() + fixedXBelowPlane.D > NEAR_EDGE_CUTOFF) {
          fixedXBelowPlane = null;
      }
      
      Plane fixedZAbovePlane = new Plane(travelPlaneFixedZ, true);
      if (-fixedZAbovePlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumZValue() + fixedZAbovePlane.D > NEAR_EDGE_CUTOFF) {
          fixedZAbovePlane = null;
      }
      
      Plane fixedZBelowPlane = new Plane(travelPlaneFixedZ, false);
      if (-fixedZBelowPlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumZValue() + fixedZBelowPlane.D > NEAR_EDGE_CUTOFF) {
          fixedZBelowPlane = null;
      }

      // Find the intersection points for each one of these and the complementary test point planes.

      final List<TraversalStrategy> traversalStrategies = new ArrayList<>(12);
      
      if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && fixedXAbovePlane != null && fixedXBelowPlane != null) {
        //check if planes intersects  inside world
        final double checkAbove = 4.0 * (fixedXAbovePlane.D * fixedXAbovePlane.D * planetModel.inverseXYScalingSquared + testPointFixedYAbovePlane.D * testPointFixedYAbovePlane.D * planetModel.inverseXYScalingSquared - 1.0);
        final double checkBelow = 4.0 * (fixedXBelowPlane.D * fixedXBelowPlane.D * planetModel.inverseXYScalingSquared + testPointFixedYBelowPlane.D * testPointFixedYBelowPlane.D * planetModel.inverseXYScalingSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
          //System.out.println("  Looking for intersections between travel and test point planes...");
          final GeoPoint[] XIntersectionsY = travelPlaneFixedX.findIntersections(planetModel, testPointFixedYPlane);
          for (final GeoPoint p : XIntersectionsY) {
            // Travel would be in YZ plane (fixed x) then in XZ (fixed y)
            // We compute distance we need to travel as a placeholder for the number of intersections we might encounter.
            //final double newDistance = p.arcDistance(testPoint) + p.arcDistance(thePoint);
            final double tpDelta1 = testPoint.x - p.x;
            final double tpDelta2 = testPoint.z - p.z;
            final double cpDelta1 = y - p.y;
            final double cpDelta2 = z - p.z;
            final double newDistance = tpDelta1 * tpDelta1 + tpDelta2 * tpDelta2 + cpDelta1 * cpDelta1 + cpDelta2 * cpDelta2;
            //final double newDistance = (testPoint.x - p.x) * (testPoint.x - p.x) + (testPoint.z - p.z) * (testPoint.z - p.z)  + (thePoint.y - p.y) * (thePoint.y - p.y) + (thePoint.z - p.z) * (thePoint.z - p.z);
            //final double newDistance = Math.abs(testPoint.x - p.x) + Math.abs(thePoint.y - p.y);
            traversalStrategies.add(new TraversalStrategy(newDistance, testPoint.y, x,
              testPointFixedYPlane, testPointFixedYAbovePlane, testPointFixedYBelowPlane,
              travelPlaneFixedX, fixedXAbovePlane, fixedXBelowPlane,
              yTree, xTree, p));
          }
        }
      }
      if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && fixedXAbovePlane != null && fixedXBelowPlane != null) {
        //check if planes intersects  inside world
        final double checkAbove = 4.0 * (fixedXAbovePlane.D * fixedXAbovePlane.D * planetModel.inverseXYScalingSquared + testPointFixedZAbovePlane.D * testPointFixedZAbovePlane.D * planetModel.inverseZScalingSquared - 1.0);
        final double checkBelow = 4.0 * (fixedXBelowPlane.D * fixedXBelowPlane.D * planetModel.inverseXYScalingSquared + testPointFixedZBelowPlane.D * testPointFixedZBelowPlane.D * planetModel.inverseZScalingSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
          //System.out.println("  Looking for intersections between travel and test point planes...");
          final GeoPoint[] XIntersectionsZ = travelPlaneFixedX.findIntersections(planetModel, testPointFixedZPlane);
          for (final GeoPoint p : XIntersectionsZ) {
            // Travel would be in YZ plane (fixed x) then in XY (fixed z)
            //final double newDistance = p.arcDistance(testPoint) + p.arcDistance(thePoint);
            final double tpDelta1 = testPoint.x - p.x;
            final double tpDelta2 = testPoint.y - p.y;
            final double cpDelta1 = y - p.y;
            final double cpDelta2 = z - p.z;
            final double newDistance = tpDelta1 * tpDelta1 + tpDelta2 * tpDelta2 + cpDelta1 * cpDelta1 + cpDelta2 * cpDelta2;
            //final double newDistance = (testPoint.x - p.x) * (testPoint.x - p.x) + (testPoint.y - p.y) * (testPoint.y - p.y)  + (thePoint.y - p.y) * (thePoint.y - p.y) + (thePoint.z - p.z) * (thePoint.z - p.z);
            //final double newDistance = Math.abs(testPoint.x - p.x) + Math.abs(thePoint.z - p.z);
            traversalStrategies.add(new TraversalStrategy(newDistance, testPoint.z, x,
              testPointFixedZPlane, testPointFixedZAbovePlane, testPointFixedZBelowPlane,
              travelPlaneFixedX, fixedXAbovePlane, fixedXBelowPlane,
              zTree, xTree, p));
          }
        }
      }
      if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && fixedYAbovePlane != null && fixedYBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedXAbovePlane.D * testPointFixedXAbovePlane.D * planetModel.inverseXYScalingSquared + fixedYAbovePlane.D * fixedYAbovePlane.D * planetModel.inverseXYScalingSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedXBelowPlane.D * testPointFixedXBelowPlane.D * planetModel.inverseXYScalingSquared + fixedYBelowPlane.D * fixedYBelowPlane.D * planetModel.inverseXYScalingSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
          //System.out.println("  Looking for intersections between travel and test point planes...");
          final GeoPoint[] YIntersectionsX = travelPlaneFixedY.findIntersections(planetModel, testPointFixedXPlane);
          for (final GeoPoint p : YIntersectionsX) {
            // Travel would be in XZ plane (fixed y) then in YZ (fixed x)
            //final double newDistance = p.arcDistance(testPoint) + p.arcDistance(thePoint);
            final double tpDelta1 = testPoint.y - p.y;
            final double tpDelta2 = testPoint.z - p.z;
            final double cpDelta1 = x - p.x;
            final double cpDelta2 = z - p.z;
            final double newDistance = tpDelta1 * tpDelta1 + tpDelta2 * tpDelta2 + cpDelta1 * cpDelta1 + cpDelta2 * cpDelta2;
            //final double newDistance = (testPoint.y - p.y) * (testPoint.y - p.y) + (testPoint.z - p.z) * (testPoint.z - p.z)  + (thePoint.x - p.x) * (thePoint.x - p.x) + (thePoint.z - p.z) * (thePoint.z - p.z);
            //final double newDistance = Math.abs(testPoint.y - p.y) + Math.abs(thePoint.x - p.x);
            traversalStrategies.add(new TraversalStrategy(newDistance, testPoint.x, y,
              testPointFixedXPlane, testPointFixedXAbovePlane, testPointFixedXBelowPlane,
              travelPlaneFixedY, fixedYAbovePlane, fixedYBelowPlane,
              xTree, yTree, p));
          }
        }
      }
      if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && fixedYAbovePlane != null && fixedYBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedZAbovePlane.D * testPointFixedZAbovePlane.D * planetModel.inverseZScalingSquared + fixedYAbovePlane.D * fixedYAbovePlane.D * planetModel.inverseXYScalingSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedZBelowPlane.D * testPointFixedZBelowPlane.D * planetModel.inverseZScalingSquared + fixedYBelowPlane.D * fixedYBelowPlane.D * planetModel.inverseXYScalingSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
          //System.out.println("  Looking for intersections between travel and test point planes...");
          final GeoPoint[] YIntersectionsZ = travelPlaneFixedY.findIntersections(planetModel, testPointFixedZPlane);
          for (final GeoPoint p : YIntersectionsZ) {
            // Travel would be in XZ plane (fixed y) then in XY (fixed z)
            //final double newDistance = p.arcDistance(testPoint) + p.arcDistance(thePoint);
            final double tpDelta1 = testPoint.x - p.x;
            final double tpDelta2 = testPoint.y - p.y;
            final double cpDelta1 = x - p.x;
            final double cpDelta2 = z - p.z;
            final double newDistance = tpDelta1 * tpDelta1 + tpDelta2 * tpDelta2 + cpDelta1 * cpDelta1 + cpDelta2 * cpDelta2;
            //final double newDistance = (testPoint.x - p.x) * (testPoint.x - p.x) + (testPoint.y - p.y) * (testPoint.y - p.y)  + (thePoint.x - p.x) * (thePoint.x - p.x) + (thePoint.z - p.z) * (thePoint.z - p.z);
            //final double newDistance = Math.abs(testPoint.y - p.y) + Math.abs(thePoint.z - p.z);
            traversalStrategies.add(new TraversalStrategy(newDistance, testPoint.z, y,
              testPointFixedZPlane, testPointFixedZAbovePlane, testPointFixedZBelowPlane,
              travelPlaneFixedY, fixedYAbovePlane, fixedYBelowPlane,
              zTree, yTree, p));
          }
        }
      }
      if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && fixedZAbovePlane != null && fixedZBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedXAbovePlane.D * testPointFixedXAbovePlane.D * planetModel.inverseXYScalingSquared + fixedZAbovePlane.D * fixedZAbovePlane.D * planetModel.inverseZScalingSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedXBelowPlane.D * testPointFixedXBelowPlane.D * planetModel.inverseXYScalingSquared + fixedZBelowPlane.D * fixedZBelowPlane.D * planetModel.inverseZScalingSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
          //System.out.println("  Looking for intersections between travel and test point planes...");
          final GeoPoint[] ZIntersectionsX = travelPlaneFixedZ.findIntersections(planetModel, testPointFixedXPlane);
          for (final GeoPoint p : ZIntersectionsX) {
            // Travel would be in XY plane (fixed z) then in YZ (fixed x)
            //final double newDistance = p.arcDistance(testPoint) + p.arcDistance(thePoint);
            final double tpDelta1 = testPoint.y - p.y;
            final double tpDelta2 = testPoint.z - p.z;
            final double cpDelta1 = y - p.y;
            final double cpDelta2 = x - p.x;
            final double newDistance = tpDelta1 * tpDelta1 + tpDelta2 * tpDelta2 + cpDelta1 * cpDelta1 + cpDelta2 * cpDelta2;
            //final double newDistance = (testPoint.y - p.y) * (testPoint.y - p.y) + (testPoint.z - p.z) * (testPoint.z - p.z)  + (thePoint.y - p.y) * (thePoint.y - p.y) + (thePoint.x - p.x) * (thePoint.x - p.x);
            //final double newDistance = Math.abs(testPoint.z - p.z) + Math.abs(thePoint.x - p.x);
            traversalStrategies.add(new TraversalStrategy(newDistance, testPoint.x, z,
              testPointFixedXPlane, testPointFixedXAbovePlane, testPointFixedXBelowPlane,
              travelPlaneFixedZ, fixedZAbovePlane, fixedZBelowPlane,
              xTree, zTree, p));
          }
        }
      }
      if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && fixedZAbovePlane != null && fixedZBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedYAbovePlane.D * testPointFixedYAbovePlane.D * planetModel.inverseXYScalingSquared + fixedZAbovePlane.D * fixedZAbovePlane.D * planetModel.inverseZScalingSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedYBelowPlane.D * testPointFixedYBelowPlane.D * planetModel.inverseXYScalingSquared + fixedZBelowPlane.D * fixedZBelowPlane.D * planetModel.inverseZScalingSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
          //System.out.println("  Looking for intersections between travel and test point planes...");
          final GeoPoint[] ZIntersectionsY = travelPlaneFixedZ.findIntersections(planetModel, testPointFixedYPlane);
          for (final GeoPoint p : ZIntersectionsY) {
            // Travel would be in XY plane (fixed z) then in XZ (fixed y)
            //final double newDistance = p.arcDistance(testPoint) + p.arcDistance(thePoint);
            final double tpDelta1 = testPoint.x - p.x;
            final double tpDelta2 = testPoint.z - p.z;
            final double cpDelta1 = y - p.y;
            final double cpDelta2 = x - p.x;
            final double newDistance = tpDelta1 * tpDelta1 + tpDelta2 * tpDelta2 + cpDelta1 * cpDelta1 + cpDelta2 * cpDelta2;
            //final double newDistance = (testPoint.x - p.x) * (testPoint.x - p.x) + (testPoint.z - p.z) * (testPoint.z - p.z)  + (thePoint.y - p.y) * (thePoint.y - p.y) + (thePoint.x - p.x) * (thePoint.x - p.x);
            //final double newDistance = Math.abs(testPoint.z - p.z) + Math.abs(thePoint.y - p.y);
            traversalStrategies.add(new TraversalStrategy(newDistance, testPoint.y, z,
              testPointFixedYPlane, testPointFixedYAbovePlane, testPointFixedYBelowPlane,
              travelPlaneFixedZ, fixedZAbovePlane, fixedZBelowPlane,
              yTree, zTree, p));
          }
        }
      }

      Collections.sort(traversalStrategies);
      
      if (traversalStrategies.size() == 0) {
        throw new IllegalArgumentException("No dual-plane travel strategies were found");
      }

      // Loop through travel strategies, in order, until we find one that works.
      for (final TraversalStrategy ts : traversalStrategies) {
        try {
          return ts.apply(testPoint, testPointInSet, x, y, z);
        } catch (IllegalArgumentException e) {
          // Continue
        }
      }
      
      throw new IllegalArgumentException("Exhausted all traversal strategies");
    }
  }
    
  @Override
  public GeoPoint[] getEdgePoints() {
    return edgePoints;
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    // Create the intersector
    final EdgeIterator intersector = new IntersectorEdgeIterator(p, notablePoints, bounds);
    // First, compute the bounds for the the plane
    final XYZBounds xyzBounds = new XYZBounds();
    p.recordBounds(planetModel, xyzBounds, bounds);
    for (final GeoPoint point : notablePoints) {
      xyzBounds.addPoint(point);
    }
    // If we have no bounds at all then the answer is "false"
    if (xyzBounds.getMaximumX() == null || xyzBounds.getMinimumX() == null ||
      xyzBounds.getMaximumY() == null || xyzBounds.getMinimumY() == null ||
      xyzBounds.getMaximumZ() == null || xyzBounds.getMinimumZ() == null) {
      return false;
    }
    // Figure out which tree likely works best
    final double xDelta = xyzBounds.getMaximumX() - xyzBounds.getMinimumX();
    final double yDelta = xyzBounds.getMaximumY() - xyzBounds.getMinimumY();
    final double zDelta = xyzBounds.getMaximumZ() - xyzBounds.getMinimumZ();
    // Select the smallest range
    if (xDelta <= yDelta && xDelta <= zDelta) {
      // Drill down in x
      return !xTree.traverse(intersector, xyzBounds.getMinimumX(), xyzBounds.getMaximumX());
    } else if (yDelta <= xDelta && yDelta <= zDelta) {
      // Drill down in y
      return !yTree.traverse(intersector, xyzBounds.getMinimumY(), xyzBounds.getMaximumY());
    } else if (zDelta <= xDelta && zDelta <= yDelta) {
      // Drill down in z
      return !zTree.traverse(intersector, xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ());
    }
    return true;
  }

  @Override
  public boolean intersects(GeoShape geoShape) {
    // Create the intersector
    final EdgeIterator intersector = new IntersectorShapeIterator(geoShape);
    // First, compute the bounds for the the plane
    final XYZBounds xyzBounds = new XYZBounds();
    geoShape.getBounds(xyzBounds);

    // Figure out which tree likely works best
    final double xDelta = xyzBounds.getMaximumX() - xyzBounds.getMinimumX();
    final double yDelta = xyzBounds.getMaximumY() - xyzBounds.getMinimumY();
    final double zDelta = xyzBounds.getMaximumZ() - xyzBounds.getMinimumZ();
    // Select the smallest range
    // Select the smallest range
    if (xDelta <= yDelta && xDelta <= zDelta) {
      // Drill down in x
      return !xTree.traverse(intersector, xyzBounds.getMinimumX(), xyzBounds.getMaximumX());
    } else if (yDelta <= xDelta && yDelta <= zDelta) {
      // Drill down in y
      return !yTree.traverse(intersector, xyzBounds.getMinimumY(), xyzBounds.getMaximumY());
    } else if (zDelta <= xDelta && zDelta <= yDelta) {
      // Drill down in z
      return !zTree.traverse(intersector, xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ());
    }
    return true;
  }


  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    for (final Edge startEdge : shapeStartEdges) {
      Edge currentEdge = startEdge;
      while (true) {
        bounds.addPoint(currentEdge.startPoint);
        bounds.addPlane(this.planetModel, currentEdge.plane, currentEdge.startPlane, currentEdge.endPlane);
        currentEdge = currentEdge.next;
        if (currentEdge == startEdge) {
          break;
        }
      }
    }
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    double minimumDistance = Double.POSITIVE_INFINITY;
    for (final Edge shapeStartEdge : shapeStartEdges) {
      Edge shapeEdge = shapeStartEdge;
      while (true) {
        final double newDist = distanceStyle.computeDistance(shapeEdge.startPoint, x, y, z);
        if (newDist < minimumDistance) {
          minimumDistance = newDist;
        }
        final double newPlaneDist = distanceStyle.computeDistance(planetModel, shapeEdge.plane, x, y, z, shapeEdge.startPlane, shapeEdge.endPlane);
        if (newPlaneDist < minimumDistance) {
          minimumDistance = newPlaneDist;
        }
        shapeEdge = shapeEdge.next;
        if (shapeEdge == shapeStartEdge) {
          break;
        }
      }
    }
    return minimumDistance;
  }

  /** Create a linear crossing edge iterator with the appropriate cutoff planes given the geometry.
   */
  private CountingEdgeIterator createLinearCrossingEdgeIterator(final GeoPoint testPoint,
    final Plane plane, final Plane abovePlane, final Plane belowPlane,
    final double thePointX, final double thePointY, final double thePointZ) {
    // If thePoint and testPoint are parallel, we won't be able to determine sidedness of the bounding planes.  So detect that case, and build the iterator differently if we find it.
    // This didn't work; not sure why not:
    //if (testPoint.isParallel(thePointX, thePointY, thePointZ)) {
    //  return new FullLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    //}
    //return new SectorLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    //
    try {
      //System.out.println(" creating sector linear crossing edge iterator");
      return new SectorLinearCrossingEdgeIterator(testPoint, plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    } catch (IllegalArgumentException e) {
      // Assume we failed because we could not construct bounding planes, so do it another way.
      //System.out.println(" create full linear crossing edge iterator");
      return new FullLinearCrossingEdgeIterator(testPoint, plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    }
  }

  private final static double[] halfProportions = new double[]{0.5};
  
  /**
   * An instance of this class describes a single edge, and includes what is necessary to reliably determine intersection
   * in the context of the even/odd algorithm used.
   */
  private static class Edge {
    public final GeoPoint startPoint;
    public final GeoPoint endPoint;
    public final GeoPoint[] notablePoints;
    public final SidedPlane startPlane;
    public final SidedPlane endPlane;
    public final SidedPlane backingPlane;
    public final Plane plane;
    public final XYZBounds planeBounds;
    public Edge previous = null;
    public Edge next = null;
    
    public Edge(final PlanetModel pm, final GeoPoint startPoint, final GeoPoint endPoint) {
      this.startPoint = startPoint;
      this.endPoint = endPoint;
      this.notablePoints = new GeoPoint[]{startPoint, endPoint};
      this.plane = new Plane(startPoint, endPoint);
      this.startPlane =  new SidedPlane(endPoint, plane, startPoint);
      this.endPlane = new SidedPlane(startPoint, plane, endPoint);
      final GeoPoint interpolationPoint = plane.interpolate(pm, startPoint, endPoint, halfProportions)[0];
      this.backingPlane = new SidedPlane(interpolationPoint, interpolationPoint, 0.0);
      this.planeBounds = new XYZBounds();
      this.planeBounds.addPoint(startPoint);
      this.planeBounds.addPoint(endPoint);
      this.planeBounds.addPlane(pm, this.plane, this.startPlane, this.endPlane, this.backingPlane);
      //System.out.println("Recording edge ["+startPoint+" --> "+endPoint+"]; bounds = "+planeBounds);
    }

    public boolean isWithin(final double thePointX, final double thePointY, final double thePointZ) {
      return plane.evaluateIsZero(thePointX, thePointY, thePointZ) && startPlane.isWithin(thePointX, thePointY, thePointZ) && endPlane.isWithin(thePointX, thePointY, thePointZ) && backingPlane.isWithin(thePointX, thePointY, thePointZ);
    }

    // Hashcode and equals are system default!!
  }
  
  /** Strategy class for describing traversals.
  * Implements Comparable so that these can be ordered by Collections.sort().
  */
  private class TraversalStrategy implements Comparable<TraversalStrategy> {
    private final double traversalDistance;
    private final double firstLegValue;
    private final double secondLegValue;
    private final Plane firstLegPlane;
    private final Plane firstLegAbovePlane;
    private final Plane firstLegBelowPlane;
    private final Plane secondLegPlane;
    private final Plane secondLegAbovePlane;
    private final Plane secondLegBelowPlane;
    private final Tree firstLegTree;
    private final Tree secondLegTree;
    private final GeoPoint intersectionPoint;
    
    public TraversalStrategy(final double traversalDistance, final double firstLegValue, final double secondLegValue,
      final Plane firstLegPlane, final Plane firstLegAbovePlane, final Plane firstLegBelowPlane,
      final Plane secondLegPlane, final Plane secondLegAbovePlane, final Plane secondLegBelowPlane,
      final Tree firstLegTree, final Tree secondLegTree,
      final GeoPoint intersectionPoint) {
      this.traversalDistance = traversalDistance;
      this.firstLegValue = firstLegValue;
      this.secondLegValue = secondLegValue;
      this.firstLegPlane = firstLegPlane;
      this.firstLegAbovePlane = firstLegAbovePlane;
      this.firstLegBelowPlane = firstLegBelowPlane;
      this.secondLegPlane = secondLegPlane;
      this.secondLegAbovePlane = secondLegAbovePlane;
      this.secondLegBelowPlane = secondLegBelowPlane;
      this.firstLegTree = firstLegTree;
      this.secondLegTree = secondLegTree;
      this.intersectionPoint = intersectionPoint;
    }

    public boolean apply(final GeoPoint testPoint, final boolean testPointInSet,
      final double x, final double y, final double z) {
      // First, try with two individual legs.  If that doesn't work, try the DualCrossingIterator.
      try {
        // First, we'll determine if the intersection point is in set or not
        //System.out.println(" Finding whether "+intersectionPoint+" is in-set, based on travel from "+testPoint+" along "+firstLegPlane+" (value="+firstLegValue+")");
        final CountingEdgeIterator testPointEdgeIterator = createLinearCrossingEdgeIterator(testPoint,
          firstLegPlane, firstLegAbovePlane, firstLegBelowPlane,
          intersectionPoint.x, intersectionPoint.y, intersectionPoint.z);
        // Traverse our way from the test point to the check point.  Use the z tree because that's fixed.
        firstLegTree.traverse(testPointEdgeIterator, firstLegValue);
        final boolean intersectionPointOnEdge = testPointEdgeIterator.isOnEdge();
        // If the intersection point is on the edge, we cannot use this combination of legs, since it's not logically possible to compute in-set or out-of-set
        // with such a starting point.
        if (intersectionPointOnEdge) {
          throw new IllegalArgumentException("Intersection point landed on an edge -- illegal path");
        }
        final boolean intersectionPointInSet = intersectionPointOnEdge || (((testPointEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet);
        
        //System.out.println("  Intersection point in-set? "+intersectionPointInSet+" On edge? "+intersectionPointOnEdge);

        // Now do the final leg
        //System.out.println(" Finding whether ["+x+","+y+","+z+"] is in-set, based on travel from "+intersectionPoint+" along "+secondLegPlane+" (value="+secondLegValue+")");
        final CountingEdgeIterator travelEdgeIterator = createLinearCrossingEdgeIterator(intersectionPoint,
          secondLegPlane, secondLegAbovePlane, secondLegBelowPlane,
          x, y, z);
        // Traverse our way from the test point to the check point.
        secondLegTree.traverse(travelEdgeIterator, secondLegValue);
        final boolean rval = travelEdgeIterator.isOnEdge() || (((travelEdgeIterator.getCrossingCount() & 1) == 0)?intersectionPointInSet:!intersectionPointInSet);
        
        //System.out.println(" Check point in set? "+rval);
        return rval;
      } catch (IllegalArgumentException e) {
        // Intersection point apparently was on edge, so try another strategy
        //System.out.println(" Trying dual crossing edge iterator");
        final CountingEdgeIterator edgeIterator = new DualCrossingEdgeIterator(testPoint,
          firstLegPlane, firstLegAbovePlane, firstLegBelowPlane,
          secondLegPlane, secondLegAbovePlane, secondLegBelowPlane,
          x, y, z, intersectionPoint);
        firstLegTree.traverse(edgeIterator, firstLegValue);
        if (edgeIterator.isOnEdge()) {
          return true;
        }
        secondLegTree.traverse(edgeIterator, secondLegValue);
        return edgeIterator.isOnEdge() || (((edgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet);
      }
    }

    @Override
    public String toString() {
      return "{firstLegValue="+firstLegValue+"; secondLegValue="+secondLegValue+"; firstLegPlane="+firstLegPlane+"; secondLegPlane="+secondLegPlane+"; intersectionPoint="+intersectionPoint+"}";
    }

    @Override
    public int compareTo(final TraversalStrategy other) {
      if (traversalDistance < other.traversalDistance) {
        return -1;
      } else if (traversalDistance > other.traversalDistance) {
        return 1;
      }
      return 0;
    }
    
  }
  
  /**
   * Iterator execution interface, for tree traversal.  Pass an object implementing this interface
   * into the traversal method of a tree, and each edge that matches will cause this object to be
   * called.
   */
  private static interface EdgeIterator {
    /**
     * @param edge is the edge that matched.
     * @return true if the iteration should continue, false otherwise.
     */
    public boolean matches(final Edge edge);
  }

  /**
   * Iterator execution interface, for tree traversal, plus count retrieval.  Pass an object implementing this interface
   * into the traversal method of a tree, and each edge that matches will cause this object to be
   * called.
   */
  private static interface CountingEdgeIterator extends EdgeIterator {
    /**
     * @return the number of edges that were crossed.
     */
    public int getCrossingCount();
    
    /**
     * @return true if the endpoint was on an edge.
     */
    public boolean isOnEdge();

  }
  
  /**
   * An instance of this class represents a node in a tree.  The tree is designed to be given
   * a value and from that to iterate over a list of edges.
   * In order to do this efficiently, each new edge is dropped into the tree using its minimum and
   * maximum value.  If the new edge's value does not overlap the range, then it gets added
   * either to the lesser side or the greater side, accordingly.  If it does overlap, then the
   * "overlapping" chain is instead traversed.
   *
   * This class is generic and can be used for any definition of "value".
   *
   */
  private static class Node {
    public final Edge edge;
    public final double low;
    public final double high;
    public Node left = null;
    public Node right = null;
    public double max;

    
    public Node(final Edge edge, final double minimumValue, final double maximumValue) {
      this.edge = edge;
      this.low = minimumValue;
      this.high = maximumValue;
      this.max = maximumValue;
    }

    public boolean traverse(final EdgeIterator edgeIterator, final double minValue, final double maxValue) {
      if (minValue <= max) {
        
        // Does this node overlap?
        if (minValue <= high && maxValue >= low) {
          if (edgeIterator.matches(edge) == false) {
            return false;
          }
        }
        
        if (left != null && left.traverse(edgeIterator, minValue, maxValue) == false) {
          return false;
        }
        if (right != null && maxValue >= low && right.traverse(edgeIterator, minValue, maxValue) == false) {
          return false;
        }
      }
      return true;
    }
    
  }
  
  /** An interface describing a tree.
   */
  private static abstract class Tree {
    private final Node rootNode;
    
    protected static final Edge[] EMPTY_ARRAY = new Edge[0];
    
    /** Constructor.
     * @param allEdges is the list of all edges for the tree.
     */
    public Tree(final List<Edge> allEdges) {
      // Dump edges into an array and then sort it
      final Node[] edges = new Node[allEdges.size()];
      int i = 0;
      for (final Edge edge : allEdges) {
        edges[i++] = new Node(edge, getMinimum(edge), getMaximum(edge));
      }
      Arrays.sort(edges, (left, right) -> {
        int ret = Double.compare(left.low, right.low);
        if (ret == 0) {
          ret = Double.compare(left.max, right.max);
        }
        return ret;
      });
      rootNode = createTree(edges, 0, edges.length - 1);
    }
    
    private static Node createTree(final Node[] edges, final int low, final int high) {
      if (low > high) {
        return null;
      }
      // add midpoint
      int mid = (low + high) >>> 1;
      final Node newNode = edges[mid];
      // add children
      newNode.left = createTree(edges, low, mid - 1);
      newNode.right = createTree(edges, mid + 1, high);
      // pull up max values to this node
      if (newNode.left != null) {
        newNode.max = Math.max(newNode.max, newNode.left.max);
      }
      if (newNode.right != null) {
        newNode.max = Math.max(newNode.max, newNode.right.max);
      }
      return newNode;
    }

    /** Get the minimum value from the edge.
     * @param edge is the edge.
     * @return the minimum value.
     */
    protected abstract double getMinimum(final Edge edge);
    
    /** Get the maximum value from the edge.
     * @param edge is the edge.
     * @return the maximum value.
     */
    protected abstract double getMaximum(final Edge edge);
    
    /** Traverse the tree, finding all edges that intersect the provided value.
     * @param edgeIterator provides the method to call for any encountered matching edge.
     * @param value is the value to match.
     * @return false if the traversal was aborted before completion.
     */
    public boolean traverse(final EdgeIterator edgeIterator, final double value) {
      return traverse(edgeIterator, value, value);
    }
    
    /** Traverse the tree, finding all edges that intersect the provided value range.
     * @param edgeIterator provides the method to call for any encountered matching edge.
     *   Edges will not be invoked more than once.
     * @param minValue is the minimum value.
     * @param maxValue is the maximum value.
     * @return false if the traversal was aborted before completion.
     */
    public boolean traverse(final EdgeIterator edgeIterator, final double minValue, final double maxValue) {
      if (rootNode == null) {
        return true;
      }
      return rootNode.traverse(edgeIterator, minValue, maxValue);
    }
    

  }
  
  /** This is the z-tree.
   */
  private static class ZTree extends Tree {
    public Node rootNode = null;
    
    public ZTree(final List<Edge> allEdges) {
      super(allEdges);
    }
    
    /*
    @Override
    public boolean traverse(final EdgeIterator edgeIterator, final double value) {
      System.err.println("Traversing in Z, value= "+value+"...");
      return super.traverse(edgeIterator, value);
    }
    */
    
    @Override
    protected double getMinimum(final Edge edge) {
      return edge.planeBounds.getMinimumZ();
    }
    
    @Override
    protected double getMaximum(final Edge edge) {
      return edge.planeBounds.getMaximumZ();
    }

  }
  
  /** This is the y-tree.
   */
  private static class YTree extends Tree {
    
    public YTree(final List<Edge> allEdges) {
      super(allEdges);
    }

    /*
    @Override
    public boolean traverse(final EdgeIterator edgeIterator, final double value) {
      System.err.println("Traversing in Y, value= "+value+"...");
      return super.traverse(edgeIterator, value);
    }
    */
    
    @Override
    protected double getMinimum(final Edge edge) {
      return edge.planeBounds.getMinimumY();
    }
    
    @Override
    protected double getMaximum(final Edge edge) {
      return edge.planeBounds.getMaximumY();
    }
    
  }

  /** This is the x-tree.
   */
  private static class XTree extends Tree {
    
    public XTree(final List<Edge> allEdges) {
      super(allEdges);
    }
    
    /*
    @Override
    public boolean traverse(final EdgeIterator edgeIterator, final double value) {
      System.err.println("Traversing in X, value= "+value+"...");
      return super.traverse(edgeIterator, value);
    }
    */
    
    @Override
    protected double getMinimum(final Edge edge) {
      return edge.planeBounds.getMinimumX();
    }
    
    @Override
    protected double getMaximum(final Edge edge) {
      return edge.planeBounds.getMaximumX();
    }
    
  }

  /** Assess whether edge intersects the provided plane plus bounds.
   */
  private class IntersectorEdgeIterator implements EdgeIterator {
    
    private final Plane plane;
    private final GeoPoint[] notablePoints;
    private final Membership[] bounds;
    
    public IntersectorEdgeIterator(final Plane plane, final GeoPoint[] notablePoints, final Membership... bounds) {
      this.plane = plane;
      this.notablePoints = notablePoints;
      this.bounds = bounds;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      return !plane.intersects(planetModel, edge.plane, notablePoints, edge.notablePoints, bounds, edge.startPlane, edge.endPlane);
    }

  }


  /** Assess whether edge intersects the provided shape.
   */
  private class IntersectorShapeIterator implements EdgeIterator {

    private final GeoShape shape;

    public IntersectorShapeIterator(final GeoShape shape) {
      this.shape = shape;
    }

    @Override
    public boolean matches(final Edge edge) {
      return !shape.intersects(edge.plane, edge.notablePoints, edge.startPlane, edge.endPlane);
    }
  }

  /*
  private void debugIntersectAllEdges(final Plane travelPlane, final Membership... bounds) {
    System.out.println("\n The following edges intersect the travel plane within the given bounds:");
    for (final Edge startEdge : shapeStartEdges) {
      Edge currentEdge = startEdge;
      while (true) {
        System.out.println("  Edge "+currentEdge.startPoint+" --> "+currentEdge.endPoint+":");
        final GeoPoint[] intersectionPoints = travelPlane.findIntersections(planetModel, currentEdge.plane, bounds, new Membership[]{currentEdge.startPlane, currentEdge.endPlane, currentEdge.backingPlane});
        if (intersectionPoints == null || intersectionPoints.length > 0) {
          System.out.println("   ... intersects!!");
        } else {
          final GeoPoint[] unboundedPoints = travelPlane.findIntersections(planetModel, currentEdge.plane);
          for (final GeoPoint point : unboundedPoints) {
            for (final Membership bound : bounds) {
              if (!bound.isWithin(point)) {
                System.out.println("   ... intersection "+point+" excluded by iterator bound ("+((SidedPlane)bound).evaluate(point)+")");
              }
            }
            if (!currentEdge.startPlane.isWithin(point)) {
              System.out.println("   ... intersection "+point+" excluded by edge start plane ("+((SidedPlane)currentEdge.startPlane).evaluate(point)+")");
            }
            if (!currentEdge.endPlane.isWithin(point)) {
              System.out.println("   ... intersection "+point+" excluded by edge end plane ("+((SidedPlane)currentEdge.endPlane).evaluate(point)+")");
            }
            if (!currentEdge.backingPlane.isWithin(point)) {
              System.out.println("   ... intersection "+point+" excluded by edge backing plane ("+((SidedPlane)currentEdge.backingPlane).evaluate(point)+")");
            }
          }
        }
        currentEdge = currentEdge.next;
        if (currentEdge == startEdge) {
          break;
        }
      }
    }
    System.out.println(" ...done\n");
  }
  */
  
  /** Count the number of verifiable edge crossings for a full 1/2 a world.
   */
  private class FullLinearCrossingEdgeIterator implements CountingEdgeIterator {
    
    private final GeoPoint testPoint;
    private final Plane plane;
    private final Plane abovePlane;
    private final Plane belowPlane;
    private final Membership bound;
    private final double thePointX;
    private final double thePointY;
    private final double thePointZ;
    
    private boolean onEdge = false;
    private int aboveCrossingCount = 0;
    private int belowCrossingCount = 0;
    
    public FullLinearCrossingEdgeIterator(final GeoPoint testPoint,
      final Plane plane, final Plane abovePlane, final Plane belowPlane,
      final double thePointX, final double thePointY, final double thePointZ) {
      assert plane.evaluateIsZero(thePointX, thePointY, thePointZ) : "Check point is not on travel plane";
      assert plane.evaluateIsZero(testPoint) : "Test point is not on travel plane";
      this.testPoint = testPoint;
      this.plane = plane;
      this.abovePlane = abovePlane;
      this.belowPlane = belowPlane;
      if (plane.isNumericallyIdentical(testPoint)) {
        throw new IllegalArgumentException("Plane vector identical to testpoint vector");
      }
      // It doesn't matter which 1/2 of the world we choose, but we must choose only one.
      this.bound = new SidedPlane(plane, testPoint);
      this.thePointX = thePointX;
      this.thePointY = thePointY;
      this.thePointZ = thePointZ;
      //System.out.println(" Constructing full linear crossing edge iterator");
      //debugIntersectAllEdges(plane, bound);
    }
    
    @Override
    public int getCrossingCount() {
      return Math.min(aboveCrossingCount, belowCrossingCount);
    }

    @Override
    public boolean isOnEdge() {
      return onEdge;
    }

    @Override
    public boolean matches(final Edge edge) {
      //System.out.println(" Edge ["+edge.startPoint+" --> "+edge.endPoint+"] potentially crosses travel plane "+plane);
      // Early exit if the point is on the edge.
      if (edge.isWithin(thePointX, thePointY, thePointZ)) {
        //System.out.println("  Point is on the edge; in-set");
        onEdge = true;
        return false;
      }
      
      // This should precisely mirror what is in DualCrossingIterator, but without the dual crossings.
      // Some edges are going to be given to us even when there's no real intersection, so do that as a sanity check, first.
      final GeoPoint[] planeCrossings = plane.findIntersections(planetModel, edge.plane, bound, edge.startPlane, edge.endPlane);
      if (planeCrossings != null && planeCrossings.length == 0) {
        // Sometimes on the hairy edge an intersection will be missed.  This check finds those.
        if (!plane.evaluateIsZero(edge.startPoint) && !plane.evaluateIsZero(edge.endPoint)) {
          return true;
        }
      }

      //System.out.println("  Edge intersects travel plane "+plane);
      
      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      final int aboveCrossings = countCrossings(edge, abovePlane, bound);
      aboveCrossingCount += aboveCrossings;
      final int belowCrossings = countCrossings(edge, belowPlane, bound);
      belowCrossingCount += belowCrossings;
      //System.out.println("  Above crossings = "+aboveCrossings+"; below crossings = "+belowCrossings);

      return true;
    }

    /** Find the intersections with an envelope plane, and assess those intersections for 
      * whether they truly describe crossings.
      */
    private int countCrossings(final Edge edge,
      final Plane envelopePlane, final Membership envelopeBound) {
      final GeoPoint[] intersections = edge.plane.findIntersections(planetModel, envelopePlane, envelopeBound);
      int crossings = 0;
      if (intersections != null) {
        for (final GeoPoint intersection : intersections) {
          if (edge.startPlane.strictlyWithin(intersection) && edge.endPlane.strictlyWithin(intersection)) {
            // It's unique, so assess it
            crossings += edgeCrossesEnvelope(edge.plane, intersection, envelopePlane)?1:0;
          }
        }
      }
      return crossings;
    }

    private boolean edgeCrossesEnvelope(final Plane edgePlane, final GeoPoint intersectionPoint, final Plane envelopePlane) {
      final GeoPoint[] adjoiningPoints = findAdjoiningPoints(edgePlane, intersectionPoint, envelopePlane);
      if (adjoiningPoints == null) {
        return true;
      }
      int withinCount = 0;
      for (final GeoPoint adjoining : adjoiningPoints) {
        if (plane.evaluateIsZero(adjoining) && bound.isWithin(adjoining)) {
          withinCount++;
        }
      }
      return (withinCount & 1) != 0;
    }


  }

  /** Count the number of verifiable edge crossings for less than 1/2 a world.
   */
  private class SectorLinearCrossingEdgeIterator implements CountingEdgeIterator {
    
    private final GeoPoint testPoint;
    private final Plane plane;
    private final Plane abovePlane;
    private final Plane belowPlane;
    private final Membership bound1;
    private final Membership bound2;
    private final double thePointX;
    private final double thePointY;
    private final double thePointZ;
    
    private boolean onEdge = false;
    private int aboveCrossingCount = 0;
    private int belowCrossingCount = 0;
    
    public SectorLinearCrossingEdgeIterator(final GeoPoint testPoint,
      final Plane plane, final Plane abovePlane, final Plane belowPlane, 
      final double thePointX, final double thePointY, final double thePointZ) {
      assert plane.evaluateIsZero(thePointX, thePointY, thePointZ) : "Check point is not on travel plane";
      assert plane.evaluateIsZero(testPoint) : "Test point is not on travel plane";
      this.testPoint = testPoint;
      this.plane = plane;
      this.abovePlane = abovePlane;
      this.belowPlane = belowPlane;
      // We have to be sure we don't accidently create two bounds that would exclude all points.
      // Not sure this can happen but...
      final SidedPlane bound1Plane = new SidedPlane(thePointX, thePointY, thePointZ, plane, testPoint);
      final SidedPlane bound2Plane = new SidedPlane(testPoint, plane, thePointX, thePointY, thePointZ);
      if (bound1Plane.isNumericallyIdentical(bound2Plane)) {
        throw new IllegalArgumentException("Sector iterator unreliable when bounds planes are numerically identical");
      }
      this.bound1 = bound1Plane;
      this.bound2 = bound2Plane;
      this.thePointX = thePointX;
      this.thePointY = thePointY;
      this.thePointZ = thePointZ;
      //System.out.println(" Constructing sector linear crossing edge iterator");
      //debugIntersectAllEdges(plane, bound1, bound2);
    }
    
    @Override
    public int getCrossingCount() {
      return Math.min(aboveCrossingCount, belowCrossingCount);
    }
    
    @Override
    public boolean isOnEdge() {
      return onEdge;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      //System.out.println(" Edge ["+edge.startPoint+" --> "+edge.endPoint+"] potentially crosses travel plane "+plane);
      // Early exit if the point is on the edge.
      if (edge.isWithin(thePointX, thePointY, thePointZ)) {
        // The point is on the edge.  This means it's "in-set" by definition, so abort.
        //System.out.println("  Point is on the edge; in-set");
        onEdge = true;
        return false;
      }
      
      //System.out.println("  Finding intersections between edge plane and travel plane...");

      // This should precisely mirror what is in DualCrossingIterator, but without the dual crossings.
      // Some edges are going to be given to us even when there's no real intersection, so do that as a sanity check, first.
      final GeoPoint[] planeCrossings = plane.findIntersections(planetModel, edge.plane, bound1, bound2, edge.startPlane, edge.endPlane);
      if (planeCrossings == null) {
        //System.out.println("  Planes were identical");
      } else if (planeCrossings.length == 0) {
        //System.out.println("  There are no intersection points within bounds.");
        /*
        // For debugging purposes, let's repeat the intersection check without bounds, and figure out which bound(s) rejected it
        final GeoPoint[] unboundedCrossings = plane.findIntersections(planetModel, edge.plane);
        for (final GeoPoint crossing : unboundedCrossings) {
          if (!bound1.isWithin(crossing)) {
            System.out.println("   Crossing point "+crossing+" rejected by bound1 ("+((SidedPlane)bound1).evaluate(crossing)+")");
          }
          if (!bound2.isWithin(crossing)) {
            System.out.println("   Crossing point "+crossing+" rejected by bound2 ("+((SidedPlane)bound2).evaluate(crossing)+")");
          }
          if (!edge.startPlane.isWithin(crossing)) {
            System.out.println("   Crossing point "+crossing+" rejected by edge.startPlane ("+((SidedPlane)edge.startPlane).evaluate(crossing)+")");
          }
          if (!edge.endPlane.isWithin(crossing)) {
            System.out.println("   Crossing point "+crossing+" rejected by edge.endPlane ("+((SidedPlane)edge.endPlane).evaluate(crossing)+")");
          }
        }
        */
        // Sometimes on the hairy edge an intersection will be missed.  This check finds those.
        if (!plane.evaluateIsZero(edge.startPoint) && !plane.evaluateIsZero(edge.endPoint)) {
          //System.out.println("   Endpoint(s) of edge are not on travel plane; distances: "+plane.evaluate(edge.startPoint)+" and "+plane.evaluate(edge.endPoint));
          // Edge doesn't actually intersect the travel plane.
          return true;
        } else {
          //System.out.println("   Endpoint(s) of edge are on travel plane!");
        }
      } else {
        //System.out.println("  There were intersection points!");
      }
      
      //System.out.println("  Edge intersects travel plane");

      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      //System.out.println("  Getting above crossings...");
      final int aboveCrossings = countCrossings(edge, abovePlane, bound1, bound2);
      aboveCrossingCount += aboveCrossings;
      //System.out.println("  Getting below crossings...");
      final int belowCrossings = countCrossings(edge, belowPlane, bound1, bound2);
      belowCrossingCount += belowCrossings;
      //System.out.println("  Above crossings = "+aboveCrossings+"; below crossings = "+belowCrossings);

      return true;
    }

    /** Find the intersections with an envelope plane, and assess those intersections for 
      * whether they truly describe crossings.
      */
    private int countCrossings(final Edge edge,
      final Plane envelopePlane, final Membership envelopeBound1, final Membership envelopeBound2) {
      final GeoPoint[] intersections = edge.plane.findIntersections(planetModel, envelopePlane, envelopeBound1, envelopeBound2);
      int crossings = 0;
      if (intersections != null) {
        for (final GeoPoint intersection : intersections) {
          if (edge.startPlane.strictlyWithin(intersection) && edge.endPlane.strictlyWithin(intersection)) {
            //System.out.println("   Envelope intersection point = "+intersection);
            // It's unique, so assess it
            final int counter = edgeCrossesEnvelope(edge.plane, intersection, envelopePlane)?1:0;
            //System.out.println("   Edge crosses envelope "+counter+" times");
            crossings += counter;
          }
        }
      } else {
        //System.out.println("   Intersections = null");
      }
      return crossings;
    }

    private boolean edgeCrossesEnvelope(final Plane edgePlane, final GeoPoint intersectionPoint, final Plane envelopePlane) {
      final GeoPoint[] adjoiningPoints = findAdjoiningPoints(edgePlane, intersectionPoint, envelopePlane);
      if (adjoiningPoints == null) {
        //System.out.println("    No adjoining points");
        return true;
      }
      int withinCount = 0;
      for (final GeoPoint adjoining : adjoiningPoints) {
        //System.out.println("    Adjoining point "+adjoining);
        if (plane.evaluateIsZero(adjoining) && bound1.isWithin(adjoining) && bound2.isWithin(adjoining)) {
          //System.out.println("     within!!");
          withinCount++;
        } else {
          //System.out.println("     evaluateIsZero? "+plane.evaluateIsZero(adjoining)+" bound1.isWithin? "+bound1.isWithin(adjoining)+" bound2.isWithin? "+bound2.isWithin(adjoining));
        }
      }
      return (withinCount & 1) != 0;
    }

  }


  /** Count the number of verifiable edge crossings for a dual-leg journey.
   */
  private class DualCrossingEdgeIterator implements CountingEdgeIterator {
    
    // This is a hash of which edges we've already looked at and tallied, so we don't repeat ourselves.
    // It is lazily initialized since most transitions cross no edges at all.
    private Set<Edge> seenEdges = null;
    
    private final GeoPoint testPoint;
    private final Plane testPointPlane;
    private final Plane testPointAbovePlane;
    private final Plane testPointBelowPlane;
    private final Plane travelPlane;
    private final Plane travelAbovePlane;
    private final Plane travelBelowPlane;
    private final double thePointX;
    private final double thePointY;
    private final double thePointZ;
    
    private final GeoPoint intersectionPoint;
    
    private final SidedPlane testPointCutoffPlane;
    private final SidedPlane checkPointCutoffPlane;
    private final SidedPlane testPointOtherCutoffPlane;
    private final SidedPlane checkPointOtherCutoffPlane;

    // These are computed on an as-needed basis
    
    private boolean computedInsideOutside = false;
    private Plane testPointInsidePlane;
    private Plane testPointOutsidePlane;
    private Plane travelInsidePlane;
    private Plane travelOutsidePlane;
    private SidedPlane insideTestPointCutoffPlane;
    private SidedPlane insideTravelCutoffPlane;
    private SidedPlane outsideTestPointCutoffPlane;
    private SidedPlane outsideTravelCutoffPlane;
    
    // The counters
    private boolean onEdge = false;
    private int innerCrossingCount = 0;
    private int outerCrossingCount = 0;

    public DualCrossingEdgeIterator(final GeoPoint testPoint,
      final Plane testPointPlane, final Plane testPointAbovePlane, final Plane testPointBelowPlane,
      final Plane travelPlane, final Plane travelAbovePlane, final Plane travelBelowPlane,
      final double thePointX, final double thePointY, final double thePointZ, final GeoPoint intersectionPoint) {
      this.testPoint = testPoint;
      this.testPointPlane = testPointPlane;
      this.testPointAbovePlane = testPointAbovePlane;
      this.testPointBelowPlane = testPointBelowPlane;
      this.travelPlane = travelPlane;
      this.travelAbovePlane = travelAbovePlane;
      this.travelBelowPlane = travelBelowPlane;
      this.thePointX = thePointX;
      this.thePointY = thePointY;
      this.thePointZ = thePointZ;
      this.intersectionPoint = intersectionPoint;
      
      //System.out.println("Intersection point = "+intersectionPoint);
      //System.out.println("TestPoint plane: "+testPoint+" -> "+intersectionPoint);
      //System.out.println("Travel plane: ["+thePointX+","+thePointY+","+thePointZ+"] -> "+intersectionPoint);
      
      assert travelPlane.evaluateIsZero(intersectionPoint) : "intersection point must be on travel plane";
      assert testPointPlane.evaluateIsZero(intersectionPoint) : "intersection point must be on test point plane";
      
      //System.out.println("Test point distance to intersection point: "+intersectionPoint.linearDistance(testPoint));
      //System.out.println("Check point distance to intersection point: "+intersectionPoint.linearDistance(thePointX, thePointY, thePointZ));

      assert !testPoint.isNumericallyIdentical(intersectionPoint) : "test point is the same as intersection point";
      assert !intersectionPoint.isNumericallyIdentical(thePointX, thePointY, thePointZ) : "check point is same as intersection point";

      /*
      final SidedPlane bound1Plane = new SidedPlane(thePointX, thePointY, thePointZ, plane, testPoint);
      final SidedPlane bound2Plane = new SidedPlane(testPoint, plane, thePointX, thePointY, thePointZ);
      if (bound1Plane.isNumericallyIdentical(bound2Plane)) {
        throw new IllegalArgumentException("Sector iterator unreliable when bounds planes are numerically identical");
      }
      */
      
      final SidedPlane testPointBound1 = new SidedPlane(intersectionPoint, testPointPlane, testPoint);
      final SidedPlane testPointBound2 = new SidedPlane(testPoint, testPointPlane, intersectionPoint);
      if (testPointBound1.isFunctionallyIdentical(testPointBound2)) {
        throw new IllegalArgumentException("Dual iterator unreliable when bounds planes are functionally identical");
      }
      this.testPointCutoffPlane = testPointBound1;
      this.testPointOtherCutoffPlane = testPointBound2;

      final SidedPlane checkPointBound1 = new SidedPlane(intersectionPoint, travelPlane, thePointX, thePointY, thePointZ);
      final SidedPlane checkPointBound2 = new SidedPlane(thePointX, thePointY, thePointZ, travelPlane, intersectionPoint);
      if (checkPointBound1.isFunctionallyIdentical(checkPointBound2)) {
        throw new IllegalArgumentException("Dual iterator unreliable when bounds planes are functionally identical");
      }
      this.checkPointCutoffPlane = checkPointBound1;
      this.checkPointOtherCutoffPlane = checkPointBound2;

      // Sanity check
      assert testPointCutoffPlane.isWithin(intersectionPoint) : "intersection must be within testPointCutoffPlane";
      assert testPointOtherCutoffPlane.isWithin(intersectionPoint) : "intersection must be within testPointOtherCutoffPlane";
      assert checkPointCutoffPlane.isWithin(intersectionPoint) : "intersection must be within checkPointCutoffPlane";
      assert checkPointOtherCutoffPlane.isWithin(intersectionPoint) : "intersection must be within checkPointOtherCutoffPlane";
      
    }
    
    protected void computeInsideOutside() {
      if (!computedInsideOutside) {
        // Convert travel plane to a sided plane
        final Membership intersectionBound1 = new SidedPlane(testPoint, travelPlane, travelPlane.D);
        // Convert testPoint plane to a sided plane
        final Membership intersectionBound2 = new SidedPlane(thePointX, thePointY, thePointZ, testPointPlane, testPointPlane.D);

        assert intersectionBound1.isWithin(intersectionPoint) : "intersection must be within intersectionBound1";
        assert intersectionBound2.isWithin(intersectionPoint) : "intersection must be within intersectionBound2";

        // Figure out which of the above/below planes are inside vs. outside.  To do this,
        // we look for the point that is within the bounds of the testPointPlane and travelPlane.  The two sides that intersected there are the inside
        // borders.
        // Each of these can generate two solutions.  We need to refine them to generate only one somehow -- the one in the same area of the world as intersectionPoint.
        // Since the travel/testpoint planes have one fixed coordinate, and that is represented by the plane's D value, it should be possible to choose based on the
        // point's coordinates. 
        final GeoPoint[] aboveAbove = travelAbovePlane.findIntersections(planetModel, testPointAbovePlane, intersectionBound1, intersectionBound2);
        assert aboveAbove != null : "Above + above should not be coplanar";
        final GeoPoint[] aboveBelow = travelAbovePlane.findIntersections(planetModel, testPointBelowPlane, intersectionBound1, intersectionBound2);
        assert aboveBelow != null : "Above + below should not be coplanar";
        final GeoPoint[] belowBelow = travelBelowPlane.findIntersections(planetModel, testPointBelowPlane, intersectionBound1, intersectionBound2);
        assert belowBelow != null : "Below + below should not be coplanar";
        final GeoPoint[] belowAbove = travelBelowPlane.findIntersections(planetModel, testPointAbovePlane, intersectionBound1, intersectionBound2);
        assert belowAbove != null : "Below + above should not be coplanar";

        assert ((aboveAbove.length > 0)?1:0) + ((aboveBelow.length > 0)?1:0) + ((belowBelow.length > 0)?1:0) + ((belowAbove.length > 0)?1:0) == 1 : "Can be exactly one inside point, instead was: aa="+aboveAbove.length+" xyScaling=" + aboveBelow.length+" bb="+ belowBelow.length+" ba=" + belowAbove.length;
        
        final GeoPoint[] insideInsidePoints;
        if (aboveAbove.length > 0) {
          travelInsidePlane = travelAbovePlane;
          testPointInsidePlane = testPointAbovePlane;
          travelOutsidePlane = travelBelowPlane;
          testPointOutsidePlane = testPointBelowPlane;
          insideInsidePoints = aboveAbove;
        } else if (aboveBelow.length > 0) {
          travelInsidePlane = travelAbovePlane;
          testPointInsidePlane = testPointBelowPlane;
          travelOutsidePlane = travelBelowPlane;
          testPointOutsidePlane = testPointAbovePlane;
          insideInsidePoints = aboveBelow;
        } else if (belowBelow.length > 0) {
          travelInsidePlane = travelBelowPlane;
          testPointInsidePlane = testPointBelowPlane;
          travelOutsidePlane = travelAbovePlane;
          testPointOutsidePlane = testPointAbovePlane;
          insideInsidePoints = belowBelow;
        } else if (belowAbove.length > 0) {
          travelInsidePlane = travelBelowPlane;
          testPointInsidePlane = testPointAbovePlane;
          travelOutsidePlane = travelAbovePlane;
          testPointOutsidePlane = testPointBelowPlane;
          insideInsidePoints = belowAbove;
        } else {
          throw new IllegalStateException("Can't find traversal intersection among: "+travelAbovePlane+", "+testPointAbovePlane+", "+travelBelowPlane+", "+testPointBelowPlane);
        }
        
        // Get the inside-inside intersection point
        // Picking which point, out of two, that corresponds to the already-selected intersectionPoint, is tricky, but it must be done.
        // We expect the choice to be within a small delta of the intersection point in 2 of the dimensions, but not the third
        final GeoPoint insideInsidePoint = pickProximate(insideInsidePoints);
        
        // Get the outside-outside intersection point
        //System.out.println("Computing outside-outside intersection");
        final GeoPoint[] outsideOutsidePoints = testPointOutsidePlane.findIntersections(planetModel, travelOutsidePlane);  //these don't add anything: , checkPointCutoffPlane, testPointCutoffPlane);
        final GeoPoint outsideOutsidePoint = pickProximate(outsideOutsidePoints);
        
        insideTravelCutoffPlane = new SidedPlane(thePointX, thePointY, thePointZ, travelInsidePlane, insideInsidePoint);
        outsideTravelCutoffPlane = new SidedPlane(thePointX, thePointY, thePointZ, travelInsidePlane, outsideOutsidePoint);
        insideTestPointCutoffPlane = new SidedPlane(testPoint, testPointInsidePlane, insideInsidePoint);
        outsideTestPointCutoffPlane = new SidedPlane(testPoint, testPointOutsidePlane, outsideOutsidePoint);
        
        /*
        System.out.println("insideTravelCutoffPlane = "+insideTravelCutoffPlane);
        System.out.println("outsideTravelCutoffPlane = "+outsideTravelCutoffPlane);
        System.out.println("insideTestPointCutoffPlane = "+insideTestPointCutoffPlane);
        System.out.println("outsideTestPointCutoffPlane = "+outsideTestPointCutoffPlane);
        */
        
        computedInsideOutside = true;
      }
    }

    private GeoPoint pickProximate(final GeoPoint[] points) {
      if (points.length == 0) {
        throw new IllegalArgumentException("No off-plane intersection points were found; can't compute traversal");
      } else if (points.length == 1) {
        return points[0];
      } else {
        final double p1dist = computeSquaredDistance(points[0], intersectionPoint);
        final double p2dist = computeSquaredDistance(points[1], intersectionPoint);
        if (p1dist < p2dist) {
          return points[0];
        } else if (p2dist < p1dist) {
          return points[1];
        } else {
          throw new IllegalArgumentException("Neither off-plane intersection point matched intersection point; intersection = "+intersectionPoint+"; offplane choice 0: "+points[0]+"; offplane choice 1: "+points[1]);
        }
      }
    }
    
    @Override
    public int getCrossingCount() {
      // Doesn't return the actual crossing count -- just gets the even/odd part right
      return Math.min(innerCrossingCount, outerCrossingCount);
    }
    
    @Override
    public boolean isOnEdge() {
      return onEdge;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      // Early exit if the point is on the edge, in which case we accidentally discovered the answer.
      if (edge.isWithin(thePointX, thePointY, thePointZ)) {
        onEdge = true;
        return false;
      }
      
      // All edges that touch the travel planes get assessed the same.  So, for each intersecting edge on both legs:
      // (1) If the edge contains the intersection point, we analyze it on only one leg.  For the other leg, we do nothing.
      // (2) We compute the crossings of the edge with ALL FOUR inner and outer bounding planes.
      // (3) We add the numbers of each kind of crossing to the total for that class of crossing (innerTotal and outerTotal).
      // (4) When done all edges tallied in this way, we take min(innerTotal, outerTotal) and assume that is the number of crossings.
      //
      // Q: What if we see the same edge in both traversals?
      // A: We should really evaluate it only in one.  Keep a hash of the edges we've looked at already and don't process edges twice.

      // Every edge should be looked at only once.
      if (seenEdges != null && seenEdges.contains(edge)) {
        return true;
      }
      if (seenEdges == null) {
        seenEdges = new HashSet<>();
      }
      seenEdges.add(edge);
      
      // We've never seen this edge before.  Evaluate it in the context of inner and outer planes.
      computeInsideOutside();

      /*
      System.out.println("\nThe following edges should intersect the travel/testpoint planes:");
      Edge thisEdge = edge;
      while (true) {
        final GeoPoint[] travelCrossings = travelPlane.findIntersections(planetModel, thisEdge.plane, checkPointCutoffPlane, checkPointOtherCutoffPlane, thisEdge.startPlane, thisEdge.endPlane);
        if (travelCrossings == null || travelCrossings.length > 0) {
          System.out.println("Travel plane: "+thisEdge.startPoint+" -> "+thisEdge.endPoint);
        }
        final GeoPoint[] testPointCrossings = testPointPlane.findIntersections(planetModel, thisEdge.plane, testPointCutoffPlane, testPointOtherCutoffPlane, thisEdge.startPlane, thisEdge.endPlane);
        if (testPointCrossings == null || testPointCrossings.length > 0) {
          System.out.println("Test point plane: "+thisEdge.startPoint+" -> "+thisEdge.endPoint);
        }
        thisEdge = thisEdge.next;
        if (thisEdge == edge) {
          break;
        }
      }
      */
      
      //System.out.println("");
      //System.out.println("Considering edge "+(edge.startPoint)+" -> "+(edge.endPoint));

      // Some edges are going to be given to us even when there's no real intersection, so do that as a sanity check, first.
      final GeoPoint[] travelCrossings = travelPlane.findIntersections(planetModel, edge.plane, checkPointCutoffPlane, checkPointOtherCutoffPlane, edge.startPlane, edge.endPlane);
      if (travelCrossings != null && travelCrossings.length == 0) {
        //System.out.println(" No intersections with travel plane...");
        final GeoPoint[] testPointCrossings = testPointPlane.findIntersections(planetModel, edge.plane, testPointCutoffPlane, testPointOtherCutoffPlane, edge.startPlane, edge.endPlane);
        if (testPointCrossings != null && testPointCrossings.length == 0) {
          // As a last resort, see if the edge endpoints are on either plane.  This is sometimes necessary because the
          // intersection computation logic might not detect near-miss edges otherwise.
          //System.out.println(" No intersections with testpoint plane...");
          if (!travelPlane.evaluateIsZero(edge.startPoint) && !travelPlane.evaluateIsZero(edge.endPoint) &&
            !testPointPlane.evaluateIsZero(edge.startPoint) && !testPointPlane.evaluateIsZero(edge.endPoint)) {
            return true;
          } else {
            //System.out.println(" Startpoint/travelPlane="+travelPlane.evaluate(edge.startPoint)+" Startpoint/testPointPlane="+testPointPlane.evaluate(edge.startPoint));
            //System.out.println(" Endpoint/travelPlane="+travelPlane.evaluate(edge.endPoint)+" Endpoint/testPointPlane="+testPointPlane.evaluate(edge.endPoint));
          }
        } else {
          //System.out.println(" Intersection found with testPoint plane...");
        }
      } else {
        //System.out.println(" Intersection found with travel plane...");
      }

      //System.out.println(" Edge intersects travel or testPoint plane");
      /*
      System.out.println(
        " start point travel dist="+travelPlane.evaluate(edge.startPoint)+"; end point travel dist="+travelPlane.evaluate(edge.endPoint));
      System.out.println(
        " start point travel above dist="+travelAbovePlane.evaluate(edge.startPoint)+"; end point travel above dist="+travelAbovePlane.evaluate(edge.endPoint));
      System.out.println(
        " start point travel below dist="+travelBelowPlane.evaluate(edge.startPoint)+"; end point travel below dist="+travelBelowPlane.evaluate(edge.endPoint));
      System.out.println(
        " start point testpoint dist="+testPointPlane.evaluate(edge.startPoint)+"; end point testpoint dist="+testPointPlane.evaluate(edge.endPoint));
      System.out.println(
        " start point testpoint above dist="+testPointAbovePlane.evaluate(edge.startPoint)+"; end point testpoint above dist="+testPointAbovePlane.evaluate(edge.endPoint));
      System.out.println(
        " start point testpoint below dist="+testPointBelowPlane.evaluate(edge.startPoint)+"; end point testpoint below dist="+testPointBelowPlane.evaluate(edge.endPoint));
      */
      
      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      //System.out.println(" Assessing inner crossings...");
      innerCrossingCount += countCrossings(edge, travelInsidePlane, checkPointCutoffPlane, insideTravelCutoffPlane, testPointInsidePlane, testPointCutoffPlane, insideTestPointCutoffPlane);
      //System.out.println(" Assessing outer crossings...");
      outerCrossingCount += countCrossings(edge, travelOutsidePlane, checkPointCutoffPlane, outsideTravelCutoffPlane, testPointOutsidePlane, testPointCutoffPlane, outsideTestPointCutoffPlane);
      /*
      final GeoPoint[] travelInnerCrossings = computeCrossings(travelInsidePlane, edge, checkPointCutoffPlane, insideTravelCutoffPlane);
      final GeoPoint[] travelOuterCrossings = computeCrossings(travelOutsidePlane, edge, checkPointCutoffPlane, outsideTravelCutoffPlane);
      final GeoPoint[] testPointInnerCrossings = computeCrossings(testPointInsidePlane, edge, testPointCutoffPlane, insideTestPointCutoffPlane);
      final GeoPoint[] testPointOuterCrossings = computeCrossings(testPointOutsidePlane, edge, testPointCutoffPlane, outsideTestPointCutoffPlane);
      */
      
      return true;
    }

    /** Find the intersections with a pair of envelope planes, and assess those intersections for duplication and for
      * whether they truly describe crossings.
      */
    private int countCrossings(final Edge edge,
      final Plane travelEnvelopePlane, final Membership travelEnvelopeBound1, final Membership travelEnvelopeBound2,
      final Plane testPointEnvelopePlane, final Membership testPointEnvelopeBound1, final Membership testPointEnvelopeBound2) {
      final GeoPoint[] travelIntersections = edge.plane.findIntersections(planetModel, travelEnvelopePlane, travelEnvelopeBound1, travelEnvelopeBound2);
      final GeoPoint[] testPointIntersections = edge.plane.findIntersections(planetModel, testPointEnvelopePlane, testPointEnvelopeBound1, testPointEnvelopeBound2);
      int crossings = 0;
      if (travelIntersections != null) {
        for (final GeoPoint intersection : travelIntersections) {
          if (edge.startPlane.strictlyWithin(intersection) && edge.endPlane.strictlyWithin(intersection)) {
            // Make sure it's not a dup
            boolean notDup = true;
            if (testPointIntersections != null) {
              for (final GeoPoint otherIntersection : testPointIntersections) {
                if (edge.startPlane.strictlyWithin(otherIntersection) && edge.endPlane.strictlyWithin(otherIntersection) && intersection.isNumericallyIdentical(otherIntersection)) {
                  //System.out.println("  Points "+intersection+" and "+otherIntersection+" are duplicates");
                  notDup = false;
                  break;
                }
              }
            }
            if (!notDup) {
              continue;
            }
            // It's unique, so assess it
            //System.out.println("  Assessing travel envelope intersection point "+intersection+", travelPlane distance="+travelPlane.evaluate(intersection)+"...");
            crossings += edgeCrossesEnvelope(edge.plane, intersection, travelEnvelopePlane)?1:0;
          }
        }
      }
      if (testPointIntersections != null) {
        for (final GeoPoint intersection : testPointIntersections) {
          if (edge.startPlane.strictlyWithin(intersection) && edge.endPlane.strictlyWithin(intersection)) {
            // It's unique, so assess it
            //System.out.println("  Assessing testpoint envelope intersection point "+intersection+", testPointPlane distance="+testPointPlane.evaluate(intersection)+"...");
            crossings += edgeCrossesEnvelope(edge.plane, intersection, testPointEnvelopePlane)?1:0;
          }
        }
      }
      return crossings;
    }

    /** Return true if the edge crosses the envelope plane, given the envelope intersection point.
      */
    private boolean edgeCrossesEnvelope(final Plane edgePlane, final GeoPoint intersectionPoint, final Plane envelopePlane) {
      final GeoPoint[] adjoiningPoints = findAdjoiningPoints(edgePlane, intersectionPoint, envelopePlane);
      if (adjoiningPoints == null) {
        // Couldn't find good adjoining points, so just assume there is a crossing.
        return true;
      }
      int withinCount = 0;
      for (final GeoPoint adjoining : adjoiningPoints) {
        if ((travelPlane.evaluateIsZero(adjoining) && checkPointCutoffPlane.isWithin(adjoining) && checkPointOtherCutoffPlane.isWithin(adjoining)) ||
          (testPointPlane.evaluateIsZero(adjoining) && testPointCutoffPlane.isWithin(adjoining) && testPointOtherCutoffPlane.isWithin(adjoining))) {
          //System.out.println("   Adjoining point "+adjoining+" (intersection dist = "+intersectionPoint.linearDistance(adjoining)+") is within");
          withinCount++;
        } else {
          //System.out.println("   Adjoining point "+adjoining+" (intersection dist = "+intersectionPoint.linearDistance(adjoining)+"; travelPlane dist="+travelPlane.evaluate(adjoining)+"; testPointPlane dist="+testPointPlane.evaluate(adjoining)+") is not within");
        }
      }
      return (withinCount & 1) != 0;
    }

  }
  

  /** This is the amount we go, roughly, in both directions, to find adjoining points to test.  If we go too far,
    * we might miss a transition, but if we go too little, we might not see it either due to numerical issues.
    */
  private final static double DELTA_DISTANCE = Vector.MINIMUM_RESOLUTION;
  /** This is the maximum number of iterations.  If we get this high, effectively the planes are parallel, and we
    * treat that as a crossing.
    */
  private final static int MAX_ITERATIONS = 100;
  /** This is the amount off of the envelope plane that we count as "enough" for a valid crossing assessment. */
  private final static double OFF_PLANE_AMOUNT = Vector.MINIMUM_RESOLUTION * 0.1;
  
  /** Given a point on the plane and the ellipsoid, this method looks for a pair of adjoining points on either side of the plane, which are
   * about MINIMUM_RESOLUTION away from the given point.  This only works for planes which go through the center of the world.
   * Returns null if the planes are effectively parallel and reasonable adjoining points cannot be determined.
   */
  private GeoPoint[] findAdjoiningPoints(final Plane plane, final GeoPoint pointOnPlane, final Plane envelopePlane) {
    // Compute a normalized perpendicular vector
    final Vector perpendicular = new Vector(plane, pointOnPlane);
    double distanceFactor = 0.0;
    for (int i = 0; i < MAX_ITERATIONS; i++) {
      distanceFactor += DELTA_DISTANCE;
      // Compute two new points along this vector from the original
      final GeoPoint pointA = planetModel.createSurfacePoint(pointOnPlane.x + perpendicular.x * distanceFactor,
        pointOnPlane.y + perpendicular.y * distanceFactor,
        pointOnPlane.z + perpendicular.z * distanceFactor);
      final GeoPoint pointB = planetModel.createSurfacePoint(pointOnPlane.x - perpendicular.x * distanceFactor,
        pointOnPlane.y - perpendicular.y * distanceFactor,
        pointOnPlane.z - perpendicular.z * distanceFactor);
      if (Math.abs(envelopePlane.evaluate(pointA)) > OFF_PLANE_AMOUNT && Math.abs(envelopePlane.evaluate(pointB)) > OFF_PLANE_AMOUNT) {
        //System.out.println("Distance: "+computeSquaredDistance(rval[0], pointOnPlane)+" and "+computeSquaredDistance(rval[1], pointOnPlane));
        return new GeoPoint[]{pointA, pointB};
      }
      // Loop back around and use a bigger delta
    }
    // Had to abort, so return null.
    //System.out.println("     Adjoining points not found.  Are planes parallel?  edge = "+plane+"; envelope = "+envelopePlane+"; perpendicular = "+perpendicular);
    return null;
  }

  private static double computeSquaredDistance(final GeoPoint checkPoint, final GeoPoint intersectionPoint) {
    final double distanceX = checkPoint.x - intersectionPoint.x;
    final double distanceY = checkPoint.y - intersectionPoint.y;
    final double distanceZ = checkPoint.z - intersectionPoint.z;
    return distanceX * distanceX + distanceY * distanceY + distanceZ * distanceZ;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoComplexPolygon))
      return false;
    final GeoComplexPolygon other = (GeoComplexPolygon) o;
    return super.equals(other) && testPoint1InSet == other.testPoint1InSet
        && testPoint1.equals(other.testPoint1)
        && pointsList.equals(other.pointsList);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Boolean.hashCode(testPoint1InSet);
    result = 31 * result + testPoint1.hashCode();
    result = 31 * result + pointsList.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder edgeDescription = new StringBuilder();
    for (final Edge shapeStartEdge : shapeStartEdges) {
      fillInEdgeDescription(edgeDescription, shapeStartEdge);
    }
    return "GeoComplexPolygon: {planetmodel=" + planetModel + ", number of shapes="+shapeStartEdges.length+", address="+ Integer.toHexString(hashCode())+", testPoint="+testPoint1+", testPointInSet="+testPoint1InSet+", shapes={"+edgeDescription+"}}";
  }
  
  private static void fillInEdgeDescription(final StringBuilder description, final Edge startEdge) {
    description.append(" {");
    Edge currentEdge = startEdge;
    int edgeCounter = 0;
    while (true) {
      if (edgeCounter > 0) {
        description.append(", ");
      }
      if (edgeCounter >= 20) {
        description.append("...");
        break;
      }
      description.append(currentEdge.startPoint);
      currentEdge = currentEdge.next;
      if (currentEdge == startEdge) {
        break;
      }
      edgeCounter++;
    }
  }
  
}
  
