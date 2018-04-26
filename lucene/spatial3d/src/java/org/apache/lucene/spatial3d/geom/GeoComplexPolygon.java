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

  private final boolean testPoint2InSet;
  private final GeoPoint testPoint2;
    
  private final Plane testPoint1FixedYPlane;
  private final Plane testPoint1FixedYAbovePlane;
  private final Plane testPoint1FixedYBelowPlane;
  private final Plane testPoint1FixedXPlane;
  private final Plane testPoint1FixedXAbovePlane;
  private final Plane testPoint1FixedXBelowPlane;
  private final Plane testPoint1FixedZPlane;
  private final Plane testPoint1FixedZAbovePlane;
  private final Plane testPoint1FixedZBelowPlane;

  private final Plane testPoint2FixedYPlane;
  private final Plane testPoint2FixedYAbovePlane;
  private final Plane testPoint2FixedYBelowPlane;
  private final Plane testPoint2FixedXPlane;
  private final Plane testPoint2FixedXAbovePlane;
  private final Plane testPoint2FixedXBelowPlane;
  private final Plane testPoint2FixedZPlane;
  private final Plane testPoint2FixedZAbovePlane;
  private final Plane testPoint2FixedZBelowPlane;
  
  private final GeoPoint[] edgePoints;
  private final Edge[] shapeStartEdges;
  
  private final static double NEAR_EDGE_CUTOFF = -Vector.MINIMUM_RESOLUTION * 1000.0;
  
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
        final Edge edge = new Edge(planetModel, lastGeoPoint, thisGeoPoint);
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
    // Pick the antipodes for testPoint2
    this.testPoint2 = new GeoPoint(-testPoint.x, -testPoint.y, -testPoint.z);

    // Construct fixed planes for testPoint1
    this.testPoint1FixedYPlane = new Plane(0.0, 1.0, 0.0, -testPoint1.y);
    this.testPoint1FixedXPlane = new Plane(1.0, 0.0, 0.0, -testPoint1.x);
    this.testPoint1FixedZPlane = new Plane(0.0, 0.0, 1.0, -testPoint1.z);
    
    Plane testPoint1FixedYAbovePlane = new Plane(testPoint1FixedYPlane, true);
    if (testPoint1FixedYAbovePlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumYValue() - testPoint1FixedYAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedYAbovePlane = null;
    }
    this.testPoint1FixedYAbovePlane = testPoint1FixedYAbovePlane;
    
    Plane testPoint1FixedYBelowPlane = new Plane(testPoint1FixedYPlane, false);
    if (testPoint1FixedYBelowPlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF ||  planetModel.getMinimumYValue() - testPoint1FixedYBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedYBelowPlane = null;
    }
    this.testPoint1FixedYBelowPlane = testPoint1FixedYBelowPlane;
    
    Plane testPoint1FixedXAbovePlane = new Plane(testPoint1FixedXPlane, true);
    if (testPoint1FixedXAbovePlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() - testPoint1FixedXAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedXAbovePlane = null;
    }
    this.testPoint1FixedXAbovePlane = testPoint1FixedXAbovePlane;
    
    Plane testPoint1FixedXBelowPlane = new Plane(testPoint1FixedXPlane, false);
    if (testPoint1FixedXBelowPlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() - testPoint1FixedXBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedXBelowPlane = null;
    }
    this.testPoint1FixedXBelowPlane = testPoint1FixedXBelowPlane;
    
    Plane testPoint1FixedZAbovePlane = new Plane(testPoint1FixedZPlane, true);
    if (testPoint1FixedZAbovePlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF ||planetModel.getMinimumZValue() - testPoint1FixedZAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedZAbovePlane = null;
    }
    this.testPoint1FixedZAbovePlane = testPoint1FixedZAbovePlane;
    
    Plane testPoint1FixedZBelowPlane = new Plane(testPoint1FixedZPlane, false);
    if (testPoint1FixedZBelowPlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumZValue() - testPoint1FixedZBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint1FixedZBelowPlane = null;
    }
    this.testPoint1FixedZBelowPlane = testPoint1FixedZBelowPlane;

    // Construct fixed planes for testPoint2
    this.testPoint2FixedYPlane = new Plane(0.0, 1.0, 0.0, -testPoint2.y);
    this.testPoint2FixedXPlane = new Plane(1.0, 0.0, 0.0, -testPoint2.x);
    this.testPoint2FixedZPlane = new Plane(0.0, 0.0, 1.0, -testPoint2.z);
    
    Plane testPoint2FixedYAbovePlane = new Plane(testPoint2FixedYPlane, true);
    if (testPoint2FixedYAbovePlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumYValue() - testPoint2FixedYAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint2FixedYAbovePlane = null;
    }
    this.testPoint2FixedYAbovePlane = testPoint2FixedYAbovePlane;
    
    Plane testPoint2FixedYBelowPlane = new Plane(testPoint2FixedYPlane, false);
    if (testPoint2FixedYBelowPlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF ||  planetModel.getMinimumYValue() - testPoint2FixedYBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint2FixedYBelowPlane = null;
    }
    this.testPoint2FixedYBelowPlane = testPoint2FixedYBelowPlane;
    
    Plane testPoint2FixedXAbovePlane = new Plane(testPoint2FixedXPlane, true);
    if (testPoint2FixedXAbovePlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() - testPoint2FixedXAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint2FixedXAbovePlane = null;
    }
    this.testPoint2FixedXAbovePlane = testPoint2FixedXAbovePlane;
    
    Plane testPoint2FixedXBelowPlane = new Plane(testPoint2FixedXPlane, false);
    if (testPoint2FixedXBelowPlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() - testPoint2FixedXBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint2FixedXBelowPlane = null;
    }
    this.testPoint2FixedXBelowPlane = testPoint2FixedXBelowPlane;
    
    Plane testPoint2FixedZAbovePlane = new Plane(testPoint2FixedZPlane, true);
    if (testPoint2FixedZAbovePlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF ||planetModel.getMinimumZValue() - testPoint2FixedZAbovePlane.D > NEAR_EDGE_CUTOFF) {
        testPoint2FixedZAbovePlane = null;
    }
    this.testPoint2FixedZAbovePlane = testPoint2FixedZAbovePlane;
    
    Plane testPoint2FixedZBelowPlane = new Plane(testPoint2FixedZPlane, false);
    if (testPoint2FixedZBelowPlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumZValue() - testPoint2FixedZBelowPlane.D > NEAR_EDGE_CUTOFF) {
        testPoint2FixedZBelowPlane = null;
    }
    this.testPoint2FixedZBelowPlane = testPoint2FixedZBelowPlane;

    // We know inset/out-of-set for testPoint1 only right now
    this.testPoint1InSet = testPointInSet;

    // We must compute the crossings from testPoint1 to testPoint2 in order to figure out whether testPoint2 is in-set or out
    this.testPoint2InSet = isInSet(testPoint2.x, testPoint2.y, testPoint2.z,
      testPoint1, 
      testPoint1InSet,
      testPoint1FixedXPlane, testPoint1FixedXAbovePlane, testPoint1FixedXBelowPlane,
      testPoint1FixedYPlane, testPoint1FixedYAbovePlane, testPoint1FixedYBelowPlane,
      testPoint1FixedZPlane, testPoint1FixedZAbovePlane, testPoint1FixedZBelowPlane);
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
    try {
      // Try with the primary test point
      return isInSet(x, y, z,
        testPoint1,
        testPoint1InSet,
        testPoint1FixedXPlane, testPoint1FixedXAbovePlane, testPoint1FixedXBelowPlane,
        testPoint1FixedYPlane, testPoint1FixedYAbovePlane, testPoint1FixedYBelowPlane,
        testPoint1FixedZPlane, testPoint1FixedZAbovePlane, testPoint1FixedZBelowPlane);
    } catch (IllegalArgumentException e) {
      // Try with an alternate test point
      return isInSet(x, y, z,
        testPoint2,
        testPoint2InSet,
        testPoint2FixedXPlane, testPoint2FixedXAbovePlane, testPoint2FixedXBelowPlane,
        testPoint2FixedYPlane, testPoint2FixedYAbovePlane, testPoint2FixedYBelowPlane,
        testPoint2FixedZPlane, testPoint2FixedZAbovePlane, testPoint2FixedZBelowPlane);
    }
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

    //System.out.println("\nIswithin called for ["+x+","+y+","+z+"]");
    // If we're right on top of the point, we know the answer.
    if (testPoint.isNumericallyIdentical(x, y, z)) {
      return testPointInSet;
    }
    
    // If we're right on top of any of the test planes, we navigate solely on that plane.
    if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && testPointFixedYPlane.evaluateIsZero(x, y, z)) {
      // Use the XZ plane exclusively.
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPoint, testPointFixedYPlane, testPointFixedYAbovePlane, testPointFixedYBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the y tree because that's fixed.
      if (!yTree.traverse(crossingEdgeIterator, testPoint.y)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && testPointFixedXPlane.evaluateIsZero(x, y, z)) {
      // Use the YZ plane exclusively.
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPoint, testPointFixedXPlane, testPointFixedXAbovePlane, testPointFixedXBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the x tree because that's fixed.
      if (!xTree.traverse(crossingEdgeIterator, testPoint.x)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && testPointFixedZPlane.evaluateIsZero(x, y, z)) {
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPoint, testPointFixedZPlane, testPointFixedZAbovePlane, testPointFixedZBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the z tree because that's fixed.
      if (!zTree.traverse(crossingEdgeIterator, testPoint.z)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedYPlane.evaluateIsZero(x, y, z) || testPointFixedXPlane.evaluateIsZero(x, y, z) || testPointFixedZPlane.evaluateIsZero(x, y, z)) {
      throw new IllegalArgumentException("Can't compute isWithin for specified point");
    } else {

      // This is the expensive part!!
      // Changing the code below has an enormous impact on the queries per second we see with the benchmark.
      
      // We need to use two planes to get there.  We don't know which two planes will do it but we can figure it out.
      final Plane travelPlaneFixedX = new Plane(1.0, 0.0, 0.0, -x);
      final Plane travelPlaneFixedY = new Plane(0.0, 1.0, 0.0, -y);
      final Plane travelPlaneFixedZ = new Plane(0.0, 0.0, 1.0, -z);

      Plane fixedYAbovePlane = new Plane(travelPlaneFixedY, true);
      if (fixedYAbovePlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumYValue() - fixedYAbovePlane.D > NEAR_EDGE_CUTOFF) {
          fixedYAbovePlane = null;
      }
      
      Plane fixedYBelowPlane = new Plane(travelPlaneFixedY, false);
      if (fixedYBelowPlane.D - planetModel.getMaximumYValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumYValue() - fixedYBelowPlane.D > NEAR_EDGE_CUTOFF) {
          fixedYBelowPlane = null;
      }
      
      Plane fixedXAbovePlane = new Plane(travelPlaneFixedX, true);
      if (fixedXAbovePlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() - fixedXAbovePlane.D > NEAR_EDGE_CUTOFF) {
          fixedXAbovePlane = null;
      }
      
      Plane fixedXBelowPlane = new Plane(travelPlaneFixedX, false);
      if (fixedXBelowPlane.D - planetModel.getMaximumXValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumXValue() - fixedXBelowPlane.D > NEAR_EDGE_CUTOFF) {
          fixedXBelowPlane = null;
      }
      
      Plane fixedZAbovePlane = new Plane(travelPlaneFixedZ, true);
      if (fixedZAbovePlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumZValue() - fixedZAbovePlane.D > NEAR_EDGE_CUTOFF) {
          fixedZAbovePlane = null;
      }
      
      Plane fixedZBelowPlane = new Plane(travelPlaneFixedZ, false);
      if (fixedZBelowPlane.D - planetModel.getMaximumZValue() > NEAR_EDGE_CUTOFF || planetModel.getMinimumZValue() - fixedZBelowPlane.D > NEAR_EDGE_CUTOFF) {
          fixedZBelowPlane = null;
      }

      // Find the intersection points for each one of these and the complementary test point planes.

      // There will be multiple intersection points found.  We choose the one that has the lowest total distance, as measured in delta X, delta Y, and delta Z.
      double bestDistance = Double.POSITIVE_INFINITY;
      double firstLegValue = 0.0;
      double secondLegValue = 0.0;
      Plane firstLegPlane = null;
      Plane firstLegAbovePlane = null;
      Plane firstLegBelowPlane = null;
      Plane secondLegPlane = null;
      Plane secondLegAbovePlane = null;
      Plane secondLegBelowPlane = null;
      Tree firstLegTree = null;
      Tree secondLegTree = null;
      GeoPoint intersectionPoint = null;

      if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && fixedXAbovePlane != null && fixedXBelowPlane != null) {
        //check if planes intersects  inside world
        final double checkAbove = 4.0 * (fixedXAbovePlane.D * fixedXAbovePlane.D * planetModel.inverseAbSquared + testPointFixedYAbovePlane.D * testPointFixedYAbovePlane.D * planetModel.inverseAbSquared - 1.0);
        final double checkBelow = 4.0 * (fixedXBelowPlane.D * fixedXBelowPlane.D * planetModel.inverseAbSquared + testPointFixedYBelowPlane.D * testPointFixedYBelowPlane.D * planetModel.inverseAbSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
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
            if (newDistance < bestDistance) {
              bestDistance = newDistance;
              firstLegValue = testPoint.y;
              secondLegValue = x;
              firstLegPlane = testPointFixedYPlane;
              firstLegAbovePlane = testPointFixedYAbovePlane;
              firstLegBelowPlane = testPointFixedYBelowPlane;
              secondLegPlane = travelPlaneFixedX;
              secondLegAbovePlane = fixedXAbovePlane;
              secondLegBelowPlane = fixedXBelowPlane;
              firstLegTree = yTree;
              secondLegTree = xTree;
              intersectionPoint = p;
            }
          }
        }
      }
      if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && fixedXAbovePlane != null && fixedXBelowPlane != null) {
        //check if planes intersects  inside world
        final double checkAbove = 4.0 * (fixedXAbovePlane.D * fixedXAbovePlane.D * planetModel.inverseAbSquared + testPointFixedZAbovePlane.D * testPointFixedZAbovePlane.D * planetModel.inverseCSquared - 1.0);
        final double checkBelow = 4.0 * (fixedXBelowPlane.D * fixedXBelowPlane.D * planetModel.inverseAbSquared + testPointFixedZBelowPlane.D * testPointFixedZBelowPlane.D * planetModel.inverseCSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
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
            if (newDistance < bestDistance) {
              bestDistance = newDistance;
              firstLegValue = testPoint.z;
              secondLegValue = x;
              firstLegPlane = testPointFixedZPlane;
              firstLegAbovePlane = testPointFixedZAbovePlane;
              firstLegBelowPlane = testPointFixedZBelowPlane;
              secondLegPlane = travelPlaneFixedX;
              secondLegAbovePlane = fixedXAbovePlane;
              secondLegBelowPlane = fixedXBelowPlane;
              firstLegTree = zTree;
              secondLegTree = xTree;
              intersectionPoint = p;
            }
          }
        }
      }
      if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && fixedYAbovePlane != null && fixedYBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedXAbovePlane.D * testPointFixedXAbovePlane.D * planetModel.inverseAbSquared + fixedYAbovePlane.D * fixedYAbovePlane.D * planetModel.inverseAbSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedXBelowPlane.D * testPointFixedXBelowPlane.D * planetModel.inverseAbSquared + fixedYBelowPlane.D * fixedYBelowPlane.D * planetModel.inverseAbSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
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
            if (newDistance < bestDistance) {
              bestDistance = newDistance;
              firstLegValue = testPoint.x;
              secondLegValue = y;
              firstLegPlane = testPointFixedXPlane;
              firstLegAbovePlane = testPointFixedXAbovePlane;
              firstLegBelowPlane = testPointFixedXBelowPlane;
              secondLegPlane = travelPlaneFixedY;
              secondLegAbovePlane = fixedYAbovePlane;
              secondLegBelowPlane = fixedYBelowPlane;
              firstLegTree = xTree;
              secondLegTree = yTree;
              intersectionPoint = p;
            }
          }
        }
      }
      if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && fixedYAbovePlane != null && fixedYBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedZAbovePlane.D * testPointFixedZAbovePlane.D * planetModel.inverseCSquared + fixedYAbovePlane.D * fixedYAbovePlane.D * planetModel.inverseAbSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedZBelowPlane.D * testPointFixedZBelowPlane.D * planetModel.inverseCSquared + fixedYBelowPlane.D * fixedYBelowPlane.D * planetModel.inverseAbSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
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
            if (newDistance < bestDistance) {
              bestDistance = newDistance;
              firstLegValue = testPoint.z;
              secondLegValue = y;
              firstLegPlane = testPointFixedZPlane;
              firstLegAbovePlane = testPointFixedZAbovePlane;
              firstLegBelowPlane = testPointFixedZBelowPlane;
              secondLegPlane = travelPlaneFixedY;
              secondLegAbovePlane = fixedYAbovePlane;
              secondLegBelowPlane = fixedYBelowPlane;
              firstLegTree = zTree;
              secondLegTree = yTree;
              intersectionPoint = p;
            }
          }
        }
      }
      if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && fixedZAbovePlane != null && fixedZBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedXAbovePlane.D * testPointFixedXAbovePlane.D * planetModel.inverseAbSquared + fixedZAbovePlane.D * fixedZAbovePlane.D * planetModel.inverseCSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedXBelowPlane.D * testPointFixedXBelowPlane.D * planetModel.inverseAbSquared + fixedZBelowPlane.D * fixedZBelowPlane.D * planetModel.inverseCSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
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
            if (newDistance < bestDistance) {
              bestDistance = newDistance;
              firstLegValue = testPoint.x;
              secondLegValue = z;
              firstLegPlane = testPointFixedXPlane;
              firstLegAbovePlane = testPointFixedXAbovePlane;
              firstLegBelowPlane = testPointFixedXBelowPlane;
              secondLegPlane = travelPlaneFixedZ;
              secondLegAbovePlane = fixedZAbovePlane;
              secondLegBelowPlane = fixedZBelowPlane;
              firstLegTree = xTree;
              secondLegTree = zTree;
              intersectionPoint = p;
            }
          }
        }
      }
      if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && fixedZAbovePlane != null && fixedZBelowPlane != null) {
        //check if planes intersects inside world
        final double checkAbove = 4.0 * (testPointFixedYAbovePlane.D * testPointFixedYAbovePlane.D * planetModel.inverseAbSquared + fixedZAbovePlane.D * fixedZAbovePlane.D * planetModel.inverseCSquared - 1.0);
        final double checkBelow = 4.0 * (testPointFixedYBelowPlane.D * testPointFixedYBelowPlane.D * planetModel.inverseAbSquared + fixedZBelowPlane.D * fixedZBelowPlane.D * planetModel.inverseCSquared - 1.0);
        if (checkAbove < Vector.MINIMUM_RESOLUTION_SQUARED && checkBelow < Vector.MINIMUM_RESOLUTION_SQUARED) {
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
            if (newDistance < bestDistance) {
              bestDistance = newDistance;
              firstLegValue = testPoint.y;
              secondLegValue = z;
              firstLegPlane = testPointFixedYPlane;
              firstLegAbovePlane = testPointFixedYAbovePlane;
              firstLegBelowPlane = testPointFixedYBelowPlane;
              secondLegPlane = travelPlaneFixedZ;
              secondLegAbovePlane = fixedZAbovePlane;
              secondLegBelowPlane = fixedZBelowPlane;
              firstLegTree = yTree;
              secondLegTree = zTree;
              intersectionPoint = p;
            }
          }
        }
      }

      assert bestDistance > 0.0 : "Best distance should not be zero unless on single plane";
      assert bestDistance < Double.POSITIVE_INFINITY : "Couldn't find an intersection point of any kind";

      // First, we'll determine if the intersection point is in set or not
      final CountingEdgeIterator testPointEdgeIterator = createLinearCrossingEdgeIterator(testPoint,
        firstLegPlane, firstLegAbovePlane, firstLegBelowPlane,
        intersectionPoint.x, intersectionPoint.y, intersectionPoint.z);
      // Traverse our way from the test point to the check point.  Use the z tree because that's fixed.
      if (!firstLegTree.traverse(testPointEdgeIterator, firstLegValue)) {
        // Endpoint is on edge
        return true;
      }
      final boolean intersectionPointInSet = ((testPointEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet;

      // Now do the final leg
      final CountingEdgeIterator travelEdgeIterator = createLinearCrossingEdgeIterator(intersectionPoint,
        secondLegPlane, secondLegAbovePlane, secondLegBelowPlane,
        x, y, z);
      // Traverse our way from the test point to the check point.  Use the z tree because that's fixed.
      if (!secondLegTree.traverse(travelEdgeIterator, secondLegValue)) {
        // Endpoint is on edge
        return true;
      }
      return ((travelEdgeIterator.getCrossingCount() & 1) == 0)?intersectionPointInSet:!intersectionPointInSet;
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
    final Plane plane, final Plane abovePlane, final Plane belowPlane, final double thePointX, final double thePointY, final double thePointZ) {
    // If thePoint and testPoint are parallel, we won't be able to determine sidedness of the bounding planes.  So detect that case, and build the iterator differently if we find it.
    // This didn't work; not sure why not:
    //if (testPoint.isParallel(thePointX, thePointY, thePointZ)) {
    //  return new FullLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    //}
    //return new SectorLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    //
    try {
      return new SectorLinearCrossingEdgeIterator(testPoint, plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    } catch (IllegalArgumentException e) {
      // Assume we failed because we could not construct bounding planes, so do it another way.
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
      final GeoPoint interpolationPoint = plane.interpolate(startPoint, endPoint, halfProportions)[0];
      this.backingPlane = new SidedPlane(interpolationPoint, interpolationPoint, 0.0);
      this.planeBounds = new XYZBounds();
      this.planeBounds.addPoint(startPoint);
      this.planeBounds.addPoint(endPoint);
      this.planeBounds.addPlane(pm, this.plane, this.startPlane, this.endPlane, this.backingPlane);
      //System.err.println("Recording edge "+this+" from "+startPoint+" to "+endPoint+"; bounds = "+planeBounds);
    }

    public boolean isWithin(final double thePointX, final double thePointY, final double thePointZ) {
      return plane.evaluateIsZero(thePointX, thePointY, thePointZ) && startPlane.isWithin(thePointX, thePointY, thePointZ) && endPlane.isWithin(thePointX, thePointY, thePointZ) && backingPlane.isWithin(thePointX, thePointY, thePointZ);
    }

    // Hashcode and equals are system default!!
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
    
    private int aboveCrossingCount = 0;
    private int belowCrossingCount = 0;
    
    public FullLinearCrossingEdgeIterator(final GeoPoint testPoint,
      final Plane plane, final Plane abovePlane, final Plane belowPlane, final double thePointX, final double thePointY, final double thePointZ) {
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
    }
    
    @Override
    public int getCrossingCount() {
      if (aboveCrossingCount < belowCrossingCount) {
        return aboveCrossingCount;
      } else {
        return belowCrossingCount;
      }
    }
    
    @Override
    public boolean matches(final Edge edge) {
      // Early exit if the point is on the edge.
      if (edge.isWithin(thePointX, thePointY, thePointZ)) {
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
      
      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      aboveCrossingCount += countCrossings(edge, abovePlane, bound);
      belowCrossingCount += countCrossings(edge, belowPlane, bound);

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
    
    private int aboveCrossingCount = 0;
    private int belowCrossingCount = 0;
    
    public SectorLinearCrossingEdgeIterator(final GeoPoint testPoint,
      final Plane plane, final Plane abovePlane, final Plane belowPlane, final double thePointX, final double thePointY, final double thePointZ) {
      this.testPoint = testPoint;
      this.plane = plane;
      this.abovePlane = abovePlane;
      this.belowPlane = belowPlane;
      // This is safe since we know we aren't doing a full 1/2 a world.
      this.bound1 = new SidedPlane(thePointX, thePointY, thePointZ, plane, testPoint);
      this.bound2 = new SidedPlane(testPoint, plane, thePointX, thePointY, thePointZ);
      this.thePointX = thePointX;
      this.thePointY = thePointY;
      this.thePointZ = thePointZ;
    }
    
    @Override
    public int getCrossingCount() {
      if (aboveCrossingCount < belowCrossingCount) {
        return aboveCrossingCount;
      } else {
        return belowCrossingCount;
      }
    }
    
    @Override
    public boolean matches(final Edge edge) {
      // Early exit if the point is on the edge.
      if (edge.isWithin(thePointX, thePointY, thePointZ)) {
        return false;
      }
      
      // This should precisely mirror what is in DualCrossingIterator, but without the dual crossings.
      // Some edges are going to be given to us even when there's no real intersection, so do that as a sanity check, first.
      final GeoPoint[] planeCrossings = plane.findIntersections(planetModel, edge.plane, bound1, bound2, edge.startPlane, edge.endPlane);
      if (planeCrossings != null && planeCrossings.length == 0) {
        // Sometimes on the hairy edge an intersection will be missed.  This check finds those.
        if (!plane.evaluateIsZero(edge.startPoint) && !plane.evaluateIsZero(edge.endPoint)) {
          return true;
        }
      }
      
      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      aboveCrossingCount += countCrossings(edge, abovePlane, bound1, bound2);
      belowCrossingCount += countCrossings(edge, belowPlane, bound1, bound2);

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
        if (plane.evaluateIsZero(adjoining) && bound1.isWithin(adjoining) && bound2.isWithin(adjoining)) {
          withinCount++;
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
        && testPoint1.equals(testPoint1)
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
  
