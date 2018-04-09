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
  private final boolean testPointInSet;
  private final GeoPoint testPoint;
  
  private final Plane testPointFixedYPlane;
  private final Plane testPointFixedYAbovePlane;
  private final Plane testPointFixedYBelowPlane;
  private final Plane testPointFixedXPlane;
  private final Plane testPointFixedXAbovePlane;
  private final Plane testPointFixedXBelowPlane;
  private final Plane testPointFixedZPlane;
  private final Plane testPointFixedZAbovePlane;
  private final Plane testPointFixedZBelowPlane;
  
  private final GeoPoint[] edgePoints;
  private final Edge[] shapeStartEdges;
  
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
    this.testPointInSet = testPointInSet;
    this.testPoint = testPoint;
    
    this.testPointFixedYPlane = new Plane(0.0, 1.0, 0.0, -testPoint.y);
    this.testPointFixedXPlane = new Plane(1.0, 0.0, 0.0, -testPoint.x);
    this.testPointFixedZPlane = new Plane(0.0, 0.0, 1.0, -testPoint.z);
    
    Plane fixedYAbovePlane = new Plane(testPointFixedYPlane, true);
    if (fixedYAbovePlane.D - planetModel.getMaximumYValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumYValue() - fixedYAbovePlane.D >= Vector.MINIMUM_RESOLUTION) {
        fixedYAbovePlane = null;
    }
    this.testPointFixedYAbovePlane = fixedYAbovePlane;
    
    Plane fixedYBelowPlane = new Plane(testPointFixedYPlane, false);
    if (fixedYBelowPlane.D - planetModel.getMaximumYValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumYValue() - fixedYBelowPlane.D >= Vector.MINIMUM_RESOLUTION) {
        fixedYBelowPlane = null;
    }
    this.testPointFixedYBelowPlane = fixedYBelowPlane;
    
    Plane fixedXAbovePlane = new Plane(testPointFixedXPlane, true);
    if (fixedXAbovePlane.D - planetModel.getMaximumXValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumXValue() - fixedXAbovePlane.D >= Vector.MINIMUM_RESOLUTION) {
        fixedXAbovePlane = null;
    }
    this.testPointFixedXAbovePlane = fixedXAbovePlane;
    
    Plane fixedXBelowPlane = new Plane(testPointFixedXPlane, false);
    if (fixedXBelowPlane.D - planetModel.getMaximumXValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumXValue() - fixedXBelowPlane.D >= Vector.MINIMUM_RESOLUTION) {
        fixedXBelowPlane = null;
    }
    this.testPointFixedXBelowPlane = fixedXBelowPlane;
    
    Plane fixedZAbovePlane = new Plane(testPointFixedZPlane, true);
    if (fixedZAbovePlane.D - planetModel.getMaximumZValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumZValue() - fixedZAbovePlane.D >= Vector.MINIMUM_RESOLUTION) {
        fixedZAbovePlane = null;
    }
    this.testPointFixedZAbovePlane = fixedZAbovePlane;
    
    Plane fixedZBelowPlane = new Plane(testPointFixedZPlane, false);
    if (fixedZBelowPlane.D - planetModel.getMaximumZValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumZValue() - fixedZBelowPlane.D >= Vector.MINIMUM_RESOLUTION) {
        fixedZBelowPlane = null;
    }
    this.testPointFixedZBelowPlane = fixedZBelowPlane;

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
    testPoint.write(outputStream);
    SerializableObject.writeBoolean(outputStream, testPointInSet);
  }

  private static void writePointsList(final OutputStream outputStream, final List<List<GeoPoint>> pointsList) throws IOException {
    SerializableObject.writeInt(outputStream, pointsList.size());
    for (final List<GeoPoint> points : pointsList) {
      SerializableObject.writePointArray(outputStream, points);
    }
  }
  
  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    //System.out.println("\nIswithin called for ["+x+","+y+","+z+"]");
    // If we're right on top of the point, we know the answer.
    if (testPoint.isNumericallyIdentical(x, y, z)) {
      return testPointInSet;
    }
    
    // If we're right on top of any of the test planes, we navigate solely on that plane.
    if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && testPointFixedYPlane.evaluateIsZero(x, y, z)) {
      // Use the XZ plane exclusively.
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPointFixedYPlane, testPointFixedYAbovePlane, testPointFixedYBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the y tree because that's fixed.
      if (!yTree.traverse(crossingEdgeIterator, testPoint.y)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && testPointFixedXPlane.evaluateIsZero(x, y, z)) {
      // Use the YZ plane exclusively.
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPointFixedXPlane, testPointFixedXAbovePlane, testPointFixedXBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the x tree because that's fixed.
      if (!xTree.traverse(crossingEdgeIterator, testPoint.x)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.getCrossingCount() & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && testPointFixedZPlane.evaluateIsZero(x, y, z)) {
      final CountingEdgeIterator crossingEdgeIterator = createLinearCrossingEdgeIterator(testPointFixedZPlane, testPointFixedZAbovePlane, testPointFixedZBelowPlane, x, y, z);
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
      if (fixedYAbovePlane.D - planetModel.getMaximumYValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumYValue() - fixedYAbovePlane.D >= Vector.MINIMUM_RESOLUTION) {
          fixedYAbovePlane = null;
      }
      
      Plane fixedYBelowPlane = new Plane(travelPlaneFixedY, false);
      if (fixedYBelowPlane.D - planetModel.getMaximumYValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumYValue() - fixedYBelowPlane.D >= Vector.MINIMUM_RESOLUTION) {
          fixedYBelowPlane = null;
      }
      
      Plane fixedXAbovePlane = new Plane(travelPlaneFixedX, true);
      if (fixedXAbovePlane.D - planetModel.getMaximumXValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumXValue() - fixedXAbovePlane.D >= Vector.MINIMUM_RESOLUTION) {
          fixedXAbovePlane = null;
      }
      
      Plane fixedXBelowPlane = new Plane(travelPlaneFixedX, false);
      if (fixedXBelowPlane.D - planetModel.getMaximumXValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumXValue() - fixedXBelowPlane.D >= Vector.MINIMUM_RESOLUTION) {
          fixedXBelowPlane = null;
      }
      
      Plane fixedZAbovePlane = new Plane(travelPlaneFixedZ, true);
      if (fixedZAbovePlane.D - planetModel.getMaximumZValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumZValue() - fixedZAbovePlane.D >= Vector.MINIMUM_RESOLUTION) {
          fixedZAbovePlane = null;
      }
      
      Plane fixedZBelowPlane = new Plane(travelPlaneFixedZ, false);
      if (fixedZBelowPlane.D - planetModel.getMaximumZValue() >= Vector.MINIMUM_RESOLUTION || planetModel.getMinimumZValue() - fixedZBelowPlane.D >= Vector.MINIMUM_RESOLUTION) {
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
      if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && fixedXAbovePlane != null && fixedXBelowPlane != null) {
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
      if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && fixedYAbovePlane != null && fixedYBelowPlane != null) {
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
      if (testPointFixedZAbovePlane != null && testPointFixedZBelowPlane != null && fixedYAbovePlane != null && fixedYBelowPlane != null) {
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
      if (testPointFixedXAbovePlane != null && testPointFixedXBelowPlane != null && fixedZAbovePlane != null && fixedZBelowPlane != null) {
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
      if (testPointFixedYAbovePlane != null && testPointFixedYBelowPlane != null && fixedZAbovePlane != null && fixedZBelowPlane != null) {
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

      assert bestDistance > 0.0 : "Best distance should not be zero unless on single plane";
      assert bestDistance < Double.POSITIVE_INFINITY : "Couldn't find an intersection point of any kind";
      
      final DualCrossingEdgeIterator edgeIterator = new DualCrossingEdgeIterator(firstLegPlane, firstLegAbovePlane, firstLegBelowPlane, secondLegPlane, secondLegAbovePlane, secondLegBelowPlane, x, y, z, intersectionPoint);
      if (!firstLegTree.traverse(edgeIterator, firstLegValue)) {
        return true;
      }
      //edgeIterator.setSecondLeg();
      if (!secondLegTree.traverse(edgeIterator, secondLegValue)) {
        return true;
      }
      //System.out.println("Polarity vs. test point: "+(((edgeIterator.getCrossingCount()  & 1) == 0)?"same":"different")+"; testPointInSet: "+testPointInSet);
      return ((edgeIterator.getCrossingCount()  & 1) == 0)?testPointInSet:!testPointInSet;

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
      this.planeBounds = new XYZBounds();
      this.planeBounds.addPoint(startPoint);
      this.planeBounds.addPoint(endPoint);
      this.planeBounds.addPlane(pm, this.plane, this.startPlane, this.endPlane);
      //System.err.println("Recording edge "+this+" from "+startPoint+" to "+endPoint+"; bounds = "+planeBounds);
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
    
    private final Plane plane;
    private final Plane abovePlane;
    private final Plane belowPlane;
    private final Membership bound;
    private final double thePointX;
    private final double thePointY;
    private final double thePointZ;
    
    private int aboveCrossingCount = 0;
    private int belowCrossingCount = 0;
    
    public FullLinearCrossingEdgeIterator(final Plane plane, final Plane abovePlane, final Plane belowPlane, final double thePointX, final double thePointY, final double thePointZ) {
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
      if (edge.plane.evaluateIsZero(thePointX, thePointY, thePointZ) && edge.startPlane.isWithin(thePointX, thePointY, thePointZ) && edge.endPlane.isWithin(thePointX, thePointY, thePointZ)) {
        return false;
      }
      
      // This should precisely mirror what is in DualCrossingIterator, but without the dual crossings.
      // Some edges are going to be given to us even when there's no real intersection, so do that as a sanity check, first.
      final GeoPoint[] planeCrossings = plane.findIntersections(planetModel, edge.plane, bound, edge.startPlane, edge.endPlane);
      if (planeCrossings != null && planeCrossings.length == 0) {
        // No actual crossing
        return true;
      }
      
      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      final GeoPoint[] aboveCrossings = abovePlane.findCrossings(planetModel, edge.plane, bound, edge.startPlane, edge.endPlane);
      final GeoPoint[] belowCrossings = belowPlane.findCrossings(planetModel, edge.plane, bound, edge.startPlane, edge.endPlane);
      
      if (aboveCrossings != null) {
        aboveCrossingCount += aboveCrossings.length;
      }
      if (belowCrossings != null) {
        belowCrossingCount += belowCrossings.length;
      }

      return true;
    }

  }

  /** Create a linear crossing edge iterator with the appropriate cutoff planes given the geometry.
   */
  private CountingEdgeIterator createLinearCrossingEdgeIterator(final Plane plane, final Plane abovePlane, final Plane belowPlane, final double thePointX, final double thePointY, final double thePointZ) {
    // If thePoint and testPoint are parallel, we won't be able to determine sidedness of the bounding planes.  So detect that case, and build the iterator differently if we find it.
    // This didn't work; not sure why not:
    //if (testPoint.isParallel(thePointX, thePointY, thePointZ)) {
    //  return new FullLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    //}
    //return new SectorLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    //
    try {
      return new SectorLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    } catch (IllegalArgumentException e) {
      // Assume we failed because we could not construct bounding planes, so do it another way.
      return new FullLinearCrossingEdgeIterator(plane, abovePlane, belowPlane, thePointX, thePointY, thePointZ);
    }
  }

  /** Count the number of verifiable edge crossings for less than 1/2 a world.
   */
  private class SectorLinearCrossingEdgeIterator implements CountingEdgeIterator {
    
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
    
    public SectorLinearCrossingEdgeIterator(final Plane plane, final Plane abovePlane, final Plane belowPlane, final double thePointX, final double thePointY, final double thePointZ) {
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
      if (edge.plane.evaluateIsZero(thePointX, thePointY, thePointZ) && edge.startPlane.isWithin(thePointX, thePointY, thePointZ) && edge.endPlane.isWithin(thePointX, thePointY, thePointZ)) {
        return false;
      }
      
      // This should precisely mirror what is in DualCrossingIterator, but without the dual crossings.
      // Some edges are going to be given to us even when there's no real intersection, so do that as a sanity check, first.
      final GeoPoint[] planeCrossings = plane.findIntersections(planetModel, edge.plane, bound1, bound2, edge.startPlane, edge.endPlane);
      if (planeCrossings != null && planeCrossings.length == 0) {
        // No actual crossing
        return true;
      }
      
      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      final GeoPoint[] aboveCrossings = abovePlane.findCrossings(planetModel, edge.plane, bound1, bound2, edge.startPlane, edge.endPlane);
      final GeoPoint[] belowCrossings = belowPlane.findCrossings(planetModel, edge.plane, bound1, bound2, edge.startPlane, edge.endPlane);
      
      if (aboveCrossings != null) {
        aboveCrossingCount += aboveCrossings.length;
      }
      if (belowCrossings != null) {
        belowCrossingCount += belowCrossings.length;
      }

      return true;
    }

  }
  
  /** Count the number of verifiable edge crossings for a dual-leg journey.
   */
  private class DualCrossingEdgeIterator implements EdgeIterator {
    
    // This is a hash of which edges we've already looked at and tallied, so we don't repeat ourselves.
    // It is lazily initialized since most transitions cross no edges at all.
    private Set<Edge> seenEdges = null;
    
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
    public int innerCrossingCount = 0;
    public int outerCrossingCount = 0;

    public DualCrossingEdgeIterator(final Plane testPointPlane, final Plane testPointAbovePlane, final Plane testPointBelowPlane,
      final Plane travelPlane, final Plane travelAbovePlane, final Plane travelBelowPlane,
      final double thePointX, final double thePointY, final double thePointZ, final GeoPoint intersectionPoint) {
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
        
      assert travelPlane.evaluateIsZero(intersectionPoint) : "intersection point must be on travel plane";
      assert testPointPlane.evaluateIsZero(intersectionPoint) : "intersection point must be on test point plane";
        
      assert !testPoint.isNumericallyIdentical(intersectionPoint) : "test point is the same as intersection point";
      assert !intersectionPoint.isNumericallyIdentical(thePointX, thePointY, thePointZ) : "check point is same is intersection point";

      this.testPointCutoffPlane = new SidedPlane(intersectionPoint, testPointPlane, testPoint);
      this.checkPointCutoffPlane = new SidedPlane(intersectionPoint, travelPlane, thePointX, thePointY, thePointZ);
      this.testPointOtherCutoffPlane = new SidedPlane(testPoint, testPointPlane, intersectionPoint);
      this.checkPointOtherCutoffPlane = new SidedPlane(thePointX, thePointY, thePointZ, travelPlane, intersectionPoint);

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

        assert ((aboveAbove.length > 0)?1:0) + ((aboveBelow.length > 0)?1:0) + ((belowBelow.length > 0)?1:0) + ((belowAbove.length > 0)?1:0) == 1 : "Can be exactly one inside point, instead was: aa="+aboveAbove.length+" ab=" + aboveBelow.length+" bb="+ belowBelow.length+" ba=" + belowAbove.length;
        
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
        } else {
          travelInsidePlane = travelBelowPlane;
          testPointInsidePlane = testPointAbovePlane;
          travelOutsidePlane = travelAbovePlane;
          testPointOutsidePlane = testPointBelowPlane;
          insideInsidePoints = belowAbove;
        }
        
        // Get the inside-inside intersection point
        // Picking which point, out of two, that corresponds to the already-selected intersectionPoint, is tricky, but it must be done.
        // We expect the choice to be within a small delta of the intersection point in 2 of the dimensions, but not the third
        final GeoPoint insideInsidePoint = pickProximate(insideInsidePoints);
        
        // Get the outside-outside intersection point
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
    
    public int getCrossingCount() {
      // Doesn't return the actual crossing count -- just gets the even/odd part right
      if (innerCrossingCount < outerCrossingCount) {
        return innerCrossingCount;
      } else {
        return outerCrossingCount;
      }
    }
    
    @Override
    public boolean matches(final Edge edge) {
      // Early exit if the point is on the edge, in which case we accidentally discovered the answer.
      if (edge.plane.evaluateIsZero(thePointX, thePointY, thePointZ) && edge.startPlane.isWithin(thePointX, thePointY, thePointZ) && edge.endPlane.isWithin(thePointX, thePointY, thePointZ)) {
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
      
      //System.out.println("Considering edge "+(edge.startPoint)+" -> "+(edge.endPoint));
      
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
      System.out.println("");
      */
      
      // Some edges are going to be given to us even when there's no real intersection, so do that as a sanity check, first.
      final GeoPoint[] travelCrossings = travelPlane.findIntersections(planetModel, edge.plane, checkPointCutoffPlane, checkPointOtherCutoffPlane, edge.startPlane, edge.endPlane);
      if (travelCrossings != null && travelCrossings.length == 0) {
        final GeoPoint[] testPointCrossings = testPointPlane.findIntersections(planetModel, edge.plane, testPointCutoffPlane, testPointOtherCutoffPlane, edge.startPlane, edge.endPlane);
        if (testPointCrossings != null && testPointCrossings.length == 0) {
          return true;
        }
      }
      
      // Determine crossings of this edge against all inside/outside planes.  There's no further need to look at the actual travel plane itself.
      final GeoPoint[] travelInnerCrossings = travelInsidePlane.findCrossings(planetModel, edge.plane, checkPointCutoffPlane, insideTravelCutoffPlane, edge.startPlane, edge.endPlane);
      final GeoPoint[] travelOuterCrossings = travelOutsidePlane.findCrossings(planetModel, edge.plane, checkPointCutoffPlane, outsideTravelCutoffPlane, edge.startPlane, edge.endPlane);
      final GeoPoint[] testPointInnerCrossings = testPointInsidePlane.findCrossings(planetModel, edge.plane, testPointCutoffPlane, insideTestPointCutoffPlane, edge.startPlane, edge.endPlane);
      final GeoPoint[] testPointOuterCrossings = testPointOutsidePlane.findCrossings(planetModel, edge.plane, testPointCutoffPlane, outsideTestPointCutoffPlane, edge.startPlane, edge.endPlane);
      
      // If the edge goes through the inner-inner intersection point, or the outer-outer intersection point, we need to be sure we count that only once.
      // It may appear in both lists.  Use a hash for this right now.
      final Set<GeoPoint> countingHash = new HashSet<>(2);
      
      if (travelInnerCrossings != null) {
        for (final GeoPoint crossing : travelInnerCrossings) {
          //System.out.println("  Travel inner point "+crossing);
          countingHash.add(crossing);
        }
      }
      if (testPointInnerCrossings != null) {
        for (final GeoPoint crossing : testPointInnerCrossings) {
          //System.out.println("  Test point inner point "+crossing);
          countingHash.add(crossing);
        }
      }
      //System.out.println(" Edge added "+countingHash.size()+" to innerCrossingCount");
      innerCrossingCount += countingHash.size();
      
      countingHash.clear();
      if (travelOuterCrossings != null) {
        for (final GeoPoint crossing : travelOuterCrossings) {
          //System.out.println("  Travel outer point "+crossing);
          countingHash.add(crossing);
        }
      }
      if (testPointOuterCrossings != null) {
        for (final GeoPoint crossing : testPointOuterCrossings) {
          //System.out.println("  Test point outer point "+crossing);
          countingHash.add(crossing);
        }
      }
      //System.out.println(" Edge added "+countingHash.size()+" to outerCrossingCount");
      outerCrossingCount += countingHash.size();

      return true;
    }

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
    return super.equals(other) && testPointInSet == other.testPointInSet
        && testPoint.equals(testPoint)
        && pointsList.equals(other.pointsList);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Boolean.hashCode(testPointInSet);
    result = 31 * result + testPoint.hashCode();
    result = 31 * result + pointsList.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder edgeDescription = new StringBuilder();
    for (final Edge shapeStartEdge : shapeStartEdges) {
      fillInEdgeDescription(edgeDescription, shapeStartEdge);
    }
    return "GeoComplexPolygon: {planetmodel=" + planetModel + ", number of shapes="+shapeStartEdges.length+", address="+ Integer.toHexString(hashCode())+", testPoint="+testPoint+", testPointInSet="+testPointInSet+", shapes={"+edgeDescription+"}}";
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
  
