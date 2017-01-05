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
    this.testPointInSet = testPointInSet;
    this.testPoint = testPoint;
    
    this.testPointFixedYPlane = new Plane(0.0, 1.0, 0.0, -testPoint.y);
    this.testPointFixedXPlane = new Plane(1.0, 0.0, 0.0, -testPoint.x);
    this.testPointFixedZPlane = new Plane(0.0, 0.0, 1.0, -testPoint.z);
    
    this.testPointFixedYAbovePlane = new Plane(testPointFixedYPlane, true);
    this.testPointFixedYBelowPlane = new Plane(testPointFixedYPlane, false);
    this.testPointFixedXAbovePlane = new Plane(testPointFixedXPlane, true);
    this.testPointFixedXBelowPlane = new Plane(testPointFixedXPlane, false);
    this.testPointFixedZAbovePlane = new Plane(testPointFixedZPlane, true);
    this.testPointFixedZBelowPlane = new Plane(testPointFixedZPlane, false);

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

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    // If we're right on top of the point, we know the answer.
    if (testPoint.isNumericallyIdentical(x, y, z)) {
      return testPointInSet;
    }
    
    // If we're right on top of any of the test planes, we navigate solely on that plane.
    if (testPointFixedYPlane.evaluateIsZero(x, y, z)) {
      // Use the XZ plane exclusively.
      final LinearCrossingEdgeIterator crossingEdgeIterator = new LinearCrossingEdgeIterator(testPointFixedYPlane, testPointFixedYAbovePlane, testPointFixedYBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the y tree because that's fixed.
      if (!yTree.traverse(crossingEdgeIterator, testPoint.y)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.crossingCount & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedXPlane.evaluateIsZero(x, y, z)) {
      // Use the YZ plane exclusively.
      final LinearCrossingEdgeIterator crossingEdgeIterator = new LinearCrossingEdgeIterator(testPointFixedXPlane, testPointFixedXAbovePlane, testPointFixedXBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the x tree because that's fixed.
      if (!xTree.traverse(crossingEdgeIterator, testPoint.x)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.crossingCount & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedZPlane.evaluateIsZero(x, y, z)) {
      // Use the XY plane exclusively.
      final LinearCrossingEdgeIterator crossingEdgeIterator = new LinearCrossingEdgeIterator(testPointFixedZPlane, testPointFixedZAbovePlane, testPointFixedZBelowPlane, x, y, z);
      // Traverse our way from the test point to the check point.  Use the z tree because that's fixed.
      if (!zTree.traverse(crossingEdgeIterator, testPoint.z)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.crossingCount & 1) == 0)?testPointInSet:!testPointInSet;
    } else {
      
      // This is the expensive part!!
      // Changing the code below has an enormous impact on the queries per second we see with the benchmark.
      
      // We need to use two planes to get there.  We don't know which two planes will do it but we can figure it out.
      final Plane travelPlaneFixedX = new Plane(1.0, 0.0, 0.0, -x);
      final Plane travelPlaneFixedY = new Plane(0.0, 1.0, 0.0, -y);
      final Plane travelPlaneFixedZ = new Plane(0.0, 0.0, 1.0, -z);

      // Find the intersection points for each one of these and the complementary test point planes.
      final GeoPoint[] XIntersectionsY = travelPlaneFixedX.findIntersections(planetModel, testPointFixedYPlane);
      final GeoPoint[] XIntersectionsZ = travelPlaneFixedX.findIntersections(planetModel, testPointFixedZPlane);
      final GeoPoint[] YIntersectionsX = travelPlaneFixedY.findIntersections(planetModel, testPointFixedXPlane);
      final GeoPoint[] YIntersectionsZ = travelPlaneFixedY.findIntersections(planetModel, testPointFixedZPlane);
      final GeoPoint[] ZIntersectionsX = travelPlaneFixedZ.findIntersections(planetModel, testPointFixedXPlane);
      final GeoPoint[] ZIntersectionsY = travelPlaneFixedZ.findIntersections(planetModel, testPointFixedYPlane);

      // There will be multiple intersection points found.  We choose the one that has the lowest total distance, as measured in delta X, delta Y, and delta Z.
      double bestDistance = Double.POSITIVE_INFINITY;
      double firstLegValue = 0.0;
      double secondLegValue = 0.0;
      Plane firstLegPlane = null;
      Plane firstLegAbovePlane = null;
      Plane firstLegBelowPlane = null;
      Plane secondLegPlane = null;
      Tree firstLegTree = null;
      Tree secondLegTree = null;
      GeoPoint intersectionPoint = null;
      
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
          firstLegTree = yTree;
          secondLegTree = xTree;
          intersectionPoint = p;
        }
      }
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
          firstLegTree = zTree;
          secondLegTree = xTree;
          intersectionPoint = p;
        }
      }
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
          firstLegTree = xTree;
          secondLegTree = yTree;
          intersectionPoint = p;
        }
      }
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
          firstLegTree = zTree;
          secondLegTree = yTree;
          intersectionPoint = p;
        }
      }
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
          firstLegTree = xTree;
          secondLegTree = zTree;
          intersectionPoint = p;
        }
      }
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
          firstLegTree = yTree;
          secondLegTree = zTree;
          intersectionPoint = p;
        }
      }

      assert bestDistance > 0.0 : "Best distance should not be zero unless on single plane";
      assert bestDistance < Double.POSITIVE_INFINITY : "Couldn't find an intersection point of any kind";
      
      final DualCrossingEdgeIterator edgeIterator = new DualCrossingEdgeIterator(firstLegPlane, firstLegAbovePlane, firstLegBelowPlane, secondLegPlane, x, y, z, intersectionPoint);
      if (!firstLegTree.traverse(edgeIterator, firstLegValue)) {
        return true;
      }
      edgeIterator.setSecondLeg();
      if (!secondLegTree.traverse(edgeIterator, secondLegValue)) {
        return true;
      }
      return ((edgeIterator.crossingCount  & 1) == 0)?testPointInSet:!testPointInSet;

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
        if (right != null && minValue >= low && right.traverse(edgeIterator, minValue, maxValue) == false) {
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
  
  /** Count the number of verifiable edge crossings.
   */
  private class LinearCrossingEdgeIterator implements EdgeIterator {
    
    private final Plane plane;
    private final Plane abovePlane;
    private final Plane belowPlane;
    private final Membership bound1;
    private final Membership bound2;
    private final double thePointX;
    private final double thePointY;
    private final double thePointZ;
    
    public int crossingCount = 0;
    
    public LinearCrossingEdgeIterator(final Plane plane, final Plane abovePlane, final Plane belowPlane, final double thePointX, final double thePointY, final double thePointZ) {
      this.plane = plane;
      this.abovePlane = abovePlane;
      this.belowPlane = belowPlane;
      this.bound1 = new SidedPlane(thePointX, thePointY, thePointZ, plane, testPoint);
      this.bound2 = new SidedPlane(testPoint, plane, thePointX, thePointY, thePointZ);
      this.thePointX = thePointX;
      this.thePointY = thePointY;
      this.thePointZ = thePointZ;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      // Early exit if the point is on the edge.
      if (edge.plane.evaluateIsZero(thePointX, thePointY, thePointZ) && edge.startPlane.isWithin(thePointX, thePointY, thePointZ) && edge.endPlane.isWithin(thePointX, thePointY, thePointZ)) {
        return false;
      }
      final GeoPoint[] crossingPoints = plane.findCrossings(planetModel, edge.plane, bound1, bound2, edge.startPlane, edge.endPlane);
      if (crossingPoints != null) {
        // We need to handle the endpoint case, which is quite tricky.
        for (final GeoPoint crossingPoint : crossingPoints) {
          countCrossingPoint(crossingPoint, edge);
        }
      }
      return true;
    }

    private void countCrossingPoint(final GeoPoint crossingPoint, final Edge edge) {
      if (crossingPoint.isNumericallyIdentical(edge.startPoint)) {
        // We have to figure out if this crossing should be counted.
        
        // Does the crossing for this edge go up, or down?  Or can't we tell?
        final GeoPoint[] aboveIntersections = abovePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
        final GeoPoint[] belowIntersections = belowPlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
        
        assert !(aboveIntersections.length > 0 && belowIntersections.length > 0) : "edge that ends in a crossing can't both up and down";
        
        if (aboveIntersections.length == 0 && belowIntersections.length == 0) {
          return;
        }

        final boolean edgeCrossesAbove = aboveIntersections.length > 0;

        // This depends on the previous edge that first departs from identicalness.
        Edge assessEdge = edge;
        GeoPoint[] assessAboveIntersections;
        GeoPoint[] assessBelowIntersections;
        while (true) {
          assessEdge = assessEdge.previous;
          assessAboveIntersections = abovePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);
          assessBelowIntersections = belowPlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);

          assert !(assessAboveIntersections.length > 0 && assessBelowIntersections.length > 0) : "assess edge that ends in a crossing can't both up and down";

          if (assessAboveIntersections.length == 0 && assessBelowIntersections.length == 0) {
            continue;
          }
          break;
        }
        
        // Basically, we now want to assess whether both edges that come together at this endpoint leave the plane in opposite
        // directions.  If they do, then we should count it as a crossing; if not, we should not.  We also have to remember that
        // each edge we look at can also be looked at again if it, too, seems to cross the plane.
        
        // To handle the latter situation, we need to know if the other edge will be looked at also, and then we can make
        // a decision whether to count or not based on that.
        
        // Compute the crossing points of this other edge.
        final GeoPoint[] otherCrossingPoints = plane.findCrossings(planetModel, assessEdge.plane, bound1, bound2, assessEdge.startPlane, assessEdge.endPlane);
        
        // Look for a matching endpoint.  If the other endpoint doesn't show up, it is either out of bounds (in which case the
        // transition won't be counted for that edge), or it is not a crossing for that edge (so, same conclusion).
        for (final GeoPoint otherCrossingPoint : otherCrossingPoints) {
          if (otherCrossingPoint.isNumericallyIdentical(assessEdge.endPoint)) {
            // Found it!
            // Both edges will try to contribute to the crossing count.  By convention, we'll only include the earlier one.
            // Since we're the latter point, we exit here in that case.
            return;
          }
        }
        
        // Both edges will not count the same point, so we can proceed.  We need to determine the direction of both edges at the
        // point where they hit the plane.  This may be complicated by the 3D geometry; it may not be safe just to look at the endpoints of the edges
        // and make an assessment that way, since a single edge can intersect the plane at more than one point.
        
        final boolean assessEdgeAbove = assessAboveIntersections.length > 0;
        if (assessEdgeAbove != edgeCrossesAbove) {
          crossingCount++;
        }
        
      } else if (crossingPoint.isNumericallyIdentical(edge.endPoint)) {
        // Figure out if the crossing should be counted.
        
        // Does the crossing for this edge go up, or down?  Or can't we tell?
        final GeoPoint[] aboveIntersections = abovePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
        final GeoPoint[] belowIntersections = belowPlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
        
        assert !(aboveIntersections.length > 0 && belowIntersections.length > 0) : "edge that ends in a crossing can't both up and down";
        
        if (aboveIntersections.length == 0 && belowIntersections.length == 0) {
          return;
        }

        final boolean edgeCrossesAbove = aboveIntersections.length > 0;

        // This depends on the previous edge that first departs from identicalness.
        Edge assessEdge = edge;
        GeoPoint[] assessAboveIntersections;
        GeoPoint[] assessBelowIntersections;
        while (true) {
          assessEdge = assessEdge.next;
          assessAboveIntersections = abovePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);
          assessBelowIntersections = belowPlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);

          assert !(assessAboveIntersections.length > 0 && assessBelowIntersections.length > 0) : "assess edge that ends in a crossing can't both up and down";

          if (assessAboveIntersections.length == 0 && assessBelowIntersections.length == 0) {
            continue;
          }
          break;
        }
        
        // Basically, we now want to assess whether both edges that come together at this endpoint leave the plane in opposite
        // directions.  If they do, then we should count it as a crossing; if not, we should not.  We also have to remember that
        // each edge we look at can also be looked at again if it, too, seems to cross the plane.
        
        // By definition, we're the earlier plane in this case, so any crossing we detect we must count, by convention.  It is unnecessary
        // to consider what the other edge does, because when we get to it, it will look back and figure out what we did for this one.
        
        // We need to determine the direction of both edges at the
        // point where they hit the plane.  This may be complicated by the 3D geometry; it may not be safe just to look at the endpoints of the edges
        // and make an assessment that way, since a single edge can intersect the plane at more than one point.

        final boolean assessEdgeAbove = assessAboveIntersections.length > 0;
        if (assessEdgeAbove != edgeCrossesAbove) {
          crossingCount++;
        }

      } else {
        crossingCount++;
      }
    }
  }
  
  /** Count the number of verifiable edge crossings for a dual-leg journey.
   */
  private class DualCrossingEdgeIterator implements EdgeIterator {
    
    private boolean isSecondLeg = false;
    
    private final Plane testPointPlane;
    private final Plane testPointAbovePlane;
    private final Plane testPointBelowPlane;
    private final Plane travelPlane;
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
    
    // The counter
    
    public int crossingCount = 0;

    public DualCrossingEdgeIterator(final Plane testPointPlane, final Plane testPointAbovePlane, final Plane testPointBelowPlane,
      final Plane travelPlane, final double thePointX, final double thePointY, final double thePointZ, final GeoPoint intersectionPoint) {
      this.testPointPlane = testPointPlane;
      this.testPointAbovePlane = testPointAbovePlane;
      this.testPointBelowPlane = testPointBelowPlane;
      this.travelPlane = travelPlane;
      this.thePointX = thePointX;
      this.thePointY = thePointY;
      this.thePointZ = thePointZ;
      this.intersectionPoint = intersectionPoint;
      
      //System.err.println("Intersection point = "+intersectionPoint);
        
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
        final Plane travelAbovePlane = new Plane(travelPlane, true);
        final Plane travelBelowPlane = new Plane(travelPlane, false);
        
        final GeoPoint[] aboveAbove = travelAbovePlane.findIntersections(planetModel, testPointAbovePlane, intersectionBound1, intersectionBound2);
        assert aboveAbove != null : "Above + above should not be coplanar";
        final GeoPoint[] aboveBelow = travelAbovePlane.findIntersections(planetModel, testPointBelowPlane, intersectionBound1, intersectionBound2);
        assert aboveBelow != null : "Above + below should not be coplanar";
        final GeoPoint[] belowBelow = travelBelowPlane.findIntersections(planetModel, testPointBelowPlane, intersectionBound1, intersectionBound2);
        assert belowBelow != null : "Below + below should not be coplanar";
        final GeoPoint[] belowAbove = travelBelowPlane.findIntersections(planetModel, testPointAbovePlane, intersectionBound1, intersectionBound2);
        assert belowAbove != null : "Below + above should not be coplanar";

        assert ((aboveAbove.length > 0)?1:0) + ((aboveBelow.length > 0)?1:0) + ((belowBelow.length > 0)?1:0) + ((belowAbove.length > 0)?1:0) == 1 : "Can be exactly one inside point, instead was: aa="+aboveAbove.length+" ab=" + aboveBelow.length+" bb="+ belowBelow.length+" ba=" + belowAbove.length;
        
        if (aboveAbove.length > 0) {
          travelInsidePlane = travelAbovePlane;
          testPointInsidePlane = testPointAbovePlane;
          travelOutsidePlane = travelBelowPlane;
          testPointOutsidePlane = testPointBelowPlane;
        } else if (aboveBelow.length > 0) {
          travelInsidePlane = travelAbovePlane;
          testPointInsidePlane = testPointBelowPlane;
          travelOutsidePlane = travelBelowPlane;
          testPointOutsidePlane = testPointAbovePlane;
        } else if (belowBelow.length > 0) {
          travelInsidePlane = travelBelowPlane;
          testPointInsidePlane = testPointBelowPlane;
          travelOutsidePlane = travelAbovePlane;
          testPointOutsidePlane = testPointAbovePlane;
        } else {
          travelInsidePlane = travelBelowPlane;
          testPointInsidePlane = testPointAbovePlane;
          travelOutsidePlane = travelAbovePlane;
          testPointOutsidePlane = testPointBelowPlane;
        }
        
        insideTravelCutoffPlane = new SidedPlane(thePointX, thePointY, thePointZ, testPointInsidePlane, testPointInsidePlane.D);
        insideTestPointCutoffPlane = new SidedPlane(testPoint, travelInsidePlane, travelInsidePlane.D);
        computedInsideOutside = true;
      }
    }

    public void setSecondLeg() {
      isSecondLeg = true;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      //System.err.println("Processing edge "+edge+", startpoint="+edge.startPoint+" endpoint="+edge.endPoint);
      // Early exit if the point is on the edge.
      if (edge.plane.evaluateIsZero(thePointX, thePointY, thePointZ) && edge.startPlane.isWithin(thePointX, thePointY, thePointZ) && edge.endPlane.isWithin(thePointX, thePointY, thePointZ)) {
        //System.err.println(" Check point is on edge: isWithin = true");
        return false;
      }
      // If the intersection point lies on this edge, we should still be able to consider crossing points only.
      // Even if an intersection point is eliminated because it's not a crossing of one plane, it will have to be a crossing
      // for at least one of the two planes in order to be a legitimate crossing of the combined path.
      final GeoPoint[] crossingPoints;
      if (isSecondLeg) {
        //System.err.println(" check point plane = "+travelPlane);
        crossingPoints = travelPlane.findCrossings(planetModel, edge.plane, checkPointCutoffPlane, checkPointOtherCutoffPlane, edge.startPlane, edge.endPlane);
      } else {
        //System.err.println(" test point plane = "+testPointPlane);
        crossingPoints = testPointPlane.findCrossings(planetModel, edge.plane, testPointCutoffPlane, testPointOtherCutoffPlane, edge.startPlane, edge.endPlane);
      }
      if (crossingPoints != null) {
        // We need to handle the endpoint case, which is quite tricky.
        for (final GeoPoint crossingPoint : crossingPoints) {
          countCrossingPoint(crossingPoint, edge);
        }
        //System.err.println(" All crossing points processed");
      } else {
        //System.err.println(" No crossing points!");
      }
      return true;
    }

    private void countCrossingPoint(final GeoPoint crossingPoint, final Edge edge) {
      //System.err.println(" Crossing point "+crossingPoint);
      // We consider crossing points only in this method.
      // Unlike the linear case, there are additional cases when:
      // (1) The crossing point and the intersection point are the same, but are not the endpoint of an edge;
      // (2) The crossing point and the intersection point are the same, and they *are* the endpoint of an edge.
      // The other logical difference is that crossings of all kinds have to be considered so that:
      // (a) both inside edges are considered together at all times;
      // (b) both outside edges are considered together at all times;
      // (c) inside edge crossings that are between the other leg's inside and outside edge are ignored.
      
      // Intersection point crossings are either simple, or a crossing on an endpoint.
      // In either case, we have to be sure to count each edge only once, since it might appear in both the
      // first leg and the second.  If the first leg can process it, it should, and the second should skip it.
      if (crossingPoint.isNumericallyIdentical(intersectionPoint)) {
        //System.err.println(" Crosses intersection point.");
        if (isSecondLeg) {
          // See whether this edge would have been processed in the first leg; if so, we skip it.
          final GeoPoint[] firstLegCrossings = testPointPlane.findCrossings(planetModel, edge.plane, testPointCutoffPlane, testPointOtherCutoffPlane, edge.startPlane, edge.endPlane);
          for (final GeoPoint firstLegCrossing : firstLegCrossings) {
            if (firstLegCrossing.isNumericallyIdentical(intersectionPoint)) {
              // We already processed it, so we're done here.
              //System.err.println("  Already processed on previous leg: exit");
              return;
            }
          }
        }
      }
        
      // Plane crossing, either first leg or second leg
      
      if (crossingPoint.isNumericallyIdentical(edge.startPoint)) {
        //System.err.println(" Crossing point = edge.startPoint");
        // We have to figure out if this crossing should be counted.
        computeInsideOutside();
        
        // Does the crossing for this edge go up, or down?  Or can't we tell?
        final GeoPoint[] insideTestPointPlaneIntersections = testPointInsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane, insideTestPointCutoffPlane);
        final GeoPoint[] insideTravelPlaneIntersections = travelInsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane, insideTravelCutoffPlane);
        final GeoPoint[] outsideTestPointPlaneIntersections = testPointOutsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
        final GeoPoint[] outsideTravelPlaneIntersections = travelOutsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
          
        assert !(insideTestPointPlaneIntersections.length + insideTravelPlaneIntersections.length > 0 && outsideTestPointPlaneIntersections.length + outsideTravelPlaneIntersections.length > 0) : "edge that ends in a crossing can't both up and down";
          
        if (insideTestPointPlaneIntersections.length + insideTravelPlaneIntersections.length == 0 && outsideTestPointPlaneIntersections.length + outsideTravelPlaneIntersections.length == 0) {
          //System.err.println(" No inside or outside crossings found");
          return;
        }

        final boolean edgeCrossesInside = insideTestPointPlaneIntersections.length + insideTravelPlaneIntersections.length > 0;

        // This depends on the previous edge that first departs from identicalness.
        Edge assessEdge = edge;
        GeoPoint[] assessInsideTestPointIntersections;
        GeoPoint[] assessInsideTravelIntersections;
        GeoPoint[] assessOutsideTestPointIntersections;
        GeoPoint[] assessOutsideTravelIntersections;
        while (true) {
          assessEdge = assessEdge.previous;
          assessInsideTestPointIntersections = testPointInsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane, insideTestPointCutoffPlane);
          assessInsideTravelIntersections = travelInsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane, insideTravelCutoffPlane);
          assessOutsideTestPointIntersections = testPointOutsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);
          assessOutsideTravelIntersections = travelOutsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);

          // An edge can cross both outside and inside, because of the corner.  But it can be considered to cross the inside ONLY if it crosses either of the inside edges.
          //assert !(assessInsideTestPointIntersections.length + assessInsideTravelIntersections.length > 0 && assessOutsideTestPointIntersections.length + assessOutsideTravelIntersections.length > 0) : "assess edge that ends in a crossing can't both up and down";

          if (assessInsideTestPointIntersections.length + assessInsideTravelIntersections.length == 0 && assessOutsideTestPointIntersections.length + assessOutsideTravelIntersections.length == 0) {
            continue;
          }
          break;
        }

        // Basically, we now want to assess whether both edges that come together at this endpoint leave the plane in opposite
        // directions.  If they do, then we should count it as a crossing; if not, we should not.  We also have to remember that
        // each edge we look at can also be looked at again if it, too, seems to cross the plane.
          
        // To handle the latter situation, we need to know if the other edge will be looked at also, and then we can make
        // a decision whether to count or not based on that.
          
        // Compute the crossing points of this other edge.
        final GeoPoint[] otherCrossingPoints;
        if (isSecondLeg) {
          otherCrossingPoints = travelPlane.findCrossings(planetModel, assessEdge.plane, checkPointCutoffPlane, checkPointOtherCutoffPlane, assessEdge.startPlane, assessEdge.endPlane);
        } else {
          otherCrossingPoints = testPointPlane.findCrossings(planetModel, assessEdge.plane, testPointCutoffPlane, testPointOtherCutoffPlane, assessEdge.startPlane, assessEdge.endPlane);
        }        
          
        // Look for a matching endpoint.  If the other endpoint doesn't show up, it is either out of bounds (in which case the
        // transition won't be counted for that edge), or it is not a crossing for that edge (so, same conclusion).
        for (final GeoPoint otherCrossingPoint : otherCrossingPoints) {
          if (otherCrossingPoint.isNumericallyIdentical(assessEdge.endPoint)) {
            // Found it!
            // Both edges will try to contribute to the crossing count.  By convention, we'll only include the earlier one.
            // Since we're the latter point, we exit here in that case.
            //System.err.println(" Earlier point fired, so this one shouldn't");
            return;
          }
        }
          
        // Both edges will not count the same point, so we can proceed.  We need to determine the direction of both edges at the
        // point where they hit the plane.  This may be complicated by the 3D geometry; it may not be safe just to look at the endpoints of the edges
        // and make an assessment that way, since a single edge can intersect the plane at more than one point.
          
        final boolean assessEdgeInside = assessInsideTestPointIntersections.length + assessInsideTravelIntersections.length > 0;
        if (assessEdgeInside != edgeCrossesInside) {
          //System.err.println(" Incrementing crossing count");
          crossingCount++;
        } else {
          //System.err.println(" Entered and exited on same side");
        }
          
      } else if (crossingPoint.isNumericallyIdentical(edge.endPoint)) {
        //System.err.println(" Crossing point = edge.endPoint");
        // Figure out if the crossing should be counted.
        computeInsideOutside();
        
        // Does the crossing for this edge go up, or down?  Or can't we tell?
        final GeoPoint[] insideTestPointPlaneIntersections = testPointInsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane, insideTestPointCutoffPlane);
        final GeoPoint[] insideTravelPlaneIntersections = travelInsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane, insideTravelCutoffPlane);
        final GeoPoint[] outsideTestPointPlaneIntersections = testPointOutsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
        final GeoPoint[] outsideTravelPlaneIntersections = travelOutsidePlane.findIntersections(planetModel, edge.plane, edge.startPlane, edge.endPlane);
        
        // An edge can cross both outside and inside, because of the corner.  But it can be considered to cross the inside ONLY if it crosses either of the inside edges.
        //assert !(insideTestPointPlaneIntersections.length + insideTravelPlaneIntersections.length > 0 && outsideTestPointPlaneIntersections.length + outsideTravelPlaneIntersections.length > 0) : "edge that ends in a crossing can't go both up and down: insideTestPointPlaneIntersections: "+insideTestPointPlaneIntersections.length+" insideTravelPlaneIntersections: "+insideTravelPlaneIntersections.length+" outsideTestPointPlaneIntersections: "+outsideTestPointPlaneIntersections.length+" outsideTravelPlaneIntersections: "+outsideTravelPlaneIntersections.length;
          
        if (insideTestPointPlaneIntersections.length + insideTravelPlaneIntersections.length == 0 && outsideTestPointPlaneIntersections.length + outsideTravelPlaneIntersections.length == 0) {
          //System.err.println(" No inside or outside crossings found");
          return;
        }

        final boolean edgeCrossesInside = insideTestPointPlaneIntersections.length + insideTravelPlaneIntersections.length > 0;

        // This depends on the previous edge that first departs from identicalness.
        Edge assessEdge = edge;
        GeoPoint[] assessInsideTestPointIntersections;
        GeoPoint[] assessInsideTravelIntersections;
        GeoPoint[] assessOutsideTestPointIntersections;
        GeoPoint[] assessOutsideTravelIntersections;
        while (true) {
          assessEdge = assessEdge.next;
          assessInsideTestPointIntersections = testPointInsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane, insideTestPointCutoffPlane);
          assessInsideTravelIntersections = travelInsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane, insideTravelCutoffPlane);
          assessOutsideTestPointIntersections = testPointOutsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);
          assessOutsideTravelIntersections = travelOutsidePlane.findIntersections(planetModel, assessEdge.plane, assessEdge.startPlane, assessEdge.endPlane);

          assert !(assessInsideTestPointIntersections.length + assessInsideTravelIntersections.length > 0 && assessOutsideTestPointIntersections.length + assessOutsideTravelIntersections.length > 0) : "assess edge that ends in a crossing can't both up and down";

          if (assessInsideTestPointIntersections.length + assessInsideTravelIntersections.length == 0 && assessOutsideTestPointIntersections.length + assessOutsideTravelIntersections.length == 0) {
            continue;
          }
          break;
        }
          
        // Basically, we now want to assess whether both edges that come together at this endpoint leave the plane in opposite
        // directions.  If they do, then we should count it as a crossing; if not, we should not.  We also have to remember that
        // each edge we look at can also be looked at again if it, too, seems to cross the plane.
          
        // By definition, we're the earlier plane in this case, so any crossing we detect we must count, by convention.  It is unnecessary
        // to consider what the other edge does, because when we get to it, it will look back and figure out what we did for this one.
          
        // We need to determine the direction of both edges at the
        // point where they hit the plane.  This may be complicated by the 3D geometry; it may not be safe just to look at the endpoints of the edges
        // and make an assessment that way, since a single edge can intersect the plane at more than one point.

        final boolean assessEdgeInside = assessInsideTestPointIntersections.length + assessInsideTravelIntersections.length > 0;
        if (assessEdgeInside != edgeCrossesInside) {
          //System.err.println(" Incrementing crossing count");
          crossingCount++;
        } else {
          //System.err.println(" Entered and exited on same side");
        }
      } else {
        //System.err.println(" Not a special case: incrementing crossing count");
        // Not a special case, so we can safely count a crossing.
        crossingCount++;
      }
    }
  }
  
  @Override
  public boolean equals(Object o) {
    // Way too expensive to do this the hard way, so each complex polygon will be considered unique.
    return this == o;
  }

  @Override
  public int hashCode() {
    // Each complex polygon is considered unique.
    return System.identityHashCode(this);
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
  
