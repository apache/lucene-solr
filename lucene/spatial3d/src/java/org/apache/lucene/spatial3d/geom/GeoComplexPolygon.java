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

import java.util.List;
import java.util.Set;
import java.util.HashSet;

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
  
  private final Tree xTree = new XTree();
  private final Tree yTree = new YTree();
  private final Tree zTree = new ZTree();
  
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
    int edgePointIndex = 0;
    for (final List<GeoPoint> shapePoints : pointsList) {
      GeoPoint lastGeoPoint = shapePoints.get(shapePoints.size()-1);
      edgePoints[edgePointIndex] = lastGeoPoint;
      Edge lastEdge = null;
      Edge firstEdge = null;
      for (final GeoPoint thisGeoPoint : shapePoints) {
        final Edge edge = new Edge(planetModel, lastGeoPoint, thisGeoPoint);
        xTree.add(edge);
        yTree.add(edge);
        zTree.add(edge);
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
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    return isWithin(new Vector(x, y, z));
  }
  
  @Override
  public boolean isWithin(final Vector thePoint) {
    // If we're right on top of the point, we know the answer.
    if (testPoint.isNumericallyIdentical(thePoint)) {
      return testPointInSet;
    }
    
    // If we're right on top of any of the test planes, we navigate solely on that plane.
    if (testPointFixedYPlane.evaluateIsZero(thePoint)) {
      // Use the XZ plane exclusively.
      final LinearCrossingEdgeIterator crossingEdgeIterator = new LinearCrossingEdgeIterator(testPointFixedYPlane, testPointFixedYAbovePlane, testPointFixedYBelowPlane, testPoint, thePoint);
      // Traverse our way from the test point to the check point.  Use the y tree because that's fixed.
      if (!yTree.traverse(crossingEdgeIterator, testPoint.y)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.crossingCount & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedXPlane.evaluateIsZero(thePoint)) {
      // Use the YZ plane exclusively.
      final LinearCrossingEdgeIterator crossingEdgeIterator = new LinearCrossingEdgeIterator(testPointFixedXPlane, testPointFixedXAbovePlane, testPointFixedXBelowPlane, testPoint, thePoint);
      // Traverse our way from the test point to the check point.  Use the x tree because that's fixed.
      if (!xTree.traverse(crossingEdgeIterator, testPoint.x)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.crossingCount & 1) == 0)?testPointInSet:!testPointInSet;
    } else if (testPointFixedZPlane.evaluateIsZero(thePoint)) {
      // Use the XY plane exclusively.
      final LinearCrossingEdgeIterator crossingEdgeIterator = new LinearCrossingEdgeIterator(testPointFixedZPlane, testPointFixedZAbovePlane, testPointFixedZBelowPlane, testPoint, thePoint);
      // Traverse our way from the test point to the check point.  Use the z tree because that's fixed.
      if (!zTree.traverse(crossingEdgeIterator, testPoint.z)) {
        // Endpoint is on edge
        return true;
      }
      return ((crossingEdgeIterator.crossingCount & 1) == 0)?testPointInSet:!testPointInSet;
    } else {
      
      // We need to use two planes to get there.  We don't know which two planes will do it but we can figure it out.
      final Plane travelPlaneFixedX = new Plane(1.0, 0.0, 0.0, -thePoint.x);
      final Plane travelPlaneFixedY = new Plane(0.0, 1.0, 0.0, -thePoint.y);
      final Plane travelPlaneFixedZ = new Plane(0.0, 0.0, 1.0, -thePoint.z);

      // Find the intersection points for each one of these and the complementary test point planes.
      final GeoPoint[] XIntersectionsY = travelPlaneFixedX.findIntersections(planetModel, testPointFixedYPlane);
      final GeoPoint[] XIntersectionsZ = travelPlaneFixedX.findIntersections(planetModel, testPointFixedZPlane);
      final GeoPoint[] YIntersectionsX = travelPlaneFixedY.findIntersections(planetModel, testPointFixedXPlane);
      final GeoPoint[] YIntersectionsZ = travelPlaneFixedY.findIntersections(planetModel, testPointFixedZPlane);
      final GeoPoint[] ZIntersectionsX = travelPlaneFixedZ.findIntersections(planetModel, testPointFixedXPlane);
      final GeoPoint[] ZIntersectionsY = travelPlaneFixedZ.findIntersections(planetModel, testPointFixedYPlane);

      // There will be multiple intersection points found.  We choose the one that has the lowest total distance, as measured in delta X, delta Y, and delta Z.
      double bestDistance = Double.MAX_VALUE;
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
        final double newDistance = Math.abs(testPoint.x - p.x) + Math.abs(thePoint.y - p.y);
        if (newDistance < bestDistance) {
          bestDistance = newDistance;
          firstLegValue = testPoint.y;
          secondLegValue = thePoint.x;
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
        final double newDistance = Math.abs(testPoint.x - p.x) + Math.abs(thePoint.z - p.z);
        if (newDistance < bestDistance) {
          bestDistance = newDistance;
          firstLegValue = testPoint.z;
          secondLegValue = thePoint.x;
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
        final double newDistance = Math.abs(testPoint.y - p.y) + Math.abs(thePoint.x - p.x);
        if (newDistance < bestDistance) {
          bestDistance = newDistance;
          firstLegValue = testPoint.x;
          secondLegValue = thePoint.y;
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
        final double newDistance = Math.abs(testPoint.y - p.y) + Math.abs(thePoint.z - p.z);
        if (newDistance < bestDistance) {
          bestDistance = newDistance;
          firstLegValue = testPoint.z;
          secondLegValue = thePoint.y;
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
        final double newDistance = Math.abs(testPoint.z - p.z) + Math.abs(thePoint.x - p.x);
        if (newDistance < bestDistance) {
          bestDistance = newDistance;
          firstLegValue = testPoint.x;
          secondLegValue = thePoint.z;
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
        final double newDistance = Math.abs(testPoint.z - p.z) + Math.abs(thePoint.y - p.y);
        if (newDistance < bestDistance) {
          bestDistance = newDistance;
          firstLegValue = testPoint.y;
          secondLegValue = thePoint.z;
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
      assert bestDistance < Double.MAX_VALUE : "Couldn't find an intersection point of any kind";
      
      final DualCrossingEdgeIterator edgeIterator = new DualCrossingEdgeIterator(firstLegPlane, firstLegAbovePlane, firstLegBelowPlane, secondLegPlane, testPoint, thePoint, intersectionPoint);
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
    double minimumDistance = Double.MAX_VALUE;
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
      this.plane.recordBounds(pm, this.planeBounds, this.startPlane, this.endPlane);
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
    public final double minimumValue;
    public final double maximumValue;
    public final Edge edge;
    public Node lesser = null;
    public Node greater = null;
    public Node within = null;
    
    public Node(final Edge edge, final double minimumValue, final double maximumValue) {
      this.edge = edge;
      this.minimumValue = minimumValue;
      this.maximumValue = maximumValue;
    }
    
  }
  
  /** An interface describing a tree.
   */
  private static abstract class Tree {
    private Node rootNode = null;
    
    protected static final int CONTAINED = 0;
    protected static final int WITHIN = 1;
    protected static final int OVERLAPS_MINIMUM = 2;
    protected static final int OVERLAPS_MAXIMUM = 3;
    protected static final int LESS = 4;
    protected static final int GREATER = 5;
    protected static final int EXACT = 6;
    
    /** Add a new edge to the tree.
     * @param edge is the edge to add.
     */
    public void add(final Edge edge) {
      rootNode = addEdge(rootNode, edge, getMinimum(edge), getMaximum(edge));
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
    
    /** Worker method for adding an edge.
     * @param node is the node to add into.
     * @param newEdge is the new edge to add.
     * @param minimumValue is the minimum limit of the subrange of the edge we'll be adding.
     * @param maximumValue is the maximum limit of the subrange of the edge we'll be adding.
     * @return the updated node reference.
     */
    protected Node addEdge(final Node node, final Edge newEdge, final double minimumValue, final double maximumValue) {
      if (node == null) {
        // Create and return a new node
        final Node rval = new Node(newEdge, minimumValue, maximumValue);
        //System.err.println("Creating new node "+rval+" for edge "+newEdge+" in tree "+this);
        return rval;
      }
      //System.err.println("Adding edge "+newEdge+" into node "+node+" in tree "+this);
      // Compare with what's here
      int result = compareForAdd(node.minimumValue, node.maximumValue, minimumValue, maximumValue);
      switch (result) {
      case CONTAINED:
       {
          final double lesserMaximum = Math.nextDown(node.minimumValue);
          final double greaterMinimum = Math.nextUp(node.maximumValue);
          node.lesser = addEdge(node.lesser, newEdge, minimumValue, lesserMaximum);
          node.greater = addEdge(node.greater, newEdge, greaterMinimum, maximumValue);
          return addEdge(node, newEdge, node.minimumValue, node.maximumValue);
       }
      case EXACT:
        // The node is exactly equal to the range provided.  We need to create a new node and insert
        // it into the "within" chain.
        final Node rval = new Node(newEdge, minimumValue, maximumValue);
        //System.err.println(" Inserting new node "+rval+" at head of current 'within' chain in tree "+this);
        rval.within = node;
        rval.lesser = node.lesser;
        rval.greater = node.greater;
        node.lesser = null;
        node.greater = null;
        return rval;
      case WITHIN:
        // The new edge is within the node provided
        //System.err.println(" Adding edge into 'within' chain in tree "+this);
        node.within = addEdge(node.within, newEdge, minimumValue, maximumValue);
        return node;
      case OVERLAPS_MINIMUM:
        {
          // The new edge overlaps the minimum value, but not the maximum value.
          // Here we need to create TWO entries: one for the lesser side, and one for the within chain.
          //System.err.println(" Inserting edge into BOTH lesser chain and within chain in tree "+this);
          final double lesserMaximum = Math.nextDown(node.minimumValue);
          node.lesser = addEdge(node.lesser, newEdge, minimumValue, lesserMaximum);
          return addEdge(node, newEdge, node.minimumValue, maximumValue);
        }
      case OVERLAPS_MAXIMUM:
        {
          // The new edge overlaps the maximum value, but not the minimum value.
          // Need to create two entries, one on the greater side, and one back into the current node.
          //System.err.println(" Inserting edge into BOTH greater chain and within chain in tree "+this);
          final double greaterMinimum = Math.nextUp(node.maximumValue);
          node.greater = addEdge(node.greater, newEdge, greaterMinimum, maximumValue);
          return addEdge(node, newEdge, minimumValue, node.maximumValue);
        }
      case LESS:
        // The new edge is clearly less than the current node.
        //System.err.println(" Edge goes into the lesser chain in tree "+this);
        node.lesser = addEdge(node.lesser, newEdge, minimumValue, maximumValue);
        return node;
      case GREATER:
        // The new edge is clearly greater than the current node.
        //System.err.println(" Edge goes into the greater chain in tree "+this);
        node.greater = addEdge(node.greater, newEdge, minimumValue, maximumValue);
        return node;
      default:
        throw new RuntimeException("Unexpected comparison result: "+result);
      }
      
    }
    
    /** Traverse the tree, finding all edges that intersect the provided value.
     * @param edgeIterator provides the method to call for any encountered matching edge.
     * @param value is the value to match.
     * @return false if the traversal was aborted before completion.
     */
    public boolean traverse(final EdgeIterator edgeIterator, final double value) {
      //System.err.println("Traversing tree, value = "+value);
      // Since there is one distinct value we are looking for, we can just do a straight descent through the nodes.
      Node currentNode = rootNode;
      while (currentNode != null) {
        if (value < currentNode.minimumValue) {
          //System.err.println(" value is less than "+currentNode.minimumValue);
          currentNode = currentNode.lesser;
        } else if (value > currentNode.maximumValue) {
          //System.err.println(" value is greater than "+currentNode.maximumValue);
          currentNode = currentNode.greater;
        } else {
          //System.err.println(" value within "+currentNode.minimumValue+" to "+currentNode.maximumValue);
          // We're within the bounds of the node.  Call the iterator, and descend
          if (!edgeIterator.matches(currentNode.edge)) {
            return false;
          }
          currentNode = currentNode.within;
        }
      }
      //System.err.println("Done with tree");
      return true;
    }
    
    /** Traverse the tree, finding all edges that intersect the provided value range.
     * @param edgeIterator provides the method to call for any encountered matching edge.
     *   Edges will not be invoked more than once.
     * @param minValue is the minimum value.
     * @param maxValue is the maximum value.
     * @return false if the traversal was aborted before completion.
     */
    public boolean traverse(final EdgeIterator edgeIterator, final double minValue, final double maxValue) {
      // This is tricky because edges are duplicated in the tree (where they got split).
      // We need to eliminate those duplicate edges as we traverse.  This requires us to keep a set of edges we've seen.
      // Luckily, the number of edges we're likely to encounter in a real-world situation is small, so we can get away with it.
      return traverseEdges(rootNode, edgeIterator, minValue, maxValue, new HashSet<>());
    }

    protected boolean traverseEdges(final Node node, final EdgeIterator edgeIterator, final double minValue, final double maxValue, final Set<Edge> edgeSet) {
      if (node == null) {
        return true;
      }
      if (maxValue < node.minimumValue) {
        return traverseEdges(node.lesser, edgeIterator, minValue, maxValue, edgeSet);
      } else if (minValue > node.maximumValue) {
        return traverseEdges(node.greater, edgeIterator, minValue, maxValue, edgeSet);
      } else {
        // There's overlap with the current node, and there may also be overlap with the lesser side and greater side
        if (minValue < node.minimumValue) {
          if (!traverseEdges(node.lesser, edgeIterator, minValue, maxValue, edgeSet)) {
            return false;
          }
        }
        if (!edgeSet.contains(node.edge)) {
          if (!edgeIterator.matches(node.edge)) {
            return false;
          }
          edgeSet.add(node.edge);
        }
        if (maxValue > node.maximumValue) {
          if (!traverseEdges(node.greater, edgeIterator, minValue, maxValue, edgeSet)) {
            return false;
          }
        }
        return traverseEdges(node.within, edgeIterator, minValue, maxValue, edgeSet);
      }
    }
    
    /** Compare a node against a subrange of a new edge.
     * @param nodeMinimumValue is the node's minimum value.
     * @param nodeMaximumValue is the node's maximum value.
     * @param minimumValue is the minimum value for the edge being added.
     * @param maximumValue is the maximum value for the edge being added.
     * @return the comparison result.
     */
    protected int compareForAdd(final double nodeMinimumValue, final double nodeMaximumValue, final double minimumValue, final double maximumValue) {
      if (minimumValue == nodeMinimumValue && maximumValue == nodeMaximumValue) {
        return EXACT;
      } else if (minimumValue <= nodeMinimumValue && maximumValue >= nodeMaximumValue) {
        return CONTAINED;
      } else if (nodeMinimumValue <= minimumValue && nodeMaximumValue >= maximumValue) {
        return WITHIN;
      } else if (maximumValue < nodeMinimumValue) {
        return LESS;
      } else if (minimumValue > nodeMaximumValue) {
        return GREATER;
      } else if (minimumValue < nodeMinimumValue) {
        return OVERLAPS_MINIMUM;
      } else {
        return OVERLAPS_MAXIMUM;
      }
    }
  }
  
  /** This is the z-tree.
   */
  private static class ZTree extends Tree {
    public Node rootNode = null;
    
    public ZTree() {
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
    
    public YTree() {
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
    
    public XTree() {
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
    private final Vector thePoint;
    
    public int crossingCount = 0;
    
    public LinearCrossingEdgeIterator(final Plane plane, final Plane abovePlane, final Plane belowPlane, final Vector testPoint, final Vector thePoint) {
      this.plane = plane;
      this.abovePlane = abovePlane;
      this.belowPlane = belowPlane;
      this.bound1 = new SidedPlane(thePoint, plane, testPoint);
      this.bound2 = new SidedPlane(testPoint, plane, thePoint);
      this.thePoint = thePoint;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      // Early exit if the point is on the edge.
      if (thePoint != null && edge.plane.evaluateIsZero(thePoint) && edge.startPlane.isWithin(thePoint) && edge.endPlane.isWithin(thePoint)) {
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
    private final Plane testPointInsidePlane;
    private final Plane testPointOutsidePlane;
    private final Plane travelPlane;
    private final Plane travelInsidePlane;
    private final Plane travelOutsidePlane;
    private final Vector thePoint;
    
    private final GeoPoint intersectionPoint;
    
    private final SidedPlane testPointCutoffPlane;
    private final SidedPlane checkPointCutoffPlane;
    private final SidedPlane testPointOtherCutoffPlane;
    private final SidedPlane checkPointOtherCutoffPlane;

    private final SidedPlane insideTestPointCutoffPlane;
    private final SidedPlane insideTravelCutoffPlane;
    
    public int crossingCount = 0;

    public DualCrossingEdgeIterator(final Plane testPointPlane, final Plane testPointAbovePlane, final Plane testPointBelowPlane,
      final Plane travelPlane, final Vector testPoint, final Vector thePoint, final GeoPoint intersectionPoint) {
      this.testPointPlane = testPointPlane;
      this.travelPlane = travelPlane;
      this.thePoint = thePoint;
      this.intersectionPoint = intersectionPoint;
      
      //System.err.println("Intersection point = "+intersectionPoint);
        
      assert travelPlane.evaluateIsZero(intersectionPoint) : "intersection point must be on travel plane";
      assert testPointPlane.evaluateIsZero(intersectionPoint) : "intersection point must be on test point plane";
        
      assert !testPoint.isNumericallyIdentical(intersectionPoint) : "test point is the same as intersection point";
      assert !thePoint.isNumericallyIdentical(intersectionPoint) : "check point is same is intersection point";

      this.testPointCutoffPlane = new SidedPlane(intersectionPoint, testPointPlane, testPoint);
      this.checkPointCutoffPlane = new SidedPlane(intersectionPoint, travelPlane, thePoint);
      this.testPointOtherCutoffPlane = new SidedPlane(testPoint, testPointPlane, intersectionPoint);
      this.checkPointOtherCutoffPlane = new SidedPlane(thePoint, travelPlane, intersectionPoint);

      // Convert travel plane to a sided plane
      final Membership intersectionBound1 = new SidedPlane(testPoint, travelPlane, travelPlane.D);
      // Convert testPoint plane to a sided plane
      final Membership intersectionBound2 = new SidedPlane(thePoint, testPointPlane, testPointPlane.D);

      // Sanity check
      assert testPointCutoffPlane.isWithin(intersectionPoint) : "intersection must be within testPointCutoffPlane";
      assert testPointOtherCutoffPlane.isWithin(intersectionPoint) : "intersection must be within testPointOtherCutoffPlane";
      assert checkPointCutoffPlane.isWithin(intersectionPoint) : "intersection must be within checkPointCutoffPlane";
      assert checkPointOtherCutoffPlane.isWithin(intersectionPoint) : "intersection must be within checkPointOtherCutoffPlane";
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
      
      final GeoPoint insideIntersection;
      if (aboveAbove.length > 0) {
        travelInsidePlane = travelAbovePlane;
        testPointInsidePlane = testPointAbovePlane;
        travelOutsidePlane = travelBelowPlane;
        testPointOutsidePlane = testPointBelowPlane;
        insideIntersection = aboveAbove[0];
      } else if (aboveBelow.length > 0) {
        travelInsidePlane = travelAbovePlane;
        testPointInsidePlane = testPointBelowPlane;
        travelOutsidePlane = travelBelowPlane;
        testPointOutsidePlane = testPointAbovePlane;
        insideIntersection = aboveBelow[0];
      } else if (belowBelow.length > 0) {
        travelInsidePlane = travelBelowPlane;
        testPointInsidePlane = testPointBelowPlane;
        travelOutsidePlane = travelAbovePlane;
        testPointOutsidePlane = testPointAbovePlane;
        insideIntersection = belowBelow[0];
      } else {
        travelInsidePlane = travelBelowPlane;
        testPointInsidePlane = testPointAbovePlane;
        travelOutsidePlane = travelAbovePlane;
        testPointOutsidePlane = testPointBelowPlane;
        insideIntersection = belowAbove[0];
      }
      
      insideTravelCutoffPlane = new SidedPlane(thePoint, testPointInsidePlane, testPointInsidePlane.D);
      insideTestPointCutoffPlane = new SidedPlane(testPoint, travelInsidePlane, travelInsidePlane.D);

    }

    public void setSecondLeg() {
      isSecondLeg = true;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      //System.err.println("Processing edge "+edge+", startpoint="+edge.startPoint+" endpoint="+edge.endPoint);
      // Early exit if the point is on the edge.
      if (thePoint != null && edge.plane.evaluateIsZero(thePoint) && edge.startPlane.isWithin(thePoint) && edge.endPlane.isWithin(thePoint)) {
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
      
      final Plane plane;
      final Plane insidePlane;
      final Plane outsidePlane;
      final SidedPlane bound1;
      final SidedPlane bound2;
      if (isSecondLeg) {
        plane = travelPlane;
        insidePlane = travelInsidePlane;
        outsidePlane = travelOutsidePlane;
        bound1 = checkPointCutoffPlane;
        bound2 = checkPointOtherCutoffPlane;
      } else {
        plane = testPointPlane;
        insidePlane = testPointInsidePlane;
        outsidePlane = testPointOutsidePlane;
        bound1 = testPointCutoffPlane;
        bound2 = testPointOtherCutoffPlane;
      }
        
      if (crossingPoint.isNumericallyIdentical(edge.startPoint)) {
        //System.err.println(" Crossing point = edge.startPoint");
        // We have to figure out if this crossing should be counted.
          
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
        final GeoPoint[] otherCrossingPoints = plane.findCrossings(planetModel, assessEdge.plane, bound1, bound2, assessEdge.startPlane, assessEdge.endPlane);
          
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
  
