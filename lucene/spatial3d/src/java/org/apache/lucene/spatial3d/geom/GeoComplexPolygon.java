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
import java.util.HashMap;
import java.util.Map;

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
  
  private final XTree xtree = new XTree();
  private final YTree ytree = new YTree();
  private final ZTree ztree = new ZTree();
  
  private final boolean testPointInSet;
  private final Plane testPointVerticalPlane;
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
    Plane p = Plane.constructNormalizedZPlane(testPoint.x, testPoint.y);
    if (p == null) {
      p = new Plane(1.0, 0.0, 0.0, 0.0);
    }
    this.testPointVerticalPlane = p;
    this.edgePoints = new GeoPoint[pointsList.size()];
    this.shapeStartEdges = new Edge[pointsList.size()];
    int edgePointIndex = 0;
    for (final List<GeoPoint> shapePoints : pointsList) {
      GeoPoint lastGeoPoint = pointsList.get(shapePoints.size()-1);
      edgePoints[edgePointIndex] = lastGeoPoint;
      Edge lastEdge = null;
      Edge firstEdge = null;
      for (final GeoPoint thisGeoPoint : shapePoints) {
        final Edge edge = new Edge(planetModel, lastGeoPoint, thisGeoPoint);
        xtree.add(edge);
        ytree.add(edge);
        ztree.add(edge);
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

  /** Compute a legal point index from a possibly illegal one, that may have wrapped.
   *@param index is the index.
   *@return the normalized index.
   */
  protected int legalIndex(int index) {
    while (index >= points.size())
      index -= points.size();
    while (index < 0) {
      index += points.size();
    }
    return index;
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    // MHL
    return false;
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
    p.recordBounds(xyzBounds);
    // Figure out which tree likely works best
    final double xDelta = xyzBounds.getMaximumX() - xyzBounds.getMinimumX();
    final double yDelta = xyzBounds.getMaximumY() - xyzBounds.getMinimumY();
    final double zDelta = xyzBounds.getMaximumZ() - xyzBounds.getMinimumZ();
    // Select the smallest range
    if (xDelta <= yDelta && xDelta <= zDelta) {
      // Drill down in x
      return !xtree.traverse(intersector, xyzBounds.getMinimumX(), xyzBounds.getMaximumX());
    } else if (yDelta <= xDelta && yDelta <= zDelta) {
      // Drill down in y
      return !ytree.traverse(intersector, xyzBounds.getMinimumY(), xyzBounds.getMaximumY());
    } else if (zDelta <= xDelta && zDelta <= yDelta) {
      // Drill down in z
      return !ztree.traverse(intersector, xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ());
    }
    return true;
  }


  @Override
  public void getBounds(Bounds bounds) {
    super.getBounds(bounds);
    for (final Edge startEdge : shapeStartEdges) {
      Edge currentEdge = startEdge;
      while (true) {
        currentEdge.plane.recordBounds(this.planetModel, currentEdge.startPlane, currentEdge.edgePlane);
        currentEdge = currentEdge.next;
        if (currentEdge == startEdge) {
          break;
        }
      }
    }
  }

  @Override
  protected double outsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    // MHL
    return 0.0;
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
      this.plane.recordBounds(pm, this.planeBounds, this.startPlane, this.endPlane);
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
   * Comparison interface for tree traversal.  An object implementing this interface
   * gets to decide the relationship between the Edge object and the criteria being considered.
   */
  private static interface TraverseComparator {
    
    /**
     * Compare an edge.
     * @param edge is the edge to compare.
     * @param minValue is the minimum value to compare (bottom of the range)
     * @param maxValue is the maximum value to compare (top of the range)
     * @return -1 if "less" than this one, 0 if overlaps, or 1 if "greater".
     */
    public int compare(final Edge edge, final double minValue, final double maxValue);
    
  }

  /**
   * Comparison interface for tree addition.  An object implementing this interface
   * gets to decide the relationship between the Edge object and the criteria being considered.
   */
  private static interface AddComparator {
    
    /**
     * Compare an edge.
     * @param edge is the edge to compare.
     * @param addEdge is the edge being added.
     * @return -1 if "less" than this one, 0 if overlaps, or 1 if "greater".
     */
    public int compare(final Edge edge, final Edge addEdge);
    
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
    public Node lesser = null;
    public Node greater = null;
    public Node overlaps = null;
    
    public Node(final Edge edge) {
      this.edge = edge;
    }
    
    public void add(final Edge newEdge, final AddComparator edgeComparator) {
      Node currentNode = this;
      while (true) {
        final int result = edgeComparator.compare(edge, newEdge);
        if (result < 0) {
          if (lesser == null) {
            lesser = new Node(newEdge);
            return;
          }
          currentNode = lesser;
        } else if (result > 0) {
          if (greater == null) {
            greater = new Node(newEdge);
            return;
          }
          currentNode = greater;
        } else {
          if (overlaps == null) {
            overlaps = new Node(newEdge);
            return;
          }
          currentNode = overlaps;
        }
      }
    }
    
    public boolean traverse(final EdgeIterator edgeIterator, final TraverseComparator edgeComparator, final double minValue, final double maxValue) {
      Node currentNode = this;
      while (currentNode != null) {
        final int result = edgeComparator.compare(currentNode.edge, minValue, maxValue);
        if (result < 0) {
          currentNode = lesser;
        } else if (result > 0) {
          currentNode = greater;
        } else {
          if (!edgeIterator.matches(edge)) {
            return false;
          }
          currentNode = overlaps;
        }
      }
      return true;
    }
  }
  
  /** This is the z-tree.
   */
  private static class ZTree implements TraverseComparator, AddComparator {
    public Node rootNode = null;
    
    public ZTree() {
    }
    
    public void add(final Edge edge) {
      if (rootNode == null) {
        rootNode = new Node(edge);
      } else {
        rootNode.add(edge, this);
      }
    }
    
    public boolean traverse(final EdgeIterator edgeIterator, final double minValue, final double maxValue) {
      if (rootNode == null) {
        return true;
      }
      return rootNode.traverse(edgeIterator, this, minValue, maxValue);
    }
    
    @Override
    public int compare(final Edge edge, final Edge addEdge) {
      if (edge.planeBounds.getMaximumZ() < addEdge.planeBounds.getMinimumZ()) {
        return 1;
      } else if (edge.planeBounds.getMinimumZ() > addEdge.planeBounds.getMaximumZ()) {
        return -1;
      }
      return 0;
    }
    
    @Override
    public int compare(final Edge edge, final double minValue, final double maxValue) {
      if (edge.planeBounds.getMinimumZ() > maxValue) {
        return -1;
      } else if (edge.planeBounds.getMaximumZ() < minValue) {
        return 1;
      }
      return 0;
    }
    
  }
  
  /** This is the y-tree.
   */
  private static class YTree implements TraverseComparator, AddComparator {
    public Node rootNode = null;
    
    public YTree() {
    }
    
    public void add(final Edge edge) {
      if (rootNode == null) {
        rootNode = new Node(edge);
      } else {
        rootNode.add(edge, this);
      }
    }
    
    public boolean traverse(final EdgeIterator edgeIterator, final double minValue, final double maxValue) {
      if (rootNode == null) {
        return true;
      }
      return rootNode.traverse(edgeIterator, this, minValue, maxValue);
    }
    
    @Override
    public int compare(final Edge edge, final Edge addEdge) {
      if (edge.planeBounds.getMaximumY() < addEdge.planeBounds.getMinimumY()) {
        return 1;
      } else if (edge.planeBounds.getMinimumY() > addEdge.planeBounds.getMaximumY()) {
        return -1;
      }
      return 0;
    }
    
    @Override
    public int compare(final Edge edge, final double minValue, final double maxValue) {
      if (edge.planeBounds.getMinimumY() > maxValue) {
        return -1;
      } else if (edge.planeBounds.getMaximumY() < minValue) {
        return 1;
      }
      return 0;
    }
    
  }

  /** This is the x-tree.
   */
  private static class XTree implements TraverseComparator, AddComparator {
    public Node rootNode = null;
    
    public XTree() {
    }
    
    public void add(final Edge edge) {
      if (rootNode == null) {
        rootNode = new Node(edge);
      } else {
        rootNode.add(edge, this);
      }
    }
    
    public boolean traverse(final EdgeIterator edgeIterator, final double minValue, final double maxValue) {
      if (rootNode == null) {
        return true;
      }
      return rootNode.traverse(edgeIterator, this, minValue, maxValue);
    }
    
    @Override
    public int compare(final Edge edge, final Edge addEdge) {
      if (edge.planeBounds.getMaximumX() < addEdge.planeBounds.getMinimumX()) {
        return 1;
      } else if (edge.planeBounds.getMinimumX() > addEdge.planeBounds.getMaximumX()) {
        return -1;
      }
      return 0;
    }
    
    @Override
    public int compare(final Edge edge, final double minValue, final double maxValue) {
      if (edge.planeBounds.getMinimumX() > maxValue) {
        return -1;
      } else if (edge.planeBounds.getMaximumX() < minValue) {
        return 1;
      }
      return 0;
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
      this notablePoints = notablePoints;
      this.bounds = bounds;
    }
    
    @Override
    public boolean matches(final Edge edge) {
      return !plane.intersects(planetModel, edge.plane, notablePoints, edge.notablePoints, bounds, edge.startPlane, edge.endPlane);
    }

  }
  
  @Override
  public boolean equals(Object o) {
    // MHL
    return false;
  }

  @Override
  public int hashCode() {
    // MHL
    return 0;
  }

  @Override
  public String toString() {
    return "GeoComplexPolygon: {planetmodel=" + planetModel + "}";
  }
}
  
