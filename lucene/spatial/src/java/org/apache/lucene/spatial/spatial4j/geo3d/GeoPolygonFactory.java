package org.apache.lucene.spatial.spatial4j.geo3d;

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

import java.util.List;
import java.util.ArrayList;

/** Class which constructs a GeoMembershipShape representing an arbitrary polygon.
*/
public class GeoPolygonFactory
{
    private GeoPolygonFactory() {
    }
  
    /** Create a GeoMembershipShape of the right kind given the specified bounds.
     *@param pointList is a list of the GeoPoints to build an arbitrary polygon out of.
     *@param convexPointIndex is the index of a single convex point whose conformation with
     * its neighbors determines inside/outside for the entire polygon.
     *@return a GeoMembershipShape corresponding to what was specified.
     */
    public static GeoMembershipShape makeGeoPolygon(List<GeoPoint> pointList, int convexPointIndex) {
        // The basic operation uses a set of points, two points determining one particular edge, and a sided plane
        // describing membership.
        return buildPolygonShape(pointList, convexPointIndex, getLegalIndex(convexPointIndex+1,pointList.size()),
            new SidedPlane(pointList.get(getLegalIndex(convexPointIndex-1,pointList.size())),
                pointList.get(convexPointIndex), pointList.get(getLegalIndex(convexPointIndex+1,pointList.size()))));
    }

    public static GeoMembershipShape buildPolygonShape(List<GeoPoint> pointsList, int startPointIndex, int endPointIndex, SidedPlane startingEdge) {
        // Algorithm as follows:
        // Start with sided edge.  Go through all points in some order.  For each new point, determine if the point is within all edges considered so far.
        // If not, put it into a list of points for recursion.  If it is within, add new edge and keep going.
        // Once we detect a point that is within, if there are points put aside for recursion, then call recursively.
        
        // Current composite.  This is what we'll actually be returning.
        GeoCompositeMembershipShape rval = new GeoCompositeMembershipShape();
        
        List<GeoPoint> recursionList = new ArrayList<GeoPoint>();
        List<GeoPoint> currentList = new ArrayList<GeoPoint>();
        List<SidedPlane> currentPlanes = new ArrayList<SidedPlane>();
        
        // Initialize the current list and current planes
        currentList.add(pointsList.get(startPointIndex));
        currentList.add(pointsList.get(endPointIndex));
        currentPlanes.add(startingEdge);
        
        // Now, scan all remaining points, in order.  We'll use an index and just add to it.
        for (int i = 0; i < pointsList.size() - 2; i++) {
            GeoPoint newPoint = pointsList.get(getLegalIndex(i + endPointIndex + 1, pointsList.size()));
            if (isWithin(newPoint, currentPlanes)) {
                // Construct a sided plane based on the last two points, and the previous point
                SidedPlane newBoundary = new SidedPlane(currentList.get(currentList.size()-2),newPoint,currentList.get(currentList.size()-1));
                // Construct a sided plane based on the return trip
                SidedPlane returnBoundary = new SidedPlane(currentList.get(currentList.size()-1),currentList.get(0),newPoint);
                // Verify that none of the points beyond the new point in the list are inside the polygon we'd
                // be creating if we stopped making the current polygon right now.
                boolean pointInside = false;
                for (int j = i + 1; j < pointsList.size() - 2; j++) {
                    GeoPoint checkPoint = pointsList.get(getLegalIndex(j + endPointIndex + 1, pointsList.size()));
                    boolean isInside = true;
                    if (isInside && !newBoundary.isWithin(checkPoint))
                        isInside = false;
                    if (isInside && !returnBoundary.isWithin(checkPoint))
                        isInside = false;
                    if (isInside) {
                        for (SidedPlane plane : currentPlanes) {
                            if (!plane.isWithin(checkPoint)) {
                                isInside = false;
                                break;
                            }
                        }
                    }
                    if (isInside) {
                        pointInside = true;
                        break;
                    }
                }
                if (!pointInside) {
                    // Any excluded points?
                    if (recursionList.size() > 0) {
                        // Handle exclusion
                        recursionList.add(newPoint);
                        recursionList.add(currentList.get(currentList.size()-1));
                        // We want the other side for the recursion
                        SidedPlane otherSideNewBoundary = new SidedPlane(newBoundary);
                        rval.addShape(buildPolygonShape(recursionList,recursionList.size()-2,recursionList.size()-1,otherSideNewBoundary));
                        recursionList.clear();
                    }
                    currentList.add(newPoint);
                    currentPlanes.add(newBoundary);
                } else {
                    recursionList.add(newPoint);
                }
            } else {
                recursionList.add(newPoint);
            }
        }
        
        if (recursionList.size() > 0) {
            // The last step back to the start point had a recursion, so take care of that before we complete our work
            recursionList.add(currentList.get(0));
            recursionList.add(currentList.get(currentList.size()-1));
            // Construct a sided plane based on these two points, and the previous point
            SidedPlane newBoundary = new SidedPlane(currentList.get(currentList.size()-2),currentList.get(0),currentList.get(currentList.size()-1));
            // We want the other side for the recursion
            SidedPlane otherSideNewBoundary = new SidedPlane(newBoundary);
            rval.addShape(buildPolygonShape(recursionList,recursionList.size()-2,recursionList.size()-1,otherSideNewBoundary));
            recursionList.clear();
        }
        
        // Now, add in the current shape.
        /*
        System.out.println("Creating polygon:");
        for (GeoPoint p : currentList) {
            System.out.println(" "+p);
        }
        */
        rval.addShape(new GeoConvexPolygon(currentList));
        //System.out.println("Done creating polygon");
        return rval;
    }

    protected static boolean isWithin(GeoPoint newPoint, List<SidedPlane> currentPlanes) {
        for (SidedPlane p : currentPlanes) {
            if (!p.isWithin(newPoint))
                return false;
        }
        return true;
    }

    protected static int getLegalIndex(int index, int size) {
        while (index < 0) {
            index += size;
        }
        while (index >= size) {
            index -= size;
        }
        return index;
    }
    
}
