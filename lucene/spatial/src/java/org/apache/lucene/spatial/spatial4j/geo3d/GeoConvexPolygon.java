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

import java.util.*;

/** GeoConvexPolygon objects are generic building blocks of more complex structures.
* The only restrictions on these objects are: (1) they must be convex; (2) they must have
* a maximum extent no larger than PI.  Violating either one of these limits will
* cause the logic to fail.
*/
public class GeoConvexPolygon extends GeoBaseExtendedShape implements GeoMembershipShape
{
    protected final List<GeoPoint> points;
    protected SidedPlane[] edges = null;
    protected GeoPoint interiorPoint = null;
    
    /** Create a convex polygon from a list of points.
    */
    public GeoConvexPolygon(List<GeoPoint> pointList) {
        this.points = pointList;
        donePoints();
    }
    
    /** Create a convex polygon, with a starting latitude and longitude.
    * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
    */
    public GeoConvexPolygon(double startLatitude, double startLongitude)
    {
        points = new ArrayList<GeoPoint>();
        // Argument checking
        if (startLatitude > Math.PI * 0.5 || startLatitude < -Math.PI * 0.5)
            throw new IllegalArgumentException("Latitude out of range");
        if (startLongitude < -Math.PI || startLongitude > Math.PI)
            throw new IllegalArgumentException("Longitude out of range");
        
        GeoPoint p = new GeoPoint(startLatitude, startLongitude);
        points.add(p);
    }
    
    /** Add a point to the polygon.
     * Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI}
     */
    public void addPoint(double latitude, double longitude) {
        // Argument checking
        if (latitude > Math.PI * 0.5 || latitude < -Math.PI * 0.5)
            throw new IllegalArgumentException("Latitude out of range");
        if (longitude < -Math.PI || longitude > Math.PI)
            throw new IllegalArgumentException("Longitude out of range");
        
        GeoPoint p = new GeoPoint(latitude, longitude);
        points.add(p);
    }

    /** Finish the polygon, by connecting the last added point with the starting point.
    */
    public void donePoints() {
        // If fewer than 3 points, can't do it.
        if (points.size() < 3)
            throw new IllegalArgumentException("Polygon needs at least three points.");
        // Time to construct the planes.  If the polygon is truly convex, then any adjacent point
        edges = new SidedPlane[points.size()];
        // to a segment can provide an interior measurement.
        for (int i = 0; i < points.size(); i++) {
            GeoPoint start = points.get(i);
            GeoPoint end = points.get(legalIndex(i+1));
            GeoPoint check = points.get(legalIndex(i+2));
            SidedPlane sp = new SidedPlane(check,start,end);
            //System.out.println("Created edge "+sp+" using start="+start+" end="+end+" check="+check);
            edges[i] = sp;
        }
        
        // In order to naively confirm that the polygon is convex, I would need to
        // check every edge, and verify that every point (other than the edge endpoints)
        // is within the edge's sided plane.  This is an order n^2 operation.  That's still
        // not wrong, though, because everything else about polygons has a similar cost.
        for (int edgeIndex = 0; edgeIndex < edges.length; edgeIndex++) {
            SidedPlane edge = edges[edgeIndex];
            for (int pointIndex =0; pointIndex < points.size(); pointIndex++) {
                if (pointIndex != edgeIndex && pointIndex != legalIndex(edgeIndex+1)) {
                    if (!edge.isWithin(points.get(pointIndex)))
                        throw new IllegalArgumentException("Polygon is not convex: Point "+points.get(pointIndex)+" Edge "+edge);
                }
            }
        }
        
        // Finally, we need to compute a single interior point that will satisfy
        // all edges.  If the polygon is convex, we know that such a point exists.
        
        // This is actually surprisingly hard.  I believe merely averaging the x, y, and z
        // values of the points will produce a point inside the shape, but it won't be
        // on the unit sphere, and it may be in fact degenerate and have a zero magnitude.
        // In that case, an alternate algorithm would be required.  But since such cases
        // are very special (or very contrived), I'm just going to not worry about that
        // for the moment.
        double sumX = 0.0;
        double sumY = 0.0;
        double sumZ = 0.0;
        for (GeoPoint p : points) {
            sumX += p.x;
            sumY += p.y;
            sumZ += p.z;
        }
        double denom = 1.0 / (double)points.size();
        sumX *= denom;
        sumY *= denom;
        sumZ *= denom;
        double magnitude = Math.sqrt(sumX * sumX + sumY * sumY + sumZ * sumZ);
        if (magnitude < 1.0e-10)
            throw new IllegalArgumentException("Polygon interior point cannot be determined");
        denom = 1.0/magnitude;
        
        interiorPoint = new GeoPoint(sumX*denom,sumY*denom,sumZ*denom);
        
        // Let's be sure that our interior point is really inside
        for (SidedPlane sp : edges) {
            if (!sp.isWithin(interiorPoint)) {
                StringBuilder sb = new StringBuilder("Interior point logic failed to produce an interior point.  Vertices: ");
                for (GeoPoint p : points) {
                    sb.append(p).append(" ");
                }
                sb.append(". Interior point: ").append(interiorPoint);
                throw new IllegalArgumentException(sb.toString());
            }
        }
    }
    
    protected int legalIndex(int index) {
        while (index >= points.size())
            index -= points.size();
        return index;
    }
    
    @Override
    public boolean isWithin(Vector point)
    {
        for (SidedPlane edge : edges) {
            if (!edge.isWithin(point))
                return false;
        }
        return true;
    }

    @Override
    public boolean isWithin(double x, double y, double z)
    {
        for (SidedPlane edge : edges) {
            if (!edge.isWithin(x,y,z))
                return false;
        }
        return true;
    }

    @Override
    public GeoPoint getInteriorPoint()
    {
        return interiorPoint;
    }
      
    @Override
    public boolean intersects(Plane p, Membership... bounds)
    {
        for (int edgeIndex = 0; edgeIndex < edges.length; edgeIndex++) {
            SidedPlane edge = edges[edgeIndex];
            // Construct boundaries
            Membership[] membershipBounds = new Membership[edges.length-1];
            int count = 0;
            for (int otherIndex = 0; otherIndex < edges.length; otherIndex++) {
                if (otherIndex != edgeIndex) {
                    membershipBounds[count++] = edges[otherIndex];
                }
            }
            if (edge.intersects(p,bounds,membershipBounds))
                return true;
        }
        return false;
    }

    /** Compute longitude/latitude bounds for the shape.
    *@param bounds is the optional input bounds object.  If this is null,
    * a bounds object will be created.  Otherwise, the input object will be modified.
    *@return a Bounds object describing the shape's bounds.  If the bounds cannot
    * be computed, then return a Bounds object with noLongitudeBound,
    * noTopLatitudeBound, and noBottomLatitudeBound.
    */
    @Override
    public Bounds getBounds(Bounds bounds)
    {
        bounds = super.getBounds(bounds);
        
        // Add all the points
        for (GeoPoint point : points) {
            bounds.addPoint(point);
        }

        // Add planes with membership.
        for (int edgeIndex = 0; edgeIndex < edges.length; edgeIndex++) {
            SidedPlane edge = edges[edgeIndex];
            // Construct boundaries
            Membership[] membershipBounds = new Membership[edges.length-1];
            int count = 0;
            for (int otherIndex = 0; otherIndex < edges.length; otherIndex++) {
                if (otherIndex != edgeIndex) {
                    membershipBounds[count++] = edges[otherIndex];
                }
            }
            edge.recordBounds(bounds,membershipBounds);
        }

        return bounds;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GeoConvexPolygon))
            return false;
        GeoConvexPolygon other = (GeoConvexPolygon)o;
        if (other.points.size() != points.size())
            return false;
        
        for (int i = 0; i < points.size(); i++) {
            if (!other.points.get(i).equals(points.get(i)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return points.hashCode();
    }

    @Override
    public String toString() {
        return "GeoConvexPolygon{" + points + "}";
    }
}
  
