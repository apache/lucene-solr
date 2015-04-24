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

/** Degenerate bounding box limited on two sides (left lon, right lon).
* The left-right maximum extent for this shape is PI; for anything larger, use
* GeoWideDegenerateHorizontalLine.
*/
public class GeoDegenerateHorizontalLine extends GeoBBoxBase
{
    public final double latitude;
    public final double leftLon;
    public final double rightLon;
      
    public final GeoPoint LHC;
    public final GeoPoint RHC;
    
    public final Plane plane;
    public final SidedPlane leftPlane;
    public final SidedPlane rightPlane;

    public final GeoPoint[] planePoints;

    public final GeoPoint centerPoint;
    public final GeoPoint[] edgePoints;
    
    /** Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI} */
    public GeoDegenerateHorizontalLine(final double latitude, final double leftLon, double rightLon)
    {
        // Argument checking
        if (latitude > Math.PI * 0.5 || latitude < -Math.PI * 0.5)
            throw new IllegalArgumentException("Latitude out of range");
        if (leftLon < -Math.PI || leftLon > Math.PI)
            throw new IllegalArgumentException("Left longitude out of range");
        if (rightLon < -Math.PI || rightLon > Math.PI)
            throw new IllegalArgumentException("Right longitude out of range");
        double extent = rightLon - leftLon;
        if (extent < 0.0) {
            extent += 2.0 * Math.PI;
        }
        if (extent > Math.PI)
            throw new IllegalArgumentException("Width of rectangle too great");

        this.latitude = latitude;
        this.leftLon = leftLon;
        this.rightLon = rightLon;
          
        final double sinLatitude = Math.sin(latitude);
        final double cosLatitude = Math.cos(latitude);
        final double sinLeftLon = Math.sin(leftLon);
        final double cosLeftLon = Math.cos(leftLon);
        final double sinRightLon = Math.sin(rightLon);
        final double cosRightLon = Math.cos(rightLon);
        
        // Now build the two points
        this.LHC = new GeoPoint(sinLatitude,sinLeftLon,cosLatitude,cosLeftLon);
        this.RHC = new GeoPoint(sinLatitude,sinRightLon,cosLatitude,cosRightLon);
        
        this.plane = new Plane(sinLatitude);

        // Normalize
        while (leftLon > rightLon) {
            rightLon += Math.PI * 2.0;
        }
        final double middleLon = (leftLon + rightLon) * 0.5;
        final double sinMiddleLon = Math.sin(middleLon);
        final double cosMiddleLon = Math.cos(middleLon);
          
        this.centerPoint = new GeoPoint(sinLatitude,sinMiddleLon,cosLatitude,cosMiddleLon);
        this.leftPlane = new SidedPlane(centerPoint,cosLeftLon,sinLeftLon);
        this.rightPlane = new SidedPlane(centerPoint,cosRightLon,sinRightLon);

        this.planePoints = new GeoPoint[]{LHC,RHC};

        this.edgePoints = new GeoPoint[]{centerPoint};
    }

    @Override
    public GeoBBox expand(final double angle)
    {
        double newTopLat = latitude + angle;
        double newBottomLat = latitude - angle;
        // Figuring out when we escalate to a special case requires some prefiguring
        double currentLonSpan = rightLon - leftLon;
        if (currentLonSpan < 0.0)
            currentLonSpan += Math.PI * 2.0;
        double newLeftLon = leftLon - angle;
        double newRightLon = rightLon + angle;
        if (currentLonSpan + 2.0 * angle >= Math.PI * 2.0) {
            newLeftLon = -Math.PI;
            newRightLon = Math.PI;
        }
        return GeoBBoxFactory.makeGeoBBox(newTopLat,newBottomLat,newLeftLon,newRightLon);
    }

    @Override
    public boolean isWithin(final Vector point)
    {
        return plane.evaluateIsZero(point) &&
          leftPlane.isWithin(point) &&
          rightPlane.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z)
    {
        return plane.evaluateIsZero(x,y,z) &&
          leftPlane.isWithin(x,y,z) &&
          rightPlane.isWithin(x,y,z);
    }

    @Override
    public double getRadius()
    {
        double topAngle = centerPoint.arcDistance(RHC);
        double bottomAngle = centerPoint.arcDistance(LHC);
        return Math.max(topAngle,bottomAngle);
    }

    /** Returns the center of a circle into which the area will be inscribed.
    *@return the center.
    */
    @Override
    public GeoPoint getCenter() {
        return centerPoint;
    }

    @Override
    public GeoPoint[] getEdgePoints()
    {
        return edgePoints;
    }
      
    @Override
    public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds)
    {
        return p.intersects(plane,notablePoints,planePoints,bounds,leftPlane,rightPlane);
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
        if (bounds == null)
            bounds = new Bounds();
        bounds.addLatitudeZone(latitude).addLongitudeSlice(leftLon,rightLon);
        return bounds;
    }

    @Override
    public int getRelationship(final GeoShape path) {
        if (path.intersects(plane,planePoints,leftPlane,rightPlane))
            return OVERLAPS;

        if (path.isWithin(centerPoint))
            return CONTAINS;

        return DISJOINT;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GeoDegenerateHorizontalLine))
            return false;
        GeoDegenerateHorizontalLine other = (GeoDegenerateHorizontalLine)o;
        return other.LHC.equals(LHC) && other.RHC.equals(RHC);
    }

    @Override
    public int hashCode() {
        int result = LHC.hashCode();
        result = 31 * result + RHC.hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        return "GeoDegenerateHorizontalLine: {latitude="+latitude+"("+latitude*180.0/Math.PI+"), leftlon="+leftLon+"("+leftLon*180.0/Math.PI+"), rightLon="+rightLon+"("+rightLon*180.0/Math.PI+")}";
    }
}
  

