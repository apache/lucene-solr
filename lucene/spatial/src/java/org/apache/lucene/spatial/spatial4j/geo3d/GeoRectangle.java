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

/** Bounding box limited on four sides (top lat, bottom lat, left lon, right lon).
* The left-right maximum extent for this shape is PI; for anything larger, use
* GeoWideRectangle.
*/
public class GeoRectangle implements GeoBBox
{
    public final double topLat;
    public final double bottomLat;
    public final double leftLon;
    public final double rightLon;
      
    public final double cosMiddleLat;
      
    public final GeoPoint ULHC;
    public final GeoPoint URHC;
    public final GeoPoint LRHC;
    public final GeoPoint LLHC;
    
    public final SidedPlane topPlane;
    public final SidedPlane bottomPlane;
    public final SidedPlane leftPlane;
    public final SidedPlane rightPlane;
      
    public final GeoPoint centerPoint;

    /** Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, lon: {@code -PI -> PI} */
    public GeoRectangle(double topLat, double bottomLat, double leftLon, double rightLon)
    {
        // Argument checking
        if (topLat > Math.PI * 0.5 || topLat < -Math.PI * 0.5)
            throw new IllegalArgumentException("Top latitude out of range");
        if (bottomLat > Math.PI * 0.5 || bottomLat < -Math.PI * 0.5)
            throw new IllegalArgumentException("Bottom latitude out of range");
        if (topLat < bottomLat)
            throw new IllegalArgumentException("Top latitude less than bottom latitude");
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

        this.topLat = topLat;
        this.bottomLat = bottomLat;
        this.leftLon = leftLon;
        this.rightLon = rightLon;
          
        double sinTopLat = Math.sin(topLat);
        double cosTopLat = Math.cos(topLat);
        double sinBottomLat = Math.sin(bottomLat);
        double cosBottomLat = Math.cos(bottomLat);
        double sinLeftLon = Math.sin(leftLon);
        double cosLeftLon = Math.cos(leftLon);
        double sinRightLon = Math.sin(rightLon);
        double cosRightLon = Math.cos(rightLon);
        
        // Now build the four points
        this.ULHC = new GeoPoint(sinTopLat,sinLeftLon,cosTopLat,cosLeftLon);
        this.URHC = new GeoPoint(sinTopLat,sinRightLon,cosTopLat,cosRightLon);
        this.LRHC = new GeoPoint(sinBottomLat,sinRightLon,cosBottomLat,cosRightLon);
        this.LLHC = new GeoPoint(sinBottomLat,sinLeftLon,cosBottomLat,cosLeftLon);
        
        double middleLat = (topLat + bottomLat) * 0.5;
        double sinMiddleLat = Math.sin(middleLat);
        cosMiddleLat = Math.cos(middleLat);
        // Normalize
        while (leftLon > rightLon) {
            rightLon += Math.PI * 2.0;
        }
        double middleLon = (leftLon + rightLon) * 0.5;
        double sinMiddleLon = Math.sin(middleLon);
        double cosMiddleLon = Math.cos(middleLon);
          
        centerPoint = new GeoPoint(sinMiddleLat,sinMiddleLon,cosMiddleLat,cosMiddleLon);

        this.topPlane = new SidedPlane(centerPoint,sinTopLat);
        this.bottomPlane = new SidedPlane(centerPoint,sinBottomLat);
        this.leftPlane = new SidedPlane(centerPoint,cosLeftLon,sinLeftLon);
        this.rightPlane = new SidedPlane(centerPoint,cosRightLon,sinRightLon);

    }

    @Override
    public GeoBBox expand(double angle)
    {
        double newTopLat = topLat + angle;
        double newBottomLat = bottomLat - angle;
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
    public boolean isWithin(Vector point)
    {
        return topPlane.isWithin(point) &&
          bottomPlane.isWithin(point) &&
          leftPlane.isWithin(point) &&
          rightPlane.isWithin(point);
    }

    @Override
    public boolean isWithin(double x, double y, double z)
    {
        return topPlane.isWithin(x,y,z) &&
          bottomPlane.isWithin(x,y,z) &&
          leftPlane.isWithin(x,y,z) &&
          rightPlane.isWithin(x,y,z);
    }

    @Override
    public double getRadius()
    {
        // Here we compute the distance from the middle point to one of the corners.  However, we need to be careful
        // to use the longest of three distances: the distance to a corner on the top; the distnace to a corner on the bottom, and
        // the distance to the right or left edge from the center.
        double centerAngle = (rightLon - (rightLon + leftLon) * 0.5) * cosMiddleLat;
        double topAngle = centerPoint.arcDistance(URHC);
        double bottomAngle = centerPoint.arcDistance(LLHC);
        return Math.max(centerAngle,Math.max(topAngle,bottomAngle));
    }
      
    @Override
    public GeoPoint getInteriorPoint()
    {
        return centerPoint;
    }
      
    @Override
    public boolean intersects(Plane p, Membership... bounds)
    {
        return p.intersects(topPlane,bounds,bottomPlane,leftPlane,rightPlane) ||
          p.intersects(bottomPlane,bounds,topPlane,leftPlane,rightPlane) ||
          p.intersects(leftPlane,bounds,rightPlane,topPlane,bottomPlane) ||
          p.intersects(rightPlane,bounds,leftPlane,topPlane,bottomPlane);
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
        bounds.addLatitudeZone(topLat).addLatitudeZone(bottomLat)
            .addLongitudeSlice(leftLon,rightLon);
        return bounds;
    }

    @Override
    public int getRelationship(GeoShape path) {
        if (path.intersects(topPlane,bottomPlane,leftPlane,rightPlane) ||
            path.intersects(bottomPlane,topPlane,leftPlane,rightPlane) ||
            path.intersects(leftPlane,topPlane,bottomPlane,rightPlane) ||
            path.intersects(rightPlane,leftPlane,topPlane,bottomPlane))
            return OVERLAPS;

        if (isWithin(path.getInteriorPoint()))
            return WITHIN;
    
        if (path.isWithin(centerPoint))
            return CONTAINS;
        
        return DISJOINT;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GeoRectangle))
            return false;
        GeoRectangle other = (GeoRectangle)o;
        return other.ULHC.equals(ULHC) && other.LRHC.equals(LRHC);
    }

    @Override
    public int hashCode() {
        int result = ULHC.hashCode();
        result = 31 * result + LRHC.hashCode();
        return result;
    }
}
  
