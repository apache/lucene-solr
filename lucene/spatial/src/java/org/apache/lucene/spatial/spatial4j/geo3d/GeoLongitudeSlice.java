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

/** Bounding box limited on left and right.
* The left-right maximum extent for this shape is PI; for anything larger, use
* GeoWideLongitudeSlice.
*/
public class GeoLongitudeSlice extends GeoBBoxBase
{
    public final double leftLon;
    public final double rightLon;
      
    public final SidedPlane leftPlane;
    public final SidedPlane rightPlane;
      
    public final GeoPoint centerPoint;
    public final GeoPoint northPole = new GeoPoint(0.0,0.0,1.0);
    public final GeoPoint[] edgePoints = new GeoPoint[]{northPole};
    
    /** Accepts only values in the following ranges: lon: {@code -PI -> PI} */
    public GeoLongitudeSlice(final double leftLon, double rightLon)
    {
        // Argument checking
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

        this.leftLon = leftLon;
        this.rightLon = rightLon;
          
        final double sinLeftLon = Math.sin(leftLon);
        final double cosLeftLon = Math.cos(leftLon);
        final double sinRightLon = Math.sin(rightLon);
        final double cosRightLon = Math.cos(rightLon);

        // Normalize
        while (leftLon > rightLon) {
            rightLon += Math.PI * 2.0;
        }
        final double middleLon = (leftLon + rightLon) * 0.5;
        this.centerPoint = new GeoPoint(0.0,middleLon);              
        
        this.leftPlane = new SidedPlane(centerPoint,cosLeftLon,sinLeftLon);
        this.rightPlane = new SidedPlane(centerPoint,cosRightLon,sinRightLon);
          
    }

    @Override
    public GeoBBox expand(final double angle)
    {
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
        return GeoBBoxFactory.makeGeoBBox(Math.PI * 0.5,-Math.PI * 0.5,newLeftLon,newRightLon);
    }

    @Override
    public boolean isWithin(final Vector point)
    {
        return leftPlane.isWithin(point) &&
          rightPlane.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z)
    {
        return leftPlane.isWithin(x,y,z) &&
          rightPlane.isWithin(x,y,z);
    }

    @Override
    public double getRadius()
    {
        // Compute the extent and divide by two
        double extent = rightLon - leftLon;
        if (extent < 0.0)
            extent += Math.PI * 2.0;
        return Math.max(Math.PI * 0.5, extent * 0.5);
    }
      
    @Override
    public GeoPoint[] getEdgePoints()
    {
        return edgePoints;
    }
      
    @Override
    public boolean intersects(final Plane p, final Membership... bounds)
    {
        return p.intersects(leftPlane,bounds,rightPlane) ||
          p.intersects(rightPlane,bounds,leftPlane);
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
        bounds.noTopLatitudeBound().noBottomLatitudeBound();
        bounds.addLongitudeSlice(leftLon,rightLon);
        return bounds;
    }

    @Override
    public int getRelationship(final GeoShape path) {
        final int insideRectangle = isShapeInsideBBox(path);
        if (insideRectangle == SOME_INSIDE)
            return OVERLAPS;

        final boolean insideShape = path.isWithin(northPole);
        
        if (insideRectangle == ALL_INSIDE && insideShape)
            return OVERLAPS;

        if (path.intersects(leftPlane,rightPlane) ||
            path.intersects(rightPlane,leftPlane)) {
            return OVERLAPS;
        }

        if (insideRectangle == ALL_INSIDE) {
            return WITHIN;
        }

        if (insideShape) {
            return CONTAINS;
        }
        
        return DISJOINT;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GeoLongitudeSlice))
            return false;
        GeoLongitudeSlice other = (GeoLongitudeSlice)o;
        return other.leftLon == leftLon && other.rightLon == rightLon;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(leftLon);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(rightLon);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
    
    @Override
    public String toString() {
        return "GeoLongitudeSlice: {leftlon="+leftLon+"("+leftLon*180.0/Math.PI+"), rightlon="+rightLon+"("+rightLon*180.0/Math.PI+")}";
    }
}
  
