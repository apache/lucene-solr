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

/** Degenerate bounding box limited on two sides (top lat, bottom lat).
*/
public class GeoDegenerateVerticalLine extends GeoBBoxBase
{
    public final double topLat;
    public final double bottomLat;
    public final double longitude;
      
    public final GeoPoint UHC;
    public final GeoPoint LHC;
    
    public final SidedPlane topPlane;
    public final SidedPlane bottomPlane;
    public final SidedPlane boundingPlane;
    public final Plane plane;
      
    public final GeoPoint centerPoint;
    public final GeoPoint[] edgePoints;
    
    /** Accepts only values in the following ranges: lat: {@code -PI/2 -> PI/2}, longitude: {@code -PI -> PI} */
    public GeoDegenerateVerticalLine(final double topLat, final double bottomLat, final double longitude)
    {
        // Argument checking
        if (topLat > Math.PI * 0.5 || topLat < -Math.PI * 0.5)
            throw new IllegalArgumentException("Top latitude out of range");
        if (bottomLat > Math.PI * 0.5 || bottomLat < -Math.PI * 0.5)
            throw new IllegalArgumentException("Bottom latitude out of range");
        if (topLat < bottomLat)
            throw new IllegalArgumentException("Top latitude less than bottom latitude");
        if (longitude < -Math.PI || longitude > Math.PI)
            throw new IllegalArgumentException("Longitude out of range");

        this.topLat = topLat;
        this.bottomLat = bottomLat;
        this.longitude = longitude;
          
        final double sinTopLat = Math.sin(topLat);
        final double cosTopLat = Math.cos(topLat);
        final double sinBottomLat = Math.sin(bottomLat);
        final double cosBottomLat = Math.cos(bottomLat);
        final double sinLongitude = Math.sin(longitude);
        final double cosLongitude = Math.cos(longitude);
        
        // Now build the two points
        this.UHC = new GeoPoint(sinTopLat,sinLongitude,cosTopLat,cosLongitude);
        this.LHC = new GeoPoint(sinBottomLat,sinLongitude,cosBottomLat,cosLongitude);
        
        this.plane = new Plane(cosLongitude,sinLongitude);
          
        final double middleLat = (topLat + bottomLat) * 0.5;
        final double sinMiddleLat = Math.sin(middleLat);
        final double cosMiddleLat = Math.cos(middleLat);
          
        this.centerPoint = new GeoPoint(sinMiddleLat,sinLongitude,cosMiddleLat,cosLongitude);

        this.topPlane = new SidedPlane(centerPoint,sinTopLat);
        this.bottomPlane = new SidedPlane(centerPoint,sinBottomLat);

        this.boundingPlane = new SidedPlane(centerPoint,-sinLongitude,cosLongitude);

        this.edgePoints = new GeoPoint[]{centerPoint};
    }

    @Override
    public GeoBBox expand(final double angle)
    {
        final double newTopLat = topLat + angle;
        final double newBottomLat = bottomLat - angle;
        double newLeftLon = longitude - angle;
        double newRightLon = longitude + angle;
        double currentLonSpan = 2.0 * angle;
        if (currentLonSpan + 2.0 * angle >= Math.PI * 2.0) {
            newLeftLon = -Math.PI;
            newRightLon = Math.PI;
        }
        return GeoBBoxFactory.makeGeoBBox(newTopLat,newBottomLat,newLeftLon,newRightLon);
    }

    @Override
    public boolean isWithin(final Vector point)
    {
        return plane.evaluate(point) == 0.0 &&
          boundingPlane.isWithin(point) &&
          topPlane.isWithin(point) &&
          bottomPlane.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z)
    {
        return plane.evaluate(x,y,z) == 0.0 &&
          boundingPlane.isWithin(x,y,z) &&
          topPlane.isWithin(x,y,z) &&
          bottomPlane.isWithin(x,y,z);
    }

    @Override
    public double getRadius()
    {
        // Here we compute the distance from the middle point to one of the corners.  However, we need to be careful
        // to use the longest of three distances: the distance to a corner on the top; the distnace to a corner on the bottom, and
        // the distance to the right or left edge from the center.
        final double topAngle = centerPoint.arcDistance(UHC);
        final double bottomAngle = centerPoint.arcDistance(LHC);
        return Math.max(topAngle,bottomAngle);
    }
      
    @Override
    public GeoPoint[] getEdgePoints()
    {
        return edgePoints;
    }
      
    @Override
    public boolean intersects(final Plane p, final Membership... bounds)
    {
        return p.intersects(plane,bounds,boundingPlane,topPlane,bottomPlane);
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
            .addLongitudeSlice(longitude,longitude);
        return bounds;
    }

    @Override
    public int getRelationship(final GeoShape path) {
        if (path.intersects(plane,boundingPlane,topPlane,bottomPlane))
            return OVERLAPS;

        if (path.isWithin(centerPoint))
            return CONTAINS;

        return DISJOINT;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GeoDegenerateVerticalLine))
            return false;
        GeoDegenerateVerticalLine other = (GeoDegenerateVerticalLine)o;
        return other.UHC.equals(UHC) && other.LHC.equals(LHC);
    }

    @Override
    public int hashCode() {
        int result = UHC.hashCode();
        result = 31 * result + LHC.hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        return "GeoDegenerateVerticalLine: {longitude="+longitude+"("+longitude*180.0/Math.PI+"), toplat="+topLat+"("+topLat*180.0/Math.PI+"), bottomlat="+bottomLat+"("+bottomLat*180.0/Math.PI+")}";
    }
}
  

