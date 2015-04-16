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

/** This GeoBBox represents an area rectangle limited only in latitude.
*/
public class GeoLatitudeZone implements GeoBBox
{
    public final double topLat;
    public final double bottomLat;
    public final double cosTopLat;
    public final double cosBottomLat;
    public final SidedPlane topPlane;
    public final SidedPlane bottomPlane;
    public final GeoPoint interiorPoint;

    public GeoLatitudeZone(double topLat, double bottomLat)
    {
        this.topLat = topLat;
        this.bottomLat = bottomLat;
          
        double sinTopLat = Math.sin(topLat);
        double sinBottomLat = Math.sin(bottomLat);
        cosTopLat = Math.cos(topLat);
        cosBottomLat = Math.cos(bottomLat);
          
        // Construct sample points, so we get our sidedness right
        Vector topPoint = new Vector(0.0,0.0,sinTopLat);
        Vector bottomPoint = new Vector(0.0,0.0,sinBottomLat);

        // Compute an interior point.  Pick one whose lat is between top and bottom.
        double middleLat = (topLat + bottomLat) * 0.5;
        double sinMiddleLat = Math.sin(middleLat);
        interiorPoint = new GeoPoint(Math.sqrt(1.0 - sinMiddleLat * sinMiddleLat),0.0,sinMiddleLat);
        
        this.topPlane = new SidedPlane(interiorPoint,sinTopLat);
        this.bottomPlane = new SidedPlane(interiorPoint,sinBottomLat);
    }

    @Override
    public GeoBBox expand(double angle)
    {
        double newTopLat = topLat + angle;
        double newBottomLat = bottomLat - angle;
        return GeoBBoxFactory.makeGeoBBox(newTopLat, newBottomLat, -Math.PI, Math.PI);
    }

    @Override
    public boolean isWithin(Vector point)
    {
        return topPlane.isWithin(point) &&
          bottomPlane.isWithin(point);
    }

    @Override
    public boolean isWithin(double x, double y, double z)
    {
        return topPlane.isWithin(x,y,z) &&
          bottomPlane.isWithin(x,y,z);
    }

    @Override
    public double getRadius()
    {
        // This is a bit tricky.  I guess we should interpret this as meaning the angle of a circle that
        // would contain all the bounding box points, when starting in the "center".
        if (topLat > 0.0 && bottomLat < 0.0)
            return Math.PI;
        double maxCosLat = cosTopLat;
        if (maxCosLat < cosBottomLat)
            maxCosLat = cosBottomLat;
        return maxCosLat * Math.PI;
    }

    @Override
    public GeoPoint getInteriorPoint()
    {
        return interiorPoint;
    }
      
    @Override
    public boolean intersects(Plane p, Membership... bounds)
    {
        return p.intersects(topPlane,bounds,bottomPlane) ||
          p.intersects(bottomPlane,bounds,topPlane);
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
        bounds.noLongitudeBound().addLatitudeZone(topLat).addLatitudeZone(bottomLat);
        return bounds;
    }

    @Override
    public int getRelationship(GeoShape path) {
        // Second, the shortcut of seeing whether endpoints are in/out is not going to 
        // work with no area endpoints.  So we rely entirely on intersections.

        if (path.intersects(topPlane,bottomPlane) ||
            path.intersects(bottomPlane,topPlane))
            return OVERLAPS;

        if (path.isWithin(interiorPoint))
            return CONTAINS;

        if (isWithin(path.getInteriorPoint()))
            return WITHIN;

        return DISJOINT;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GeoLatitudeZone))
            return false;
        GeoLatitudeZone other = (GeoLatitudeZone)o;
        return other.topPlane.equals(topPlane) && other.bottomPlane.equals(bottomPlane);
    }

    @Override
    public int hashCode() {
        int result = topPlane.hashCode();
        result = 31 * result + bottomPlane.hashCode();
        return result;
    }
}
