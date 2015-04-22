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

/** This class represents a degenerate point bounding box.
* It is not a simple GeoPoint because we must have the latitude and longitude.
*/
public class GeoDegeneratePoint extends GeoPoint implements GeoBBox
{
    public final double latitude;
    public final double longitude;
    public final GeoPoint[] edgePoints;
	
    public GeoDegeneratePoint(final double lat, final double lon)
    {
        super(lat,lon);
        this.latitude = lat;
        this.longitude = lon;
        this.edgePoints = new GeoPoint[]{this};
    }

    /** Expand box by specified angle.
     *@param angle is the angle amount to expand the GeoBBox by.
     *@return a new GeoBBox.
     */
    @Override
    public GeoBBox expand(final double angle) {
        final double newTopLat = latitude + angle;
        final double newBottomLat = latitude - angle;
        final double newLeftLon = longitude - angle;
        final double newRightLon = longitude + angle;
        return GeoBBoxFactory.makeGeoBBox(newTopLat, newBottomLat, newLeftLon, newRightLon);
    }

    /** Return a sample point that is on the edge of the shape.
     *@return an interior point.
     */
    @Override
    public GeoPoint[] getEdgePoints() {
        return edgePoints;
    }
    
    /** Assess whether a plane, within the provided bounds, intersects
     * with the shape.
     *@param plane is the plane to assess for intersection with the shape's edges or
     *  bounding curves.
     *@param bounds are a set of bounds that define an area that an
     *  intersection must be within in order to qualify (provided by a GeoArea).
     *@return true if there's such an intersection, false if not.
     */
    @Override
    public boolean intersects(final Plane plane, final GeoPoint[] notablePoints, final Membership... bounds) {
        if (plane.evaluate(this) == 0.0)
            return false;
        
        for (Membership m : bounds) {
            if (!m.isWithin(this))
                return false;
        }
        return true;
    }

    /** Compute longitude/latitude bounds for the shape.
    *@param bounds is the optional input bounds object.  If this is null,
    * a bounds object will be created.  Otherwise, the input object will be modified.
    *@return a Bounds object describing the shape's bounds.  If the bounds cannot
    * be computed, then return a Bounds object with noLongitudeBound,
    * noTopLatitudeBound, and noBottomLatitudeBound.
    */
    @Override
    public Bounds getBounds(Bounds bounds) {
        if (bounds == null)
            bounds = new Bounds();
        bounds.addPoint(latitude,longitude);
        return bounds;
    }

    /** Equals */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GeoDegeneratePoint))
            return false;
        GeoDegeneratePoint other = (GeoDegeneratePoint)o;
        return other.latitude == latitude && other.longitude == longitude;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(latitude);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(longitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
    
    @Override
    public String toString() {
        return "GeoDegeneratePoint: {lat="+latitude+"("+latitude*180.0/Math.PI+"), lon="+longitude+"("+longitude*180.0/Math.PI+")}";
    }
    
    /** Check if a point is within this shape.
     *@param point is the point to check.
     *@return true if the point is within this shape
     */
    @Override
    public boolean isWithin(final Vector point) {
        return isWithin(point.x,point.y,point.z);
    }

    /** Check if a point is within this shape.
     *@param x is x coordinate of point to check.
     *@param y is y coordinate of point to check.
     *@param z is z coordinate of point to check.
     *@return true if the point is within this shape
     */
    @Override
    public boolean isWithin(final double x, final double y, final double z) {
        return x == this.x && y == this.y && z == this.z;
    }

    /** Returns the radius of a circle into which the GeoSizeable area can
     * be inscribed.
     *@return the radius.
     */
    @Override
    public double getRadius() {
        return 0.0;
    }

    /** Find the spatial relationship between a shape and the current geo area.
     * Note: return value is how the GeoShape relates to the GeoArea, not the
     * other way around. For example, if this GeoArea is entirely within the
     * shape, then CONTAINS should be returned.  If the shape is entirely enclosed
     * by this GeoArea, then WITHIN should be returned.
     *@param shape is the shape to consider.
     *@return the relationship, from the perspective of the shape.
     */
    @Override
    public int getRelationship(final GeoShape shape) {
        if (shape.isWithin(this))
            return CONTAINS;

        return DISJOINT;
    }

}

