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

/** Circular area with a center and radius.
*/
public class GeoCircle extends GeoBaseExtendedShape implements GeoDistanceShape, GeoSizeable
{
    public final GeoPoint center;
    public final double cutoffAngle;
    public final double cutoffNormalDistance;
    public final double cutoffLinearDistance;
    public final SidedPlane circlePlane;
    public final GeoPoint[] edgePoints;
    
    public GeoCircle(final double lat, final double lon, final double cutoffAngle)
    {
        super();
        if (lat < -Math.PI * 0.5 || lat > Math.PI * 0.5)
            throw new IllegalArgumentException("Latitude out of bounds");
        if (lon < -Math.PI || lon > Math.PI)
            throw new IllegalArgumentException("Longitude out of bounds");
        if (cutoffAngle < 0.0 || cutoffAngle > Math.PI)
            throw new IllegalArgumentException("Cutoff angle out of bounds");
        final double sinAngle = Math.sin(cutoffAngle);
        final double cosAngle = Math.cos(cutoffAngle);
        this.center = new GeoPoint(lat,lon);
        this.cutoffNormalDistance = sinAngle;
        // Need the chord distance.  This is just the chord distance: sqrt((1 - cos(angle))^2 + (sin(angle))^2).
        final double xDiff = 1.0 - cosAngle;
        this.cutoffLinearDistance = Math.sqrt(xDiff * xDiff + sinAngle * sinAngle);
        this.cutoffAngle = cutoffAngle;
        this.circlePlane = new SidedPlane(center, center, -cosAngle);
        
        // Compute a point on the circle boundary.  This can be any point that is easy to compute.
        // This requires some math, so I've implemented it in Plane.
        this.edgePoints = new GeoPoint[]{center.getSamplePoint(sinAngle,cosAngle)};
    }
    
    @Override
    public double getRadius() {
        return cutoffAngle;
    }

    /** Compute an estimate of "distance" to the GeoPoint.
    * A return value of Double.MAX_VALUE should be returned for
    * points outside of the shape.
    */
    @Override
    public double computeNormalDistance(final GeoPoint point)
    {
        double normalDistance = this.center.normalDistance(point);
        if (normalDistance > cutoffNormalDistance)
            return Double.MAX_VALUE;
        return normalDistance;
    }

    /** Compute an estimate of "distance" to the GeoPoint.
    * A return value of Double.MAX_VALUE should be returned for
    * points outside of the shape.
    */
    @Override
    public double computeNormalDistance(final double x, final double y, final double z)
    {
        double normalDistance = this.center.normalDistance(x,y,z);
        if (normalDistance > cutoffNormalDistance)
            return Double.MAX_VALUE;
        return normalDistance;
    }
      
    /** Compute a squared estimate of the "distance" to the
    * GeoPoint.  Double.MAX_VALUE indicates a point outside of the
    * shape.
    */
    @Override
    public double computeSquaredNormalDistance(final GeoPoint point)
    {
        double normalDistanceSquared = this.center.normalDistanceSquared(point);
        if (normalDistanceSquared > cutoffNormalDistance * cutoffNormalDistance)
            return Double.MAX_VALUE;
        return normalDistanceSquared;
    }
    
    /** Compute a squared estimate of the "distance" to the
    * GeoPoint.  Double.MAX_VALUE indicates a point outside of the
    * shape.
    */
    @Override
    public double computeSquaredNormalDistance(final double x, final double y, final double z)
    {
        double normalDistanceSquared = this.center.normalDistanceSquared(x,y,z);
        if (normalDistanceSquared > cutoffNormalDistance * cutoffNormalDistance)
            return Double.MAX_VALUE;
        return normalDistanceSquared;
    }
    
    /** Compute a linear distance to the vector.
    * return Double.MAX_VALUE for points outside the shape.
    */
    @Override
    public double computeLinearDistance(final GeoPoint point)
    {
        double linearDistance = this.center.linearDistance(point);
        if (linearDistance > cutoffLinearDistance)
            return Double.MAX_VALUE;
        return linearDistance;
    }
    
    /** Compute a linear distance to the vector.
    * return Double.MAX_VALUE for points outside the shape.
    */
    @Override
    public double computeLinearDistance(final double x, final double y, final double z)
    {
        double linearDistance = this.center.linearDistance(x,y,z);
        if (linearDistance > cutoffLinearDistance)
            return Double.MAX_VALUE;
        return linearDistance;
    }
    
    /** Compute a squared linear distance to the vector.
    */
    @Override
    public double computeSquaredLinearDistance(final GeoPoint point)
    {
        double linearDistanceSquared = this.center.linearDistanceSquared(point);
        if (linearDistanceSquared > cutoffLinearDistance * cutoffLinearDistance)
            return Double.MAX_VALUE;
        return linearDistanceSquared;
    }
    
    /** Compute a squared linear distance to the vector.
    */
    @Override
    public double computeSquaredLinearDistance(final double x, final double y, final double z)
    {
        double linearDistanceSquared = this.center.linearDistanceSquared(x,y,z);
        if (linearDistanceSquared > cutoffLinearDistance * cutoffLinearDistance)
            return Double.MAX_VALUE;
        return linearDistanceSquared;
    }
    
    /** Compute a true, accurate, great-circle distance.
    * Double.MAX_VALUE indicates a point is outside of the shape.
    */
    @Override
    public double computeArcDistance(final GeoPoint point)
    {
        double dist = this.center.arcDistance(point);
        if (dist > cutoffAngle)
            return Double.MAX_VALUE;
        return dist;
    }

    @Override
    public boolean isWithin(final Vector point)
    {
        if (point == null)
            return false;
        // Fastest way of determining membership
        return circlePlane.isWithin(point);
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z)
    {
        // Fastest way of determining membership
        return circlePlane.isWithin(x,y,z);
    }

    @Override
    public GeoPoint[] getEdgePoints()
    {
        return edgePoints;
    }
      
    @Override
    public boolean intersects(final Plane p, final Membership... bounds)
    {
        return circlePlane.intersects(p, bounds);
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
        circlePlane.recordBounds(bounds);
        return bounds;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GeoCircle))
            return false;
        GeoCircle other = (GeoCircle)o;
        return other.center.equals(center) && other.cutoffAngle == cutoffAngle;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = center.hashCode();
        temp = Double.doubleToLongBits(cutoffAngle);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
    
    @Override
    public String toString() {
        return "GeoCircle: {center="+center+", radius="+cutoffAngle+"("+cutoffAngle*180.0/Math.PI+")}";
    }
}
