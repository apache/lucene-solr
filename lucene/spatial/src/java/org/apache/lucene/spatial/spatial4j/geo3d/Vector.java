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

/** A 3d vector in space, not necessarily
 * going through the origin. */
public class Vector
{
    public final double x;
    public final double y;
    public final double z;

    /** Construct from (U.S.) x,y,z coordinates.
     */
    public Vector(double x, double y, double z)
    {
        this.x = x;
        this.y = y;
        this.z = z;
    }
    
    /** Construct a vector that is perpendicular to
     * two other (non-zero) vectors.  If the vectors are parallel,
     * the result vector will have magnitude 0.
     *@param A is the first vector
     *@param B is the second
     */
    public Vector(Vector A, Vector B) {
        // x = u2v3 - u3v2
        // y = u3v1 - u1v3
        // z = u1v2 - u2v1

        this(A.y * B.z - A.z * B.y,
             A.z * B.x - A.x * B.z,
             A.x * B.y - A.y * B.x);
    }

    /** Compute a normalized unit vector based on the current vector.
     *@return the normalized vector, or null if the current vector has
     * a magnitude of zero.
     */
    public Vector normalize() {
        double denom = magnitude();
        if (denom < 1e-10)
            // Degenerate, can't normalize
            return null;
        double normFactor = 1.0/denom;
        return new Vector(x*normFactor,y*normFactor,z*normFactor);
    }
    
    /** Evaluate a vector (do a dot product).
     *@param v is the vector to evaluate.
     *@return the result.
     */
    public double evaluate(Vector v) {
        return this.x * v.x + this.y * v.y + this.z * v.z;
    }

    /** Evaluate a vector (do a dot product).
     *@param x is the x value of the vector to evaluate.
     *@param y is the x value of the vector to evaluate.
     *@param z is the x value of the vector to evaluate.
     *@return the result.
     */
    public double evaluate(double x, double y, double z) {
        return this.x * x + this.y * y + this.z * z;
    }

    /** Determine if this vector, taken from the origin,
     * describes a point within a set of planes.
     *@param bounds is the first part of the set of planes.
     *@param moreBounds is the second part of the set of planes.
     *@return true if the point is within the bounds.
     */
    public boolean isWithin(Membership[] bounds, Membership[] moreBounds) {
        // Return true if the point described is within all provided bounds
        for (Membership bound : bounds) {
            if (!bound.isWithin(this))
                return false;
        }
        for (Membership bound : moreBounds) {
            if (!bound.isWithin(this))
                return false;
        }
        return true;
    }

    /** Compute the square of a straight-line distance to a point described by the
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI.
     *@param v is the vector to compute a distance to.
     *@return the square of the linear distance.
     */
    public double linearDistanceSquared(Vector v) {
        double deltaX = this.x - v.x;
        double deltaY = this.y - v.y;
        double deltaZ = this.z - v.z;
        return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
    }
  
    /** Compute the square of a straight-line distance to a point described by the
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI.
     *@param x is the x part of the vector to compute a distance to.
     *@param y is the y part of the vector to compute a distance to.
     *@param z is the z part of the vector to compute a distance to.
     *@return the square of the linear distance.
     */
    public double linearDistanceSquared(double x, double y, double z) {
        double deltaX = this.x - x;
        double deltaY = this.y - y;
        double deltaZ = this.z - z;
        return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
    }

    /** Compute the straight-line distance to a point described by the
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI.
     *@param v is the vector to compute a distance to.
     *@return the linear distance.
     */
    public double linearDistance(Vector v) {
        return Math.sqrt(linearDistanceSquared(v));
    }
    
    /** Compute the straight-line distance to a point described by the
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI.
     *@param x is the x part of the vector to compute a distance to.
     *@param y is the y part of the vector to compute a distance to.
     *@param z is the z part of the vector to compute a distance to.
     *@return the linear distance.
     */
    public double linearDistance(double x, double y, double z) {
        return Math.sqrt(linearDistanceSquared(x,y,z));
    }
    
    /** Compute the square of the normal distance to a vector described by a
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI/2.
     *@param v is the vector to compute a distance to.
     *@return the square of the normal distance.
     */
    public double normalDistanceSquared(Vector v) {
        double t = this.evaluate(v);
        double deltaX = this.x * t - v.x;
        double deltaY = this.y * t - v.y;
        double deltaZ = this.z * t - v.z;
        return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
    }

    /** Compute the square of the normal distance to a vector described by a
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI/2.
     *@param x is the x part of the vector to compute a distance to.
     *@param y is the y part of the vector to compute a distance to.
     *@param z is the z part of the vector to compute a distance to.
     *@return the square of the normal distance.
     */
    public double normalDistanceSquared(double x, double y, double z) {
        double t = this.evaluate(x,y,z);
        double deltaX = this.x * t - x;
        double deltaY = this.y * t - y;
        double deltaZ = this.z * t - z;
        return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
    }
    
    /** Compute the normal (perpendicular) distance to a vector described by a
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI/2.
     *@param v is the vector to compute a distance to.
     *@return the normal distance.
     */
    public double normalDistance(Vector v) {
        return Math.sqrt(normalDistanceSquared(v));
    }

    /** Compute the normal (perpendicular) distance to a vector described by a
     * vector taken from the origin.
     * Monotonically increasing for arc distances up to PI/2.
     *@param x is the x part of the vector to compute a distance to.
     *@param y is the y part of the vector to compute a distance to.
     *@param z is the z part of the vector to compute a distance to.
     *@return the normal distance.
     */
    public double normalDistance(double x, double y, double z) {
        return Math.sqrt(normalDistanceSquared(x,y,z));
    }
    
    /** Compute the magnitude of this vector.
     *@return the magnitude.
     */
    public double magnitude() {
        return Math.sqrt(x * x + y * y + z * z);
    }
                
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Vector))
            return false;
        Vector other = (Vector)o;
        return (other.x == x && other.y == y && other.z == z);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(x);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(y);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(z);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "[X="+x+", Y="+y+", Z="+z+"]";
    }
}
