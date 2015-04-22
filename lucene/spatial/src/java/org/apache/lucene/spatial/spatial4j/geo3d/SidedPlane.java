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

/** Combination of a plane, and a sign value indicating what evaluation values are on the correct
* side of the plane.
*/
public class SidedPlane extends Plane implements Membership
{
    public final double sigNum;

    /** Construct a SidedPlane identical to an existing one, but reversed.
    *@param sidedPlane is the existing plane.
    */
    public SidedPlane(SidedPlane sidedPlane) {
        super(sidedPlane,sidedPlane.D);
        this.sigNum = -sidedPlane.sigNum;
    }

    /** Construct a sided plane from a pair of vectors describing points, and including
     * origin, plus a point p which describes the side.
     *@param p point to evaluate
     *@param A is the first in-plane point
     *@param B is the second in-plane point
     */
    public SidedPlane(Vector p, Vector A, Vector B) {
        super(A,B);
        sigNum = Math.signum(evaluate(p));
    }

    /** Construct a sided plane from a point and a Z coordinate.
     *@param p point to evaluate.
     *@param height is the Z coordinate of the plane.
     */
    public SidedPlane(Vector p, double height) {
        super(height);
        sigNum = Math.signum(evaluate(p));
    }

    /** Construct a sided vertical plane from a point and specified x and y coordinates.
     *@param p point to evaluate.
     *@param x is the specified x.
     *@param y is the specified y.
     */
    public SidedPlane(Vector p, double x, double y) {
        super(x,y);
        sigNum = Math.signum(evaluate(p));
    }

    /** Construct a sided plane with a normal vector and offset.
     *@param p point to evaluate.
     *@param v is the normal vector.
     *@param D is the origin offset for the plan.
     */
    public SidedPlane(Vector p, Vector v, double D) {
        super(v,D);
        sigNum = Math.signum(evaluate(p));
    }

    /** Check if a point is within this shape.
     *@param point is the point to check.
     *@return true if the point is within this shape
     */
    @Override
    public boolean isWithin(Vector point)
    {
        double evalResult = evaluate(point);
        if (Math.abs(evalResult) < MINIMUM_RESOLUTION)
            return true;
        double sigNum = Math.signum(evalResult);
        return sigNum == this.sigNum;
    }

    /** Check if a point is within this shape.
     *@param x is x coordinate of point to check.
     *@param y is y coordinate of point to check.
     *@param z is z coordinate of point to check.
     *@return true if the point is within this shape
     */
    @Override
    public boolean isWithin(double x, double y, double z)
    {
        double evalResult = this.x * x + this.y * y + this.z * z;
        if (Math.abs(evalResult) < MINIMUM_RESOLUTION)
            return true;
        double sigNum = Math.signum(evalResult);
        return sigNum == this.sigNum;
    }
    

    @Override
    public String toString() {
        return "[A="+x+", B="+y+", C="+z+", D="+D+", side="+sigNum+"]";
    }
}
  
