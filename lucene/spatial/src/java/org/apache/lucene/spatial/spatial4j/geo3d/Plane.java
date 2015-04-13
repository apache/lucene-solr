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

/** We know about three kinds of planes.  First kind: general plain through two points and origin
* Second kind: horizontal plane at specified height.  Third kind: vertical plane with specified x and y value, through origin.
*/
public class Plane extends Vector
{
    public final double D;
  
    /** Construct a plane through two points and origin.
     *@param A is the first point (origin based).
     *@param B is the second point (origin based).
     */
    public Plane(Vector A, Vector B) {
        super(A,B);
        D = 0.0;
    }
  
    /** Construct a horizontal plane at a specified Z.
     *@param height is the specified Z coordinate.
     */
    public Plane(double height) {
        super(0.0,0.0,1.0);
        D = -height;
    }
  
    /** Construct a vertical plane through a specified
     * x, y and origin.
     *@param x is the specified x value.
     *@param y is the specified y value.
     */
    public Plane(double x, double y) {
        super(y,-x,0.0);
        D = 0.0;
    }
  
    /** Construct a plane with a specific vector, and D offset
     * from origin.
     *@param D is the D offset from the origin.
     */
    public Plane(Vector v, double D) {
        super(v.x,v.y,v.z);
        this.D = D;
    }
  
    /** Evaluate the plane equation for a given point, as represented
     * by a vector.
     *@param v is the vector.
     *@return the result of the evaluation.
     */
    public double evaluate(Vector v) {
        return super.evaluate(v) + D;
    }
  
    /** Build a normalized plane, so that the vector is normalized.
     *@return the normalized plane object, or null if the plane is indeterminate.
     */
    public Plane normalize() {
        Vector normVect = super.normalize();
        if (normVect == null)
            return null;
        return new Plane(normVect,this.D);
    }

    /** Find the intersection points between two planes, given a set of bounds.
    *@param q is the plane to intersect with.
    *@param bounds is the set of bounds.
    *@param moreBounds is another set of bounds.
    *@return the intersection point(s) on the unit sphere, if there are any.
    */
    protected GeoPoint[] findIntersections(Plane q, Membership[] bounds, Membership[] moreBounds) {
        Vector lineVector = new Vector(this,q);
        if (lineVector.x == 0.0 && lineVector.y == 0.0 && lineVector.z == 0.0) {
            // Degenerate case: parallel planes
            return new GeoPoint[0];
        }

        // The line will have the equation: A t + A0 = x, B t + B0 = y, C t + C0 = z.
        // We have A, B, and C.  In order to come up with A0, B0, and C0, we need to find a point that is on both planes.
        // To do this, we find the largest vector value (either x, y, or z), and look for a point that solves both plane equations
        // simultaneous.  For example, let's say that the vector is (0.5,0.5,1), and the two plane equations are:
        // 0.7 x + 0.3 y + 0.1 z + 0.0 = 0
        // and
        // 0.9 x - 0.1 y + 0.2 z + 4.0 = 0
        // Then we'd pick z = 0, so the equations to solve for x and y would be:
        // 0.7 x + 0.3y = 0.0
        // 0.9 x - 0.1y = -4.0
        // ... which can readily be solved using standard linear algebra.  Generally:
        // Q0 x + R0 y = S0
        // Q1 x + R1 y = S1
        // ... can be solved by Cramer's rule:
        // x = det(S0 R0 / S1 R1) / det(Q0 R0 / Q1 R1)
        // y = det(Q0 S0 / Q1 S1) / det(Q0 R0 / Q1 R1)
        // ... where det( a b / c d ) = ad - bc, so:
        // x = (S0 * R1 - R0 * S1) / (Q0 * R1 - R0 * Q1)
        // y = (Q0 * S1 - S0 * Q1) / (Q0 * R1 - R0 * Q1)
        double x0;
        double y0;
        double z0;
        // We try to maximize the determinant in the denominator
        double denomYZ = this.y*q.z - this.z*q.y;
        double denomXZ = this.x*q.z - this.z*q.x;
        double denomXY = this.x*q.y - this.y*q.x;
        if (Math.abs(denomYZ) >= Math.abs(denomXZ) && Math.abs(denomYZ) >= Math.abs(denomXY)) {
            // X is the biggest, so our point will have x0 = 0.0
            if (Math.abs(denomYZ) < 1.0e-35)
                return new GeoPoint[0];
            double denom = 1.0 / denomYZ;
            x0 = 0.0;
            y0 = (-this.D * q.z - this.z * -q.D) * denom;
            z0 = (this.y * -q.D + this.D * q.y) * denom;
        } else if (Math.abs(denomXZ) >= Math.abs(denomXY) && Math.abs(denomXZ) >= Math.abs(denomYZ)) {
            // Y is the biggest, so y0 = 0.0
            if (Math.abs(denomXZ) < 1.0e-35)
                return new GeoPoint[0];
            double denom = 1.0 / denomXZ;
            x0 = (-this.D * q.z - this.z * -q.D) * denom;
            y0 = 0.0;
            z0 = (this.x * -q.D + this.D * q.x) * denom;
        } else {
            // Z is the biggest, so Z0 = 0.0
            if (Math.abs(denomXY) < 1.0e-35)
                return new GeoPoint[0];
            double denom = 1.0 / denomXY;
            x0 = (-this.D * q.y - this.y * -q.D) * denom;
            y0 = (this.x * -q.D + this.D * q.x) * denom;
            z0 = 0.0;
        }

        // Once an intersecting line is determined, the next step is to intersect that line with the unit sphere, which
        // will yield zero, one, or two points.
        // The equation of the sphere is: 1.0 = x^2 + y^2 + z^2.  Plugging in the parameterized line values yields:
        // 1.0 = (At+A0)^2 + (Bt+B0)^2 + (Ct+C0)^2
        // A^2 t^2 + 2AA0t + A0^2 + B^2 t^2 + 2BB0t + B0^2 + C^2 t^2 + 2CC0t + C0^2 - 1,0 = 0.0
        // [A^2 + B^2 + C^2] t^2 + [2AA0 + 2BB0 + 2CC0] t + [A0^2 + B0^2 + C0^2 - 1,0] = 0.0
        // Use the quadratic formula to determine t values and candidate point(s)
        double A = lineVector.x * lineVector.x + lineVector.y * lineVector.y + lineVector.z * lineVector.z;
        double B = 2.0*(lineVector.x * x0 + lineVector.y * y0 + lineVector.z * z0);
        double C = x0*x0 + y0*y0 + z0*z0 - 1.0;

        double BsquaredMinus = B * B - 4.0 * A * C;
        if (BsquaredMinus < 0.0)
            return new GeoPoint[0];
        double inverse2A = 1.0 / (2.0 * A);
        if (BsquaredMinus == 0.0) {
            // One solution only
            double t = -B * inverse2A;
            GeoPoint point = new GeoPoint(lineVector.x * t + x0, lineVector.y * t + y0, lineVector.z * t + z0);
            if (point.isWithin(bounds,moreBounds))
                return new GeoPoint[]{point};
            return new GeoPoint[0];
        } else {
            // Two solutions
            double sqrtTerm = Math.sqrt(BsquaredMinus);
            double t1 = (-B + sqrtTerm) * inverse2A;
            double t2 = (-B - sqrtTerm) * inverse2A;
            GeoPoint point1 = new GeoPoint(lineVector.x * t1 + x0, lineVector.y * t1 + y0, lineVector.z * t1 + z0);
            GeoPoint point2 = new GeoPoint(lineVector.x * t2 + x0, lineVector.y * t2 + y0, lineVector.z * t2 + z0);
            if (point1.isWithin(bounds,moreBounds)) {
                if (point2.isWithin(bounds,moreBounds))
                    return new GeoPoint[]{point1,point2};
                return new GeoPoint[]{point1};
            }
            if (point2.isWithin(bounds,moreBounds))
                return new GeoPoint[]{point2};
            return new GeoPoint[0];
        }
    }
    
    /** Accumulate bounds information for this plane, intersected with another plane
    * and with the unit sphere.
    * Updates both latitude and longitude information, using max/min points found
    * within the specified bounds.
    *@param q is the plane to intersect with.
    *@param boundsInfo is the info to update with additional bounding information.
    *@param bounds are the surfaces delineating what's inside the shape.
    */
    public void recordBounds(Plane q, Bounds boundsInfo, Membership... bounds) {
        GeoPoint[] intersectionPoints = findIntersections(q,bounds,new Membership[0]);
        for (GeoPoint intersectionPoint : intersectionPoints) {
            boundsInfo.addPoint(intersectionPoint);
        }
    }

    /** Accumulate bounds information for this plane, intersected with the unit sphere.
    * Updates both latitude and longitude information, using max/min points found
    * within the specified bounds.
    *@param boundsInfo is the info to update with additional bounding information.
    *@param bounds are the surfaces delineating what's inside the shape.
    */
    public void recordBounds(Bounds boundsInfo, Membership... bounds) {
        // For clarity, load local variables with good names
        double A = this.x;
        double B = this.y;
        double C = this.z;

        // Now compute latitude min/max points
        if (!boundsInfo.checkNoTopLatitudeBound() || !boundsInfo.checkNoBottomLatitudeBound()) {
            if ((Math.abs(A) >= 1e-10 || Math.abs(B) >= 1e-10)) {
                //System.out.println("A = "+A+" B = "+B+" C = "+C+" D = "+D);
                // sin (phi) = z
                // cos (theta - phi) = D
                // sin (theta) = C  (the dot product of (0,0,1) and (A,B,C) )
                // Q: what is z?
                //
                // cos (theta-phi) = cos(theta)cos(phi) + sin(theta)sin(phi) = D

                if (Math.abs(C) < 1.0e-10) {
                    // Special case: circle is vertical.
                    //System.out.println("Degenerate case; it's vertical circle");
                    // cos(phi) = D, and we want sin(phi) = z
                    // There are two solutions for phi given cos(phi) = D: a positive solution and a negative solution.
                    // So, when we compute z = sqrt(1-D^2), it's really z = +/- sqrt(1-D^2) .
                    
                    double z;
                    double x;
                    double y;

                    double denom = 1.0 / (A*A + B*B);

                    z = Math.sqrt(1.0 - D*D);
                    y = -B * D * denom;
                    x = -A * D * denom;
                    addPoint(boundsInfo, bounds, x, y, z);

                    z = -z;
                    addPoint(boundsInfo, bounds, x, y, z);
                } else {
                    // We might be able to identify a specific new latitude maximum or minimum.
                    //
                    // cos (theta-phi) = cos(theta)cos(phi) + sin(theta)sin(phi) = D
                    //
                    // This is tricky.  If cos(phi) = something, and we want to figure out
                    // what sin(phi) is, in order to capture all solutions we need to recognize
                    // that sin(phi) = +/- sqrt(1 - cos(phi)^2).  Basically, this means that
                    // whatever solution we find we have to mirror it across the x-y plane,
                    // and include both +z and -z solutions.
                    //
                    // cos (phi) = +/- sqrt(1-sin(phi)^2) = +/- sqrt(1-z^2)
                    // cos (theta) = +/- sqrt(1-sin(theta)^2) = +/- sqrt(1-C^2)
                    //
                    // D = cos(theta)cos(phi) + sin(theta)sin(phi)
                    // Substitute:
                    // D = sqrt(1-C^2) * sqrt(1-z^2) + C * z
                    // Solve for z...
                    // D-Cz = sqrt(1-C^2)*sqrt(1-z^2) = sqrt(1 - z^2 - C^2 + z^2*C^2)
                    // Square both sides.
                    // (D-Cz)^2 = 1 - z^2 - C^2 + z^2*C^2
                    // D^2 - 2DCz + C^2*z^2 = 1 - z^2 - C^2 + z^2*C^2
                    // D^2 - 2DCz  = 1 - C^2 - z^2
                    // 0 = z^2 - 2DCz + (C^2 +D^2-1) = 0
                    //
                    // z = (2DC +/- sqrt(4*D^2*C^2 - 4*(C^2+D^2-1))) / (2)
                    // z  = DC +/- sqrt(D^2*C^2 + 1 - C^2 - D^2 )
                    //    = DC +/- sqrt(D^2*C^2 + 1 - C^2 - D^2)

                    double z;
                    double x;
                    double y;
                    
                    double sqrtValue = D*D*C*C + 1.0 - C*C - D*D;
                    if (sqrtValue >= 0.0) {
                        // y = -B[D+Cz] / [A^2 + B^2]
                        // x = -A[D+Cz] / [A^2 + B^2]
                        double denom = 1.0 / (A*A + B*B);
                        if (sqrtValue == 0.0) {
                            //System.out.println("Zero sqrt term");
                            z = D*C;
                            // Since we squared both sides of the equation, we may have introduced spurious solutions, so we have to check.
                            if (Math.abs(D-C*z - Math.sqrt(1.0 - z*z - C*C + z*z*C*C)) < 1.0e-10) {
                                y = -B * (D + C*z) * denom;
                                x = -A * (D + C*z) * denom;
                                addPoint(boundsInfo, bounds, x, y, z);
                            }
                            z = -D*C;
                            // Since we squared both sides of the equation, we may have introduced spurious solutions, so we have to check.
                            if (Math.abs(D+C*z + Math.sqrt(1.0 - z*z - C*C + z*z*C*C)) < 1.0e-10) {
                                y = -B * (D + C*z) * denom;
                                x = -A * (D + C*z) * denom;
                                addPoint(boundsInfo, bounds, x, y, z);
                            }
                        } else {
                            double sqrtResult = Math.sqrt(sqrtValue);
                            z = D*C + sqrtResult;
                            //System.out.println("z= "+z+" D-C*z = " + (D-C*z) + " Math.sqrt(1.0 - z*z - C*C + z*z*C*C) = "+(Math.sqrt(1.0 - z*z - C*C + z*z*C*C)));
                            // Since we squared both sides of the equation, we may have introduced spurios solutions, so we have to check.
                            if (Math.abs(D-C*z - Math.sqrt(1.0 - z*z - C*C + z*z*C*C)) < 1.0e-10) {
                                //System.out.println("found a point; z = "+z);
                                y = -B * (D + C*z) * denom;
                                x = -A * (D + C*z) * denom;
                                addPoint(boundsInfo, bounds, x, y, z);
                            }
                            z = D*C - sqrtResult;
                            //System.out.println("z= "+z+" D-C*z = " + (D-C*z) + " Math.sqrt(1.0 - z*z - C*C + z*z*C*C) = "+(Math.sqrt(1.0 - z*z - C*C + z*z*C*C)));
                            // Since we squared both sides of the equation, we may have introduced spurios solutions, so we have to check.
                            if (Math.abs(D-C*z - Math.sqrt(1.0 - z*z - C*C + z*z*C*C)) < 1.0e-10) {
                                //System.out.println("found a point; z="+z);
                                y = -B * (D + C*z) * denom;
                                x = -A * (D + C*z) * denom;
                                addPoint(boundsInfo, bounds, x, y, z);
                            }
                            z = -(D*C + sqrtResult);
                            //System.out.println("z= "+z+" D+C*z = " + (D+C*z) + " -Math.sqrt(1.0 - z*z - C*C + z*z*C*C) = "+(-Math.sqrt(1.0 - z*z - C*C + z*z*C*C)));
                            // Since we squared both sides of the equation, we may have introduced spurios solutions, so we have to check.
                            if (Math.abs(D+C*z + Math.sqrt(1.0 - z*z - C*C + z*z*C*C)) < 1.0e-10) {
                                //System.out.println("found a point; z = "+z);
                                y = -B * (D + C*z) * denom;
                                x = -A * (D + C*z) * denom;
                                addPoint(boundsInfo, bounds, x, y, z);
                            }
                            z = -(D*C - sqrtResult);
                            //System.out.println("z= "+z+" D+C*z = " + (D+C*z) + " -Math.sqrt(1.0 - z*z - C*C + z*z*C*C) = "+(-Math.sqrt(1.0 - z*z - C*C + z*z*C*C)));
                            // Since we squared both sides of the equation, we may have introduced spurios solutions, so we have to check.
                            if (Math.abs(D+C*z + Math.sqrt(1 - z*z - C*C + z*z*C*C)) < 1.0e-10) {
                                //System.out.println("found a point; z="+z);
                                y = -B * (D + C*z) * denom;
                                x = -A * (D + C*z) * denom;
                                addPoint(boundsInfo, bounds, x, y, z);
                            }
                        }
                    }
                }
            } else {
                // Horizontal circle.
                // Since the recordBounds() method will be called ONLY for planes that constitute edges of a shape,
                // we can be sure that some part of the horizontal circle will be part of the boundary, so we don't need
                // to check Membership objects.
                boundsInfo.addHorizontalCircle(-D * C);
            }
        }
        
        // First, figure out our longitude bounds, unless we no longer need to consider that
        if (!boundsInfo.checkNoLongitudeBound()) {
            
            //System.out.println("A = "+A+" B = "+B+" C = "+C+" D = "+D);
            // Compute longitude bounds
            
            double a;
            double b;
            double c;
            
            if (Math.abs(C) < 1e-10) {
                // Degenerate; the equation describes a line
                //System.out.println("It's a zero-width ellipse");
                // Ax + By + D = 0
                if (Math.abs(D) >= 1e-10) {
                    if (Math.abs(A) > Math.abs(B)) {
                        // Use equation suitable for A != 0
                        // We need to find the endpoints of the zero-width ellipse.
                        // Geometrically, we have a line segment in x-y space.  We need to locate the endpoints
                        // of that line.  But luckily, we know some things: specifically, since it is a
                        // degenerate situation in projection, the C value had to have been 0.  That
                        // means that our line's endpoints will coincide with the unit circle.  All we
                        // need to do then is to find the intersection of the unit circle and the line
                        // equation:
                        //
                        // A x + B y + D = 0
                        // 
                        // Since A != 0:
                        // x = (-By - D)/A
                        // 
                        // The unit circle:
                        // x^2 + y^2 - 1 = 0
                        // Substitute:
                        // [(-By-D)/A]^2 + y^2 -1 = 0
                        // Multiply through by A^2:
                        // [-By - D]^2 + A^2*y^2 - A^2 = 0
                        // Multiply out:
                        // B^2*y^2 + 2BDy + D^2 + A^2*y^2 - A^2 = 0
                        // Group:
                        // y^2 * [B^2 + A^2] + y [2BD] + [D^2-A^2] = 0
                        
                        a = B * B + A * A;
                        b = 2.0 * B * D;
                        c = D * D - A * A;
                        
                        double sqrtClause = b * b - 4.0 * a * c;
                            
                        if (sqrtClause >= 0.0) {
                            if (sqrtClause == 0.0) {
                                double y0 = -b / (2.0 * a);
                                double x0 = (-D - B * y0) / A;
                                double z0 = 0.0;
                                addPoint(boundsInfo, bounds, x0, y0, z0);
                            } else {
                                double sqrtResult = Math.sqrt(sqrtClause);
                                double denom = 1.0 / (2.0 * a);
                                double Hdenom = 1.0 / A;
                                
                                double y0a = (-b + sqrtResult ) * denom;
                                double y0b = (-b - sqrtResult ) * denom;

                                double x0a = (-D - B * y0a) * Hdenom;
                                double x0b = (-D - B * y0b) * Hdenom;
                                
                                double z0a = 0.0;
                                double z0b = 0.0;

                                addPoint(boundsInfo, bounds, x0a, y0a, z0a);
                                addPoint(boundsInfo, bounds, x0b, y0b, z0b);
                            }
                        }

                    } else {
                        // Use equation suitable for B != 0
                        // Since I != 0, we rewrite:
                        // y = (-Ax - D)/B
                        a = B * B + A * A;
                        b = 2.0 * A * D;
                        c = D * D - B * B;
                        
                        double sqrtClause = b * b - 4.0 * a * c;

                        if (sqrtClause >= 0.0) {

                            if (sqrtClause == 0.0) {
                                double x0 = -b / (2.0 * a);
                                double y0 = (- D - A * x0) / B;
                                double z0 = 0.0;
                                addPoint(boundsInfo, bounds, x0, y0, z0);
                            } else {
                                double sqrtResult = Math.sqrt(sqrtClause);
                                double denom = 1.0 / (2.0 * a);
                                double Idenom = 1.0 / B;
                                
                                double x0a = (-b + sqrtResult ) * denom;
                                double x0b = (-b - sqrtResult ) * denom;
                                double y0a = (- D - A * x0a) * Idenom;
                                double y0b = (- D - A * x0b) * Idenom;
                                double z0a = 0.0;
                                double z0b = 0.0;

                                addPoint(boundsInfo, bounds, x0a, y0a, z0a);
                                addPoint(boundsInfo, bounds, x0b, y0b, z0b);
                            }
                        }
                    }
                }
                
            } else {

                // (1) Intersect the plane and the unit sphere, and project the results into the x-y plane:
                // From plane:
                // z = (-Ax - By - D) / C
                // From unit sphere:
                // x^2 + y^2 + [(-Ax - By - D) / C]^2 = 1
                // Simplify/expand:
                // C^2*x^2 + C^2*y^2 + (-Ax - By - D)^2 = C^2
                // 
                // x^2 * C^2 + y^2 * C^2 + x^2 * (A^2 + ABxy + ADx) + (ABxy + y^2 * B^2 + BDy) + (ADx + BDy + D^2) = C^2
                // Group:
                // [A^2 + C^2] x^2 + [B^2 + C^2] y^2 + [2AB]xy + [2AD]x + [2BD]y + [D^2-C^2] = 0
                // For convenience, introduce post-projection coefficient variables to make life easier.
                // E x^2 + F y^2 + G xy + H x + I y + J = 0
                double E = A * A + C * C;
                double F = B * B + C * C;
                double G = 2.0 * A * B;
                double H = 2.0 * A * D;
                double I = 2.0 * B * D;
                double J = D * D - C * C;

                //System.out.println("E = " + E + " F = " + F + " G = " + G + " H = "+ H + " I = " + I + " J = " + J);
                
                // Check if the origin is within, by substituting x = 0, y = 0 and seeing if less than zero
                if (J > 0.0) {
                    // The derivative of the curve above is:
                    // 2Exdx + 2Fydy + G(xdy+ydx) + Hdx + Idy = 0
                    // (2Ex + Gy + H)dx + (2Fy + Gx + I)dy = 0
                    // dy/dx = - (2Ex + Gy + H) / (2Fy + Gx + I)
                    //
                    // The equation of a line going through the origin with the slope dy/dx is:
                    // y = dy/dx x
                    // y = - (2Ex + Gy + H) / (2Fy + Gx + I)  x
                    // Rearrange:
                    // (2Fy + Gx + I) y + (2Ex + Gy + H) x = 0
                    // 2Fy^2 + Gxy + Iy + 2Ex^2 + Gxy + Hx = 0
                    // 2Ex^2 + 2Fy^2 + 2Gxy + Hx + Iy = 0
                    //
                    // Multiply the original equation by 2:
                    // 2E x^2 + 2F y^2 + 2G xy + 2H x + 2I y + 2J = 0
                    // Subtract one from the other, to remove the high-order terms:
                    // Hx + Iy + 2J = 0
                    // Now, we can substitute either x = or y = into the derivative equation, or into the original equation.
                    // But we will need to base this on which coefficient is non-zero
                    
                    if (Math.abs(H) > Math.abs(I)) {
                        //System.out.println("Using the y quadratic");
                        // x = (-2J - Iy)/H
                        
                        // Plug into the original equation:
                        // E [(-2J - Iy)/H]^2 + F y^2 + G [(-2J - Iy)/H]y + H [(-2J - Iy)/H] + I y + J = 0
                        // E [(-2J - Iy)/H]^2 + F y^2 + G [(-2J - Iy)/H]y - J = 0
                        // Same equation as derivative equation, except for a factor of 2!  So it doesn't matter which we pick.
                        
                        // Plug into derivative equation:
                        // 2E[(-2J - Iy)/H]^2 + 2Fy^2 + 2G[(-2J - Iy)/H]y + H[(-2J - Iy)/H] + Iy = 0
                        // 2E[(-2J - Iy)/H]^2 + 2Fy^2 + 2G[(-2J - Iy)/H]y - 2J = 0
                        // E[(-2J - Iy)/H]^2 + Fy^2 + G[(-2J - Iy)/H]y - J = 0

                        // Multiply by H^2 to make manipulation easier
                        // E[(-2J - Iy)]^2 + F*H^2*y^2 + GH[(-2J - Iy)]y - J*H^2 = 0
                        // Do the square
                        // E[4J^2 + 4IJy + I^2*y^2] + F*H^2*y^2 + GH(-2Jy - I*y^2) - J*H^2 = 0

                        // Multiply it out
                        // 4E*J^2 + 4EIJy + E*I^2*y^2 + H^2*Fy^2 - 2GHJy - GH*I*y^2 - J*H^2 = 0
                        // Group:
                        // y^2 [E*I^2 - GH*I + F*H^2] + y [4EIJ - 2GHJ] + [4E*J^2 - J*H^2] = 0

                        a = E * I * I - G * H * I + F*H*H;
                        b = 4.0 * E * I * J - 2.0 * G * H * J;
                        c = 4.0 * E * J * J - J * H * H;
                        
                        //System.out.println("a="+a+" b="+b+" c="+c);
                        double sqrtClause = b * b - 4.0 * a * c;
                        //System.out.println("sqrtClause="+sqrtClause);
                        
                        if (sqrtClause >= 0.0) {
                            if (sqrtClause == 0.0) {
                                //System.out.println("One solution");
                                double y0 = -b / (2.0 * a);
                                double x0 = (-2.0 * J - I * y0) / H;
                                double z0 = (-A*x0 - B*y0 - D)/C;

                                addPoint(boundsInfo, bounds, x0, y0, z0);
                            } else {
                                //System.out.println("Two solutions");
                                double sqrtResult = Math.sqrt(sqrtClause);
                                double denom = 1.0 / (2.0 * a);
                                double Hdenom = 1.0 / H;
                                double Cdenom = 1.0 / C;
                                    
                                double y0a = (-b + sqrtResult ) * denom;
                                double y0b = (-b - sqrtResult ) * denom;
                                double x0a = (-2.0 * J - I * y0a) * Hdenom;
                                double x0b = (-2.0 * J - I * y0b) * Hdenom;
                                double z0a = (-A*x0a - B*y0a - D) * Cdenom;
                                double z0b = (-A*x0b - B*y0b - D) * Cdenom;

                                addPoint(boundsInfo, bounds, x0a, y0a, z0a);
                                addPoint(boundsInfo, bounds, x0b, y0b, z0b);
                            }
                        }                        

                    } else {
                        //System.out.println("Using the x quadratic");
                        // y = (-2J - Hx)/I
                        
                        // Plug into the original equation:
                        // E x^2 + F [(-2J - Hx)/I]^2 + G x[(-2J - Hx)/I] - J = 0

                        // Multiply by I^2 to make manipulation easier
                        // E * I^2 * x^2 + F [(-2J - Hx)]^2 + GIx[(-2J - Hx)] - J * I^2 = 0
                        // Do the square
                        // E * I^2 * x^2 + F [ 4J^2 + 4JHx + H^2*x^2] + GI[(-2Jx - H*x^2)] - J * I^2 = 0

                        // Multiply it out
                        // E * I^2 * x^2 + 4FJ^2 + 4FJHx + F*H^2*x^2 - 2GIJx - HGI*x^2 - J * I^2 = 0
                        // Group:
                        // x^2 [E*I^2 - GHI + F*H^2] + x [4FJH - 2GIJ] + [4FJ^2 - J*I^2] = 0

                        a = E * I * I - G * H * I + F*H*H;
                        b = 4.0 * F * H * J - 2.0 * G * I * J;
                        c = 4.0 * F * J * J - J * I * I;
                        
                        //System.out.println("a="+a+" b="+b+" c="+c);
                        double sqrtClause = b * b - 4.0 * a * c;
                        //System.out.println("sqrtClause="+sqrtClause);
                        
                        if (sqrtClause >= 0.0) {
                            
                            if (sqrtClause == 0.0) {
                                //System.out.println("One solution");
                                double x0 = -b / (2.0 * a);
                                double y0 = (-2.0 * J - H * x0) / I;
                                double z0 = (-A*x0 - B*y0 - D)/C;
                                addPoint(boundsInfo, bounds, x0, y0, z0);
                            } else {
                                //System.out.println("Two solutions");
                                double sqrtResult = Math.sqrt(sqrtClause);
                                double denom = 1.0 / (2.0 * a);
                                double Idenom = 1.0 / I;
                                double Cdenom = 1.0 / C;
                                    
                                double x0a = (-b + sqrtResult ) * denom;
                                double x0b = (-b - sqrtResult ) * denom;
                                double y0a = (-2.0 * J - H * x0a) * Idenom;
                                double y0b = (-2.0 * J - H * x0b) * Idenom;
                                double z0a = (-A*x0a - B*y0a - D) * Cdenom;
                                double z0b = (-A*x0b - B*y0b - D) * Cdenom;

                                addPoint(boundsInfo, bounds, x0a, y0a, z0a);
                                addPoint(boundsInfo, bounds, x0b, y0b, z0b);
                            }
                        }
                    }
                }
            }
        }

    }
    
    protected static void addPoint(Bounds boundsInfo, Membership[] bounds, double x, double y, double z) {
        // Make sure the discovered point is within the bounds
        for (Membership bound : bounds) {
            if (!bound.isWithin(x,y,z))
                return;
        }
        // Add the point
        //System.out.println("Adding point x="+x+" y="+y+" z="+z);
        boundsInfo.addPoint(x,y,z);
    }
    
    /** Determine whether the plane intersects another plane within the
     * bounds provided.
     *@param q is the other plane.
     *@param bounds is one part of the bounds.
     *@param moreBounds are more bounds.
     *@return true if there's an intersection.
     */
    public boolean intersects(Plane q, Membership[] bounds, Membership... moreBounds) {
        return findIntersections(q,bounds,moreBounds).length > 0;
    }
    
    @Override
    public String toString() {
        return "[A="+x+", B="+y+"; C="+z+"; D="+D+"]";
    }
    
    @Override
    public boolean equals(Object o) {
        if (!super.equals(o))
            return false;
        if (!(o instanceof Plane))
            return false;
        Plane other = (Plane)o;
        return other.D == D;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(D);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
