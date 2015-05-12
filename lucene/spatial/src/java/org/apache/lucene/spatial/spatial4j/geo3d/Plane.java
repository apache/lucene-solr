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

/**
 * We know about three kinds of planes.  First kind: general plain through two points and origin
 * Second kind: horizontal plane at specified height.  Third kind: vertical plane with specified x and y value, through origin.
 *
 * @lucene.experimental
 */
public class Plane extends Vector {
  protected final static GeoPoint[] NO_POINTS = new GeoPoint[0];
  protected final static Membership[] NO_BOUNDS = new Membership[0];

  public final double D;

  /**
   * Construct a plane with all four coefficients defined.
   */
  public Plane(final double A, final double B, final double C, final double D) {
    super(A, B, C);
    this.D = D;
  }

  /**
   * Construct a plane through two points and origin.
   *
   * @param A is the first point (origin based).
   * @param B is the second point (origin based).
   */
  public Plane(final Vector A, final Vector B) {
    super(A, B);
    D = 0.0;
  }

  /**
   * Construct a horizontal plane at a specified Z.
   *
   * @param height is the specified Z coordinate.
   */
  public Plane(final double height) {
    super(0.0, 0.0, 1.0);
    D = -height;
  }

  /**
   * Construct a vertical plane through a specified
   * x, y and origin.
   *
   * @param x is the specified x value.
   * @param y is the specified y value.
   */
  public Plane(final double x, final double y) {
    super(y, -x, 0.0);
    D = 0.0;
  }

  /**
   * Construct a plane with a specific vector, and D offset
   * from origin.
   *
   * @param D is the D offset from the origin.
   */
  public Plane(final Vector v, final double D) {
    super(v.x, v.y, v.z);
    this.D = D;
  }

  /**
   * Evaluate the plane equation for a given point, as represented
   * by a vector.
   *
   * @param v is the vector.
   * @return the result of the evaluation.
   */
  public double evaluate(final Vector v) {
    return dotProduct(v) + D;
  }

  /**
   * Evaluate the plane equation for a given point, as represented
   * by a vector.
   */
  public double evaluate(final double x, final double y, final double z) {
    return dotProduct(x, y, z) + D;
  }

  /**
   * Evaluate the plane equation for a given point, as represented
   * by a vector.
   *
   * @param v is the vector.
   * @return true if the result is on the plane.
   */
  public boolean evaluateIsZero(final Vector v) {
    return Math.abs(evaluate(v)) < MINIMUM_RESOLUTION;
  }

  /**
   * Evaluate the plane equation for a given point, as represented
   * by a vector.
   *
   * @return true if the result is on the plane.
   */
  public boolean evaluateIsZero(final double x, final double y, final double z) {
    return Math.abs(evaluate(x, y, z)) < MINIMUM_RESOLUTION;
  }

  /**
   * Build a normalized plane, so that the vector is normalized.
   *
   * @return the normalized plane object, or null if the plane is indeterminate.
   */
  public Plane normalize() {
    Vector normVect = super.normalize();
    if (normVect == null)
      return null;
    return new Plane(normVect, this.D);
  }

  /**
   * Find points on the boundary of the intersection of a plane and the unit sphere,
   * given a starting point, and ending point, and a list of proportions of the arc (e.g. 0.25, 0.5, 0.75).
   * The angle between the starting point and ending point is assumed to be less than pi.
   */
  public GeoPoint[] interpolate(final GeoPoint start, final GeoPoint end, final double[] proportions) {
    // Steps:
    // (1) Translate (x0,y0,z0) of endpoints into origin-centered place:
    // x1 = x0 + D*A
    // y1 = y0 + D*B
    // z1 = z0 + D*C
    // (2) Rotate counterclockwise in x-y:
    // ra = -atan2(B,A)
    // x2 = x1 cos ra - y1 sin ra
    // y2 = x1 sin ra + y1 cos ra
    // z2 = z1
    // Faster:
    // cos ra = A/sqrt(A^2+B^2+C^2)
    // sin ra = -B/sqrt(A^2+B^2+C^2)
    // cos (-ra) = A/sqrt(A^2+B^2+C^2)
    // sin (-ra) = B/sqrt(A^2+B^2+C^2)
    // (3) Rotate clockwise in x-z:
    // ha = pi/2 - asin(C/sqrt(A^2+B^2+C^2))
    // x3 = x2 cos ha - z2 sin ha
    // y3 = y2
    // z3 = x2 sin ha + z2 cos ha
    // At this point, z3 should be zero.
    // Faster:
    // sin(ha) = cos(asin(C/sqrt(A^2+B^2+C^2))) = sqrt(1 - C^2/(A^2+B^2+C^2)) = sqrt(A^2+B^2)/sqrt(A^2+B^2+C^2)
    // cos(ha) = sin(asin(C/sqrt(A^2+B^2+C^2))) = C/sqrt(A^2+B^2+C^2)
    // (4) Compute interpolations by getting longitudes of original points
    // la = atan2(y3,x3)
    // (5) Rotate new points (xN0, yN0, zN0) counter-clockwise in x-z:
    // ha = -(pi - asin(C/sqrt(A^2+B^2+C^2)))
    // xN1 = xN0 cos ha - zN0 sin ha
    // yN1 = yN0
    // zN1 = xN0 sin ha + zN0 cos ha
    // (6) Rotate new points clockwise in x-y:
    // ra = atan2(B,A)
    // xN2 = xN1 cos ra - yN1 sin ra
    // yN2 = xN1 sin ra + yN1 cos ra
    // zN2 = zN1
    // (7) Translate new points:
    // xN3 = xN2 - D*A
    // yN3 = yN2 - D*B
    // zN3 = zN2 - D*C

    // First, calculate the angles and their sin/cos values
    double A = x;
    double B = y;
    double C = z;

    // Translation amounts
    final double transX = -D * A;
    final double transY = -D * B;
    final double transZ = -D * C;

    double cosRA;
    double sinRA;
    double cosHA;
    double sinHA;

    double magnitude = magnitude();
    if (magnitude >= MINIMUM_RESOLUTION) {
      final double denom = 1.0 / magnitude;
      A *= denom;
      B *= denom;
      C *= denom;

      // cos ra = A/sqrt(A^2+B^2+C^2)
      // sin ra = -B/sqrt(A^2+B^2+C^2)
      // cos (-ra) = A/sqrt(A^2+B^2+C^2)
      // sin (-ra) = B/sqrt(A^2+B^2+C^2)
      final double xyMagnitude = Math.sqrt(A * A + B * B);
      if (xyMagnitude >= MINIMUM_RESOLUTION) {
        final double xyDenom = 1.0 / xyMagnitude;
        cosRA = A * xyDenom;
        sinRA = -B * xyDenom;
      } else {
        cosRA = 1.0;
        sinRA = 0.0;
      }

      // sin(ha) = cos(asin(C/sqrt(A^2+B^2+C^2))) = sqrt(1 - C^2/(A^2+B^2+C^2)) = sqrt(A^2+B^2)/sqrt(A^2+B^2+C^2)
      // cos(ha) = sin(asin(C/sqrt(A^2+B^2+C^2))) = C/sqrt(A^2+B^2+C^2)
      sinHA = xyMagnitude;
      cosHA = C;
    } else {
      cosRA = 1.0;
      sinRA = 0.0;
      cosHA = 1.0;
      sinHA = 0.0;
    }

    // Forward-translate the start and end points
    final Vector modifiedStart = modify(start, transX, transY, transZ, sinRA, cosRA, sinHA, cosHA);
    final Vector modifiedEnd = modify(end, transX, transY, transZ, sinRA, cosRA, sinHA, cosHA);
    if (Math.abs(modifiedStart.z) >= MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("Start point was not on plane: " + modifiedStart.z);
    if (Math.abs(modifiedEnd.z) >= MINIMUM_RESOLUTION)
      throw new IllegalArgumentException("End point was not on plane: " + modifiedEnd.z);

    // Compute the angular distance between start and end point
    final double startAngle = Math.atan2(modifiedStart.y, modifiedStart.x);
    final double endAngle = Math.atan2(modifiedEnd.y, modifiedEnd.x);

    final double startMagnitude = Math.sqrt(modifiedStart.x * modifiedStart.x + modifiedStart.y * modifiedStart.y);
    double delta;
    double beginAngle;

    double newEndAngle = endAngle;
    while (newEndAngle < startAngle) {
      newEndAngle += Math.PI * 2.0;
    }

    if (newEndAngle - startAngle <= Math.PI) {
      delta = newEndAngle - startAngle;
      beginAngle = startAngle;
    } else {
      double newStartAngle = startAngle;
      while (newStartAngle < endAngle) {
        newStartAngle += Math.PI * 2.0;
      }
      delta = newStartAngle - endAngle;
      beginAngle = endAngle;
    }

    final GeoPoint[] returnValues = new GeoPoint[proportions.length];
    for (int i = 0; i < returnValues.length; i++) {
      final double newAngle = startAngle + proportions[i] * delta;
      final double sinNewAngle = Math.sin(newAngle);
      final double cosNewAngle = Math.cos(newAngle);
      final Vector newVector = new Vector(cosNewAngle * startMagnitude, sinNewAngle * startMagnitude, 0.0);
      returnValues[i] = reverseModify(newVector, transX, transY, transZ, sinRA, cosRA, sinHA, cosHA);
    }

    return returnValues;
  }

  /**
   * Modify a point to produce a vector in translated/rotated space.
   */
  protected static Vector modify(final GeoPoint start, final double transX, final double transY, final double transZ,
                                 final double sinRA, final double cosRA, final double sinHA, final double cosHA) {
    return start.translate(transX, transY, transZ).rotateXY(sinRA, cosRA).rotateXZ(sinHA, cosHA);
  }

  /**
   * Reverse modify a point to produce a GeoPoint in normal space.
   */
  protected static GeoPoint reverseModify(final Vector point, final double transX, final double transY, final double transZ,
                                          final double sinRA, final double cosRA, final double sinHA, final double cosHA) {
    final Vector result = point.rotateXZ(-sinHA, cosHA).rotateXY(-sinRA, cosRA).translate(-transX, -transY, -transZ);
    return new GeoPoint(result.x, result.y, result.z);
  }

  /**
   * Find the intersection points between two planes, given a set of bounds.
   *
   * @param q          is the plane to intersect with.
   * @param bounds     is the set of bounds.
   * @param moreBounds is another set of bounds.
   * @return the intersection point(s) on the unit sphere, if there are any.
   */
  protected GeoPoint[] findIntersections(final Plane q, final Membership[] bounds, final Membership[] moreBounds) {
    final Vector lineVector = new Vector(this, q);
    if (Math.abs(lineVector.x) < MINIMUM_RESOLUTION && Math.abs(lineVector.y) < MINIMUM_RESOLUTION && Math.abs(lineVector.z) < MINIMUM_RESOLUTION) {
      // Degenerate case: parallel planes
      //System.err.println(" planes are parallel - no intersection");
      return NO_POINTS;
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
    final double denomYZ = this.y * q.z - this.z * q.y;
    final double denomXZ = this.x * q.z - this.z * q.x;
    final double denomXY = this.x * q.y - this.y * q.x;
    if (Math.abs(denomYZ) >= Math.abs(denomXZ) && Math.abs(denomYZ) >= Math.abs(denomXY)) {
      // X is the biggest, so our point will have x0 = 0.0
      if (Math.abs(denomYZ) < MINIMUM_RESOLUTION_SQUARED) {
        //System.err.println(" Denominator is zero: no intersection");
        return NO_POINTS;
      }
      final double denom = 1.0 / denomYZ;
      x0 = 0.0;
      y0 = (-this.D * q.z - this.z * -q.D) * denom;
      z0 = (this.y * -q.D + this.D * q.y) * denom;
    } else if (Math.abs(denomXZ) >= Math.abs(denomXY) && Math.abs(denomXZ) >= Math.abs(denomYZ)) {
      // Y is the biggest, so y0 = 0.0
      if (Math.abs(denomXZ) < MINIMUM_RESOLUTION_SQUARED) {
        //System.err.println(" Denominator is zero: no intersection");
        return NO_POINTS;
      }
      final double denom = 1.0 / denomXZ;
      x0 = (-this.D * q.z - this.z * -q.D) * denom;
      y0 = 0.0;
      z0 = (this.x * -q.D + this.D * q.x) * denom;
    } else {
      // Z is the biggest, so Z0 = 0.0
      if (Math.abs(denomXY) < MINIMUM_RESOLUTION_SQUARED) {
        //System.err.println(" Denominator is zero: no intersection");
        return NO_POINTS;
      }
      final double denom = 1.0 / denomXY;
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
    final double A = lineVector.x * lineVector.x + lineVector.y * lineVector.y + lineVector.z * lineVector.z;
    final double B = 2.0 * (lineVector.x * x0 + lineVector.y * y0 + lineVector.z * z0);
    final double C = x0 * x0 + y0 * y0 + z0 * z0 - 1.0;

    final double BsquaredMinus = B * B - 4.0 * A * C;
    if (Math.abs(BsquaredMinus) < MINIMUM_RESOLUTION_SQUARED) {
      //System.err.println(" One point of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // One solution only
      final double t = -B * inverse2A;
      GeoPoint point = new GeoPoint(lineVector.x * t + x0, lineVector.y * t + y0, lineVector.z * t + z0);
      if (point.isWithin(bounds, moreBounds))
        return new GeoPoint[]{point};
      return NO_POINTS;
    } else if (BsquaredMinus > 0.0) {
      //System.err.println(" Two points of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // Two solutions
      final double sqrtTerm = Math.sqrt(BsquaredMinus);
      final double t1 = (-B + sqrtTerm) * inverse2A;
      final double t2 = (-B - sqrtTerm) * inverse2A;
      GeoPoint point1 = new GeoPoint(lineVector.x * t1 + x0, lineVector.y * t1 + y0, lineVector.z * t1 + z0);
      GeoPoint point2 = new GeoPoint(lineVector.x * t2 + x0, lineVector.y * t2 + y0, lineVector.z * t2 + z0);
      //System.err.println("  "+point1+" and "+point2);
      if (point1.isWithin(bounds, moreBounds)) {
        if (point2.isWithin(bounds, moreBounds))
          return new GeoPoint[]{point1, point2};
        return new GeoPoint[]{point1};
      }
      if (point2.isWithin(bounds, moreBounds))
        return new GeoPoint[]{point2};
      return NO_POINTS;
    } else {
      //System.err.println(" no solutions - no intersection");
      return NO_POINTS;
    }
  }

  /**
   * Accumulate bounds information for this plane, intersected with another plane
   * and with the unit sphere.
   * Updates both latitude and longitude information, using max/min points found
   * within the specified bounds.
   *
   * @param q          is the plane to intersect with.
   * @param boundsInfo is the info to update with additional bounding information.
   * @param bounds     are the surfaces delineating what's inside the shape.
   */
  public void recordBounds(final Plane q, final Bounds boundsInfo, final Membership... bounds) {
    final GeoPoint[] intersectionPoints = findIntersections(q, bounds, NO_BOUNDS);
    for (GeoPoint intersectionPoint : intersectionPoints) {
      boundsInfo.addPoint(intersectionPoint);
    }
  }

  /**
   * Accumulate bounds information for this plane, intersected with the unit sphere.
   * Updates both latitude and longitude information, using max/min points found
   * within the specified bounds.
   *
   * @param boundsInfo is the info to update with additional bounding information.
   * @param bounds     are the surfaces delineating what's inside the shape.
   */
  public void recordBounds(final Bounds boundsInfo, final Membership... bounds) {
    // For clarity, load local variables with good names
    final double A = this.x;
    final double B = this.y;
    final double C = this.z;

    // Now compute latitude min/max points
    if (!boundsInfo.checkNoTopLatitudeBound() || !boundsInfo.checkNoBottomLatitudeBound()) {
      //System.err.println("Looking at latitude for plane "+this);
      if ((Math.abs(A) >= MINIMUM_RESOLUTION || Math.abs(B) >= MINIMUM_RESOLUTION)) {
        //System.out.println("A = "+A+" B = "+B+" C = "+C+" D = "+D);
        // sin (phi) = z
        // cos (theta - phi) = D
        // sin (theta) = C  (the dot product of (0,0,1) and (A,B,C) )
        // Q: what is z?
        //
        // cos (theta-phi) = cos(theta)cos(phi) + sin(theta)sin(phi) = D

        if (Math.abs(C) < MINIMUM_RESOLUTION) {
          // Special case: circle is vertical.
          //System.err.println(" Degenerate case; it's vertical circle");
          // cos(phi) = D, and we want sin(phi) = z
          // There are two solutions for phi given cos(phi) = D: a positive solution and a negative solution.
          // So, when we compute z = sqrt(1-D^2), it's really z = +/- sqrt(1-D^2) .

          double z;
          double x;
          double y;

          final double denom = 1.0 / (A * A + B * B);

          z = Math.sqrt(1.0 - D * D);
          y = -B * D * denom;
          x = -A * D * denom;
          addPoint(boundsInfo, bounds, x, y, z);

          z = -z;
          addPoint(boundsInfo, bounds, x, y, z);
        } else if (Math.abs(D) < MINIMUM_RESOLUTION) {
          //System.err.println(" Plane through origin case");
          // The general case is degenerate when the plane goes through the origin.
          // Luckily there's a pretty good way to figure out the max and min for that case though.
          // We find the two z values by computing the angle of the plane's inclination with the normal.
          // E.g., if this.z == 1, then our z value is 0, and if this.z == 0, our z value is 1.
          // Also if this.z == -1, then z value is 0 again.
          // Another way of putting this is that our z = sqrt(this.x^2 + this.y^2).
          //
          // The only tricky part is computing x and y.
          double z;
          double x;
          double y;

          final double denom = 1.0 / (A * A + B * B);

          z = Math.sqrt((A * A + B * B) / (A * A + B * B + C * C));
          y = -B * (C * z) * denom;
          x = -A * (C * z) * denom;
          addPoint(boundsInfo, bounds, x, y, z);

          z = -z;
          y = -B * (C * z) * denom;
          x = -A * (C * z) * denom;
          addPoint(boundsInfo, bounds, x, y, z);

        } else {
          //System.err.println(" General latitude case");
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
          // D = sqrt(1-C^2) * sqrt(1-z^2) -/+ C * z
          // Solve for z...
          // D +/- Cz = sqrt(1-C^2)*sqrt(1-z^2) = sqrt(1 - z^2 - C^2 + z^2*C^2)
          // Square both sides.
          // (D +/- Cz)^2 = 1 - z^2 - C^2 + z^2*C^2
          // D^2 +/- 2DCz + C^2*z^2 = 1 - z^2 - C^2 + z^2*C^2
          // D^2 +/- 2DCz  = 1 - C^2 - z^2
          // 0 = z^2 +/- 2DCz + (C^2 +D^2-1) = 0
          //
          // z = (+/- 2DC +/- sqrt(4*D^2*C^2 - 4*(C^2+D^2-1))) / (2)
          // z  = +/- DC +/- sqrt(D^2*C^2 + 1 - C^2 - D^2 )
          //    = +/- DC +/- sqrt(D^2*C^2 + 1 - C^2 - D^2)
          //
          // NOTE WELL: The above is clearly degenerate when D = 0.  So we'll have to
          // code a different solution for that case!

          // To get x and y, we need to plug z into the equations, as follows:
          //
          // Ax + By = -Cz - D
          // x^2 + y^2 = 1 - z^2
          //
          // x = (-Cz -D -By) /A
          // y = (-Cz -D -Ax) /B
          //
          // [(-Cz -D -By) /A]^2 + y^2 = 1 - z^2
          // [-Cz -D -By]^2 + A^2*y^2 = A^2 - A^2*z^2
          // C^2*z^2 + D^2 + B^2*y^2 + 2CDz + 2CBzy + 2DBy + A^2*y^2 - A^2 + A^2*z^2 = 0
          // y^2 [A^2 + B^2]  + y [2DB + 2CBz] + [C^2*z^2 + D^2 + 2CDz - A^2 + A^2*z^2] = 0
          //
          //
          // Use quadratic formula, where:
          // a = [A^2 + B^2]
          // b = [2BD + 2CBz]
          // c = [C^2*z^2 + D^2 + 2CDz - A^2 + A^2*z^2]
          //
          // y = (-[2BD + 2CBz] +/- sqrt([2BD + 2CBz]^2 - 4 * [A^2 + B^2] * [C^2*z^2 + D^2 + 2CDz - A^2 + A^2*z^2]) ) / (2 * [A^2 + B^2])
          // Take out a 2:
          // y = (-[DB +CBz] +/- sqrt([DB + CBz]^2 - [A^2 + B^2] * [C^2*z^2 + D^2 + 2CDz - A^2 + A^2*z^2]) ) / [A^2 + B^2]
          //
          // The sqrt term simplifies:
          //
          // B^2*D^2 + C^2*B^2*z^2 + 2C*D*B^2*z - [A^2 + B^2] * [C^2*z^2 + D^2 + 2CDz - A^2 + A^2*z^2] = ?
          // B^2*D^2 + C^2*B^2*z^2 + 2C*D*B^2*z - [A^2 * C^2 * z^2 + A^2 * D^2 + 2 * A^2 * CDz - A^4 + A^4*z^2
          //                  + B^2 * C^2 * z^2 + B^2 * D^2 + 2 * B^2 * CDz - A^2 * B^2 + B^2 * A^2 * z^2] =?
          // C^2*B^2*z^2 + 2C*D*B^2*z - [A^2 * C^2 * z^2 + A^2 * D^2 + 2 * A^2 * CDz - A^4 + A^4*z^2
          //                  + B^2 * C^2 * z^2 + 2 * B^2 * CDz - A^2 * B^2 + B^2 * A^2 * z^2] =?
          // 2C*D*B^2*z - [A^2 * C^2 * z^2 + A^2 * D^2 + 2 * A^2 * CDz - A^4 + A^4*z^2
          //                  + 2 * B^2 * CDz - A^2 * B^2 + B^2 * A^2 * z^2] =?
          // - [A^2 * C^2 * z^2 + A^2 * D^2 + 2 * A^2 * CDz - A^4 + A^4*z^2
          //                  - A^2 * B^2 + B^2 * A^2 * z^2] =?
          // - A^2 * [C^2 * z^2 + D^2 + 2 * CDz - A^2 + A^2*z^2
          //                  - B^2 + B^2 * z^2] =?
          // - A^2 * [z^2[A^2 + B^2 + C^2] - [A^2 + B^2 - D^2] + 2CDz] =?
          // - A^2 * [z^2 - [A^2 + B^2 - D^2] + 2CDz] =?
          //
          // y = (-[DB +CBz] +/- A*sqrt([A^2 + B^2 - D^2] - z^2 - 2CDz) ) / [A^2 + B^2]
          //
          // correspondingly:
          // x = (-[DA +CAz] +/- B*sqrt([A^2 + B^2 - D^2] - z^2 - 2CDz) ) / [A^2 + B^2]
          //
          // However, for the maximum or minimum we seek, the clause inside the sqrt should be zero.  If
          // it is NOT zero, then we aren't looking at the right z value.

          double z;
          double x;
          double y;

          double sqrtValue = D * D * C * C + 1.0 - C * C - D * D;
          double denom = 1.0 / (A * A + B * B);
          if (Math.abs(sqrtValue) < MINIMUM_RESOLUTION_SQUARED) {
            //System.err.println(" One latitude solution");
            double insideValue;
            double sqrtTerm;

            z = D * C;
            // Since we squared both sides of the equation, we may have introduced spurious solutions, so we have to check.
            // But the same check applies to BOTH solutions -- the +z one as well as the -z one.
            insideValue = A * A + B * B - D * D - z * z - 2.0 * C * D * z;
            if (Math.abs(insideValue) < MINIMUM_RESOLUTION) {
              y = -B * (D + C * z) * denom;
              x = -A * (D + C * z) * denom;
              if (evaluateIsZero(x, y, z)) {
                addPoint(boundsInfo, bounds, x, y, z);
              }
            }
            // Check the solution on the other side of the x-y plane
            z = -z;
            insideValue = A * A + B * B - D * D - z * z - 2.0 * C * D * z;
            if (Math.abs(insideValue) < MINIMUM_RESOLUTION) {
              y = -B * (D + C * z) * denom;
              x = -A * (D + C * z) * denom;
              if (evaluateIsZero(x, y, z)) {
                addPoint(boundsInfo, bounds, x, y, z);
              }
            }
          } else if (sqrtValue > 0.0) {
            //System.err.println(" Two latitude solutions");
            double sqrtResult = Math.sqrt(sqrtValue);

            double insideValue;
            double sqrtTerm;

            z = D * C + sqrtResult;
            //System.out.println("z= "+z+" D-C*z = " + (D-C*z) + " Math.sqrt(1.0 - z*z - C*C + z*z*C*C) = "+(Math.sqrt(1.0 - z*z - C*C + z*z*C*C)));
            // Since we squared both sides of the equation, we may have introduced spurios solutions, so we have to check.
            // But the same check applies to BOTH solutions -- the +z one as well as the -z one.
            insideValue = A * A + B * B - D * D - z * z - 2.0 * C * D * z;
            //System.err.println(" z="+z+" C="+C+" D="+D+" inside value "+insideValue);
            if (Math.abs(insideValue) < MINIMUM_RESOLUTION) {
              y = -B * (D + C * z) * denom;
              x = -A * (D + C * z) * denom;
              if (evaluateIsZero(x, y, z)) {
                addPoint(boundsInfo, bounds, x, y, z);
              }
            }
            // Check the solution on the other side of the x-y plane
            z = -z;
            insideValue = A * A + B * B - D * D - z * z - 2.0 * C * D * z;
            //System.err.println(" z="+z+" C="+C+" D="+D+" inside value "+insideValue);
            if (Math.abs(insideValue) < MINIMUM_RESOLUTION) {
              y = -B * (D + C * z) * denom;
              x = -A * (D + C * z) * denom;
              if (evaluateIsZero(x, y, z)) {
                addPoint(boundsInfo, bounds, x, y, z);
              }
            }
            z = D * C - sqrtResult;
            //System.out.println("z= "+z+" D-C*z = " + (D-C*z) + " Math.sqrt(1.0 - z*z - C*C + z*z*C*C) = "+(Math.sqrt(1.0 - z*z - C*C + z*z*C*C)));
            // Since we squared both sides of the equation, we may have introduced spurios solutions, so we have to check.
            // But the same check applies to BOTH solutions -- the +z one as well as the -z one.
            insideValue = A * A + B * B - D * D - z * z - 2.0 * C * D * z;
            //System.err.println(" z="+z+" C="+C+" D="+D+" inside value "+insideValue);
            if (Math.abs(insideValue) < MINIMUM_RESOLUTION) {
              y = -B * (D + C * z) * denom;
              x = -A * (D + C * z) * denom;
              if (evaluateIsZero(x, y, z)) {
                addPoint(boundsInfo, bounds, x, y, z);
              }
            }
            // Check the solution on the other side of the x-y plane
            z = -z;
            insideValue = A * A + B * B - D * D - z * z - 2.0 * C * D * z;
            //System.err.println(" z="+z+" C="+C+" D="+D+" inside value "+insideValue);
            if (Math.abs(insideValue) < MINIMUM_RESOLUTION) {
              y = -B * (D + C * z) * denom;
              x = -A * (D + C * z) * denom;
              if (evaluateIsZero(x, y, z)) {
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
      //System.err.println("Done latitude bounds");
    }

    // First, figure out our longitude bounds, unless we no longer need to consider that
    if (!boundsInfo.checkNoLongitudeBound()) {
      //System.err.println("Computing longitude bounds for "+this);
      //System.out.println("A = "+A+" B = "+B+" C = "+C+" D = "+D);
      // Compute longitude bounds

      double a;
      double b;
      double c;

      if (Math.abs(C) < MINIMUM_RESOLUTION) {
        // Degenerate; the equation describes a line
        //System.out.println("It's a zero-width ellipse");
        // Ax + By + D = 0
        if (Math.abs(D) >= MINIMUM_RESOLUTION) {
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

            if (Math.abs(sqrtClause) < MINIMUM_RESOLUTION_SQUARED) {
              double y0 = -b / (2.0 * a);
              double x0 = (-D - B * y0) / A;
              double z0 = 0.0;
              addPoint(boundsInfo, bounds, x0, y0, z0);
            } else if (sqrtClause > 0.0) {
              double sqrtResult = Math.sqrt(sqrtClause);
              double denom = 1.0 / (2.0 * a);
              double Hdenom = 1.0 / A;

              double y0a = (-b + sqrtResult) * denom;
              double y0b = (-b - sqrtResult) * denom;

              double x0a = (-D - B * y0a) * Hdenom;
              double x0b = (-D - B * y0b) * Hdenom;

              double z0a = 0.0;
              double z0b = 0.0;

              addPoint(boundsInfo, bounds, x0a, y0a, z0a);
              addPoint(boundsInfo, bounds, x0b, y0b, z0b);
            }

          } else {
            // Use equation suitable for B != 0
            // Since I != 0, we rewrite:
            // y = (-Ax - D)/B
            a = B * B + A * A;
            b = 2.0 * A * D;
            c = D * D - B * B;

            double sqrtClause = b * b - 4.0 * a * c;

            if (Math.abs(sqrtClause) < MINIMUM_RESOLUTION_SQUARED) {
              double x0 = -b / (2.0 * a);
              double y0 = (-D - A * x0) / B;
              double z0 = 0.0;
              addPoint(boundsInfo, bounds, x0, y0, z0);
            } else if (sqrtClause > 0.0) {
              double sqrtResult = Math.sqrt(sqrtClause);
              double denom = 1.0 / (2.0 * a);
              double Idenom = 1.0 / B;

              double x0a = (-b + sqrtResult) * denom;
              double x0b = (-b - sqrtResult) * denom;
              double y0a = (-D - A * x0a) * Idenom;
              double y0b = (-D - A * x0b) * Idenom;
              double z0a = 0.0;
              double z0b = 0.0;

              addPoint(boundsInfo, bounds, x0a, y0a, z0a);
              addPoint(boundsInfo, bounds, x0b, y0b, z0b);
            }
          }
        }

      } else {
        //System.err.println("General longitude bounds...");

        // NOTE WELL: The x,y,z values generated here are NOT on the unit sphere.
        // They are for lat/lon calculation purposes only.  x-y is meant to be used for longitude determination,
        // and z for latitude, and that's all the values are good for.

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

        //System.err.println("E = " + E + " F = " + F + " G = " + G + " H = "+ H + " I = " + I + " J = " + J);

        // Check if the origin is within, by substituting x = 0, y = 0 and seeing if less than zero
        if (Math.abs(J) >= MINIMUM_RESOLUTION && J > 0.0) {
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
            //System.err.println(" Using the y quadratic");
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

            a = E * I * I - G * H * I + F * H * H;
            b = 4.0 * E * I * J - 2.0 * G * H * J;
            c = 4.0 * E * J * J - J * H * H;

            //System.out.println("a="+a+" b="+b+" c="+c);
            double sqrtClause = b * b - 4.0 * a * c;
            //System.out.println("sqrtClause="+sqrtClause);

            if (Math.abs(sqrtClause) < MINIMUM_RESOLUTION_CUBED) {
              //System.err.println(" One solution");
              double y0 = -b / (2.0 * a);
              double x0 = (-2.0 * J - I * y0) / H;
              double z0 = (-A * x0 - B * y0 - D) / C;

              addPoint(boundsInfo, bounds, x0, y0, z0);
            } else if (sqrtClause > 0.0) {
              //System.err.println(" Two solutions");
              double sqrtResult = Math.sqrt(sqrtClause);
              double denom = 1.0 / (2.0 * a);
              double Hdenom = 1.0 / H;
              double Cdenom = 1.0 / C;

              double y0a = (-b + sqrtResult) * denom;
              double y0b = (-b - sqrtResult) * denom;
              double x0a = (-2.0 * J - I * y0a) * Hdenom;
              double x0b = (-2.0 * J - I * y0b) * Hdenom;
              double z0a = (-A * x0a - B * y0a - D) * Cdenom;
              double z0b = (-A * x0b - B * y0b - D) * Cdenom;

              addPoint(boundsInfo, bounds, x0a, y0a, z0a);
              addPoint(boundsInfo, bounds, x0b, y0b, z0b);
            }

          } else {
            //System.err.println(" Using the x quadratic");
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

            // E x^2 + F y^2 + G xy + H x + I y + J = 0

            a = E * I * I - G * H * I + F * H * H;
            b = 4.0 * F * H * J - 2.0 * G * I * J;
            c = 4.0 * F * J * J - J * I * I;

            //System.out.println("a="+a+" b="+b+" c="+c);
            double sqrtClause = b * b - 4.0 * a * c;
            //System.out.println("sqrtClause="+sqrtClause);
            if (Math.abs(sqrtClause) < MINIMUM_RESOLUTION_CUBED) {
              //System.err.println(" One solution; sqrt clause was "+sqrtClause);
              double x0 = -b / (2.0 * a);
              double y0 = (-2.0 * J - H * x0) / I;
              double z0 = (-A * x0 - B * y0 - D) / C;
              // Verify that x&y fulfill the equation
              // 2Ex^2 + 2Fy^2 + 2Gxy + Hx + Iy = 0
              addPoint(boundsInfo, bounds, x0, y0, z0);
            } else if (sqrtClause > 0.0) {
              //System.err.println(" Two solutions");
              double sqrtResult = Math.sqrt(sqrtClause);
              double denom = 1.0 / (2.0 * a);
              double Idenom = 1.0 / I;
              double Cdenom = 1.0 / C;

              double x0a = (-b + sqrtResult) * denom;
              double x0b = (-b - sqrtResult) * denom;
              double y0a = (-2.0 * J - H * x0a) * Idenom;
              double y0b = (-2.0 * J - H * x0b) * Idenom;
              double z0a = (-A * x0a - B * y0a - D) * Cdenom;
              double z0b = (-A * x0b - B * y0b - D) * Cdenom;

              addPoint(boundsInfo, bounds, x0a, y0a, z0a);
              addPoint(boundsInfo, bounds, x0b, y0b, z0b);
            }
          }
        }
      }
    }

  }

  protected static void addPoint(final Bounds boundsInfo, final Membership[] bounds, final double x, final double y, final double z) {
    //System.err.println(" Want to add point x="+x+" y="+y+" z="+z);
    // Make sure the discovered point is within the bounds
    for (Membership bound : bounds) {
      if (!bound.isWithin(x, y, z))
        return;
    }
    // Add the point
    //System.err.println("  point added");
    //System.out.println("Adding point x="+x+" y="+y+" z="+z);
    boundsInfo.addPoint(x, y, z);
  }

  /**
   * Determine whether the plane intersects another plane within the
   * bounds provided.
   *
   * @param q                 is the other plane.
   * @param notablePoints     are points to look at to disambiguate cases when the two planes are identical.
   * @param moreNotablePoints are additional points to look at to disambiguate cases when the two planes are identical.
   * @param bounds            is one part of the bounds.
   * @param moreBounds        are more bounds.
   * @return true if there's an intersection.
   */
  public boolean intersects(final Plane q, final GeoPoint[] notablePoints, final GeoPoint[] moreNotablePoints, final Membership[] bounds, final Membership... moreBounds) {
    //System.err.println("Does plane "+this+" intersect with plane "+q);
    // If the two planes are identical, then the math will find no points of intersection.
    // So a special case of this is to check for plane equality.  But that is not enough, because
    // what we really need at that point is to determine whether overlap occurs between the two parts of the intersection
    // of plane and circle.  That is, are there *any* points on the plane that are within the bounds described?
    if (isNumericallyIdentical(q)) {
      //System.err.println(" Identical plane");
      // The only way to efficiently figure this out will be to have a list of trial points available to evaluate.
      // We look for any point that fulfills all the bounds.
      for (GeoPoint p : notablePoints) {
        if (meetsAllBounds(p, bounds, moreBounds)) {
          //System.err.println("  found a notable point in bounds, so intersects");
          return true;
        }
      }
      for (GeoPoint p : moreNotablePoints) {
        if (meetsAllBounds(p, bounds, moreBounds)) {
          //System.err.println("  found a notable point in bounds, so intersects");
          return true;
        }
      }
      //System.err.println("  no notable points inside found; no intersection");
      return false;
    }
    return findIntersections(q, bounds, moreBounds).length > 0;
  }

  /**
   * Returns true if this plane and the other plane are identical within the margin of error.
   */
  protected boolean isNumericallyIdentical(final Plane p) {
    // We can get the correlation by just doing a parallel plane check.  If that passes, then compute a point on the plane
    // (using D) and see if it also on the other plane.
    if (Math.abs(this.y * p.z - this.z * p.y) >= MINIMUM_RESOLUTION)
      return false;
    if (Math.abs(this.z * p.x - this.x * p.z) >= MINIMUM_RESOLUTION)
      return false;
    if (Math.abs(this.x * p.y - this.y * p.x) >= MINIMUM_RESOLUTION)
      return false;

    // Now, see whether the parallel planes are in fact on top of one another.
    // The math:
    // We need a single point that fulfills:
    // Ax + By + Cz + D = 0
    // Pick:
    // x0 = -(A * D) / (A^2 + B^2 + C^2)
    // y0 = -(B * D) / (A^2 + B^2 + C^2)
    // z0 = -(C * D) / (A^2 + B^2 + C^2)
    // Check:
    // A (x0) + B (y0) + C (z0) + D =? 0
    // A (-(A * D) / (A^2 + B^2 + C^2)) + B (-(B * D) / (A^2 + B^2 + C^2)) + C (-(C * D) / (A^2 + B^2 + C^2)) + D ?= 0
    // -D [ A^2 / (A^2 + B^2 + C^2) + B^2 / (A^2 + B^2 + C^2) + C^2 / (A^2 + B^2 + C^2)] + D ?= 0
    // Yes.
    final double denom = 1.0 / (p.x * p.x + p.y * p.y + p.z * p.z);
    return evaluateIsZero(-p.x * p.D * denom, -p.y * p.D * denom, -p.z * p.D * denom);
  }

  protected static boolean meetsAllBounds(final GeoPoint p, final Membership[] bounds, final Membership[] moreBounds) {
    for (final Membership bound : bounds) {
      if (!bound.isWithin(p))
        return false;
    }
    for (final Membership bound : moreBounds) {
      if (!bound.isWithin(p))
        return false;
    }
    return true;
  }

  /**
   * Find a sample point on the intersection between two planes and the unit sphere.
   */
  public GeoPoint getSampleIntersectionPoint(final Plane q) {
    final GeoPoint[] intersections = findIntersections(q, NO_BOUNDS, NO_BOUNDS);
    if (intersections.length == 0)
      return null;
    return intersections[0];
  }

  @Override
  public String toString() {
    return "[A=" + x + ", B=" + y + "; C=" + z + "; D=" + D + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;
    if (!(o instanceof Plane))
      return false;
    Plane other = (Plane) o;
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
