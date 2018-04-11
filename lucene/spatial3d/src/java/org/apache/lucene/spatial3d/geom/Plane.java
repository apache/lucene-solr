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
package org.apache.lucene.spatial3d.geom;

/**
 * We know about three kinds of planes.  First kind: general plain through two points and origin
 * Second kind: horizontal plane at specified height.  Third kind: vertical plane with specified x and y value, through origin.
 *
 * @lucene.experimental
 */
public class Plane extends Vector {
  /** An array with no points in it */
  public final static GeoPoint[] NO_POINTS = new GeoPoint[0];
  /** An array with no bounds in it */
  public final static Membership[] NO_BOUNDS = new Membership[0];
  /** A vertical plane normal to the Y axis */
  public final static Plane normalYPlane = new Plane(0.0,1.0,0.0,0.0);
  /** A vertical plane normal to the X axis */
  public final static Plane normalXPlane = new Plane(1.0,0.0,0.0,0.0);
  /** A vertical plane normal to the Z axis */
  public final static Plane normalZPlane = new Plane(0.0,0.0,1.0,0.0);

  /** Ax + By + Cz + D = 0 */
  public final double D;

  /**
   * Construct a plane with all four coefficients defined.
   *@param A is A
   *@param B is B
   *@param C is C
   *@param D is D
   */
  public Plane(final double A, final double B, final double C, final double D) {
    super(A, B, C);
    this.D = D;
  }

  /**
   * Construct a plane through two points and origin.
   *
   * @param A is the first point (origin based).
   * @param BX is the second point X (origin based).
   * @param BY is the second point Y (origin based).
   * @param BZ is the second point Z (origin based).
   */
  public Plane(final Vector A, final double BX, final double BY, final double BZ) {
    super(A, BX, BY, BZ);
    D = 0.0;
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
   * @param planetModel is the planet model.
   * @param sinLat is the sin(latitude).
   */
  public Plane(final PlanetModel planetModel, final double sinLat) {
    super(0.0, 0.0, 1.0);
    D = -sinLat * computeDesiredEllipsoidMagnitude(planetModel, sinLat);
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
   * @param v is the normal vector.
   * @param D is the D offset from the origin.
   */
  public Plane(final Vector v, final double D) {
    super(v.x, v.y, v.z);
    this.D = D;
  }

  /** Construct a plane that is parallel to the one provided, but which is just barely numerically
   * distinguishable from it, in the direction desired.
   * @param basePlane is the starting plane.
   * @param above is set to true if the desired plane is in the positive direction from the base plane,
   *   or false in the negative direction.
   */
  public Plane(final Plane basePlane, final boolean above) {
    this(basePlane.x, basePlane.y, basePlane.z, above?Math.nextUp(basePlane.D + MINIMUM_RESOLUTION):Math.nextDown(basePlane.D - MINIMUM_RESOLUTION));
  }
  
  /** Construct the most accurate normalized plane through an x-y point and including the Z axis.
   * If none of the points can determine the plane, return null.
   * @param planePoints is a set of points to choose from.  The best one for constructing the most precise plane is picked.
   * @return the plane
   */
  public static Plane constructNormalizedZPlane(final Vector... planePoints) {
    // Pick the best one (with the greatest x-y distance)
    double bestDistance = 0.0;
    Vector bestPoint = null;
    for (final Vector point : planePoints) {
      final double pointDist = point.x * point.x + point.y * point.y;
      if (pointDist > bestDistance) {
        bestDistance = pointDist;
        bestPoint = point;
      }
    }
    return constructNormalizedZPlane(bestPoint.x, bestPoint.y);
  }

  /** Construct the most accurate normalized plane through an x-z point and including the Y axis.
   * If none of the points can determine the plane, return null.
   * @param planePoints is a set of points to choose from.  The best one for constructing the most precise plane is picked.
   * @return the plane
   */
  public static Plane constructNormalizedYPlane(final Vector... planePoints) {
    // Pick the best one (with the greatest x-z distance)
    double bestDistance = 0.0;
    Vector bestPoint = null;
    for (final Vector point : planePoints) {
      final double pointDist = point.x * point.x + point.z * point.z;
      if (pointDist > bestDistance) {
        bestDistance = pointDist;
        bestPoint = point;
      }
    }
    return constructNormalizedYPlane(bestPoint.x, bestPoint.z, 0.0);
  }

  /** Construct the most accurate normalized plane through an y-z point and including the X axis.
   * If none of the points can determine the plane, return null.
   * @param planePoints is a set of points to choose from.  The best one for constructing the most precise plane is picked.
   * @return the plane
   */
  public static Plane constructNormalizedXPlane(final Vector... planePoints) {
    // Pick the best one (with the greatest y-z distance)
    double bestDistance = 0.0;
    Vector bestPoint = null;
    for (final Vector point : planePoints) {
      final double pointDist = point.y * point.y + point.z * point.z;
      if (pointDist > bestDistance) {
        bestDistance = pointDist;
        bestPoint = point;
      }
    }
    return constructNormalizedXPlane(bestPoint.y, bestPoint.z, 0.0);
  }

  /** Construct a normalized plane through an x-y point and including the Z axis.
   * If the x-y point is at (0,0), return null.
   * @param x is the x value.
   * @param y is the y value.
   * @return a plane passing through the Z axis and (x,y,0).
   */
  public static Plane constructNormalizedZPlane(final double x, final double y) {
    if (Math.abs(x) < MINIMUM_RESOLUTION && Math.abs(y) < MINIMUM_RESOLUTION)
      return null;
    final double denom = 1.0 / Math.sqrt(x*x + y*y);
    return new Plane(y * denom, -x * denom, 0.0, 0.0);
  }

  /** Construct a normalized plane through an x-z point and parallel to the Y axis.
   * If the x-z point is at (0,0), return null.
   * @param x is the x value.
   * @param z is the z value.
   * @param DValue is the offset from the origin for the plane.
   * @return a plane parallel to the Y axis and perpendicular to the x and z values given.
   */
  public static Plane constructNormalizedYPlane(final double x, final double z, final double DValue) {
    if (Math.abs(x) < MINIMUM_RESOLUTION && Math.abs(z) < MINIMUM_RESOLUTION)
      return null;
    final double denom = 1.0 / Math.sqrt(x*x + z*z);
    return new Plane(z * denom, 0.0, -x * denom, DValue);
  }

  /** Construct a normalized plane through a y-z point and parallel to the X axis.
   * If the y-z point is at (0,0), return null.
   * @param y is the y value.
   * @param z is the z value.
   * @param DValue is the offset from the origin for the plane.
   * @return a plane parallel to the X axis and perpendicular to the y and z values given.
   */
  public static Plane constructNormalizedXPlane(final double y, final double z, final double DValue) {
    if (Math.abs(y) < MINIMUM_RESOLUTION && Math.abs(z) < MINIMUM_RESOLUTION)
      return null;
    final double denom = 1.0 / Math.sqrt(y*y + z*z);
    return new Plane(0.0, z * denom, -y * denom, DValue);
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
   * @param x is the x value.
   * @param y is the y value.
   * @param z is the z value.
   * @return the result of the evaluation.
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
   * @param x is the x value.
   * @param y is the y value.
   * @param z is the z value.
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

  /** Compute arc distance from plane to a vector expressed with a {@link GeoPoint}.
   *  @see #arcDistance(PlanetModel, double, double, double, Membership...) */
  public double arcDistance(final PlanetModel planetModel, final GeoPoint v, final Membership... bounds) {
    return arcDistance(planetModel, v.x, v.y, v.z, bounds);
  }
    
  /**
   * Compute arc distance from plane to a vector.
   * @param planetModel is the planet model.
   * @param x is the x vector value.
   * @param y is the y vector value.
   * @param z is the z vector value.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the arc distance.
   */
  public double arcDistance(final PlanetModel planetModel, final double x, final double y, final double z, final Membership... bounds) {

    if (evaluateIsZero(x,y,z)) {
      if (meetsAllBounds(x,y,z, bounds))
        return 0.0;
      return Double.POSITIVE_INFINITY;
    }
    
    // First, compute the perpendicular plane.
    final Plane perpPlane = new Plane(this.y * z - this.z * y, this.z * x - this.x * z, this.x * y - this.y * x, 0.0);

    // We need to compute the intersection of two planes on the geo surface: this one, and its perpendicular.
    // Then, we need to choose which of the two points we want to compute the distance to.  We pick the
    // shorter distance always.
    
    final GeoPoint[] intersectionPoints = findIntersections(planetModel, perpPlane);
    
    // For each point, compute a linear distance, and take the minimum of them
    double minDistance = Double.POSITIVE_INFINITY;
    
    for (final GeoPoint intersectionPoint : intersectionPoints) {
      if (meetsAllBounds(intersectionPoint, bounds)) {
        final double theDistance = intersectionPoint.arcDistance(x,y,z);
        if (theDistance < minDistance) {
          minDistance = theDistance;
        }
      }
    }
    return minDistance;

  }

  /**
   * Compute normal distance from plane to a vector.
   * @param v is the vector.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the normal distance.
   */
  public double normalDistance(final Vector v, final Membership... bounds) {
    return normalDistance(v.x, v.y, v.z, bounds);
  }
    
  /**
   * Compute normal distance from plane to a vector.
   * @param x is the vector x.
   * @param y is the vector y.
   * @param z is the vector z.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the normal distance.
   */
  public double normalDistance(final double x, final double y, final double z, final Membership... bounds) {

    final double dist = evaluate(x,y,z);
    final double perpX = x - dist * this.x;
    final double perpY = y - dist * this.y;
    final double perpZ = z - dist * this.z;

    if (!meetsAllBounds(perpX, perpY, perpZ, bounds)) {
      return Double.POSITIVE_INFINITY;
    }
    
    return Math.abs(dist);
  }
  
  /**
   * Compute normal distance squared from plane to a vector.
   * @param v is the vector.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the normal distance squared.
   */
  public double normalDistanceSquared(final Vector v, final Membership... bounds) {
    return normalDistanceSquared(v.x, v.y, v.z, bounds);
  }
  
  /**
   * Compute normal distance squared from plane to a vector.
   * @param x is the vector x.
   * @param y is the vector y.
   * @param z is the vector z.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the normal distance squared.
   */
  public double normalDistanceSquared(final double x, final double y, final double z, final Membership... bounds) {
    final double normal = normalDistance(x,y,z,bounds);
    if (normal == Double.POSITIVE_INFINITY)
      return normal;
    return normal * normal;
  }

  /**
   * Compute linear distance from plane to a vector.  This is defined
   * as the distance from the given point to the nearest intersection of 
   * this plane with the planet surface.
   * @param planetModel is the planet model.
   * @param v is the point.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the linear distance.
   */
  public double linearDistance(final PlanetModel planetModel, final GeoPoint v, final Membership... bounds) {
    return linearDistance(planetModel, v.x, v.y, v.z, bounds);
  }
    
  /**
   * Compute linear distance from plane to a vector.  This is defined
   * as the distance from the given point to the nearest intersection of 
   * this plane with the planet surface.
   * @param planetModel is the planet model.
   * @param x is the vector x.
   * @param y is the vector y.
   * @param z is the vector z.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the linear distance.
   */
  public double linearDistance(final PlanetModel planetModel, final double x, final double y, final double z, final Membership... bounds) {
    if (evaluateIsZero(x,y,z)) {
      if (meetsAllBounds(x,y,z, bounds))
        return 0.0;
      return Double.POSITIVE_INFINITY;
    }
    
    // First, compute the perpendicular plane.
    final Plane perpPlane = new Plane(this.y * z - this.z * y, this.z * x - this.x * z, this.x * y - this.y * x, 0.0);

    // We need to compute the intersection of two planes on the geo surface: this one, and its perpendicular.
    // Then, we need to choose which of the two points we want to compute the distance to.  We pick the
    // shorter distance always.
    
    final GeoPoint[] intersectionPoints = findIntersections(planetModel, perpPlane);
    
    // For each point, compute a linear distance, and take the minimum of them
    double minDistance = Double.POSITIVE_INFINITY;
    
    for (final GeoPoint intersectionPoint : intersectionPoints) {
      if (meetsAllBounds(intersectionPoint, bounds)) {
        final double theDistance = intersectionPoint.linearDistance(x,y,z);
        if (theDistance < minDistance) {
          minDistance = theDistance;
        }
      }
    }
    return minDistance;
  }
      
  /**
   * Compute linear distance squared from plane to a vector.  This is defined
   * as the distance from the given point to the nearest intersection of 
   * this plane with the planet surface.
   * @param planetModel is the planet model.
   * @param v is the point.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the linear distance squared.
   */
  public double linearDistanceSquared(final PlanetModel planetModel, final GeoPoint v, final Membership... bounds) {
    return linearDistanceSquared(planetModel, v.x, v.y, v.z, bounds);
  }
  
  /**
   * Compute linear distance squared from plane to a vector.  This is defined
   * as the distance from the given point to the nearest intersection of 
   * this plane with the planet surface.
   * @param planetModel is the planet model.
   * @param x is the vector x.
   * @param y is the vector y.
   * @param z is the vector z.
   * @param bounds are the bounds which constrain the intersection point.
   * @return the linear distance squared.
   */
  public double linearDistanceSquared(final PlanetModel planetModel, final double x, final double y, final double z, final Membership... bounds) {
    final double linearDistance = linearDistance(planetModel, x, y, z, bounds);
    return linearDistance * linearDistance;
  }

  /**
   * Find points on the boundary of the intersection of a plane and the unit sphere,
   * given a starting point, and ending point, and a list of proportions of the arc (e.g. 0.25, 0.5, 0.75).
   * The angle between the starting point and ending point is assumed to be less than pi.
   * @param start is the start point.
   * @param end is the end point.
   * @param proportions is an array of fractional proportions measured between start and end.
   * @return an array of points corresponding to the proportions passed in.
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

    double newEndAngle = endAngle;
    while (newEndAngle < startAngle) {
      newEndAngle += Math.PI * 2.0;
    }

    if (newEndAngle - startAngle <= Math.PI) {
      delta = newEndAngle - startAngle;
    } else {
      double newStartAngle = startAngle;
      while (newStartAngle < endAngle) {
        newStartAngle += Math.PI * 2.0;
      }
      delta = newStartAngle - endAngle;
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
   * @param start is the start point.
   * @param transX is the translation x value.
   * @param transY is the translation y value.
   * @param transZ is the translation z value.
   * @param sinRA is the sine of the ascension angle.
   * @param cosRA is the cosine of the ascension angle.
   * @param sinHA is the sine of the height angle.
   * @param cosHA is the cosine of the height angle.
   * @return the modified point.
   */
  protected static Vector modify(final GeoPoint start, final double transX, final double transY, final double transZ,
                                 final double sinRA, final double cosRA, final double sinHA, final double cosHA) {
    return start.translate(transX, transY, transZ).rotateXY(sinRA, cosRA).rotateXZ(sinHA, cosHA);
  }

  /**
   * Reverse modify a point to produce a GeoPoint in normal space.
   * @param point is the translated point.
   * @param transX is the translation x value.
   * @param transY is the translation y value.
   * @param transZ is the translation z value.
   * @param sinRA is the sine of the ascension angle.
   * @param cosRA is the cosine of the ascension angle.
   * @param sinHA is the sine of the height angle.
   * @param cosHA is the cosine of the height angle.
   * @return the original point.
   */
  protected static GeoPoint reverseModify(final Vector point, final double transX, final double transY, final double transZ,
                                          final double sinRA, final double cosRA, final double sinHA, final double cosHA) {
    final Vector result = point.rotateXZ(-sinHA, cosHA).rotateXY(-sinRA, cosRA).translate(-transX, -transY, -transZ);
    return new GeoPoint(result.x, result.y, result.z);
  }

  /**
   * Find the intersection points between two planes, given a set of bounds.
   * @param planetModel is the planet model.
   * @param q is the plane to intersect with.
   * @param bounds are the bounds to consider to determine legal intersection points.
   * @return the set of legal intersection points, or null if the planes are numerically identical.
   */
  public GeoPoint[] findIntersections(final PlanetModel planetModel, final Plane q, final Membership... bounds) {
    if (isNumericallyIdentical(q)) {
      return null;
    }
    return findIntersections(planetModel, q, bounds, NO_BOUNDS);
  }

  /**
   * Find the points between two planes, where one plane crosses the other, given a set of bounds.
   * Crossing is not just intersection; the planes cannot touch at just one point on the ellipsoid,
   * but must cross at two.
   *
   * @param planetModel is the planet model.
   * @param q is the plane to intersect with.
   * @param bounds are the bounds to consider to determine legal intersection points.
   * @return the set of legal crossing points, or null if the planes are numerically identical.
   */
  public GeoPoint[] findCrossings(final PlanetModel planetModel, final Plane q, final Membership... bounds) {
    if (isNumericallyIdentical(q)) {
      return null;
    }
    return findCrossings(planetModel, q, bounds, NO_BOUNDS);
  }

  /**
   * Checks if three points are coplanar in any of the three planes they can describe.
   * The planes are all assumed to go through the origin.
   *
   * @param A The first point.
   * @param B The second point.
   * @param C The third point
   * @return true if provided points are coplanar in any of the three planes they can describe.
   */
  public static boolean arePointsCoplanar(final GeoPoint A, final GeoPoint B, final GeoPoint C) {
    return Vector.crossProductEvaluateIsZero(A, B, C) ||
      Vector.crossProductEvaluateIsZero(A, C, B) ||
      Vector.crossProductEvaluateIsZero(B, C, A);
  }
  
  /**
   * Find the intersection points between two planes, given a set of bounds.
   *
   * @param planetModel is the planet model to use in finding points.
   * @param q          is the plane to intersect with.
   * @param bounds     is the set of bounds.
   * @param moreBounds is another set of bounds.
   * @return the intersection point(s) on the unit sphere, if there are any.
   */
  protected GeoPoint[] findIntersections(final PlanetModel planetModel, final Plane q, final Membership[] bounds, final Membership[] moreBounds) {
    //System.err.println("Looking for intersection between plane "+this+" and plane "+q+" within bounds");
    // Unnormalized, unchecked...
    final double lineVectorX = y * q.z - z * q.y;
    final double lineVectorY = z * q.x - x * q.z;
    final double lineVectorZ = x * q.y - y * q.x;
    if (Math.abs(lineVectorX) < MINIMUM_RESOLUTION && Math.abs(lineVectorY) < MINIMUM_RESOLUTION && Math.abs(lineVectorZ) < MINIMUM_RESOLUTION) {
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

    // Once an intersecting line is determined, the next step is to intersect that line with the ellipsoid, which
    // will yield zero, one, or two points.
    // The ellipsoid equation: 1,0 = x^2/a^2 + y^2/b^2 + z^2/c^2
    // 1.0 = (At+A0)^2/a^2 + (Bt+B0)^2/b^2 + (Ct+C0)^2/c^2
    // A^2 t^2 / a^2 + 2AA0t / a^2 + A0^2 / a^2 + B^2 t^2 / b^2 + 2BB0t / b^2 + B0^2 / b^2 + C^2 t^2 / c^2 + 2CC0t / c^2 + C0^2 / c^2  - 1,0 = 0.0
    // [A^2 / a^2 + B^2 / b^2 + C^2 / c^2] t^2 + [2AA0 / a^2 + 2BB0 / b^2 + 2CC0 / c^2] t + [A0^2 / a^2 + B0^2 / b^2 + C0^2 / c^2 - 1,0] = 0.0
    // Use the quadratic formula to determine t values and candidate point(s)
    final double A = lineVectorX * lineVectorX * planetModel.inverseAbSquared +
      lineVectorY * lineVectorY * planetModel.inverseAbSquared +
      lineVectorZ * lineVectorZ * planetModel.inverseCSquared;
    final double B = 2.0 * (lineVectorX * x0 * planetModel.inverseAbSquared + lineVectorY * y0 * planetModel.inverseAbSquared + lineVectorZ * z0 * planetModel.inverseCSquared);
    final double C = x0 * x0 * planetModel.inverseAbSquared + y0 * y0 * planetModel.inverseAbSquared + z0 * z0 * planetModel.inverseCSquared - 1.0;

    final double BsquaredMinus = B * B - 4.0 * A * C;
    if (Math.abs(BsquaredMinus) < MINIMUM_RESOLUTION_SQUARED) {
      //System.err.println(" One point of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // One solution only
      final double t = -B * inverse2A;
      // Maybe we can save ourselves the cost of construction of a point?
      final double pointX = lineVectorX * t + x0;
      final double pointY = lineVectorY * t + y0;
      final double pointZ = lineVectorZ * t + z0;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(pointX, pointY, pointZ)) {
          return NO_POINTS;
        }
      }
      for (final Membership bound : moreBounds) {
        if (!bound.isWithin(pointX, pointY, pointZ)) {
          return NO_POINTS;
        }
      }
      return new GeoPoint[]{new GeoPoint(pointX, pointY, pointZ)};
    } else if (BsquaredMinus > 0.0) {
      //System.err.println(" Two points of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // Two solutions
      final double sqrtTerm = Math.sqrt(BsquaredMinus);
      final double t1 = (-B + sqrtTerm) * inverse2A;
      final double t2 = (-B - sqrtTerm) * inverse2A;
      // Up to two points being returned.  Do what we can to save on object creation though.
      final double point1X = lineVectorX * t1 + x0;
      final double point1Y = lineVectorY * t1 + y0;
      final double point1Z = lineVectorZ * t1 + z0;
      final double point2X = lineVectorX * t2 + x0;
      final double point2Y = lineVectorY * t2 + y0;
      final double point2Z = lineVectorZ * t2 + z0;
      boolean point1Valid = true;
      boolean point2Valid = true;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point1X, point1Y, point1Z)) {
          point1Valid = false;
          break;
        }
      }
      if (point1Valid) {
        for (final Membership bound : moreBounds) {
          if (!bound.isWithin(point1X, point1Y, point1Z)) {
            point1Valid = false;
            break;
          }
        }
      }
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point2X, point2Y, point2Z)) {
          point2Valid = false;
          break;
        }
      }
      if (point2Valid) {
        for (final Membership bound : moreBounds) {
          if (!bound.isWithin(point2X, point2Y, point2Z)) {
            point2Valid = false;
            break;
          }
        }
      }

      if (point1Valid && point2Valid) {
        return new GeoPoint[]{new GeoPoint(point1X, point1Y, point1Z), new GeoPoint(point2X, point2Y, point2Z)};
      }
      if (point1Valid) {
        return new GeoPoint[]{new GeoPoint(point1X, point1Y, point1Z)};
      }
      if (point2Valid) {
        return new GeoPoint[]{new GeoPoint(point2X, point2Y, point2Z)};
      }
      return NO_POINTS;
    } else {
      //System.err.println(" no solutions - no intersection");
      return NO_POINTS;
    }
  }

  /**
   * Find the points between two planes, where one plane crosses the other, given a set of bounds.
   * Crossing is not just intersection; the planes cannot touch at just one point on the ellipsoid,
   * but must cross at two.
   *
   * @param planetModel is the planet model to use in finding points.
   * @param q          is the plane to intersect with.
   * @param bounds     is the set of bounds.
   * @param moreBounds is another set of bounds.
   * @return the intersection point(s) on the ellipsoid, if there are any.
   */
  protected GeoPoint[] findCrossings(final PlanetModel planetModel, final Plane q, final Membership[] bounds, final Membership[] moreBounds) {
    // This code in this method is very similar to findIntersections(), but eliminates the cases where
    // crossings are detected.
    // Unnormalized, unchecked...
    final double lineVectorX = y * q.z - z * q.y;
    final double lineVectorY = z * q.x - x * q.z;
    final double lineVectorZ = x * q.y - y * q.x;
    if (Math.abs(lineVectorX) < MINIMUM_RESOLUTION && Math.abs(lineVectorY) < MINIMUM_RESOLUTION && Math.abs(lineVectorZ) < MINIMUM_RESOLUTION) {
      // Degenerate case: parallel planes
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
        return NO_POINTS;
      }
      final double denom = 1.0 / denomYZ;
      x0 = 0.0;
      y0 = (-this.D * q.z - this.z * -q.D) * denom;
      z0 = (this.y * -q.D + this.D * q.y) * denom;
    } else if (Math.abs(denomXZ) >= Math.abs(denomXY) && Math.abs(denomXZ) >= Math.abs(denomYZ)) {
      // Y is the biggest, so y0 = 0.0
      if (Math.abs(denomXZ) < MINIMUM_RESOLUTION_SQUARED) {
        return NO_POINTS;
      }
      final double denom = 1.0 / denomXZ;
      x0 = (-this.D * q.z - this.z * -q.D) * denom;
      y0 = 0.0;
      z0 = (this.x * -q.D + this.D * q.x) * denom;
    } else {
      // Z is the biggest, so Z0 = 0.0
      if (Math.abs(denomXY) < MINIMUM_RESOLUTION_SQUARED) {
        return NO_POINTS;
      }
      final double denom = 1.0 / denomXY;
      x0 = (-this.D * q.y - this.y * -q.D) * denom;
      y0 = (this.x * -q.D + this.D * q.x) * denom;
      z0 = 0.0;
    }

    // Once an intersecting line is determined, the next step is to intersect that line with the ellipsoid, which
    // will yield zero, one, or two points.
    // The ellipsoid equation: 1,0 = x^2/a^2 + y^2/b^2 + z^2/c^2
    // 1.0 = (At+A0)^2/a^2 + (Bt+B0)^2/b^2 + (Ct+C0)^2/c^2
    // A^2 t^2 / a^2 + 2AA0t / a^2 + A0^2 / a^2 + B^2 t^2 / b^2 + 2BB0t / b^2 + B0^2 / b^2 + C^2 t^2 / c^2 + 2CC0t / c^2 + C0^2 / c^2  - 1,0 = 0.0
    // [A^2 / a^2 + B^2 / b^2 + C^2 / c^2] t^2 + [2AA0 / a^2 + 2BB0 / b^2 + 2CC0 / c^2] t + [A0^2 / a^2 + B0^2 / b^2 + C0^2 / c^2 - 1,0] = 0.0
    // Use the quadratic formula to determine t values and candidate point(s)
    final double A = lineVectorX * lineVectorX * planetModel.inverseAbSquared +
      lineVectorY * lineVectorY * planetModel.inverseAbSquared +
      lineVectorZ * lineVectorZ * planetModel.inverseCSquared;
    final double B = 2.0 * (lineVectorX * x0 * planetModel.inverseAbSquared + lineVectorY * y0 * planetModel.inverseAbSquared + lineVectorZ * z0 * planetModel.inverseCSquared);
    final double C = x0 * x0 * planetModel.inverseAbSquared + y0 * y0 * planetModel.inverseAbSquared + z0 * z0 * planetModel.inverseCSquared - 1.0;

    final double BsquaredMinus = B * B - 4.0 * A * C;
    if (Math.abs(BsquaredMinus) < MINIMUM_RESOLUTION_SQUARED) {
      // One point of intersection: cannot be a crossing.
      return NO_POINTS;
    } else if (BsquaredMinus > 0.0) {
      final double inverse2A = 1.0 / (2.0 * A);
      // Two solutions
      final double sqrtTerm = Math.sqrt(BsquaredMinus);
      final double t1 = (-B + sqrtTerm) * inverse2A;
      final double t2 = (-B - sqrtTerm) * inverse2A;
      // Up to two points being returned.  Do what we can to save on object creation though.
      final double point1X = lineVectorX * t1 + x0;
      final double point1Y = lineVectorY * t1 + y0;
      final double point1Z = lineVectorZ * t1 + z0;
      final double point2X = lineVectorX * t2 + x0;
      final double point2Y = lineVectorY * t2 + y0;
      final double point2Z = lineVectorZ * t2 + z0;
      boolean point1Valid = true;
      boolean point2Valid = true;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point1X, point1Y, point1Z)) {
          point1Valid = false;
          break;
        }
      }
      if (point1Valid) {
        for (final Membership bound : moreBounds) {
          if (!bound.isWithin(point1X, point1Y, point1Z)) {
            point1Valid = false;
            break;
          }
        }
      }
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point2X, point2Y, point2Z)) {
          point2Valid = false;
          break;
        }
      }
      if (point2Valid) {
        for (final Membership bound : moreBounds) {
          if (!bound.isWithin(point2X, point2Y, point2Z)) {
            point2Valid = false;
            break;
          }
        }
      }

      if (point1Valid && point2Valid) {
        return new GeoPoint[]{new GeoPoint(point1X, point1Y, point1Z), new GeoPoint(point2X, point2Y, point2Z)};
      }
      if (point1Valid) {
        return new GeoPoint[]{new GeoPoint(point1X, point1Y, point1Z)};
      }
      if (point2Valid) {
        return new GeoPoint[]{new GeoPoint(point2X, point2Y, point2Z)};
      }
      return NO_POINTS;
    } else {
      // No solutions.
      return NO_POINTS;
    }
  }

  /**
   * Record intersection points for planes with error bounds.
   * This method calls the Bounds object with every intersection point it can find that matches the criteria.
   * Each plane is considered to have two sides, one that is D + MINIMUM_RESOLUTION, and one that is 
   * D - MINIMUM_RESOLUTION.  Both are examined and intersection points determined.
   */
  protected void findIntersectionBounds(final PlanetModel planetModel, final Bounds boundsInfo, final Plane q, final Membership... bounds) {
    //System.out.println("Finding intersection bounds");
    // Unnormalized, unchecked...
    final double lineVectorX = y * q.z - z * q.y;
    final double lineVectorY = z * q.x - x * q.z;
    final double lineVectorZ = x * q.y - y * q.x;
    if (Math.abs(lineVectorX) < MINIMUM_RESOLUTION && Math.abs(lineVectorY) < MINIMUM_RESOLUTION && Math.abs(lineVectorZ) < MINIMUM_RESOLUTION) {
      // Degenerate case: parallel planes
      //System.out.println(" planes are parallel - no intersection");
      return;
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
    // We try to maximize the determinant in the denominator
    final double denomYZ = this.y * q.z - this.z * q.y;
    final double denomXZ = this.x * q.z - this.z * q.x;
    final double denomXY = this.x * q.y - this.y * q.x;
    if (Math.abs(denomYZ) >= Math.abs(denomXZ) && Math.abs(denomYZ) >= Math.abs(denomXY)) {
      //System.out.println("X biggest");
      // X is the biggest, so our point will have x0 = 0.0
      if (Math.abs(denomYZ) < MINIMUM_RESOLUTION_SQUARED) {
        //System.out.println(" Denominator is zero: no intersection");
        return;
      }
      final double denom = 1.0 / denomYZ;
      // Each value of D really is two values of D.  That makes 4 combinations.
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        0.0, (-(this.D+MINIMUM_RESOLUTION) * q.z - this.z * -(q.D+MINIMUM_RESOLUTION)) * denom, (this.y * -(q.D+MINIMUM_RESOLUTION) + (this.D+MINIMUM_RESOLUTION) * q.y) * denom,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        0.0, (-(this.D-MINIMUM_RESOLUTION) * q.z - this.z * -(q.D+MINIMUM_RESOLUTION)) * denom, (this.y * -(q.D+MINIMUM_RESOLUTION) + (this.D-MINIMUM_RESOLUTION) * q.y) * denom,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        0.0, (-(this.D+MINIMUM_RESOLUTION) * q.z - this.z * -(q.D-MINIMUM_RESOLUTION)) * denom, (this.y * -(q.D-MINIMUM_RESOLUTION) + (this.D+MINIMUM_RESOLUTION) * q.y) * denom,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        0.0, (-(this.D-MINIMUM_RESOLUTION) * q.z - this.z * -(q.D-MINIMUM_RESOLUTION)) * denom, (this.y * -(q.D-MINIMUM_RESOLUTION) + (this.D-MINIMUM_RESOLUTION) * q.y) * denom,
        bounds);
    } else if (Math.abs(denomXZ) >= Math.abs(denomXY) && Math.abs(denomXZ) >= Math.abs(denomYZ)) {
      //System.out.println("Y biggest");
      // Y is the biggest, so y0 = 0.0
      if (Math.abs(denomXZ) < MINIMUM_RESOLUTION_SQUARED) {
        //System.out.println(" Denominator is zero: no intersection");
        return;
      }
      final double denom = 1.0 / denomXZ;
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D+MINIMUM_RESOLUTION) * q.z - this.z * -(q.D+MINIMUM_RESOLUTION)) * denom, 0.0, (this.x * -(q.D+MINIMUM_RESOLUTION) + (this.D+MINIMUM_RESOLUTION) * q.x) * denom,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D-MINIMUM_RESOLUTION) * q.z - this.z * -(q.D+MINIMUM_RESOLUTION)) * denom, 0.0, (this.x * -(q.D+MINIMUM_RESOLUTION) + (this.D-MINIMUM_RESOLUTION) * q.x) * denom,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D+MINIMUM_RESOLUTION) * q.z - this.z * -(q.D-MINIMUM_RESOLUTION)) * denom, 0.0, (this.x * -(q.D-MINIMUM_RESOLUTION) + (this.D+MINIMUM_RESOLUTION) * q.x) * denom,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D-MINIMUM_RESOLUTION) * q.z - this.z * -(q.D-MINIMUM_RESOLUTION)) * denom, 0.0, (this.x * -(q.D-MINIMUM_RESOLUTION) + (this.D-MINIMUM_RESOLUTION) * q.x) * denom,
        bounds);
    } else {
      //System.out.println("Z biggest");
      // Z is the biggest, so Z0 = 0.0
      if (Math.abs(denomXY) < MINIMUM_RESOLUTION_SQUARED) {
        //System.out.println(" Denominator is zero: no intersection");
        return;
      }
      final double denom = 1.0 / denomXY;
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D+MINIMUM_RESOLUTION) * q.y - this.y * -(q.D+MINIMUM_RESOLUTION)) * denom, (this.x * -(q.D+MINIMUM_RESOLUTION) + (this.D+MINIMUM_RESOLUTION) * q.x) * denom, 0.0,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D-MINIMUM_RESOLUTION) * q.y - this.y * -(q.D+MINIMUM_RESOLUTION)) * denom, (this.x * -(q.D+MINIMUM_RESOLUTION) + (this.D-MINIMUM_RESOLUTION) * q.x) * denom, 0.0,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D+MINIMUM_RESOLUTION) * q.y - this.y * -(q.D-MINIMUM_RESOLUTION)) * denom, (this.x * -(q.D-MINIMUM_RESOLUTION) + (this.D+MINIMUM_RESOLUTION) * q.x) * denom, 0.0,
        bounds);
      recordLineBounds(planetModel, boundsInfo,
        lineVectorX, lineVectorY, lineVectorZ,
        (-(this.D-MINIMUM_RESOLUTION) * q.y - this.y * -(q.D-MINIMUM_RESOLUTION)) * denom, (this.x * -(q.D-MINIMUM_RESOLUTION) + (this.D-MINIMUM_RESOLUTION) * q.x) * denom, 0.0,
        bounds);
    }
  }
  
  private static void recordLineBounds(final PlanetModel planetModel,
    final Bounds boundsInfo,
    final double lineVectorX, final double lineVectorY, final double lineVectorZ,
    final double x0, final double y0, final double z0,
    final Membership... bounds) {
    // Once an intersecting line is determined, the next step is to intersect that line with the ellipsoid, which
    // will yield zero, one, or two points.
    // The ellipsoid equation: 1,0 = x^2/a^2 + y^2/b^2 + z^2/c^2
    // 1.0 = (At+A0)^2/a^2 + (Bt+B0)^2/b^2 + (Ct+C0)^2/c^2
    // A^2 t^2 / a^2 + 2AA0t / a^2 + A0^2 / a^2 + B^2 t^2 / b^2 + 2BB0t / b^2 + B0^2 / b^2 + C^2 t^2 / c^2 + 2CC0t / c^2 + C0^2 / c^2  - 1,0 = 0.0
    // [A^2 / a^2 + B^2 / b^2 + C^2 / c^2] t^2 + [2AA0 / a^2 + 2BB0 / b^2 + 2CC0 / c^2] t + [A0^2 / a^2 + B0^2 / b^2 + C0^2 / c^2 - 1,0] = 0.0
    // Use the quadratic formula to determine t values and candidate point(s)
    final double A = lineVectorX * lineVectorX * planetModel.inverseAbSquared +
      lineVectorY * lineVectorY * planetModel.inverseAbSquared +
      lineVectorZ * lineVectorZ * planetModel.inverseCSquared;
    final double B = 2.0 * (lineVectorX * x0 * planetModel.inverseAbSquared + lineVectorY * y0 * planetModel.inverseAbSquared + lineVectorZ * z0 * planetModel.inverseCSquared);
    final double C = x0 * x0 * planetModel.inverseAbSquared + y0 * y0 * planetModel.inverseAbSquared + z0 * z0 * planetModel.inverseCSquared - 1.0;

    final double BsquaredMinus = B * B - 4.0 * A * C;
    if (Math.abs(BsquaredMinus) < MINIMUM_RESOLUTION_SQUARED) {
      //System.err.println(" One point of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // One solution only
      final double t = -B * inverse2A;
      // Maybe we can save ourselves the cost of construction of a point?
      final double pointX = lineVectorX * t + x0;
      final double pointY = lineVectorY * t + y0;
      final double pointZ = lineVectorZ * t + z0;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(pointX, pointY, pointZ)) {
          return;
        }
      }
      boundsInfo.addPoint(new GeoPoint(pointX, pointY, pointZ));
    } else if (BsquaredMinus > 0.0) {
      //System.err.println(" Two points of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // Two solutions
      final double sqrtTerm = Math.sqrt(BsquaredMinus);
      final double t1 = (-B + sqrtTerm) * inverse2A;
      final double t2 = (-B - sqrtTerm) * inverse2A;
      // Up to two points being returned.  Do what we can to save on object creation though.
      final double point1X = lineVectorX * t1 + x0;
      final double point1Y = lineVectorY * t1 + y0;
      final double point1Z = lineVectorZ * t1 + z0;
      final double point2X = lineVectorX * t2 + x0;
      final double point2Y = lineVectorY * t2 + y0;
      final double point2Z = lineVectorZ * t2 + z0;
      boolean point1Valid = true;
      boolean point2Valid = true;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point1X, point1Y, point1Z)) {
          point1Valid = false;
          break;
        }
      }
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point2X, point2Y, point2Z)) {
          point2Valid = false;
          break;
        }
      }

      if (point1Valid) {
        boundsInfo.addPoint(new GeoPoint(point1X, point1Y, point1Z));
      }
      if (point2Valid) {
        boundsInfo.addPoint(new GeoPoint(point2X, point2Y, point2Z));
      }
    } else {
      // If we can't intersect line with world, then it's outside the world, so
      // we have to assume everything is included.
      boundsInfo.noBound(planetModel);
    }
  }

  /*
  protected void verifyPoint(final PlanetModel planetModel, final GeoPoint point, final Plane q) {
    if (!evaluateIsZero(point))
      throw new RuntimeException("Intersection point not on original plane; point="+point+", plane="+this);
    if (!q.evaluateIsZero(point))
      throw new RuntimeException("Intersection point not on intersected plane; point="+point+", plane="+q);
    if (Math.abs(point.x * point.x * planetModel.inverseASquared + point.y * point.y * planetModel.inverseBSquared + point.z * point.z * planetModel.inverseCSquared - 1.0) >= MINIMUM_RESOLUTION) 
      throw new RuntimeException("Intersection point not on ellipsoid; point="+point);
  }
  */

  /**
   * Accumulate (x,y,z) bounds information for this plane, intersected with another and the
   * world.
   * Updates min/max information using intersection points found.  These include the error
   * envelope for the planes (D +/- MINIMUM_RESOLUTION).
   * @param planetModel is the planet model to use in determining bounds.
   * @param boundsInfo is the xyz info to update with additional bounding information.
   * @param p is the other plane.
   * @param bounds     are the surfaces delineating what's inside the shape.
   */
  public void recordBounds(final PlanetModel planetModel, final XYZBounds boundsInfo, final Plane p, final Membership... bounds) {
    findIntersectionBounds(planetModel, boundsInfo, p, bounds);
  }


  /**
   * Accumulate (x,y,z) bounds information for this plane, intersected with the unit sphere.
   * Updates min/max information, using max/min points found
   * within the specified bounds.
   *
   * @param planetModel is the planet model to use in determining bounds.
   * @param boundsInfo is the xyz info to update with additional bounding information.
   * @param bounds     are the surfaces delineating what's inside the shape.
   */
  public void recordBounds(final PlanetModel planetModel, final XYZBounds boundsInfo, final Membership... bounds) {
    // Basic plan is to do three intersections of the plane and the planet.
    // For min/max x, we intersect a vertical plane such that y = 0.
    // For min/max y, we intersect a vertical plane such that x = 0.
    // For min/max z, we intersect a vertical plane that is chosen to go through the high point of the arc.
    // For clarity, load local variables with good names
    final double A = this.x;
    final double B = this.y;
    final double C = this.z;

    // Do Z.  This can be done simply because it is symmetrical.
    if (!boundsInfo.isSmallestMinZ(planetModel) || !boundsInfo.isLargestMaxZ(planetModel)) {
      //System.err.println("    computing Z bound");
      // Compute Z bounds for this arc
      // With ellipsoids, we really have only one viable way to do this computation.
      // Specifically, we compute an appropriate vertical plane, based on the current plane's x-y orientation, and
      // then intersect it with this one and with the ellipsoid.  This gives us zero, one, or two points to use
      // as bounds.
      // There is one special case: horizontal circles.  These require TWO vertical planes: one for the x, and one for
      // the y, and we use all four resulting points in the bounds computation.
      if ((Math.abs(A) >= MINIMUM_RESOLUTION || Math.abs(B) >= MINIMUM_RESOLUTION)) {
        // NOT a degenerate case
        //System.err.println("    not degenerate");
        final Plane normalizedZPlane = constructNormalizedZPlane(A,B);
        final GeoPoint[] points = findIntersections(planetModel, normalizedZPlane, bounds, NO_BOUNDS);
        for (final GeoPoint point : points) {
          assert planetModel.pointOnSurface(point);
          //System.err.println("      Point = "+point+"; this.evaluate(point)="+this.evaluate(point)+"; normalizedZPlane.evaluate(point)="+normalizedZPlane.evaluate(point));
          addPoint(boundsInfo, bounds, point);
        }
      } else {
        // Since a==b==0, any plane including the Z axis suffices.
        //System.err.println("      Perpendicular to z");
        final GeoPoint[] points = findIntersections(planetModel, normalYPlane, NO_BOUNDS, NO_BOUNDS);
        if (points.length > 0) {
          boundsInfo.addZValue(points[0]);
        }
      }
    }

    // First, compute common subexpressions
    final double k = 1.0 / ((x*x + y*y)*planetModel.ab*planetModel.ab + z*z*planetModel.c*planetModel.c);
    final double abSquared = planetModel.ab * planetModel.ab;
    final double cSquared = planetModel.c * planetModel.c;
    final double ASquared = A * A;
    final double BSquared = B * B;
    final double CSquared = C * C;
    
    final double r = 2.0*D*k;
    final double rSquared = r * r;
    
    if (!boundsInfo.isSmallestMinX(planetModel) || !boundsInfo.isLargestMaxX(planetModel)) {
      // For min/max x, we need to use lagrange multipliers.
      //
      // For this, we need grad(F(x,y,z)) = (dF/dx, dF/dy, dF/dz).
      //
      // Minimize and maximize f(x,y,z) = x, with respect to g(x,y,z) = Ax + By + Cz - D and h(x,y,z) = x^2/ab^2 + y^2/ab^2 + z^2/c^2 - 1
      //
      // grad(f(x,y,z)) = (1,0,0)
      // grad(g(x,y,z)) = (A,B,C)
      // grad(h(x,y,z)) = (2x/ab^2,2y/ab^2,2z/c^2)
      //
      // Equations we need to simultaneously solve:
      // 
      // grad(f(x,y,z)) = l * grad(g(x,y,z)) + m * grad(h(x,y,z))
      // g(x,y,z) = 0
      // h(x,y,z) = 0
      // 
      // Equations:
      // 1 = l*A + m*2x/ab^2
      // 0 = l*B + m*2y/ab^2
      // 0 = l*C + m*2z/c^2
      // Ax + By + Cz + D = 0
      // x^2/ab^2 + y^2/ab^2 + z^2/c^2 - 1 = 0
      // 
      // Solve for x,y,z in terms of (l, m):
      // 
      // x = ((1 - l*A) * ab^2 ) / (2 * m)
      // y = (-l*B * ab^2) / ( 2 * m)
      // z = (-l*C * c^2)/ (2 * m)
      // 
      // Two equations, two unknowns:
      // 
      // A * (((1 - l*A) * ab^2 ) / (2 * m)) + B * ((-l*B * ab^2) / ( 2 * m)) + C * ((-l*C * c^2)/ (2 * m)) + D = 0
      // 
      // and
      // 
      // (((1 - l*A) * ab^2 ) / (2 * m))^2/ab^2 + ((-l*B * ab^2) / ( 2 * m))^2/ab^2 + ((-l*C * c^2)/ (2 * m))^2/c^2 - 1 = 0
      // 
      // Simple: solve for l and m, then find x from it.
      // 
      // (a) Use first equation to find l in terms of m.
      // 
      // A * (((1 - l*A) * ab^2 ) / (2 * m)) + B * ((-l*B * ab^2) / ( 2 * m)) + C * ((-l*C * c^2)/ (2 * m)) + D = 0
      // A * ((1 - l*A) * ab^2 ) + B * (-l*B * ab^2) + C * (-l*C * c^2) + D * 2 * m = 0
      // A * ab^2 - l*A^2* ab^2 - B^2 * l * ab^2 - C^2 * l * c^2 + D * 2 * m = 0
      // - l *(A^2* ab^2 + B^2 * ab^2 + C^2 * c^2) + (A * ab^2 + D * 2 * m) = 0
      // l = (A * ab^2 + D * 2 * m) / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2)
      // l = A * ab^2 / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2) + m * 2 * D / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2)
      // 
      // For convenience:
      // 
      // k = 1.0 / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2)
      // 
      // Then:
      // 
      // l = A * ab^2 * k + m * 2 * D * k
      // l = k * (A*ab^2 + m*2*D)
      //
      // For further convenience:
      //
      // q = A*ab^2*k
      // r = 2*D*k
      //
      // l = (r*m + q)
      // l^2 = (r^2 * m^2 + 2*r*m*q + q^2)
      // 
      // (b) Simplify the second equation before substitution
      // 
      // (((1 - l*A) * ab^2 ) / (2 * m))^2/ab^2 + ((-l*B * ab^2) / ( 2 * m))^2/ab^2 + ((-l*C * c^2)/ (2 * m))^2/c^2 - 1 = 0
      // ((1 - l*A) * ab^2 )^2/ab^2 + (-l*B * ab^2)^2/ab^2 + (-l*C * c^2)^2/c^2 = 4 * m^2
      // (1 - l*A)^2 * ab^2 + (-l*B)^2 * ab^2 + (-l*C)^2 * c^2 = 4 * m^2
      // (1 - 2*l*A + l^2*A^2) * ab^2 + l^2*B^2 * ab^2 + l^2*C^2 * c^2 = 4 * m^2
      // ab^2 - 2*A*ab^2*l + A^2*ab^2*l^2 + B^2*ab^2*l^2 + C^2*c^2*l^2 - 4*m^2 = 0
      // 
      // (c) Substitute for l, l^2
      //
      // ab^2 - 2*A*ab^2*(r*m + q) + A^2*ab^2*(r^2 * m^2 + 2*r*m*q + q^2) + B^2*ab^2*(r^2 * m^2 + 2*r*m*q + q^2) + C^2*c^2*(r^2 * m^2 + 2*r*m*q + q^2) - 4*m^2 = 0
      // ab^2 - 2*A*ab^2*r*m - 2*A*ab^2*q + A^2*ab^2*r^2*m^2 + 2*A^2*ab^2*r*q*m +
      //        A^2*ab^2*q^2 + B^2*ab^2*r^2*m^2 + 2*B^2*ab^2*r*q*m + B^2*ab^2*q^2 + C^2*c^2*r^2*m^2 + 2*C^2*c^2*r*q*m + C^2*c^2*q^2 - 4*m^2 = 0
      //
      // (d) Group
      //
      // m^2 * [A^2*ab^2*r^2 + B^2*ab^2*r^2 + C^2*c^2*r^2 - 4] +
      // m * [- 2*A*ab^2*r + 2*A^2*ab^2*r*q + 2*B^2*ab^2*r*q + 2*C^2*c^2*r*q] +
      // [ab^2 - 2*A*ab^2*q + A^2*ab^2*q^2 + B^2*ab^2*q^2 + C^2*c^2*q^2]  =  0
      
      // Useful subexpressions for this bound
      final double q = A*abSquared*k;
      final double qSquared = q * q;

      // Quadratic equation
      final double a = ASquared*abSquared*rSquared + BSquared*abSquared*rSquared + CSquared*cSquared*rSquared - 4.0;
      final double b = - 2.0*A*abSquared*r + 2.0*ASquared*abSquared*r*q + 2.0*BSquared*abSquared*r*q + 2.0*CSquared*cSquared*r*q;
      final double c = abSquared - 2.0*A*abSquared*q + ASquared*abSquared*qSquared + BSquared*abSquared*qSquared + CSquared*cSquared*qSquared;
      
      if (Math.abs(a) >= MINIMUM_RESOLUTION_SQUARED) {
        final double sqrtTerm = b*b - 4.0*a*c;
        if (Math.abs(sqrtTerm) < MINIMUM_RESOLUTION_SQUARED) {
          // One solution
          final double m = -b / (2.0 * a);
          // Valid?
          if (Math.abs(m) >= MINIMUM_RESOLUTION) {
            final double l = r * m + q;
            // x = ((1 - l*A) * ab^2 ) / (2 * m)
            // y = (-l*B * ab^2) / ( 2 * m)
            // z = (-l*C * c^2)/ (2 * m)
            final double denom0 = 0.5 / m;
            final GeoPoint thePoint = new GeoPoint((1.0-l*A) * abSquared * denom0, -l*B * abSquared * denom0, -l*C * cSquared * denom0);
            //Math is not quite accurate enough for this
            //assert planetModel.pointOnSurface(thePoint): "Point: "+thePoint+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
            //  (thePoint.x*thePoint.x*planetModel.inverseAb*planetModel.inverseAb + thePoint.y*thePoint.y*planetModel.inverseAb*planetModel.inverseAb + thePoint.z*thePoint.z*planetModel.inverseC*planetModel.inverseC);
            //assert evaluateIsZero(thePoint): "Evaluation of point: "+evaluate(thePoint);
            addPoint(boundsInfo, bounds, thePoint);
          } else {
            // This is a plane of the form A=n B=0 C=0.  We can set a bound only by noting the D value.
            boundsInfo.addXValue(-D/A);
          }
        } else if (sqrtTerm > 0.0) {
          // Two solutions
          final double sqrtResult = Math.sqrt(sqrtTerm);
          final double commonDenom = 0.5/a;
          final double m1 = (-b + sqrtResult) * commonDenom;
          assert Math.abs(a * m1 * m1 + b * m1 + c) < MINIMUM_RESOLUTION;
          final double m2 = (-b - sqrtResult) * commonDenom;
          assert Math.abs(a * m2 * m2 + b * m2 + c) < MINIMUM_RESOLUTION;
          if (Math.abs(m1) >= MINIMUM_RESOLUTION || Math.abs(m2) >= MINIMUM_RESOLUTION) {
            final double l1 = r * m1 + q;
            final double l2 = r * m2 + q;
            // x = ((1 - l*A) * ab^2 ) / (2 * m)
            // y = (-l*B * ab^2) / ( 2 * m)
            // z = (-l*C * c^2)/ (2 * m)
            final double denom1 = 0.5 / m1;
            final double denom2 = 0.5 / m2;
            final GeoPoint thePoint1 = new GeoPoint((1.0-l1*A) * abSquared * denom1, -l1*B * abSquared * denom1, -l1*C * cSquared * denom1);
            final GeoPoint thePoint2 = new GeoPoint((1.0-l2*A) * abSquared * denom2, -l2*B * abSquared * denom2, -l2*C * cSquared * denom2);
            //Math is not quite accurate enough for this
            //assert planetModel.pointOnSurface(thePoint1): "Point1: "+thePoint1+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
            //  (thePoint1.x*thePoint1.x*planetModel.inverseAb*planetModel.inverseAb + thePoint1.y*thePoint1.y*planetModel.inverseAb*planetModel.inverseAb + thePoint1.z*thePoint1.z*planetModel.inverseC*planetModel.inverseC);
            //assert planetModel.pointOnSurface(thePoint2): "Point1: "+thePoint2+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
            //  (thePoint2.x*thePoint2.x*planetModel.inverseAb*planetModel.inverseAb + thePoint2.y*thePoint2.y*planetModel.inverseAb*planetModel.inverseAb + thePoint2.z*thePoint2.z*planetModel.inverseC*planetModel.inverseC);
            //assert evaluateIsZero(thePoint1): "Evaluation of point1: "+evaluate(thePoint1);
            //assert evaluateIsZero(thePoint2): "Evaluation of point2: "+evaluate(thePoint2);
            addPoint(boundsInfo, bounds, thePoint1);
            addPoint(boundsInfo, bounds, thePoint2);
          } else {
            // This is a plane of the form A=n B=0 C=0.  We can set a bound only by noting the D value.
            boundsInfo.addXValue(-D/A);
          }
        } else {
          // No solutions
        }
      } else if (Math.abs(b) > MINIMUM_RESOLUTION_SQUARED) {
        // a = 0, so m = - c / b
        final double m = -c / b;
        final double l = r * m + q;
        // x = ((1 - l*A) * ab^2 ) / (2 * m)
        // y = (-l*B * ab^2) / ( 2 * m)
        // z = (-l*C * c^2)/ (2 * m)
        final double denom0 = 0.5 / m;
        final GeoPoint thePoint = new GeoPoint((1.0-l*A) * abSquared * denom0, -l*B * abSquared * denom0, -l*C * cSquared * denom0);
        //Math is not quite accurate enough for this
        //assert planetModel.pointOnSurface(thePoint): "Point: "+thePoint+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
        //  (thePoint.x*thePoint.x*planetModel.inverseAb*planetModel.inverseAb + thePoint.y*thePoint.y*planetModel.inverseAb*planetModel.inverseAb + thePoint.z*thePoint.z*planetModel.inverseC*planetModel.inverseC);
        //assert evaluateIsZero(thePoint): "Evaluation of point: "+evaluate(thePoint);
        addPoint(boundsInfo, bounds, thePoint);
      } else {
        // Something went very wrong; a = b = 0
      }
    }
    
    // Do Y
    if (!boundsInfo.isSmallestMinY(planetModel) || !boundsInfo.isLargestMaxY(planetModel)) {
      // For min/max x, we need to use lagrange multipliers.
      //
      // For this, we need grad(F(x,y,z)) = (dF/dx, dF/dy, dF/dz).
      //
      // Minimize and maximize f(x,y,z) = y, with respect to g(x,y,z) = Ax + By + Cz - D and h(x,y,z) = x^2/ab^2 + y^2/ab^2 + z^2/c^2 - 1
      //
      // grad(f(x,y,z)) = (0,1,0)
      // grad(g(x,y,z)) = (A,B,C)
      // grad(h(x,y,z)) = (2x/ab^2,2y/ab^2,2z/c^2)
      //
      // Equations we need to simultaneously solve:
      // 
      // grad(f(x,y,z)) = l * grad(g(x,y,z)) + m * grad(h(x,y,z))
      // g(x,y,z) = 0
      // h(x,y,z) = 0
      // 
      // Equations:
      // 0 = l*A + m*2x/ab^2
      // 1 = l*B + m*2y/ab^2
      // 0 = l*C + m*2z/c^2
      // Ax + By + Cz + D = 0
      // x^2/ab^2 + y^2/ab^2 + z^2/c^2 - 1 = 0
      // 
      // Solve for x,y,z in terms of (l, m):
      // 
      // x = (-l*A * ab^2 ) / (2 * m)
      // y = ((1 - l*B) * ab^2) / ( 2 * m)
      // z = (-l*C * c^2)/ (2 * m)
      // 
      // Two equations, two unknowns:
      // 
      // A * ((-l*A * ab^2 ) / (2 * m)) + B * (((1 - l*B) * ab^2) / ( 2 * m)) + C * ((-l*C * c^2)/ (2 * m)) + D = 0
      // 
      // and
      // 
      // ((-l*A * ab^2 ) / (2 * m))^2/ab^2 + (((1 - l*B) * ab^2) / ( 2 * m))^2/ab^2 + ((-l*C * c^2)/ (2 * m))^2/c^2 - 1 = 0
      // 
      // Simple: solve for l and m, then find y from it.
      // 
      // (a) Use first equation to find l in terms of m.
      // 
      // A * ((-l*A * ab^2 ) / (2 * m)) + B * (((1 - l*B) * ab^2) / ( 2 * m)) + C * ((-l*C * c^2)/ (2 * m)) + D = 0
      // A * (-l*A * ab^2 ) + B * ((1-l*B) * ab^2) + C * (-l*C * c^2) + D * 2 * m = 0
      // -A^2*l*ab^2 + B*ab^2 - l*B^2*ab^2 - C^2*l*c^2 + D*2*m = 0
      // - l *(A^2* ab^2 + B^2 * ab^2 + C^2 * c^2) + (B * ab^2 + D * 2 * m) = 0
      // l = (B * ab^2 + D * 2 * m) / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2)
      // l = B * ab^2 / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2) + m * 2 * D / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2)
      // 
      // For convenience:
      // 
      // k = 1.0 / (A^2* ab^2 + B^2 * ab^2 + C^2 * c^2)
      // 
      // Then:
      // 
      // l = B * ab^2 * k + m * 2 * D * k
      // l = k * (B*ab^2 + m*2*D)
      //
      // For further convenience:
      //
      // q = B*ab^2*k
      // r = 2*D*k
      //
      // l = (r*m + q)
      // l^2 = (r^2 * m^2 + 2*r*m*q + q^2)
      // 
      // (b) Simplify the second equation before substitution
      // 
      // ((-l*A * ab^2 ) / (2 * m))^2/ab^2 + (((1 - l*B) * ab^2) / ( 2 * m))^2/ab^2 + ((-l*C * c^2)/ (2 * m))^2/c^2 - 1 = 0
      // (-l*A * ab^2 )^2/ab^2 + ((1 - l*B) * ab^2)^2/ab^2 + (-l*C * c^2)^2/c^2 = 4 * m^2
      // (-l*A)^2 * ab^2 + (1 - l*B)^2 * ab^2 + (-l*C)^2 * c^2 = 4 * m^2
      // l^2*A^2 * ab^2 + (1 - 2*l*B + l^2*B^2) * ab^2 + l^2*C^2 * c^2 = 4 * m^2
      // A^2*ab^2*l^2 + ab^2 - 2*B*ab^2*l + B^2*ab^2*l^2 + C^2*c^2*l^2 - 4*m^2 = 0
      // 
      // (c) Substitute for l, l^2
      //
      // A^2*ab^2*(r^2 * m^2 + 2*r*m*q + q^2) + ab^2 - 2*B*ab^2*(r*m + q) + B^2*ab^2*(r^2 * m^2 + 2*r*m*q + q^2) + C^2*c^2*(r^2 * m^2 + 2*r*m*q + q^2) - 4*m^2 = 0
      // A^2*ab^2*r^2*m^2 + 2*A^2*ab^2*r*q*m + A^2*ab^2*q^2 + ab^2 - 2*B*ab^2*r*m - 2*B*ab^2*q + B^2*ab^2*r^2*m^2 +
      //    2*B^2*ab^2*r*q*m + B^2*ab^2*q^2 + C^2*c^2*r^2*m^2 + 2*C^2*c^2*r*q*m + C^2*c^2*q^2 - 4*m^2 = 0
      //
      // (d) Group
      //
      // m^2 * [A^2*ab^2*r^2 + B^2*ab^2*r^2 + C^2*c^2*r^2 - 4] +
      // m * [2*A^2*ab^2*r*q - 2*B*ab^2*r + 2*B^2*ab^2*r*q + 2*C^2*c^2*r*q] +
      // [A^2*ab^2*q^2 + ab^2 - 2*B*ab^2*q + B^2*ab^2*q^2 + C^2*c^2*q^2]  =  0

      //System.err.println("    computing Y bound");
      
      // Useful subexpressions for this bound
      final double q = B*abSquared*k;
      final double qSquared = q * q;

      // Quadratic equation
      final double a = ASquared*abSquared*rSquared + BSquared*abSquared*rSquared + CSquared*cSquared*rSquared - 4.0;
      final double b = 2.0*ASquared*abSquared*r*q - 2.0*B*abSquared*r + 2.0*BSquared*abSquared*r*q + 2.0*CSquared*cSquared*r*q;
      final double c = ASquared*abSquared*qSquared + abSquared - 2.0*B*abSquared*q + BSquared*abSquared*qSquared + CSquared*cSquared*qSquared;

      if (Math.abs(a) >= MINIMUM_RESOLUTION_SQUARED) {
        final double sqrtTerm = b*b - 4.0*a*c;
        if (Math.abs(sqrtTerm) < MINIMUM_RESOLUTION_SQUARED) {
          // One solution
          final double m = -b / (2.0 * a);
          // Valid?
          if (Math.abs(m) >= MINIMUM_RESOLUTION) {
            final double l = r * m + q;
            // x = (-l*A * ab^2 ) / (2 * m)
            // y = ((1.0-l*B) * ab^2) / ( 2 * m)
            // z = (-l*C * c^2)/ (2 * m)
            final double denom0 = 0.5 / m;
            final GeoPoint thePoint = new GeoPoint(-l*A * abSquared * denom0, (1.0-l*B) * abSquared * denom0, -l*C * cSquared * denom0);
            //Math is not quite accurate enough for this
            //assert planetModel.pointOnSurface(thePoint): "Point: "+thePoint+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
            //  (thePoint1.x*thePoint.x*planetModel.inverseAb*planetModel.inverseAb + thePoint.y*thePoint.y*planetModel.inverseAb*planetModel.inverseAb + thePoint.z*thePoint.z*planetModel.inverseC*planetModel.inverseC);
            //assert evaluateIsZero(thePoint): "Evaluation of point: "+evaluate(thePoint);
            addPoint(boundsInfo, bounds, thePoint);
          } else {
            // This is a plane of the form A=0 B=n C=0.  We can set a bound only by noting the D value.
            boundsInfo.addYValue(-D/B);
          }
        } else if (sqrtTerm > 0.0) {
          // Two solutions
          final double sqrtResult = Math.sqrt(sqrtTerm);
          final double commonDenom = 0.5/a;
          final double m1 = (-b + sqrtResult) * commonDenom;
          assert Math.abs(a * m1 * m1 + b * m1 + c) < MINIMUM_RESOLUTION;
          final double m2 = (-b - sqrtResult) * commonDenom;
          assert Math.abs(a * m2 * m2 + b * m2 + c) < MINIMUM_RESOLUTION;
          if (Math.abs(m1) >= MINIMUM_RESOLUTION || Math.abs(m2) >= MINIMUM_RESOLUTION) {
            final double l1 = r * m1 + q;
            final double l2 = r * m2 + q;
            // x = (-l*A * ab^2 ) / (2 * m)
            // y = ((1.0-l*B) * ab^2) / ( 2 * m)
            // z = (-l*C * c^2)/ (2 * m)
            final double denom1 = 0.5 / m1;
            final double denom2 = 0.5 / m2;
            final GeoPoint thePoint1 = new GeoPoint(-l1*A * abSquared * denom1, (1.0-l1*B) * abSquared * denom1, -l1*C * cSquared * denom1);
            final GeoPoint thePoint2 = new GeoPoint(-l2*A * abSquared * denom2, (1.0-l2*B) * abSquared * denom2, -l2*C * cSquared * denom2);
            //Math is not quite accurate enough for this
            //assert planetModel.pointOnSurface(thePoint1): "Point1: "+thePoint1+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
            //  (thePoint1.x*thePoint1.x*planetModel.inverseAb*planetModel.inverseAb + thePoint1.y*thePoint1.y*planetModel.inverseAb*planetModel.inverseAb + thePoint1.z*thePoint1.z*planetModel.inverseC*planetModel.inverseC);
            //assert planetModel.pointOnSurface(thePoint2): "Point2: "+thePoint2+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
            //  (thePoint2.x*thePoint2.x*planetModel.inverseAb*planetModel.inverseAb + thePoint2.y*thePoint2.y*planetModel.inverseAb*planetModel.inverseAb + thePoint2.z*thePoint2.z*planetModel.inverseC*planetModel.inverseC);
            //assert evaluateIsZero(thePoint1): "Evaluation of point1: "+evaluate(thePoint1);
            //assert evaluateIsZero(thePoint2): "Evaluation of point2: "+evaluate(thePoint2);
            addPoint(boundsInfo, bounds, thePoint1);
            addPoint(boundsInfo, bounds, thePoint2);
          } else {
            // This is a plane of the form A=0 B=n C=0.  We can set a bound only by noting the D value.
            boundsInfo.addYValue(-D/B);
          }
        } else {
          // No solutions
        }
      } else if (Math.abs(b) > MINIMUM_RESOLUTION_SQUARED) {
        // a = 0, so m = - c / b
        final double m = -c / b;
        final double l = r * m + q;
        // x = ( -l*A * ab^2 ) / (2 * m)
        // y = ((1-l*B) * ab^2) / ( 2 * m)
        // z = (-l*C * c^2)/ (2 * m)
        final double denom0 = 0.5 / m;
        final GeoPoint thePoint = new GeoPoint(-l*A * abSquared * denom0, (1.0-l*B) * abSquared * denom0, -l*C * cSquared * denom0);
        //Math is not quite accurate enough for this
        //assert planetModel.pointOnSurface(thePoint): "Point: "+thePoint+"; Planetmodel="+planetModel+"; A="+A+" B="+B+" C="+C+" D="+D+" planetfcn="+
        //  (thePoint.x*thePoint.x*planetModel.inverseAb*planetModel.inverseAb + thePoint.y*thePoint.y*planetModel.inverseAb*planetModel.inverseAb + thePoint.z*thePoint.z*planetModel.inverseC*planetModel.inverseC);
        //assert evaluateIsZero(thePoint): "Evaluation of point: "+evaluate(thePoint);
        addPoint(boundsInfo, bounds, thePoint);
      } else {
        // Something went very wrong; a = b = 0
      }
    }
  }
  
  /**
   * Accumulate bounds information for this plane, intersected with another plane
   * and the world.
   * Updates both latitude and longitude information, using max/min points found
   * within the specified bounds.  Also takes into account the error envelope for all
   * planes being intersected.
   *
   * @param planetModel is the planet model to use in determining bounds.
   * @param boundsInfo is the lat/lon info to update with additional bounding information.
   * @param p is the other plane.
   * @param bounds     are the surfaces delineating what's inside the shape.
   */
  public void recordBounds(final PlanetModel planetModel, final LatLonBounds boundsInfo, final Plane p, final Membership... bounds) {
    findIntersectionBounds(planetModel, boundsInfo, p, bounds);
  }

  /**
   * Accumulate bounds information for this plane, intersected with the unit sphere.
   * Updates both latitude and longitude information, using max/min points found
   * within the specified bounds.
   *
   * @param planetModel is the planet model to use in determining bounds.
   * @param boundsInfo is the lat/lon info to update with additional bounding information.
   * @param bounds     are the surfaces delineating what's inside the shape.
   */
  public void recordBounds(final PlanetModel planetModel, final LatLonBounds boundsInfo, final Membership... bounds) {
    // For clarity, load local variables with good names
    final double A = this.x;
    final double B = this.y;
    final double C = this.z;

    // Now compute latitude min/max points
    if (!boundsInfo.checkNoTopLatitudeBound() || !boundsInfo.checkNoBottomLatitudeBound()) {
      //System.err.println("Looking at latitude for plane "+this);
      // With ellipsoids, we really have only one viable way to do this computation.
      // Specifically, we compute an appropriate vertical plane, based on the current plane's x-y orientation, and
      // then intersect it with this one and with the ellipsoid.  This gives us zero, one, or two points to use
      // as bounds.
      // There is one special case: horizontal circles.  These require TWO vertical planes: one for the x, and one for
      // the y, and we use all four resulting points in the bounds computation.
      if ((Math.abs(A) >= MINIMUM_RESOLUTION || Math.abs(B) >= MINIMUM_RESOLUTION)) {
        // NOT a horizontal circle!
        //System.err.println(" Not a horizontal circle");
        final Plane verticalPlane = constructNormalizedZPlane(A,B);
        final GeoPoint[] points = findIntersections(planetModel, verticalPlane, bounds, NO_BOUNDS);
        for (final GeoPoint point : points) {
          addPoint(boundsInfo, bounds, point);
        }
      } else {
        // Horizontal circle.  Since a==b, any vertical plane suffices.
        final GeoPoint[] points = findIntersections(planetModel, normalXPlane, NO_BOUNDS, NO_BOUNDS);
        boundsInfo.addZValue(points[0]);
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
            // means that our line's endpoints will coincide with the projected ellipse.  All we
            // need to do then is to find the intersection of the projected ellipse and the line
            // equation:
            //
            // A x + B y + D = 0
            //
            // Since A != 0:
            // x = (-By - D)/A
            //
            // The projected ellipse:
            // x^2/a^2 + y^2/b^2 - 1 = 0
            // Substitute:
            // [(-By-D)/A]^2/a^2 + y^2/b^2 -1 = 0
            // Multiply through by A^2:
            // [-By - D]^2/a^2 + A^2*y^2/b^2 - A^2 = 0
            // Multiply out:
            // B^2*y^2/a^2 + 2BDy/a^2 + D^2/a^2 + A^2*y^2/b^2 - A^2 = 0
            // Group:
            // y^2 * [B^2/a^2 + A^2/b^2] + y [2BD/a^2] + [D^2/a^2-A^2] = 0

            a = B * B * planetModel.inverseAbSquared + A * A * planetModel.inverseAbSquared;
            b = 2.0 * B * D * planetModel.inverseAbSquared;
            c = D * D * planetModel.inverseAbSquared - A * A;

            double sqrtClause = b * b - 4.0 * a * c;

            if (Math.abs(sqrtClause) < MINIMUM_RESOLUTION_SQUARED) {
              double y0 = -b / (2.0 * a);
              double x0 = (-D - B * y0) / A;
              double z0 = 0.0;
              addPoint(boundsInfo, bounds, new GeoPoint(x0, y0, z0));
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

              addPoint(boundsInfo, bounds, new GeoPoint(x0a, y0a, z0a));
              addPoint(boundsInfo, bounds, new GeoPoint(x0b, y0b, z0b));
            }

          } else {
            // Use equation suitable for B != 0
            // Since I != 0, we rewrite:
            // y = (-Ax - D)/B
            a = B * B * planetModel.inverseAbSquared + A * A * planetModel.inverseAbSquared;
            b = 2.0 * A * D * planetModel.inverseAbSquared;
            c = D * D * planetModel.inverseAbSquared - B * B;

            double sqrtClause = b * b - 4.0 * a * c;

            if (Math.abs(sqrtClause) < MINIMUM_RESOLUTION_SQUARED) {
              double x0 = -b / (2.0 * a);
              double y0 = (-D - A * x0) / B;
              double z0 = 0.0;
              addPoint(boundsInfo, bounds, new GeoPoint(x0, y0, z0));
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

              addPoint(boundsInfo, bounds, new GeoPoint(x0a, y0a, z0a));
              addPoint(boundsInfo, bounds, new GeoPoint(x0b, y0b, z0b));
            }
          }
        }

      } else {
        //System.err.println("General longitude bounds...");

        // NOTE WELL: The x,y,z values generated here are NOT on the unit sphere.
        // They are for lat/lon calculation purposes only.  x-y is meant to be used for longitude determination,
        // and z for latitude, and that's all the values are good for.

        // (1) Intersect the plane and the ellipsoid, and project the results into the x-y plane:
        // From plane:
        // z = (-Ax - By - D) / C
        // From ellipsoid:
        // x^2/a^2 + y^2/b^2 + [(-Ax - By - D) / C]^2/c^2 = 1
        // Simplify/expand:
        // C^2*x^2/a^2 + C^2*y^2/b^2 + (-Ax - By - D)^2/c^2 = C^2
        //
        // x^2 * C^2/a^2 + y^2 * C^2/b^2 + x^2 * A^2/c^2 + ABxy/c^2 + ADx/c^2 + ABxy/c^2 + y^2 * B^2/c^2 + BDy/c^2 + ADx/c^2 + BDy/c^2 + D^2/c^2 = C^2
        // Group:
        // [A^2/c^2 + C^2/a^2] x^2 + [B^2/c^2 + C^2/b^2] y^2 + [2AB/c^2]xy + [2AD/c^2]x + [2BD/c^2]y + [D^2/c^2-C^2] = 0
        // For convenience, introduce post-projection coefficient variables to make life easier.
        // E x^2 + F y^2 + G xy + H x + I y + J = 0
        double E = A * A * planetModel.inverseCSquared + C * C * planetModel.inverseAbSquared;
        double F = B * B * planetModel.inverseCSquared + C * C * planetModel.inverseAbSquared;
        double G = 2.0 * A * B * planetModel.inverseCSquared;
        double H = 2.0 * A * D * planetModel.inverseCSquared;
        double I = 2.0 * B * D * planetModel.inverseCSquared;
        double J = D * D * planetModel.inverseCSquared - C * C;

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

              addPoint(boundsInfo, bounds, new GeoPoint(x0, y0, z0));
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

              addPoint(boundsInfo, bounds, new GeoPoint(x0a, y0a, z0a));
              addPoint(boundsInfo, bounds, new GeoPoint(x0b, y0b, z0b));
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
              addPoint(boundsInfo, bounds, new GeoPoint(x0, y0, z0));
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

              addPoint(boundsInfo, bounds, new GeoPoint(x0a, y0a, z0a));
              addPoint(boundsInfo, bounds, new GeoPoint(x0b, y0b, z0b));
            }
          }
        }
      }
    }

  }

  /** Add a point to boundsInfo if within a specifically bounded area.
   * @param boundsInfo is the object to be modified.
   * @param bounds is the area that the point must be within.
   * @param point is the point.
   */
  private static void addPoint(final Bounds boundsInfo, final Membership[] bounds, final GeoPoint point) {
    // Make sure the discovered point is within the bounds
    for (Membership bound : bounds) {
      if (!bound.isWithin(point))
        return;
    }
    // Add the point
    boundsInfo.addPoint(point);
  }

  /**
   * Determine whether the plane intersects another plane within the
   * bounds provided.
   *
   * @param planetModel is the planet model to use in determining intersection.
   * @param q                 is the other plane.
   * @param notablePoints     are points to look at to disambiguate cases when the two planes are identical.
   * @param moreNotablePoints are additional points to look at to disambiguate cases when the two planes are identical.
   * @param bounds            is one part of the bounds.
   * @param moreBounds        are more bounds.
   * @return true if there's an intersection.
   */
  public boolean intersects(final PlanetModel planetModel, final Plane q, final GeoPoint[] notablePoints, final GeoPoint[] moreNotablePoints, final Membership[] bounds, final Membership... moreBounds) {
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
    
    // Save on allocations; do inline instead of calling findIntersections
    //System.err.println("Looking for intersection between plane "+this+" and plane "+q+" within bounds");
    // Unnormalized, unchecked...
    final double lineVectorX = y * q.z - z * q.y;
    final double lineVectorY = z * q.x - x * q.z;
    final double lineVectorZ = x * q.y - y * q.x;

    if (Math.abs(lineVectorX) < MINIMUM_RESOLUTION && Math.abs(lineVectorY) < MINIMUM_RESOLUTION && Math.abs(lineVectorZ) < MINIMUM_RESOLUTION) {
      // Degenerate case: parallel planes
      //System.err.println(" planes are parallel - no intersection");
      return false;
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
        return false;
      }
      final double denom = 1.0 / denomYZ;
      x0 = 0.0;
      y0 = (-this.D * q.z - this.z * -q.D) * denom;
      z0 = (this.y * -q.D + this.D * q.y) * denom;
    } else if (Math.abs(denomXZ) >= Math.abs(denomXY) && Math.abs(denomXZ) >= Math.abs(denomYZ)) {
      // Y is the biggest, so y0 = 0.0
      if (Math.abs(denomXZ) < MINIMUM_RESOLUTION_SQUARED) {
        //System.err.println(" Denominator is zero: no intersection");
        return false;
      }
      final double denom = 1.0 / denomXZ;
      x0 = (-this.D * q.z - this.z * -q.D) * denom;
      y0 = 0.0;
      z0 = (this.x * -q.D + this.D * q.x) * denom;
    } else {
      // Z is the biggest, so Z0 = 0.0
      if (Math.abs(denomXY) < MINIMUM_RESOLUTION_SQUARED) {
        //System.err.println(" Denominator is zero: no intersection");
        return false;
      }
      final double denom = 1.0 / denomXY;
      x0 = (-this.D * q.y - this.y * -q.D) * denom;
      y0 = (this.x * -q.D + this.D * q.x) * denom;
      z0 = 0.0;
    }

    // Once an intersecting line is determined, the next step is to intersect that line with the ellipsoid, which
    // will yield zero, one, or two points.
    // The ellipsoid equation: 1,0 = x^2/a^2 + y^2/b^2 + z^2/c^2
    // 1.0 = (At+A0)^2/a^2 + (Bt+B0)^2/b^2 + (Ct+C0)^2/c^2
    // A^2 t^2 / a^2 + 2AA0t / a^2 + A0^2 / a^2 + B^2 t^2 / b^2 + 2BB0t / b^2 + B0^2 / b^2 + C^2 t^2 / c^2 + 2CC0t / c^2 + C0^2 / c^2  - 1,0 = 0.0
    // [A^2 / a^2 + B^2 / b^2 + C^2 / c^2] t^2 + [2AA0 / a^2 + 2BB0 / b^2 + 2CC0 / c^2] t + [A0^2 / a^2 + B0^2 / b^2 + C0^2 / c^2 - 1,0] = 0.0
    // Use the quadratic formula to determine t values and candidate point(s)
    final double A = lineVectorX * lineVectorX * planetModel.inverseAbSquared +
      lineVectorY * lineVectorY * planetModel.inverseAbSquared +
      lineVectorZ * lineVectorZ * planetModel.inverseCSquared;
    final double B = 2.0 * (lineVectorX * x0 * planetModel.inverseAbSquared + lineVectorY * y0 * planetModel.inverseAbSquared + lineVectorZ * z0 * planetModel.inverseCSquared);
    final double C = x0 * x0 * planetModel.inverseAbSquared + y0 * y0 * planetModel.inverseAbSquared + z0 * z0 * planetModel.inverseCSquared - 1.0;

    final double BsquaredMinus = B * B - 4.0 * A * C;
    if (Math.abs(BsquaredMinus) < MINIMUM_RESOLUTION_SQUARED) {
      //System.err.println(" One point of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // One solution only
      final double t = -B * inverse2A;
      // Maybe we can save ourselves the cost of construction of a point?
      final double pointX = lineVectorX * t + x0;
      final double pointY = lineVectorY * t + y0;
      final double pointZ = lineVectorZ * t + z0;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(pointX, pointY, pointZ)) {
          return false;
        }
      }
      for (final Membership bound : moreBounds) {
        if (!bound.isWithin(pointX, pointY, pointZ)) {
          return false;
        }
      }
      return true;
    } else if (BsquaredMinus > 0.0) {
      //System.err.println(" Two points of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // Two solutions
      final double sqrtTerm = Math.sqrt(BsquaredMinus);
      final double t1 = (-B + sqrtTerm) * inverse2A;
      final double t2 = (-B - sqrtTerm) * inverse2A;
      // Up to two points being returned.  Do what we can to save on object creation though.
      final double point1X = lineVectorX * t1 + x0;
      final double point1Y = lineVectorY * t1 + y0;
      final double point1Z = lineVectorZ * t1 + z0;
      boolean point1Valid = true;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point1X, point1Y, point1Z)) {
          point1Valid = false;
          break;
        }
      }
      if (point1Valid) {
        for (final Membership bound : moreBounds) {
          if (!bound.isWithin(point1X, point1Y, point1Z)) {
            point1Valid = false;
            break;
          }
        }
      }
      if (point1Valid) {
        return true;
      }
      final double point2X = lineVectorX * t2 + x0;
      final double point2Y = lineVectorY * t2 + y0;
      final double point2Z = lineVectorZ * t2 + z0;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point2X, point2Y, point2Z)) {
          return false;
        }
      }
      for (final Membership bound : moreBounds) {
        if (!bound.isWithin(point2X, point2Y, point2Z)) {
          return false;
        }
      }
      return true;
    } else {
      //System.err.println(" no solutions - no intersection");
      return false;
    }
  }

  /**
   * Determine whether the plane crosses another plane within the
   * bounds provided.  Crossing is defined as intersecting with the geo surface at two points.
   *
   * @param planetModel is the planet model to use in determining intersection.
   * @param q                 is the other plane.
   * @param notablePoints     are points to look at to disambiguate cases when the two planes are identical.
   * @param moreNotablePoints are additional points to look at to disambiguate cases when the two planes are identical.
   * @param bounds            is one part of the bounds.
   * @param moreBounds        are more bounds.
   * @return true if there's a crossing.
   */
  public boolean crosses(final PlanetModel planetModel, final Plane q, final GeoPoint[] notablePoints, final GeoPoint[] moreNotablePoints, final Membership[] bounds, final Membership... moreBounds) {
    //System.err.println("Does plane "+this+" cross plane "+q);
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
    
    // Save on allocations; do inline instead of calling findIntersections
    //System.err.println("Looking for intersection between plane "+this+" and plane "+q+" within bounds");
    // Unnormalized, unchecked...
    final double lineVectorX = y * q.z - z * q.y;
    final double lineVectorY = z * q.x - x * q.z;
    final double lineVectorZ = x * q.y - y * q.x;

    if (Math.abs(lineVectorX) < MINIMUM_RESOLUTION && Math.abs(lineVectorY) < MINIMUM_RESOLUTION && Math.abs(lineVectorZ) < MINIMUM_RESOLUTION) {
      // Degenerate case: parallel planes
      //System.err.println(" planes are parallel - no intersection");
      return false;
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
        return false;
      }
      final double denom = 1.0 / denomYZ;
      x0 = 0.0;
      y0 = (-this.D * q.z - this.z * -q.D) * denom;
      z0 = (this.y * -q.D + this.D * q.y) * denom;
    } else if (Math.abs(denomXZ) >= Math.abs(denomXY) && Math.abs(denomXZ) >= Math.abs(denomYZ)) {
      // Y is the biggest, so y0 = 0.0
      if (Math.abs(denomXZ) < MINIMUM_RESOLUTION_SQUARED) {
        //System.err.println(" Denominator is zero: no intersection");
        return false;
      }
      final double denom = 1.0 / denomXZ;
      x0 = (-this.D * q.z - this.z * -q.D) * denom;
      y0 = 0.0;
      z0 = (this.x * -q.D + this.D * q.x) * denom;
    } else {
      // Z is the biggest, so Z0 = 0.0
      if (Math.abs(denomXY) < MINIMUM_RESOLUTION_SQUARED) {
        //System.err.println(" Denominator is zero: no intersection");
        return false;
      }
      final double denom = 1.0 / denomXY;
      x0 = (-this.D * q.y - this.y * -q.D) * denom;
      y0 = (this.x * -q.D + this.D * q.x) * denom;
      z0 = 0.0;
    }

    // Once an intersecting line is determined, the next step is to intersect that line with the ellipsoid, which
    // will yield zero, one, or two points.
    // The ellipsoid equation: 1,0 = x^2/a^2 + y^2/b^2 + z^2/c^2
    // 1.0 = (At+A0)^2/a^2 + (Bt+B0)^2/b^2 + (Ct+C0)^2/c^2
    // A^2 t^2 / a^2 + 2AA0t / a^2 + A0^2 / a^2 + B^2 t^2 / b^2 + 2BB0t / b^2 + B0^2 / b^2 + C^2 t^2 / c^2 + 2CC0t / c^2 + C0^2 / c^2  - 1,0 = 0.0
    // [A^2 / a^2 + B^2 / b^2 + C^2 / c^2] t^2 + [2AA0 / a^2 + 2BB0 / b^2 + 2CC0 / c^2] t + [A0^2 / a^2 + B0^2 / b^2 + C0^2 / c^2 - 1,0] = 0.0
    // Use the quadratic formula to determine t values and candidate point(s)
    final double A = lineVectorX * lineVectorX * planetModel.inverseAbSquared +
      lineVectorY * lineVectorY * planetModel.inverseAbSquared +
      lineVectorZ * lineVectorZ * planetModel.inverseCSquared;
    final double B = 2.0 * (lineVectorX * x0 * planetModel.inverseAbSquared + lineVectorY * y0 * planetModel.inverseAbSquared + lineVectorZ * z0 * planetModel.inverseCSquared);
    final double C = x0 * x0 * planetModel.inverseAbSquared + y0 * y0 * planetModel.inverseAbSquared + z0 * z0 * planetModel.inverseCSquared - 1.0;

    final double BsquaredMinus = B * B - 4.0 * A * C;
    if (Math.abs(BsquaredMinus) < MINIMUM_RESOLUTION_SQUARED) {
      //System.err.println(" One point of intersection");
      // We're not interested in situations where there is only one solution; these are intersections but not crossings
      return false;
    } else if (BsquaredMinus > 0.0) {
      //System.err.println(" Two points of intersection");
      final double inverse2A = 1.0 / (2.0 * A);
      // Two solutions
      final double sqrtTerm = Math.sqrt(BsquaredMinus);
      final double t1 = (-B + sqrtTerm) * inverse2A;
      final double t2 = (-B - sqrtTerm) * inverse2A;
      // Up to two points being returned.  Do what we can to save on object creation though.
      final double point1X = lineVectorX * t1 + x0;
      final double point1Y = lineVectorY * t1 + y0;
      final double point1Z = lineVectorZ * t1 + z0;
      boolean point1Valid = true;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point1X, point1Y, point1Z)) {
          point1Valid = false;
          break;
        }
      }
      if (point1Valid) {
        for (final Membership bound : moreBounds) {
          if (!bound.isWithin(point1X, point1Y, point1Z)) {
            point1Valid = false;
            break;
          }
        }
      }
      if (point1Valid) {
        return true;
      }
      final double point2X = lineVectorX * t2 + x0;
      final double point2Y = lineVectorY * t2 + y0;
      final double point2Z = lineVectorZ * t2 + z0;
      for (final Membership bound : bounds) {
        if (!bound.isWithin(point2X, point2Y, point2Z)) {
          return false;
        }
      }
      for (final Membership bound : moreBounds) {
        if (!bound.isWithin(point2X, point2Y, point2Z)) {
          return false;
        }
      }
      return true;
    } else {
      //System.err.println(" no solutions - no intersection");
      return false;
    }
  }

  /**
   * Returns true if this plane and the other plane are functionally identical within the margin of error.
   * Functionally identical means that the planes are so close to parallel that many aspects of planar math,
   * like intersections, no longer have answers to within the required precision.
   * @param p is the plane to compare against.
   * @return true if the planes are functionally identical.
   */
  public boolean isFunctionallyIdentical(final Plane p) {
    // We can get the correlation by just doing a parallel plane check.  That's basically finding
    // out if the magnitude of the cross-product is "zero".
    final double cross1 = this.y * p.z - this.z * p.y;
    final double cross2 = this.z * p.x - this.x * p.z;
    final double cross3 = this.x * p.y - this.y * p.x;
    //System.out.println("cross product magnitude = "+(cross1 * cross1 + cross2 * cross2 + cross3 * cross3));
    // Should be MINIMUM_RESOLUTION_SQUARED, but that gives us planes that are *almost* parallel, and those are problematic too,
    // so we have a tighter constraint on parallelism in this method.
    if (cross1 * cross1 + cross2 * cross2 + cross3 * cross3 >= MINIMUM_RESOLUTION) {
      return false;
    }
    
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
  
  /**
   * Returns true if this plane and the other plane are identical within the margin of error.
   * @param p is the plane to compare against.
   * @return true if the planes are numerically identical.
   */
  public boolean isNumericallyIdentical(final Plane p) {
    // We can get the correlation by just doing a parallel plane check.  That's basically finding
    // out if the magnitude of the cross-product is "zero".
    final double cross1 = this.y * p.z - this.z * p.y;
    final double cross2 = this.z * p.x - this.x * p.z;
    final double cross3 = this.x * p.y - this.y * p.x;
    //System.out.println("cross product magnitude = "+(cross1 * cross1 + cross2 * cross2 + cross3 * cross3));
    if (cross1 * cross1 + cross2 * cross2 + cross3 * cross3 >= MINIMUM_RESOLUTION_SQUARED) {
      return false;
    }
    /* Old method
    if (Math.abs(this.y * p.z - this.z * p.y) >= MINIMUM_RESOLUTION)
      return false;
    if (Math.abs(this.z * p.x - this.x * p.z) >= MINIMUM_RESOLUTION)
      return false;
    if (Math.abs(this.x * p.y - this.y * p.x) >= MINIMUM_RESOLUTION)
      return false;
    */
    
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

  /**
   * Locate a point that is within the specified bounds and on the specified plane, that has an arcDistance as
   * specified from the startPoint.
   * @param planetModel is the planet model.
   * @param arcDistanceValue is the arc distance.
   * @param startPoint is the starting point.
   * @param bounds are the bounds.
   * @return zero, one, or two points.
   */
  public GeoPoint[] findArcDistancePoints(final PlanetModel planetModel, final double arcDistanceValue, final GeoPoint startPoint, final Membership... bounds) {
    if (Math.abs(D) >= MINIMUM_RESOLUTION) {
      throw new IllegalStateException("Can't find arc distance using plane that doesn't go through origin");
    }
    if (!evaluateIsZero(startPoint)) {
      throw new IllegalArgumentException("Start point is not on plane");
    }
    
    // The following assertion fails at times even for planes that were *explicitly* normalized, so I've disabled the check.
    //assert Math.abs(x*x + y*y + z*z - 1.0) < MINIMUM_RESOLUTION_SQUARED : "Plane needs to be normalized";
    
    // The first step is to rotate coordinates for the point so that the plane lies on the x-y plane.
    // To acheive this, there will need to be three rotations:
    // (1) rotate the plane in x-y so that the y axis lies in it.
    // (2) rotate the plane in x-z so that the plane lies on the x-y plane.
    // (3) rotate in x-y so that the starting vector points to (1,0,0).
    
    // This presumes a normalized plane!!
    final double azimuthMagnitude = Math.sqrt(this.x * this.x + this.y * this.y);
    final double cosPlaneAltitude = this.z;
    final double sinPlaneAltitude = azimuthMagnitude;
    final double cosPlaneAzimuth = this.x / azimuthMagnitude;
    final double sinPlaneAzimuth = this.y / azimuthMagnitude;
    
    assert Math.abs(sinPlaneAltitude * sinPlaneAltitude + cosPlaneAltitude * cosPlaneAltitude - 1.0) < MINIMUM_RESOLUTION : "Improper sin/cos of altitude: "+(sinPlaneAltitude * sinPlaneAltitude + cosPlaneAltitude * cosPlaneAltitude);
    assert Math.abs(sinPlaneAzimuth * sinPlaneAzimuth + cosPlaneAzimuth * cosPlaneAzimuth - 1.0) < MINIMUM_RESOLUTION : "Improper sin/cos of azimuth: "+(sinPlaneAzimuth * sinPlaneAzimuth + cosPlaneAzimuth * cosPlaneAzimuth);

    // Coordinate rotation formula:
    // xT = xS cos T - yS sin T
    // yT = xS sin T + yS cos T
    // But we're rotating backwards, so use:
    // sin (-T) = -sin (T)
    // cos (-T) = cos (T)
    
    // Now, rotate startpoint in x-y
    final double x0 = startPoint.x;
    final double y0 = startPoint.y;
    final double z0 = startPoint.z;
    
    final double x1 = x0 * cosPlaneAzimuth + y0 * sinPlaneAzimuth;
    final double y1 = -x0 * sinPlaneAzimuth + y0 * cosPlaneAzimuth;
    final double z1 = z0;
    
    // Rotate now in x-z
    final double x2 = x1 * cosPlaneAltitude - z1 * sinPlaneAltitude;
    final double y2 = y1;
    final double z2 = +x1 * sinPlaneAltitude + z1 * cosPlaneAltitude;
    
    assert Math.abs(z2) < MINIMUM_RESOLUTION : "Rotation should have put startpoint on x-y plane, instead has value "+z2;
    
    // Ok, we have the start point on the x-y plane.  To apply the arc distance, we
    // next need to convert to an angle (in radians).
    final double startAngle = Math.atan2(y2, x2);
    
    // To apply the arc distance, just add to startAngle.
    final double point1Angle = startAngle + arcDistanceValue;
    final double point2Angle = startAngle - arcDistanceValue;
    // Convert each point to x-y
    final double point1x2 = Math.cos(point1Angle);
    final double point1y2 = Math.sin(point1Angle);
    final double point1z2 = 0.0;
    
    final double point2x2 = Math.cos(point2Angle);
    final double point2y2 = Math.sin(point2Angle);
    final double point2z2 = 0.0;
    
    // Now, do the reverse rotations for both points
    // Altitude...
    final double point1x1 = point1x2 * cosPlaneAltitude + point1z2 * sinPlaneAltitude;
    final double point1y1 = point1y2;
    final double point1z1 = -point1x2 * sinPlaneAltitude + point1z2 * cosPlaneAltitude;
    
    final double point2x1 = point2x2 * cosPlaneAltitude + point2z2 * sinPlaneAltitude;
    final double point2y1 = point2y2;
    final double point2z1 = -point2x2 * sinPlaneAltitude + point2z2 * cosPlaneAltitude;

    // Azimuth...
    final double point1x0 = point1x1 * cosPlaneAzimuth - point1y1 * sinPlaneAzimuth;
    final double point1y0 = point1x1 * sinPlaneAzimuth + point1y1 * cosPlaneAzimuth;
    final double point1z0 = point1z1;

    final double point2x0 = point2x1 * cosPlaneAzimuth - point2y1 * sinPlaneAzimuth;
    final double point2y0 = point2x1 * sinPlaneAzimuth + point2y1 * cosPlaneAzimuth;
    final double point2z0 = point2z1;

    final GeoPoint point1 = planetModel.createSurfacePoint(point1x0, point1y0, point1z0);
    final GeoPoint point2 = planetModel.createSurfacePoint(point2x0, point2y0, point2z0);
    
    // Figure out what to return
    boolean isPoint1Inside = meetsAllBounds(point1, bounds);
    boolean isPoint2Inside = meetsAllBounds(point2, bounds);
    
    if (isPoint1Inside) {
      if (isPoint2Inside) {
        return new GeoPoint[]{point1, point2};
      } else {
        return new GeoPoint[]{point1};
      }
    } else {
      if (isPoint2Inside) {
        return new GeoPoint[]{point2};
      } else {
        return new GeoPoint[0];
      }
    }
  }
  
  /**
   * Check if a vector meets the provided bounds.
   * @param p is the vector.
   * @param bounds are the bounds.
   * @return true if the vector describes a point within the bounds.
   */
  private static boolean meetsAllBounds(final Vector p, final Membership[] bounds) {
    return meetsAllBounds(p.x, p.y, p.z, bounds);
  }

  /**
   * Check if a vector meets the provided bounds.
   * @param x is the x value.
   * @param y is the y value.
   * @param z is the z value.
   * @param bounds are the bounds.
   * @return true if the vector describes a point within the bounds.
   */
  private static boolean meetsAllBounds(final double x, final double y, final double z, final Membership[] bounds) {
    for (final Membership bound : bounds) {
      if (!bound.isWithin(x,y,z))
        return false;
    }
    return true;
  }

  /**
   * Check if a vector meets the provided bounds.
   * @param p is the vector.
   * @param bounds are the bounds.
   * @param moreBounds are an additional set of bounds.
   * @return true if the vector describes a point within the bounds.
   */
  private static boolean meetsAllBounds(final Vector p, final Membership[] bounds, final Membership[] moreBounds) {
    return meetsAllBounds(p.x, p.y, p.z, bounds, moreBounds);
  }

  /**
   * Check if a vector meets the provided bounds.
   * @param x is the x value.
   * @param y is the y value.
   * @param z is the z value.
   * @param bounds are the bounds.
   * @param moreBounds are an additional set of bounds.
   * @return true if the vector describes a point within the bounds.
   */
  private static boolean meetsAllBounds(final double x, final double y, final double z, final Membership[] bounds,
                                          final Membership[] moreBounds) {
    return meetsAllBounds(x,y,z, bounds) && meetsAllBounds(x,y,z, moreBounds);
  }

  /**
   * Find a sample point on the intersection between two planes and the world.
   * @param planetModel is the planet model.
   * @param q is the second plane to consider.
   * @return a sample point that is on the intersection between the two planes and the world.
   */
  public GeoPoint getSampleIntersectionPoint(final PlanetModel planetModel, final Plane q) {
    final GeoPoint[] intersections = findIntersections(planetModel, q, NO_BOUNDS, NO_BOUNDS);
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
