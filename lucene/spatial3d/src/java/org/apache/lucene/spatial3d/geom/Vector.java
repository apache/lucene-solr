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
 * A 3d vector in space, not necessarily going through the origin.
 *
 * @lucene.experimental
 */
public class Vector {
  /** Values that are all considered to be essentially zero have a magnitude less than this. */
  public static final double MINIMUM_RESOLUTION = 1.0e-12;
  /** Angular version of minimum resolution. */
  public static final double MINIMUM_ANGULAR_RESOLUTION = Math.PI * MINIMUM_RESOLUTION;
  /** For squared quantities, the bound is squared too. */
  public static final double MINIMUM_RESOLUTION_SQUARED = MINIMUM_RESOLUTION * MINIMUM_RESOLUTION;
  /** For cubed quantities, cube the bound. */
  public static final double MINIMUM_RESOLUTION_CUBED =
      MINIMUM_RESOLUTION_SQUARED * MINIMUM_RESOLUTION;

  /** The x value */
  public final double x;
  /** The y value */
  public final double y;
  /** The z value */
  public final double z;

  /**
   * Gram-Schmidt convergence envelope is a bit smaller than we really need because we don't want
   * the math to fail afterwards in other places.
   */
  private static final double MINIMUM_GRAM_SCHMIDT_ENVELOPE = MINIMUM_RESOLUTION * 0.5;

  /**
   * Construct from (U.S.) x,y,z coordinates.
   *
   * @param x is the x value.
   * @param y is the y value.
   * @param z is the z value.
   */
  public Vector(double x, double y, double z) {
    this.x = x;
    this.y = y;
    this.z = z;
  }

  /**
   * Construct a vector that is perpendicular to two other (non-zero) vectors. If the vectors are
   * parallel, IllegalArgumentException will be thrown. Produces a normalized final vector.
   *
   * @param A is the first vector
   * @param BX is the X value of the second
   * @param BY is the Y value of the second
   * @param BZ is the Z value of the second
   */
  public Vector(final Vector A, final double BX, final double BY, final double BZ) {
    // We're really looking at two vectors and computing a perpendicular one from that.
    this(A.x, A.y, A.z, BX, BY, BZ);
  }

  /**
   * Construct a vector that is perpendicular to two other (non-zero) vectors. If the vectors are
   * parallel, IllegalArgumentException will be thrown. Produces a normalized final vector.
   *
   * @param AX is the X value of the first
   * @param AY is the Y value of the first
   * @param AZ is the Z value of the first
   * @param BX is the X value of the second
   * @param BY is the Y value of the second
   * @param BZ is the Z value of the second
   */
  public Vector(
      final double AX,
      final double AY,
      final double AZ,
      final double BX,
      final double BY,
      final double BZ) {
    // We're really looking at two vectors and computing a perpendicular one from that.
    // We can think of this as having three points -- the origin, and two points that aren't the
    // origin. Normally, we can compute the perpendicular vector this way:
    // x = u2v3 - u3v2
    // y = u3v1 - u1v3
    // z = u1v2 - u2v1
    // Sometimes that produces a plane that does not contain the original three points, however, due
    // to numerical precision issues.  Then we continue making the answer more precise using the
    // Gram-Schmidt process: https://en.wikipedia.org/wiki/Gram%E2%80%93Schmidt_process

    // Compute the naive perpendicular
    final double thisX = AY * BZ - AZ * BY;
    final double thisY = AZ * BX - AX * BZ;
    final double thisZ = AX * BY - AY * BX;

    final double magnitude = magnitude(thisX, thisY, thisZ);
    if (magnitude == 0.0) {
      throw new IllegalArgumentException("Degenerate/parallel vector constructed");
    }
    final double inverseMagnitude = 1.0 / magnitude;

    double normalizeX = thisX * inverseMagnitude;
    double normalizeY = thisY * inverseMagnitude;
    double normalizeZ = thisZ * inverseMagnitude;
    // For a plane to work, the dot product between the normal vector
    // and the points needs to be less than the minimum resolution.
    // This is sometimes not true for points that are very close. Therefore
    // we need to adjust
    int i = 0;
    while (true) {
      final double currentDotProdA = AX * normalizeX + AY * normalizeY + AZ * normalizeZ;
      final double currentDotProdB = BX * normalizeX + BY * normalizeY + BZ * normalizeZ;
      if (Math.abs(currentDotProdA) < MINIMUM_GRAM_SCHMIDT_ENVELOPE
          && Math.abs(currentDotProdB) < MINIMUM_GRAM_SCHMIDT_ENVELOPE) {
        break;
      }
      // Converge on the one that has largest dot product
      final double currentVectorX;
      final double currentVectorY;
      final double currentVectorZ;
      final double currentDotProd;
      if (Math.abs(currentDotProdA) > Math.abs(currentDotProdB)) {
        currentVectorX = AX;
        currentVectorY = AY;
        currentVectorZ = AZ;
        currentDotProd = currentDotProdA;
      } else {
        currentVectorX = BX;
        currentVectorY = BY;
        currentVectorZ = BZ;
        currentDotProd = currentDotProdB;
      }

      // Adjust
      normalizeX = normalizeX - currentDotProd * currentVectorX;
      normalizeY = normalizeY - currentDotProd * currentVectorY;
      normalizeZ = normalizeZ - currentDotProd * currentVectorZ;
      // Normalize
      final double correctedMagnitude = magnitude(normalizeX, normalizeY, normalizeZ);
      final double inverseCorrectedMagnitude = 1.0 / correctedMagnitude;
      normalizeX = normalizeX * inverseCorrectedMagnitude;
      normalizeY = normalizeY * inverseCorrectedMagnitude;
      normalizeZ = normalizeZ * inverseCorrectedMagnitude;
      // This is probably not needed as the method seems to converge
      // quite quickly. But it is safer to have a way out.
      if (i++ > 10) {
        throw new IllegalArgumentException(
            "Plane could not be constructed! Could not find a normal vector.");
      }
    }
    this.x = normalizeX;
    this.y = normalizeY;
    this.z = normalizeZ;
  }

  /**
   * Construct a vector that is perpendicular to two other (non-zero) vectors. If the vectors are
   * parallel, IllegalArgumentException will be thrown. Produces a normalized final vector.
   *
   * @param A is the first vector
   * @param B is the second
   */
  public Vector(final Vector A, final Vector B) {
    this(A, B.x, B.y, B.z);
  }

  /** Compute a magnitude of an x,y,z value. */
  public static double magnitude(final double x, final double y, final double z) {
    return Math.sqrt(x * x + y * y + z * z);
  }

  /**
   * Compute a normalized unit vector based on the current vector.
   *
   * @return the normalized vector, or null if the current vector has a magnitude of zero.
   */
  public Vector normalize() {
    double denom = magnitude();
    if (denom < MINIMUM_RESOLUTION)
      // Degenerate, can't normalize
      return null;
    double normFactor = 1.0 / denom;
    return new Vector(x * normFactor, y * normFactor, z * normFactor);
  }

  /**
   * Evaluate the cross product of two vectors against a point. If the dot product of the resultant
   * vector resolves to "zero", then return true.
   *
   * @param A is the first vector to use for the cross product.
   * @param B is the second vector to use for the cross product.
   * @param point is the point to evaluate.
   * @return true if we get a zero dot product.
   */
  public static boolean crossProductEvaluateIsZero(
      final Vector A, final Vector B, final Vector point) {
    // Include Gram-Schmidt in-line so we avoid creating objects unnecessarily
    // Compute the naive perpendicular
    final double thisX = A.y * B.z - A.z * B.y;
    final double thisY = A.z * B.x - A.x * B.z;
    final double thisZ = A.x * B.y - A.y * B.x;

    final double magnitude = magnitude(thisX, thisY, thisZ);
    if (magnitude == 0.0) {
      return true;
    }

    final double inverseMagnitude = 1.0 / magnitude;

    double normalizeX = thisX * inverseMagnitude;
    double normalizeY = thisY * inverseMagnitude;
    double normalizeZ = thisZ * inverseMagnitude;
    // For a plane to work, the dot product between the normal vector
    // and the points needs to be less than the minimum resolution.
    // This is sometimes not true for points that are very close. Therefore
    // we need to adjust
    int i = 0;
    while (true) {
      final double currentDotProdA = A.x * normalizeX + A.y * normalizeY + A.z * normalizeZ;
      final double currentDotProdB = B.x * normalizeX + B.y * normalizeY + B.z * normalizeZ;
      if (Math.abs(currentDotProdA) < MINIMUM_GRAM_SCHMIDT_ENVELOPE
          && Math.abs(currentDotProdB) < MINIMUM_GRAM_SCHMIDT_ENVELOPE) {
        break;
      }
      // Converge on the one that has largest dot product
      final double currentVectorX;
      final double currentVectorY;
      final double currentVectorZ;
      final double currentDotProd;
      if (Math.abs(currentDotProdA) > Math.abs(currentDotProdB)) {
        currentVectorX = A.x;
        currentVectorY = A.y;
        currentVectorZ = A.z;
        currentDotProd = currentDotProdA;
      } else {
        currentVectorX = B.x;
        currentVectorY = B.y;
        currentVectorZ = B.z;
        currentDotProd = currentDotProdB;
      }

      // Adjust
      normalizeX = normalizeX - currentDotProd * currentVectorX;
      normalizeY = normalizeY - currentDotProd * currentVectorY;
      normalizeZ = normalizeZ - currentDotProd * currentVectorZ;
      // Normalize
      final double correctedMagnitude = magnitude(normalizeX, normalizeY, normalizeZ);
      final double inverseCorrectedMagnitude = 1.0 / correctedMagnitude;
      normalizeX = normalizeX * inverseCorrectedMagnitude;
      normalizeY = normalizeY * inverseCorrectedMagnitude;
      normalizeZ = normalizeZ * inverseCorrectedMagnitude;
      // This is probably not needed as the method seems to converge
      // quite quickly. But it is safer to have a way out.
      if (i++ > 10) {
        throw new IllegalArgumentException(
            "Plane could not be constructed! Could not find a normal vector.");
      }
    }
    return Math.abs(normalizeX * point.x + normalizeY * point.y + normalizeZ * point.z)
        < MINIMUM_RESOLUTION;
  }

  /**
   * Do a dot product.
   *
   * @param v is the vector to multiply.
   * @return the result.
   */
  public double dotProduct(final Vector v) {
    return this.x * v.x + this.y * v.y + this.z * v.z;
  }

  /**
   * Do a dot product.
   *
   * @param x is the x value of the vector to multiply.
   * @param y is the y value of the vector to multiply.
   * @param z is the z value of the vector to multiply.
   * @return the result.
   */
  public double dotProduct(final double x, final double y, final double z) {
    return this.x * x + this.y * y + this.z * z;
  }

  /**
   * Determine if this vector, taken from the origin, describes a point within a set of planes.
   *
   * @param bounds is the first part of the set of planes.
   * @param moreBounds is the second part of the set of planes.
   * @return true if the point is within the bounds.
   */
  public boolean isWithin(final Membership[] bounds, final Membership... moreBounds) {
    // Return true if the point described is within all provided bounds
    // System.err.println("  checking if " + this + " is within bounds");
    for (final Membership bound : bounds) {
      if (bound != null && !bound.isWithin(this)) {
        // System.err.println("    NOT within "+bound);
        return false;
      }
    }
    for (final Membership bound : moreBounds) {
      if (bound != null && !bound.isWithin(this)) {
        // System.err.println("    NOT within " + bound);
        return false;
      }
    }
    // System.err.println("    is within");
    return true;
  }

  /** Translate vector. */
  public Vector translate(final double xOffset, final double yOffset, final double zOffset) {
    return new Vector(x - xOffset, y - yOffset, z - zOffset);
  }

  /** Rotate vector counter-clockwise in x-y by an angle. */
  public Vector rotateXY(final double angle) {
    return rotateXY(Math.sin(angle), Math.cos(angle));
  }

  /** Rotate vector counter-clockwise in x-y by an angle, expressed as sin and cos. */
  public Vector rotateXY(final double sinAngle, final double cosAngle) {
    return new Vector(x * cosAngle - y * sinAngle, x * sinAngle + y * cosAngle, z);
  }

  /** Rotate vector counter-clockwise in x-z by an angle. */
  public Vector rotateXZ(final double angle) {
    return rotateXZ(Math.sin(angle), Math.cos(angle));
  }

  /** Rotate vector counter-clockwise in x-z by an angle, expressed as sin and cos. */
  public Vector rotateXZ(final double sinAngle, final double cosAngle) {
    return new Vector(x * cosAngle - z * sinAngle, y, x * sinAngle + z * cosAngle);
  }

  /** Rotate vector counter-clockwise in z-y by an angle. */
  public Vector rotateZY(final double angle) {
    return rotateZY(Math.sin(angle), Math.cos(angle));
  }

  /** Rotate vector counter-clockwise in z-y by an angle, expressed as sin and cos. */
  public Vector rotateZY(final double sinAngle, final double cosAngle) {
    return new Vector(x, z * sinAngle + y * cosAngle, z * cosAngle - y * sinAngle);
  }

  /**
   * Compute the square of a straight-line distance to a point described by the vector taken from
   * the origin. Monotonically increasing for arc distances up to PI.
   *
   * @param v is the vector to compute a distance to.
   * @return the square of the linear distance.
   */
  public double linearDistanceSquared(final Vector v) {
    double deltaX = this.x - v.x;
    double deltaY = this.y - v.y;
    double deltaZ = this.z - v.z;
    return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
  }

  /**
   * Compute the square of a straight-line distance to a point described by the vector taken from
   * the origin. Monotonically increasing for arc distances up to PI.
   *
   * @param x is the x part of the vector to compute a distance to.
   * @param y is the y part of the vector to compute a distance to.
   * @param z is the z part of the vector to compute a distance to.
   * @return the square of the linear distance.
   */
  public double linearDistanceSquared(final double x, final double y, final double z) {
    double deltaX = this.x - x;
    double deltaY = this.y - y;
    double deltaZ = this.z - z;
    return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
  }

  /**
   * Compute the straight-line distance to a point described by the vector taken from the origin.
   * Monotonically increasing for arc distances up to PI.
   *
   * @param v is the vector to compute a distance to.
   * @return the linear distance.
   */
  public double linearDistance(final Vector v) {
    return Math.sqrt(linearDistanceSquared(v));
  }

  /**
   * Compute the straight-line distance to a point described by the vector taken from the origin.
   * Monotonically increasing for arc distances up to PI.
   *
   * @param x is the x part of the vector to compute a distance to.
   * @param y is the y part of the vector to compute a distance to.
   * @param z is the z part of the vector to compute a distance to.
   * @return the linear distance.
   */
  public double linearDistance(final double x, final double y, final double z) {
    return Math.sqrt(linearDistanceSquared(x, y, z));
  }

  /**
   * Compute the square of the normal distance to a vector described by a vector taken from the
   * origin. Monotonically increasing for arc distances up to PI/2.
   *
   * @param v is the vector to compute a distance to.
   * @return the square of the normal distance.
   */
  public double normalDistanceSquared(final Vector v) {
    double t = dotProduct(v);
    double deltaX = this.x * t - v.x;
    double deltaY = this.y * t - v.y;
    double deltaZ = this.z * t - v.z;
    return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
  }

  /**
   * Compute the square of the normal distance to a vector described by a vector taken from the
   * origin. Monotonically increasing for arc distances up to PI/2.
   *
   * @param x is the x part of the vector to compute a distance to.
   * @param y is the y part of the vector to compute a distance to.
   * @param z is the z part of the vector to compute a distance to.
   * @return the square of the normal distance.
   */
  public double normalDistanceSquared(final double x, final double y, final double z) {
    double t = dotProduct(x, y, z);
    double deltaX = this.x * t - x;
    double deltaY = this.y * t - y;
    double deltaZ = this.z * t - z;
    return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ;
  }

  /**
   * Compute the normal (perpendicular) distance to a vector described by a vector taken from the
   * origin. Monotonically increasing for arc distances up to PI/2.
   *
   * @param v is the vector to compute a distance to.
   * @return the normal distance.
   */
  public double normalDistance(final Vector v) {
    return Math.sqrt(normalDistanceSquared(v));
  }

  /**
   * Compute the normal (perpendicular) distance to a vector described by a vector taken from the
   * origin. Monotonically increasing for arc distances up to PI/2.
   *
   * @param x is the x part of the vector to compute a distance to.
   * @param y is the y part of the vector to compute a distance to.
   * @param z is the z part of the vector to compute a distance to.
   * @return the normal distance.
   */
  public double normalDistance(final double x, final double y, final double z) {
    return Math.sqrt(normalDistanceSquared(x, y, z));
  }

  /**
   * Compute the magnitude of this vector.
   *
   * @return the magnitude.
   */
  public double magnitude() {
    return magnitude(x, y, z);
  }

  /**
   * Compute whether two vectors are numerically identical.
   *
   * @param otherX is the other vector X.
   * @param otherY is the other vector Y.
   * @param otherZ is the other vector Z.
   * @return true if they are numerically identical.
   */
  public boolean isNumericallyIdentical(
      final double otherX, final double otherY, final double otherZ) {
    final double deltaX = x - otherX;
    final double deltaY = y - otherY;
    final double deltaZ = z - otherZ;
    return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ < MINIMUM_RESOLUTION_SQUARED;
  }

  /**
   * Compute whether two vectors are numerically identical.
   *
   * @param other is the other vector.
   * @return true if they are numerically identical.
   */
  public boolean isNumericallyIdentical(final Vector other) {
    final double deltaX = x - other.x;
    final double deltaY = y - other.y;
    final double deltaZ = z - other.z;
    return deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ < MINIMUM_RESOLUTION_SQUARED;
  }

  /**
   * Compute whether two vectors are parallel.
   *
   * @param otherX is the other vector X.
   * @param otherY is the other vector Y.
   * @param otherZ is the other vector Z.
   * @return true if they are parallel.
   */
  public boolean isParallel(final double otherX, final double otherY, final double otherZ) {
    final double thisX = y * otherZ - z * otherY;
    final double thisY = z * otherX - x * otherZ;
    final double thisZ = x * otherY - y * otherX;
    return thisX * thisX + thisY * thisY + thisZ * thisZ < MINIMUM_RESOLUTION_SQUARED;
  }

  /**
   * Compute whether two vectors are numerically identical.
   *
   * @param other is the other vector.
   * @return true if they are parallel.
   */
  public boolean isParallel(final Vector other) {
    final double thisX = y * other.z - z * other.y;
    final double thisY = z * other.x - x * other.z;
    final double thisZ = x * other.y - y * other.x;
    return thisX * thisX + thisY * thisY + thisZ * thisZ < MINIMUM_RESOLUTION_SQUARED;
  }

  /**
   * Compute the desired magnitude of a unit vector projected to a given planet model.
   *
   * @param planetModel is the planet model.
   * @param x is the unit vector x value.
   * @param y is the unit vector y value.
   * @param z is the unit vector z value.
   * @return a magnitude value for that (x,y,z) that projects the vector onto the specified
   *     ellipsoid.
   */
  static double computeDesiredEllipsoidMagnitude(
      final PlanetModel planetModel, final double x, final double y, final double z) {
    return 1.0
        / Math.sqrt(
            x * x * planetModel.inverseXYScalingSquared
                + y * y * planetModel.inverseXYScalingSquared
                + z * z * planetModel.inverseZScalingSquared);
  }

  /**
   * Compute the desired magnitude of a unit vector projected to a given planet model. The unit
   * vector is specified only by a z value.
   *
   * @param planetModel is the planet model.
   * @param z is the unit vector z value.
   * @return a magnitude value for that z value that projects the vector onto the specified
   *     ellipsoid.
   */
  static double computeDesiredEllipsoidMagnitude(final PlanetModel planetModel, final double z) {
    return 1.0
        / Math.sqrt(
            (1.0 - z * z) * planetModel.inverseXYScalingSquared
                + z * z * planetModel.inverseZScalingSquared);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Vector)) {
      return false;
    }
    Vector other = (Vector) o;
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
    return "[X=" + x + ", Y=" + y + ", Z=" + z + "]";
  }
}
