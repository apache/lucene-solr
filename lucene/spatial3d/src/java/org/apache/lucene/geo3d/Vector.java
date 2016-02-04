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
package org.apache.lucene.geo3d;

/**
 * A 3d vector in space, not necessarily
 * going through the origin.
 *
 * @lucene.experimental
 */
public class Vector {
  /**
   * Values that are all considered to be essentially zero have a magnitude
   * less than this.
   */
  public static final double MINIMUM_RESOLUTION = 1.0e-12;
  /**
   * For squared quantities, the bound is squared too.
   */
  public static final double MINIMUM_RESOLUTION_SQUARED = MINIMUM_RESOLUTION * MINIMUM_RESOLUTION;
  /**
   * For cubed quantities, cube the bound.
   */
  public static final double MINIMUM_RESOLUTION_CUBED = MINIMUM_RESOLUTION_SQUARED * MINIMUM_RESOLUTION;

  /** The x value */
  public final double x;
  /** The y value */
  public final double y;
  /** The z value */
  public final double z;

  /**
   * Construct from (U.S.) x,y,z coordinates.
   *@param x is the x value.
   *@param y is the y value.
   *@param z is the z value.
   */
  public Vector(double x, double y, double z) {
    this.x = x;
    this.y = y;
    this.z = z;
  }

  /**
   * Construct a vector that is perpendicular to
   * two other (non-zero) vectors.  If the vectors are parallel,
   * IllegalArgumentException will be thrown.
   * Produces a normalized final vector.
   *
   * @param A is the first vector
   * @param B is the second
   */
  public Vector(final Vector A, final Vector B) {
    // x = u2v3 - u3v2
    // y = u3v1 - u1v3
    // z = u1v2 - u2v1
    final double thisX = A.y * B.z - A.z * B.y;
    final double thisY = A.z * B.x - A.x * B.z;
    final double thisZ = A.x * B.y - A.y * B.x;
    final double magnitude = magnitude(thisX, thisY, thisZ);
    if (Math.abs(magnitude) < MINIMUM_RESOLUTION) {
      throw new IllegalArgumentException("Degenerate/parallel vector constructed");
    }
    final double inverseMagnitude = 1.0 / magnitude;
    this.x = thisX * inverseMagnitude;
    this.y = thisY * inverseMagnitude;
    this.z = thisZ * inverseMagnitude;
  }

  /** Compute a magnitude of an x,y,z value.
   */
  public static double magnitude(final double x, final double y, final double z) {
    return Math.sqrt(x*x + y*y + z*z);
  }
  
  /**
   * Compute a normalized unit vector based on the current vector.
   *
   * @return the normalized vector, or null if the current vector has
   * a magnitude of zero.
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
   * Determine if this vector, taken from the origin,
   * describes a point within a set of planes.
   *
   * @param bounds     is the first part of the set of planes.
   * @param moreBounds is the second part of the set of planes.
   * @return true if the point is within the bounds.
   */
  public boolean isWithin(final Membership[] bounds, final Membership[] moreBounds) {
    // Return true if the point described is within all provided bounds
    //System.err.println("  checking if "+this+" is within bounds");
    for (Membership bound : bounds) {
      if (bound != null && !bound.isWithin(this)) {
        //System.err.println("    NOT within "+bound);
        return false;
      }
    }
    for (Membership bound : moreBounds) {
      if (bound != null && !bound.isWithin(this)) {
        //System.err.println("    NOT within "+bound);
        return false;
      }
    }
    //System.err.println("    is within");
    return true;
  }

  /**
   * Translate vector.
   */
  public Vector translate(final double xOffset, final double yOffset, final double zOffset) {
    return new Vector(x - xOffset, y - yOffset, z - zOffset);
  }

  /**
   * Rotate vector counter-clockwise in x-y by an angle.
   */
  public Vector rotateXY(final double angle) {
    return rotateXY(Math.sin(angle), Math.cos(angle));
  }

  /**
   * Rotate vector counter-clockwise in x-y by an angle, expressed as sin and cos.
   */
  public Vector rotateXY(final double sinAngle, final double cosAngle) {
    return new Vector(x * cosAngle - y * sinAngle, x * sinAngle + y * cosAngle, z);
  }

  /**
   * Rotate vector counter-clockwise in x-z by an angle.
   */
  public Vector rotateXZ(final double angle) {
    return rotateXZ(Math.sin(angle), Math.cos(angle));
  }

  /**
   * Rotate vector counter-clockwise in x-z by an angle, expressed as sin and cos.
   */
  public Vector rotateXZ(final double sinAngle, final double cosAngle) {
    return new Vector(x * cosAngle - z * sinAngle, y, x * sinAngle + z * cosAngle);
  }

  /**
   * Rotate vector counter-clockwise in z-y by an angle.
   */
  public Vector rotateZY(final double angle) {
    return rotateZY(Math.sin(angle), Math.cos(angle));
  }

  /**
   * Rotate vector counter-clockwise in z-y by an angle, expressed as sin and cos.
   */
  public Vector rotateZY(final double sinAngle, final double cosAngle) {
    return new Vector(x, z * sinAngle + y * cosAngle, z * cosAngle - y * sinAngle);
  }

  /**
   * Compute the square of a straight-line distance to a point described by the
   * vector taken from the origin.
   * Monotonically increasing for arc distances up to PI.
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
   * Compute the square of a straight-line distance to a point described by the
   * vector taken from the origin.
   * Monotonically increasing for arc distances up to PI.
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
   * Compute the straight-line distance to a point described by the
   * vector taken from the origin.
   * Monotonically increasing for arc distances up to PI.
   *
   * @param v is the vector to compute a distance to.
   * @return the linear distance.
   */
  public double linearDistance(final Vector v) {
    return Math.sqrt(linearDistanceSquared(v));
  }

  /**
   * Compute the straight-line distance to a point described by the
   * vector taken from the origin.
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
   * Compute the square of the normal distance to a vector described by a
   * vector taken from the origin.
   * Monotonically increasing for arc distances up to PI/2.
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
   * Compute the square of the normal distance to a vector described by a
   * vector taken from the origin.
   * Monotonically increasing for arc distances up to PI/2.
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
   * Compute the normal (perpendicular) distance to a vector described by a
   * vector taken from the origin.
   * Monotonically increasing for arc distances up to PI/2.
   *
   * @param v is the vector to compute a distance to.
   * @return the normal distance.
   */
  public double normalDistance(final Vector v) {
    return Math.sqrt(normalDistanceSquared(v));
  }

  /**
   * Compute the normal (perpendicular) distance to a vector described by a
   * vector taken from the origin.
   * Monotonically increasing for arc distances up to PI/2.
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
    return magnitude(x,y,z);
  }

  /** Compute the desired magnitude of a unit vector projected to a given
   * planet model.
   * @param planetModel is the planet model.
   * @param x is the unit vector x value.
   * @param y is the unit vector y value.
   * @param z is the unit vector z value.
   * @return a magnitude value for that (x,y,z) that projects the vector onto the specified ellipsoid.
   */
  protected static double computeDesiredEllipsoidMagnitude(final PlanetModel planetModel, final double x, final double y, final double z) {
    return 1.0 / Math.sqrt(x*x*planetModel.inverseAbSquared + y*y*planetModel.inverseAbSquared + z*z*planetModel.inverseCSquared);
  }

  /** Compute the desired magnitude of a unit vector projected to a given
   * planet model.  The unit vector is specified only by a z value.
   * @param planetModel is the planet model.
   * @param z is the unit vector z value.
   * @return a magnitude value for that z value that projects the vector onto the specified ellipsoid.
   */
  protected static double computeDesiredEllipsoidMagnitude(final PlanetModel planetModel, final double z) {
    return 1.0 / Math.sqrt((1.0-z*z)*planetModel.inverseAbSquared + z*z*planetModel.inverseCSquared);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Vector))
      return false;
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
