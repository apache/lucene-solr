package org.apache.lucene.geo3d;

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
 * Combination of a plane, and a sign value indicating what evaluation values are on the correct
 * side of the plane.
 *
 * @lucene.experimental
 */
public class SidedPlane extends Plane implements Membership {
  /** The sign value for evaluation of a point on the correct side of the plane */
  public final double sigNum;

  /**
   * Construct a SidedPlane identical to an existing one, but reversed.
   *
   * @param sidedPlane is the existing plane.
   */
  public SidedPlane(SidedPlane sidedPlane) {
    super(sidedPlane, sidedPlane.D);
    this.sigNum = -sidedPlane.sigNum;
  }

  /**
   * Construct a sided plane from a pair of vectors describing points, and including
   * origin, plus a point p which describes the side.
   *
   * @param p point to evaluate
   * @param A is the first in-plane point
   * @param B is the second in-plane point
   */
  public SidedPlane(Vector p, Vector A, Vector B) {
    super(A, B);
    sigNum = Math.signum(evaluate(p));
    if (sigNum == 0.0)
      throw new IllegalArgumentException("Cannot determine sidedness because check point is on plane.");
  }

  /**
   * Construct a sided plane from a point and a Z coordinate.
   *
   * @param p      point to evaluate.
   * @param planetModel is the planet model.
   * @param sinLat is the sin of the latitude of the plane.
   */
  public SidedPlane(Vector p, final PlanetModel planetModel, double sinLat) {
    super(planetModel, sinLat);
    sigNum = Math.signum(evaluate(p));
    if (sigNum == 0.0)
      throw new IllegalArgumentException("Cannot determine sidedness because check point is on plane.");
  }

  /**
   * Construct a sided vertical plane from a point and specified x and y coordinates.
   *
   * @param p point to evaluate.
   * @param x is the specified x.
   * @param y is the specified y.
   */
  public SidedPlane(Vector p, double x, double y) {
    super(x, y);
    sigNum = Math.signum(evaluate(p));
    if (sigNum == 0.0)
      throw new IllegalArgumentException("Cannot determine sidedness because check point is on plane.");
  }

  /**
   * Construct a sided plane with a normal vector and offset.
   *
   * @param p point to evaluate.
   * @param v is the normal vector.
   * @param D is the origin offset for the plan.
   */
  public SidedPlane(Vector p, Vector v, double D) {
    super(v, D);
    sigNum = Math.signum(evaluate(p));
    if (sigNum == 0.0)
      throw new IllegalArgumentException("Cannot determine sidedness because check point is on plane.");
  }

  /**
   * Construct a sided plane with a normal vector and offset.
   *
   * @param pX X coord of point to evaluate.
   * @param pY Y coord of point to evaluate.
   * @param pZ Z coord of point to evaluate.
   * @param v is the normal vector.
   * @param D is the origin offset for the plan.
   */
  public SidedPlane(double pX, double pY, double pZ, Vector v, double D) {
    super(v, D);
    sigNum = Math.signum(evaluate(pX,pY,pZ));
    if (sigNum == 0.0)
      throw new IllegalArgumentException("Cannot determine sidedness because check point is on plane.");
  }

  /** Construct a sided plane from two points and a third normal vector.
   */
  public static SidedPlane constructNormalizedPerpendicularSidedPlane(final Vector insidePoint,
    final Vector normalVector, final Vector point1, final Vector point2) {
    final Vector pointsVector = new Vector(point1.x - point2.x, point1.y - point2.y, point1.z - point2.z);
    final Vector newNormalVector = new Vector(normalVector, pointsVector);
    try {
      // To construct the plane, we now just need D, which is simply the negative of the evaluation of the circle normal vector at one of the points.
      return new SidedPlane(insidePoint, newNormalVector, -newNormalVector.dotProduct(point1));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
  
  /** Construct a sided plane from three points.
   */
  public static SidedPlane constructNormalizedThreePointSidedPlane(final Vector insidePoint,
    final Vector point1, final Vector point2, final Vector point3) {
    try {
      final Vector planeNormal = new Vector(
        new Vector(point1.x - point2.x, point1.y - point2.y, point1.z - point2.z),
        new Vector(point2.x - point3.x, point2.y - point3.y, point2.z - point3.z));
      return new SidedPlane(insidePoint, planeNormal, -planeNormal.dotProduct(point2));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  @Override
  public boolean isWithin(double x, double y, double z) {
    double evalResult = evaluate(x, y, z);
    if (Math.abs(evalResult) < MINIMUM_RESOLUTION)
      return true;
    double sigNum = Math.signum(evalResult);
    return sigNum == this.sigNum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SidedPlane)) return false;
    if (!super.equals(o)) return false;

    SidedPlane that = (SidedPlane) o;

    return Double.compare(that.sigNum, sigNum) == 0;

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(sigNum);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "[A=" + x + ", B=" + y + ", C=" + z + ", D=" + D + ", side=" + sigNum + "]";
  }
}
  
