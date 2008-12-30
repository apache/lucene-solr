/**
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

package org.apache.lucene.spatial.geometry.shape;


/**
 * 2d line segment.
 */
public class LineSegment {
  public final Point2D A = new Point2D();
  public final Point2D B = new Point2D();

  public LineSegment() {
    A.set(0, 0);
    B.set(0, 0);
  }

  public LineSegment(Point2D p1, Point2D p2) {
    A.set(p1);
    B.set(p2);
  }

  /**
   * Finds the distance of a specified point from the line segment and the
   * closest point on the segement to the specified point.
   * 
   * @param P
   *            Test point.
   * @param closestPt
   *            (Return) Closest point on the segment to c.
   * 
   * @return Returns the distance from P to the closest point on the segment.
   */
  public double distance(Point2D P, Point2D /* out */closestPt) {
    if (closestPt == null)
      closestPt = new Point2D();

    // Construct vector v (AB) and w (AP)
    Vector2D v = new Vector2D(A, B);
    Vector2D w = new Vector2D(A, P);

    // Numerator of the component of w onto v. If <= 0 then A
    // is the closest point. By separating into the numerator
    // and denominator of the component we avoid a division unless
    // it is necessary.
    double n = w.dot(v);
    if (n <= 0.0f) {
      closestPt.set(A);
      return w.norm();
    }

    // Get the denominator of the component. If the component >= 1
    // (d <= n) then point B is the closest point
    double d = v.dot(v);
    if (d <= n) {
      closestPt.set(B);
      return new Vector2D(B, P).norm();
    }

    // Closest point is along the segment. The point is the projection of
    // w onto v.
    closestPt.set(v.mult(n / d));
    closestPt.add(A);
    return new Vector2D(closestPt, P).norm();
  }
}
