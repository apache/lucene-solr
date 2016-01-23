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
 * Ellipse shape. From C++ gl.
 *
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class Ellipse implements Geometry2D {
  private Point2D center;

  /**
   * Half length of major axis
   */
  private double a;

  /**
   * Half length of minor axis
   */
  private double b;

  private double k1, k2, k3;

  /**
   * sin of rotation angle
   */
  private double s;

  /**
   * cos of rotation angle
   */
  private double c;

  public Ellipse() {
    center = new Point2D(0, 0);
  }

  private double SQR(double d) {
    return d * d;
  }

  /**
   * Constructor given bounding rectangle and a rotation.
   */
  public Ellipse(Point2D p1, Point2D p2, double angle) {
    center = new Point2D();

    // Set the center
    center.x((p1.x() + p2.x()) * 0.5f);
    center.y((p1.y() + p2.y()) * 0.5f);

    // Find sin and cos of the angle
    double angleRad = Math.toRadians(angle);
    c = Math.cos(angleRad);
    s = Math.sin(angleRad);

    // Find the half lengths of the semi-major and semi-minor axes
    double dx = Math.abs(p2.x() - p1.x()) * 0.5;
    double dy = Math.abs(p2.y() - p1.y()) * 0.5;
    if (dx >= dy) {
      a = dx;
      b = dy;
    } else {
      a = dy;
      b = dx;
    }

    // Find k1, k2, k3 - define when a point x,y is on the ellipse
    k1 = SQR(c / a) + SQR(s / b);
    k2 = 2 * s * c * ((1 / SQR(a)) - (1 / SQR(b)));
    k3 = SQR(s / a) + SQR(c / b);
  }

  /**
   * Determines if a line segment intersects the ellipse and if so finds the
   * point(s) of intersection.
   * 
   * @param seg
   *            Line segment to test for intersection
   * @param pt0
   *            OUT - intersection point (if it exists)
   * @param pt1
   *            OUT - second intersection point (if it exists)
   * 
   * @return Returns the number of intersection points (0, 1, or 2).
   */
  public int intersect(LineSegment seg, Point2D pt0, Point2D pt1) {
    if (pt0 == null)
      pt0 = new Point2D();
    if (pt1 == null)
      pt1 = new Point2D();

    // Solution is found by parameterizing the line segment and
    // substituting those values into the ellipse equation.
    // Results in a quadratic equation.
    double x1 = center.x();
    double y1 = center.y();
    double u1 = seg.A.x();
    double v1 = seg.A.y();
    double u2 = seg.B.x();
    double v2 = seg.B.y();
    double dx = u2 - u1;
    double dy = v2 - v1;
    double q0 = k1 * SQR(u1 - x1) + k2 * (u1 - x1) * (v1 - y1) + k3
        * SQR(v1 - y1) - 1;
    double q1 = (2 * k1 * dx * (u1 - x1)) + (k2 * dx * (v1 - y1))
        + (k2 * dy * (u1 - x1)) + (2 * k3 * dy * (v1 - y1));
    double q2 = (k1 * SQR(dx)) + (k2 * dx * dy) + (k3 * SQR(dy));

    // Compare q1^2 to 4*q0*q2 to see how quadratic solves
    double d = SQR(q1) - (4 * q0 * q2);
    if (d < 0) {
      // Roots are complex valued. Line containing the segment does
      // not intersect the ellipse
      return 0;
    }

    if (d == 0) {
      // One real-valued root - line is tangent to the ellipse
      double t = -q1 / (2 * q2);
      if (0 <= t && t <= 1) {
        // Intersection occurs along line segment
        pt0.x(u1 + t * dx);
        pt0.y(v1 + t * dy);
        return 1;
      } else
        return 0;
    } else {
      // Two distinct real-valued roots. Solve for the roots and see if
      // they fall along the line segment
      int n = 0;
      double q = Math.sqrt(d);
      double t = (-q1 - q) / (2 * q2);
      if (0 <= t && t <= 1) {
        // Intersection occurs along line segment
        pt0.x(u1 + t * dx);
        pt0.y(v1 + t * dy);
        n++;
      }

      // 2nd root
      t = (-q1 + q) / (2 * q2);
      if (0 <= t && t <= 1) {
        if (n == 0) {
          pt0.x(u1 + t * dx);
          pt0.y(v1 + t * dy);
          n++;
        } else {
          pt1.x(u1 + t * dx);
          pt1.y(v1 + t * dy);
          n++;
        }
      }
      return n;
    }
  }

  public IntersectCase intersect(Rectangle r) {
    // Test if all 4 corners of the rectangle are inside the ellipse
    Point2D ul = new Point2D(r.MinPt().x(), r.MaxPt().y());
    Point2D ur = new Point2D(r.MaxPt().x(), r.MaxPt().y());
    Point2D ll = new Point2D(r.MinPt().x(), r.MinPt().y());
    Point2D lr = new Point2D(r.MaxPt().x(), r.MinPt().y());
    if (contains(ul) && contains(ur) && contains(ll) && contains(lr))
      return IntersectCase.CONTAINS;

    // Test if any of the rectangle edges intersect
    Point2D pt0 = new Point2D(), pt1 = new Point2D();
    LineSegment bottom = new LineSegment(ll, lr);
    if (intersect(bottom, pt0, pt1) > 0)
      return IntersectCase.INTERSECTS;

    LineSegment top = new LineSegment(ul, ur);
    if (intersect(top, pt0, pt1) > 0)
      return IntersectCase.INTERSECTS;

    LineSegment left = new LineSegment(ll, ul);
    if (intersect(left, pt0, pt1) > 0)
      return IntersectCase.INTERSECTS;

    LineSegment right = new LineSegment(lr, ur);
    if (intersect(right, pt0, pt1) > 0)
      return IntersectCase.INTERSECTS;

    // Ellipse does not intersect any edge : since the case for the ellipse
    // containing the rectangle was considered above then if the center
    // is inside the ellipse is fully inside and if center is outside
    // the ellipse is fully outside
    return (r.contains(center)) ? IntersectCase.WITHIN
        : IntersectCase.OUTSIDE;
  }

  public double area() {
    throw new UnsupportedOperationException();
  }

  public Point2D centroid() {
    throw new UnsupportedOperationException();
  }

  public boolean contains(Point2D pt) {
    // Plug in equation for ellipse, If evaluates to <= 0 then the
    // point is in or on the ellipse.
    double dx = pt.x() - center.x();
    double dy = pt.y() - center.y();
    double eq=(((k1 * SQR(dx)) + (k2 * dx * dy) + (k3 * SQR(dy)) - 1));
    
    return eq<=0;
  }

  public void translate(Vector2D v) {
    throw new UnsupportedOperationException();
  }

}
