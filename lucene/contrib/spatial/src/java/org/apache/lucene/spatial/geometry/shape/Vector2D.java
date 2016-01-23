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
 * 2D vector
 *
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class Vector2D {
  private double x;
  private double y;

  /**
   * Create a vector from the origin of the coordinate system to the given
   * point
   * 
   * @param x
   * @param y
   */
  public Vector2D(double x, double y) {
    this.x = x;
    this.y = y;
  }

  /**
   * Create a vector from the origin of the coordinate system to the given
   * point
   */
  public Vector2D(Point2D p) {
    this(p.getX(), p.getY());
  }

  /**
   * Create a vector from one point to another
   * 
   * @param from
   * @param to
   */
  public Vector2D(Point2D from, Point2D to) {
    this(to.getX() - from.getX(), to.getY() - from.getY());
  }

  public Vector2D() {
    this.x = 0;
    this.y = 0;
  }

  public Vector2D(Vector2D other) {
    this.x = other.x;
    this.y = other.y;
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  public void setX(double x) {
    this.x = x;
  }

  public void setY(double y) {
    this.y = y;
  }

  public void set(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public boolean equals(Vector2D other) {
    return other != null && x == other.x && y == other.y;
  }

  public double dot(Vector2D in) {
    return ((x) * in.x) + (y * in.y);
  }

  /**
   * Vector length (magnitude) squared
   */
  public double normSqr() {
    // Cast to F to prevent overflows
    return (x * x) + (y * y);
  }

  public double norm() {
    return Math.sqrt(normSqr());
  }

  public Vector2D mult(double d) {
    return new Vector2D(x*d, y*d);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp;
    temp = Double.doubleToLongBits(x);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(y);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Vector2D other = (Vector2D) obj;
    if (Double.doubleToLongBits(x) != Double.doubleToLongBits(other.x))
      return false;
    if (Double.doubleToLongBits(y) != Double.doubleToLongBits(other.y))
      return false;
    return true;
  }
  
}
