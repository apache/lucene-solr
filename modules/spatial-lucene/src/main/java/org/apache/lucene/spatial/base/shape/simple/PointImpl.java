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

package org.apache.lucene.spatial.base.shape.simple;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.spatial.base.shape.SpatialRelation;
import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.shape.Rectangle;
import org.apache.lucene.spatial.base.shape.Point;
import org.apache.lucene.spatial.base.shape.Shape;


public class PointImpl implements Point {

  private final double x;
  private final double y;

  public PointImpl(double x, double y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public double getX() {
    return x;
  }

  @Override
  public double getY() {
    return y;
  }
  @Override
  public Rectangle getBoundingBox() {
    return new RectangleImpl(x, x, y, y);
  }

  @Override
  public PointImpl getCenter() {
    return this;
  }

  @Override
  public SpatialRelation relate(Shape other, SpatialContext ctx) {
    if (other instanceof Point)
      return this.equals(other) ? SpatialRelation.INTERSECTS : SpatialRelation.DISJOINT;
    return other.relate(this, ctx).transpose();
  }

  @Override
  public boolean hasArea() {
    return false;
  }

  @Override
  public String toString() {
    return "Pt(x="+x+",y="+y+")";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    PointImpl rhs = (PointImpl) obj;
    return new EqualsBuilder()
                  .append(x, rhs.x)
                  .append(y, rhs.y)
                  .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(5, 89).
      append(x).
      append(y).
      toHashCode();
  }
}
