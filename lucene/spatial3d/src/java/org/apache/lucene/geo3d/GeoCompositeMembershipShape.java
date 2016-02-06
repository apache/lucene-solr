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

import java.util.ArrayList;
import java.util.List;

/**
 * GeoComposite is a set of GeoMembershipShape's, treated as a unit.
 *
 * @lucene.experimental
 */
public class GeoCompositeMembershipShape implements GeoMembershipShape {
  /** The list of shapes. */
  protected final List<GeoMembershipShape> shapes = new ArrayList<GeoMembershipShape>();

  /** Constructor.
   */
  public GeoCompositeMembershipShape() {
  }

  /**
   * Add a shape to the composite.
   *@param shape is the shape to add.
   */
  public void addShape(final GeoMembershipShape shape) {
    shapes.add(shape);
  }

  @Override
  public boolean isWithin(final Vector point) {
    return isWithin(point.x, point.y, point.z);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    for (GeoMembershipShape shape : shapes) {
      if (shape.isWithin(x, y, z))
        return true;
    }
    return false;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    return shapes.get(0).getEdgePoints();
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    for (GeoMembershipShape shape : shapes) {
      if (shape.intersects(p, notablePoints, bounds))
        return true;
    }
    return false;
  }

  @Override
  public void getBounds(Bounds bounds) {
    for (GeoMembershipShape shape : shapes) {
      shape.getBounds(bounds);
    }
  }

  @Override
  public double computeOutsideDistance(final DistanceStyle distanceStyle, final GeoPoint point) {
    return computeOutsideDistance(distanceStyle, point.x, point.y, point.z);
  }

  @Override
  public double computeOutsideDistance(final DistanceStyle distanceStyle, final double x, final double y, final double z) {
    if (isWithin(x,y,z))
      return 0.0;
    double distance = Double.MAX_VALUE;
    for (GeoMembershipShape shape : shapes) {
      final double normalDistance = shape.computeOutsideDistance(distanceStyle, x, y, z);
      if (normalDistance < distance) {
        distance = normalDistance;
      }
    }
    return distance;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoCompositeMembershipShape))
      return false;
    GeoCompositeMembershipShape other = (GeoCompositeMembershipShape) o;

    return super.equals(o) && shapes.equals(other.shapes);
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 31 + shapes.hashCode();//TODO cache
  }

  @Override
  public String toString() {
    return "GeoCompositeMembershipShape: {" + shapes + '}';
  }
}
  
