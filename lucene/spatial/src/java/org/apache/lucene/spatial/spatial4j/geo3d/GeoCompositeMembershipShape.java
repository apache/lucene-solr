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

import java.util.ArrayList;
import java.util.List;

/**
 * GeoComposite is a set of GeoMembershipShape's, treated as a unit.
 *
 * @lucene.experimental
 */
public class GeoCompositeMembershipShape implements GeoMembershipShape {
  protected final List<GeoMembershipShape> shapes = new ArrayList<GeoMembershipShape>();

  public GeoCompositeMembershipShape() {
  }

  /**
   * Add a shape to the composite.
   */
  public void addShape(final GeoMembershipShape shape) {
    shapes.add(shape);
  }

  @Override
  public boolean isWithin(final Vector point) {
    //System.err.println("Checking whether point "+point+" is within Composite");
    for (GeoMembershipShape shape : shapes) {
      if (shape.isWithin(point)) {
        //System.err.println(" Point is within "+shape);
        return true;
      }
    }
    return false;
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

  /**
   * Compute longitude/latitude bounds for the shape.
   *
   * @param bounds is the optional input bounds object.  If this is null,
   *               a bounds object will be created.  Otherwise, the input object will be modified.
   * @return a Bounds object describing the shape's bounds.  If the bounds cannot
   * be computed, then return a Bounds object with noLongitudeBound,
   * noTopLatitudeBound, and noBottomLatitudeBound.
   */
  @Override
  public Bounds getBounds(Bounds bounds) {
    if (bounds == null)
      bounds = new Bounds();
    for (GeoMembershipShape shape : shapes) {
      bounds = shape.getBounds(bounds);
    }
    return bounds;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoCompositeMembershipShape))
      return false;
    GeoCompositeMembershipShape other = (GeoCompositeMembershipShape) o;
    if (other.shapes.size() != shapes.size())
      return false;

    for (int i = 0; i < shapes.size(); i++) {
      if (!other.shapes.get(i).equals(shapes.get(i)))
        return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return shapes.hashCode();//TODO cache
  }

  @Override
  public String toString() {
    return "GeoCompositeMembershipShape: {" + shapes + '}';
  }
}
  
