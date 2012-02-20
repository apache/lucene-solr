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

package org.apache.lucene.spatial.base.shape;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.spatial.base.context.SpatialContext;

import java.util.Collection;

/**
 * A collection of Shape objects.
 */
public class MultiShape implements Shape {
  private final Collection<Shape> geoms;
  private final Rectangle bbox;

  public MultiShape(Collection<Shape> geoms, SpatialContext ctx) {
    this.geoms = geoms;
    double minX = Double.MAX_VALUE;
    double minY = Double.MAX_VALUE;
    double maxX = Double.MIN_VALUE;
    double maxY = Double.MIN_VALUE;
    for (Shape geom : geoms) {
      Rectangle r = geom.getBoundingBox();
      minX = Math.min(minX,r.getMinX());
      minY = Math.min(minY,r.getMinY());
      maxX = Math.max(maxX,r.getMaxX());
      maxY = Math.max(maxY,r.getMaxY());
    }
    this.bbox = ctx.makeRect(minX, maxX, minY, maxY);
  }

  @Override
  public Rectangle getBoundingBox() {
    return bbox;
  }

  @Override
  public Point getCenter() {
    return bbox.getCenter();
  }

  @Override
  public boolean hasArea() {
    for (Shape geom : geoms) {
      if( geom.hasArea() ) {
        return true;
      }
    }
    return false;
  }

  @Override
  public SpatialRelation relate(Shape other, SpatialContext ctx) {
    boolean allOutside = true;
    boolean allContains = true;
    for (Shape geom : geoms) {
      SpatialRelation sect = geom.relate(other, ctx);
      if (sect != SpatialRelation.DISJOINT)
        allOutside = false;
      if (sect != SpatialRelation.CONTAINS)
        allContains = false;
      if (!allContains && !allOutside)
        return SpatialRelation.INTERSECTS;//short circuit
    }
    if (allOutside)
      return SpatialRelation.DISJOINT;
    if (allContains)
      return SpatialRelation.CONTAINS;
    return SpatialRelation.INTERSECTS;
  }


  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    MultiShape rhs = (MultiShape) obj;
    return new EqualsBuilder()
                  .append(geoms, rhs.geoms)
                  .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(83, 29).append(geoms.hashCode()).
      toHashCode();
  }
}
