package org.apache.lucene.spatial.spatial4j;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.spatial.spatial4j.geo3d.Bounds;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoArea;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoAreaFactory;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPoint;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoShape;

/**
 * A 3D planar geometry based Spatial4j Shape implementation.
 *
 * @lucene.experimental
 */
public class Geo3dShape implements Shape {

  public final SpatialContext ctx;
  public final GeoShape shape;

  private Rectangle boundingBox = null;

  public final static double RADIANS_PER_DEGREE = Math.PI / 180.0;
  public final static double DEGREES_PER_RADIAN = 1.0 / RADIANS_PER_DEGREE;

  public Geo3dShape(GeoShape shape, SpatialContext ctx) {
    if (!ctx.isGeo()) {
      throw new IllegalArgumentException("SpatialContext.isGeo() must be true");
    }
    this.ctx = ctx;
    this.shape = shape;
  }

  @Override
  public SpatialRelation relate(Shape other) {
    if (other instanceof Rectangle)
      return relate((Rectangle)other);
    else if (other instanceof Point)
      return relate((Point)other);
    else
      throw new RuntimeException("Unimplemented shape relationship determination: " + other.getClass());
  }

  protected SpatialRelation relate(Rectangle r) {
    // Construct the right kind of GeoArea first
    GeoArea geoArea = GeoAreaFactory.makeGeoArea(r.getMaxY() * RADIANS_PER_DEGREE,
        r.getMinY() * RADIANS_PER_DEGREE,
        r.getMinX() * RADIANS_PER_DEGREE,
        r.getMaxX() * RADIANS_PER_DEGREE);
    int relationship = geoArea.getRelationship(shape);
    if (relationship == GeoArea.WITHIN)
      return SpatialRelation.WITHIN;
    else if (relationship == GeoArea.CONTAINS)
      return SpatialRelation.CONTAINS;
    else if (relationship == GeoArea.OVERLAPS)
      return SpatialRelation.INTERSECTS;
    else if (relationship == GeoArea.DISJOINT)
      return SpatialRelation.DISJOINT;
    else
      throw new RuntimeException("Unknown relationship returned: "+relationship);
  }

  protected SpatialRelation relate(Point p) {
    // Create a GeoPoint
    GeoPoint point = new GeoPoint(p.getY()*RADIANS_PER_DEGREE, p.getX()*RADIANS_PER_DEGREE);
    if (shape.isWithin(point)) {
      // Point within shape
      return SpatialRelation.CONTAINS;
    }
    return SpatialRelation.DISJOINT;
  }

  protected final double ROUNDOFF_ADJUSTMENT = 0.01;
  
  @Override
  public Rectangle getBoundingBox() {
    if (boundingBox == null) {
      Bounds bounds = shape.getBounds(null);
      double leftLon;
      double rightLon;
      if (bounds.checkNoLongitudeBound()) {
        leftLon = -180.0;
        rightLon = 180.0;
      } else {
        leftLon = bounds.getLeftLongitude().doubleValue() * DEGREES_PER_RADIAN;
        rightLon = bounds.getRightLongitude().doubleValue() * DEGREES_PER_RADIAN;
      }
      double minLat;
      if (bounds.checkNoBottomLatitudeBound()) {
        minLat = -90.0;
      } else {
        minLat = bounds.getMinLatitude().doubleValue() * DEGREES_PER_RADIAN;
      }
      double maxLat;
      if (bounds.checkNoTopLatitudeBound()) {
        maxLat = 90.0;
      } else {
        maxLat = bounds.getMaxLatitude().doubleValue() * DEGREES_PER_RADIAN;
      }
      boundingBox = new RectangleImpl(leftLon, rightLon, minLat, maxLat, ctx).getBuffered(ROUNDOFF_ADJUSTMENT, ctx);
    }
    return boundingBox;
  }

  @Override
  public boolean hasArea() {
    return true;
  }

  @Override
  public double getArea(SpatialContext ctx) {
    throw new RuntimeException("Unimplemented");
  }

  @Override
  public Point getCenter() {
    throw new RuntimeException("Unimplemented");
  }

  @Override
  public Shape getBuffered(double distance, SpatialContext ctx) {
    return this;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public String toString() {
    return "Geo3dShape{" + shape + '}';
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Geo3dShape))
      return false;
    Geo3dShape tr = (Geo3dShape)other;
    return tr.shape.equals(shape);
  }

  @Override
  public int hashCode() {
    return shape.hashCode();
  }
}
