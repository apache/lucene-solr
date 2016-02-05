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
package org.apache.lucene.spatial.spatial4j;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.geo3d.LatLonBounds;
import org.apache.lucene.geo3d.GeoArea;
import org.apache.lucene.geo3d.GeoAreaFactory;
import org.apache.lucene.geo3d.GeoPoint;
import org.apache.lucene.geo3d.GeoShape;
import org.apache.lucene.geo3d.PlanetModel;

/**
 * A Spatial4j Shape wrapping a {@link GeoShape} ("Geo3D") -- a 3D planar geometry based Spatial4j Shape implementation.
 * Geo3D implements shapes on the surface of a sphere or ellipsoid.
 *
 * @lucene.experimental
 */
public class Geo3dShape implements Shape {
  /** The required size of this adjustment depends on the actual planetary model chosen.
   * This value is big enough to account for WGS84. */
  protected static final double ROUNDOFF_ADJUSTMENT = 0.05;

  public final SpatialContext ctx;
  public final GeoShape shape;
  public final PlanetModel planetModel;

  private volatile Rectangle boundingBox = null; // lazy initialized

  public Geo3dShape(final GeoShape shape, final SpatialContext ctx) {
    this(PlanetModel.SPHERE, shape, ctx);
  }
  
  public Geo3dShape(final PlanetModel planetModel, final GeoShape shape, final SpatialContext ctx) {
    if (!ctx.isGeo()) {
      throw new IllegalArgumentException("SpatialContext.isGeo() must be true");
    }
    this.ctx = ctx;
    this.planetModel = planetModel;
    this.shape = shape;
  }

  @Override
  public SpatialContext getContext() {
    return ctx;
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
    GeoArea geoArea = GeoAreaFactory.makeGeoArea(planetModel,
        r.getMaxY() * DistanceUtils.DEGREES_TO_RADIANS,
        r.getMinY() * DistanceUtils.DEGREES_TO_RADIANS,
        r.getMinX() * DistanceUtils.DEGREES_TO_RADIANS,
        r.getMaxX() * DistanceUtils.DEGREES_TO_RADIANS);
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
    GeoPoint point = new GeoPoint(planetModel, p.getY()* DistanceUtils.DEGREES_TO_RADIANS, p.getX()* DistanceUtils.DEGREES_TO_RADIANS);
    if (shape.isWithin(point)) {
      // Point within shape
      return SpatialRelation.CONTAINS;
    }
    return SpatialRelation.DISJOINT;
  }


  
  @Override
  public Rectangle getBoundingBox() {
    Rectangle bbox = this.boundingBox;//volatile read once
    if (bbox == null) {
      LatLonBounds bounds = new LatLonBounds();
      shape.getBounds(bounds);
      double leftLon;
      double rightLon;
      if (bounds.checkNoLongitudeBound()) {
        leftLon = -180.0;
        rightLon = 180.0;
      } else {
        leftLon = bounds.getLeftLongitude().doubleValue() * DistanceUtils.RADIANS_TO_DEGREES;
        rightLon = bounds.getRightLongitude().doubleValue() * DistanceUtils.RADIANS_TO_DEGREES;
      }
      double minLat;
      if (bounds.checkNoBottomLatitudeBound()) {
        minLat = -90.0;
      } else {
        minLat = bounds.getMinLatitude().doubleValue() * DistanceUtils.RADIANS_TO_DEGREES;
      }
      double maxLat;
      if (bounds.checkNoTopLatitudeBound()) {
        maxLat = 90.0;
      } else {
        maxLat = bounds.getMaxLatitude().doubleValue() * DistanceUtils.RADIANS_TO_DEGREES;
      }
      bbox = new RectangleImpl(leftLon, rightLon, minLat, maxLat, ctx).getBuffered(ROUNDOFF_ADJUSTMENT, ctx);
      this.boundingBox = bbox;
    }
    return bbox;
  }

  @Override
  public boolean hasArea() {
    return true;
  }

  @Override
  public double getArea(SpatialContext ctx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Point getCenter() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Shape getBuffered(double distance, SpatialContext ctx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public String toString() {
    return "Geo3dShape{planetmodel=" + planetModel + ", shape=" + shape + '}';
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Geo3dShape))
      return false;
    Geo3dShape tr = (Geo3dShape)other;
    return tr.ctx.equals(ctx) && tr.planetModel.equals(planetModel) && tr.shape.equals(shape);
  }

  @Override
  public int hashCode() {
    return planetModel.hashCode() + shape.hashCode();
  }
}
