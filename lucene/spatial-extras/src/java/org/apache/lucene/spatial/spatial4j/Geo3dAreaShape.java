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

import org.apache.lucene.spatial3d.geom.GeoArea;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoBBox;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * A Spatial4j Shape wrapping a {@link GeoAreaShape} ("Geo3D") -- a 3D planar geometry
 * based Spatial4j Shape implementation.
 * Geo3D implements shapes on the surface of a sphere or ellipsoid.
 *
 * @param <T> is the type of {@link GeoAreaShape}
 *
 * @lucene.experimental
 */

public class Geo3dAreaShape<T extends GeoAreaShape> implements Shape {

  protected final SpatialContext spatialcontext;

  protected T shape;
  protected volatile Rectangle boundingBox = null; // lazy initialized
  protected volatile Point center = null; // lazy initialized

  public Geo3dAreaShape(final T shape, final SpatialContext spatialcontext){
    this.spatialcontext = spatialcontext;
    this.shape = shape;
  }

  @Override
  public SpatialRelation relate(Shape other) {
    if (other instanceof Geo3dAreaShape<?>){
      int relationship = shape.getRelationship(((Geo3dAreaShape<?>)other).shape);
      switch(relationship){
        case GeoArea.DISJOINT: return SpatialRelation.DISJOINT;
        case GeoArea.OVERLAPS: return SpatialRelation.INTERSECTS;
        case GeoArea.CONTAINS: return SpatialRelation.WITHIN;
        case GeoArea.WITHIN: return SpatialRelation.CONTAINS;

      }
    }
    throw new RuntimeException("Unimplemented shape relationship determination: " + other.getClass());
  }

  @Override
  public Rectangle getBoundingBox() {
    Rectangle bbox = this.boundingBox;//volatile read once
    if (bbox == null) {
      LatLonBounds bounds = new LatLonBounds();
      shape.getBounds(bounds);
      double leftLon  = bounds.checkNoLongitudeBound() ? -Math.PI : bounds.getLeftLongitude();
      double rightLon = bounds.checkNoLongitudeBound() ? Math.PI : bounds.getRightLongitude();
      double minLat = bounds.checkNoBottomLatitudeBound() ?  -Math.PI * 0.5 : bounds.getMinLatitude();
      double maxLat = bounds.checkNoTopLatitudeBound() ?  Math.PI * 0.5 : bounds.getMaxLatitude();
      GeoBBox geoBBox = GeoBBoxFactory.makeGeoBBox(shape.getPlanetModel(),maxLat, minLat, leftLon, rightLon);
      bbox = new Geo3dRectangleShape(geoBBox, spatialcontext);
      this.boundingBox = bbox;
    }
    return bbox;
  }

  @Override
  public boolean hasArea() {
    return true;
  }

  @Override
  public double getArea(SpatialContext spatialContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Point getCenter()
  {
    Point center = this.center;//volatile read once
    if (center == null) {
      center = getBoundingBox().getCenter();
      this.center = center;
    }
    return center;
  }

  @Override
  public Shape getBuffered(double distance, SpatialContext spatialContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public SpatialContext getContext() {
    return spatialcontext;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Geo3dAreaShape<?>))
      return false;
    final Geo3dAreaShape<?> other = (Geo3dAreaShape<?>) o;
    return (other.shape.equals(shape));
  }

  @Override
  public int hashCode() {
    return shape.hashCode();
  }
}
