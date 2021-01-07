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
import org.apache.lucene.spatial3d.geom.GeoAreaFactory;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoBBox;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * A Spatial4j Shape wrapping a {@link GeoAreaShape} ("Geo3D") -- a 3D planar geometry based
 * Spatial4j Shape implementation. Geo3D implements shapes on the surface of a sphere or ellipsoid.
 *
 * @param <T> is the type of {@link GeoAreaShape}
 * @lucene.experimental
 */
public class Geo3dShape<T extends GeoAreaShape> implements Shape {

  protected final SpatialContext spatialcontext;

  protected T shape;
  protected volatile Rectangle boundingBox = null; // lazy initialized
  protected volatile Point center = null; // lazy initialized

  public Geo3dShape(final T shape, final SpatialContext spatialcontext) {
    this.spatialcontext = spatialcontext;
    this.shape = shape;
  }

  @Override
  public SpatialRelation relate(Shape other) {
    int relationship;
    if (other instanceof Geo3dShape<?>) {
      relationship = relate((Geo3dShape<?>) other);
    } else if (other instanceof Rectangle) {
      relationship = relate((Rectangle) other);
    } else if (other instanceof Point) {
      relationship = relate((Point) other);
    } else {
      throw new RuntimeException(
          "Unimplemented shape relationship determination: " + other.getClass());
    }

    switch (relationship) {
      case GeoArea.DISJOINT:
        return SpatialRelation.DISJOINT;
      case GeoArea.OVERLAPS:
        return (other instanceof Point ? SpatialRelation.CONTAINS : SpatialRelation.INTERSECTS);
      case GeoArea.CONTAINS:
        return (other instanceof Point ? SpatialRelation.CONTAINS : SpatialRelation.WITHIN);
      case GeoArea.WITHIN:
        return SpatialRelation.CONTAINS;
    }

    throw new RuntimeException("Undetermined shape relationship: " + relationship);
  }

  private int relate(Geo3dShape<?> s) {
    return shape.getRelationship(s.shape);
  }

  private int relate(Rectangle r) {
    // Construct the right kind of GeoArea first
    GeoArea geoArea =
        GeoAreaFactory.makeGeoArea(
            shape.getPlanetModel(),
            r.getMaxY() * DistanceUtils.DEGREES_TO_RADIANS,
            r.getMinY() * DistanceUtils.DEGREES_TO_RADIANS,
            r.getMinX() * DistanceUtils.DEGREES_TO_RADIANS,
            r.getMaxX() * DistanceUtils.DEGREES_TO_RADIANS);

    return geoArea.getRelationship(shape);
  }

  private int relate(Point p) {
    GeoPoint point =
        new GeoPoint(
            shape.getPlanetModel(),
            p.getY() * DistanceUtils.DEGREES_TO_RADIANS,
            p.getX() * DistanceUtils.DEGREES_TO_RADIANS);

    if (shape.isWithin(point)) {
      return GeoArea.WITHIN;
    }
    return GeoArea.DISJOINT;
  }

  @Override
  public Rectangle getBoundingBox() {
    Rectangle bbox = this.boundingBox; // volatile read once
    if (bbox == null) {
      LatLonBounds bounds = new LatLonBounds();
      shape.getBounds(bounds);
      GeoBBox geoBBox = GeoBBoxFactory.makeGeoBBox(shape.getPlanetModel(), bounds);
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
  public Point getCenter() {
    Point center = this.center; // volatile read once
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
  public String toString() {
    return "Geo3D:" + shape.toString();
  } // note: the shape usually prints its planet model

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Geo3dShape<?>)) return false;
    final Geo3dShape<?> other = (Geo3dShape<?>) o;
    return (other.spatialcontext.equals(spatialcontext) && other.shape.equals(shape));
  }

  @Override
  public int hashCode() {
    return spatialcontext.hashCode() + shape.hashCode();
  }
}
