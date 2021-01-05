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

import org.apache.lucene.spatial3d.geom.GeoBBox;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPointShapeFactory;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * Specialization of a {@link Geo3dShape} which represents a {@link Rectangle}.
 *
 * @lucene.experimental
 */
public class Geo3dRectangleShape extends Geo3dShape<GeoBBox> implements Rectangle {

  private double minX;
  private double maxX;
  private double minY;
  private double maxY;

  public Geo3dRectangleShape(
      final GeoBBox shape,
      final SpatialContext spatialcontext,
      double minX,
      double maxX,
      double minY,
      double maxY) {
    super(shape, spatialcontext);
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
  }

  public Geo3dRectangleShape(final GeoBBox shape, final SpatialContext spatialcontext) {
    super(shape, spatialcontext);
    setBoundsFromshape();
  }

  /** Set the bounds from the wrapped GeoBBox. */
  private void setBoundsFromshape() {
    LatLonBounds bounds = new LatLonBounds();
    shape.getBounds(bounds);
    minX =
        bounds.checkNoLongitudeBound()
            ? -180.0
            : bounds.getLeftLongitude() * DistanceUtils.RADIANS_TO_DEGREES;
    minY =
        bounds.checkNoBottomLatitudeBound()
            ? -90.0
            : bounds.getMinLatitude() * DistanceUtils.RADIANS_TO_DEGREES;
    maxX =
        bounds.checkNoLongitudeBound()
            ? 180.0
            : bounds.getRightLongitude() * DistanceUtils.RADIANS_TO_DEGREES;
    maxY =
        bounds.checkNoTopLatitudeBound()
            ? 90.0
            : bounds.getMaxLatitude() * DistanceUtils.RADIANS_TO_DEGREES;
  }

  @Override
  public Point getCenter() {
    Point center = this.center; // volatile read once
    if (center == null) {
      GeoPoint point = shape.getCenter();
      center =
          new Geo3dPointShape(
              GeoPointShapeFactory.makeGeoPointShape(
                  shape.getPlanetModel(), point.getLatitude(), point.getLongitude()),
              spatialcontext);
      this.center = center;
    }
    return center;
  }

  @Override
  public void reset(double minX, double maxX, double minY, double maxY) {
    shape =
        GeoBBoxFactory.makeGeoBBox(
            shape.getPlanetModel(),
            maxY * DistanceUtils.DEGREES_TO_RADIANS,
            minY * DistanceUtils.DEGREES_TO_RADIANS,
            minX * DistanceUtils.DEGREES_TO_RADIANS,
            maxX * DistanceUtils.DEGREES_TO_RADIANS);
    center = null;
    boundingBox = null;
  }

  @Override
  public Rectangle getBoundingBox() {
    return this;
  }

  @Override
  public double getWidth() {
    double result = getMaxX() - getMinX();
    if (result < 0) {
      result += 360;
    }
    return result;
  }

  @Override
  public double getHeight() {
    return getMaxY() - getMinY();
  }

  @Override
  public double getMinX() {
    return minX;
  }

  @Override
  public double getMinY() {
    return minY;
  }

  @Override
  public double getMaxX() {
    return maxX;
  }

  @Override
  public double getMaxY() {
    return maxY;
  }

  @Override
  public boolean getCrossesDateLine() {
    return (getMaxX() > 0 && getMinX() < 0);
  }

  @Override
  public SpatialRelation relateYRange(double minY, double maxY) {
    Rectangle r = spatialcontext.getShapeFactory().rect(-180, 180, minY, maxY);
    return relate(r);
  }

  @Override
  public SpatialRelation relateXRange(double minX, double maxX) {
    Rectangle r = spatialcontext.getShapeFactory().rect(minX, maxX, -90, 90);
    return relate(r);
  }

  @Override
  public Shape getBuffered(double distance, SpatialContext spatialContext) {
    GeoBBox bBox = shape.expand(distance * DistanceUtils.DEGREES_TO_RADIANS);
    return new Geo3dRectangleShape(bBox, spatialContext);
  }
}
