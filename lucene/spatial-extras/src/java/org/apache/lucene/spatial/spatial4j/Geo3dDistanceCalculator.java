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

import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPointShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;

/**
 * Geo3d implementation of {@link DistanceCalculator}
 *
 * @lucene.experimental
 */
public class Geo3dDistanceCalculator implements DistanceCalculator {

  protected final PlanetModel planetModel;

  public Geo3dDistanceCalculator(PlanetModel planetModel) {
    this.planetModel = planetModel;
  }

  @Override
  public double distance(Point from, Point to) {
    if (from instanceof Geo3dPointShape && to instanceof Geo3dPointShape) {
      GeoPointShape pointShape1 = ((Geo3dPointShape) from).shape;
      GeoPointShape pointShape2 = ((Geo3dPointShape) to).shape;
      return planetModel.surfaceDistance(pointShape1.getCenter(), pointShape2.getCenter())
          * DistanceUtils.RADIANS_TO_DEGREES;
    }
    return distance(from, to.getX(), to.getY());
  }

  @Override
  public double distance(Point from, double toX, double toY) {
    GeoPoint fromGeoPoint;
    if (from instanceof Geo3dPointShape) {
      fromGeoPoint = (((Geo3dPointShape) from).shape).getCenter();
    } else {
      fromGeoPoint =
          new GeoPoint(
              planetModel,
              from.getY() * DistanceUtils.DEGREES_TO_RADIANS,
              from.getX() * DistanceUtils.DEGREES_TO_RADIANS);
    }
    GeoPoint toGeoPoint =
        new GeoPoint(
            planetModel,
            toY * DistanceUtils.DEGREES_TO_RADIANS,
            toX * DistanceUtils.DEGREES_TO_RADIANS);
    return planetModel.surfaceDistance(fromGeoPoint, toGeoPoint) * DistanceUtils.RADIANS_TO_DEGREES;
  }

  @Override
  public boolean within(Point from, double toX, double toY, double distance) {
    return (distance < distance(from, toX, toY));
  }

  @Override
  public Point pointOnBearing(
      Point from, double distDEG, double bearingDEG, SpatialContext ctx, Point reuse) {
    Geo3dPointShape geoFrom = (Geo3dPointShape) from;
    GeoPoint point = (GeoPoint) geoFrom.shape;
    double dist = DistanceUtils.DEGREES_TO_RADIANS * distDEG;
    double bearing = DistanceUtils.DEGREES_TO_RADIANS * bearingDEG;
    GeoPoint newPoint = planetModel.surfacePointOnBearing(point, dist, bearing);
    double newLat = newPoint.getLatitude() * DistanceUtils.RADIANS_TO_DEGREES;
    double newLon = newPoint.getLongitude() * DistanceUtils.RADIANS_TO_DEGREES;
    if (reuse != null) {
      reuse.reset(newLon, newLat);
      return reuse;
    } else {
      return ctx.getShapeFactory().pointXY(newLon, newLat);
    }
  }

  @Override
  public Rectangle calcBoxByDistFromPt(
      Point from, double distDEG, SpatialContext ctx, Rectangle reuse) {
    Circle circle = ctx.getShapeFactory().circle(from, distDEG);
    return circle.getBoundingBox();
  }

  @Override
  public double calcBoxByDistFromPt_yHorizAxisDEG(Point from, double distDEG, SpatialContext ctx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double area(Rectangle rect) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double area(Circle circle) {
    throw new UnsupportedOperationException();
  }
}
