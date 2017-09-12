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
import org.apache.lucene.spatial3d.geom.Vector;
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
      return planetModel.surfaceDistance(pointShape1.getCenter(), pointShape2.getCenter()) * DistanceUtils.RADIANS_TO_DEGREES;
    }
    throw new IllegalArgumentException("Invalid class type " + from.getClass().getName() + "and " + to.getClass().getName());
  }

  @Override
  public double distance(Point from, double toX, double toY) {
    if (from instanceof Geo3dPointShape) {
      GeoPointShape pointShape = ((Geo3dPointShape)from).shape;
      GeoPoint point = new GeoPoint(planetModel,
          toY * DistanceUtils.DEGREES_TO_RADIANS,
          toX * DistanceUtils.DEGREES_TO_RADIANS);
      return planetModel.surfaceDistance(pointShape.getCenter(),point) * DistanceUtils.RADIANS_TO_DEGREES;
    }
    throw new IllegalArgumentException("Invalid class type " + from.getClass().getName());
  }

  @Override
  public boolean within(Point from, double toX, double toY, double distance) {
    return (distance < distance(from, toX, toY));
  }

  @Override
  public Point pointOnBearing(Point from, double distDEG, double bearingDEG, SpatialContext ctx, Point reuse) {
    // direct geodetic problem solved using vectors
    // Implemented using algorithm in http://www.movable-type.co.uk/scripts/latlong-vectors.html
    // only works on the sphere.

    //N = {0,0,1} – vector representing north pole
    //d̂e = N×a – east vector at a
    //dn = a×de – north vector at a
    //d = dn·cosθ + de·sinθ – direction vector in dir’n of θ
    //b = a·cosδ + d·sinδ

    double dist = DistanceUtils.DEGREES_TO_RADIANS * distDEG;
    double bearing = DistanceUtils.DEGREES_TO_RADIANS * bearingDEG;
    Geo3dPointShape geoFrom = (Geo3dPointShape) from;
    GeoPoint point = (GeoPoint)geoFrom.shape;

    GeoPoint north = planetModel.NORTH_POLE;
    Vector east = new Vector(north, point);
    Vector northEast = new Vector(point, east);
    double xDir = northEast.x * Math.cos(bearing) + east.x * Math.sin(bearing);
    double yDir = northEast.y * Math.cos(bearing) + east.y * Math.sin(bearing);
    double zDir = northEast.z * Math.cos(bearing) + east.z * Math.sin(bearing);

    double pX = point.x * Math.cos(dist) + xDir * Math.sin(dist);
    double pY = point.y * Math.cos(dist) + yDir * Math.sin(dist);
    double pZ = point.z * Math.cos(dist) + zDir * Math.sin(dist);
    GeoPoint newPoint = planetModel.createSurfacePoint(pX ,pY, pZ);
    return ctx.getShapeFactory().pointXY(newPoint.getLongitude() * DistanceUtils.RADIANS_TO_DEGREES,
                                         newPoint.getLatitude()  * DistanceUtils.RADIANS_TO_DEGREES);
  }

  @Override
  public Rectangle calcBoxByDistFromPt(Point from, double distDEG, SpatialContext ctx, Rectangle reuse) {
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
