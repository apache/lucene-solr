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
      return planetModel.surfaceDistance(pointShape1.getCenter(), pointShape2.getCenter()) * DistanceUtils.RADIANS_TO_DEGREES;
    }
    return distance(from, to.getX(), to.getY());
  }

  @Override
  public double distance(Point from, double toX, double toY) {
    GeoPoint fromGeoPoint;
    if (from instanceof Geo3dPointShape) {
      fromGeoPoint = (((Geo3dPointShape) from).shape).getCenter();
    } else {
      fromGeoPoint = new GeoPoint(planetModel,
          from.getY() * DistanceUtils.DEGREES_TO_RADIANS,
          from.getX() * DistanceUtils.DEGREES_TO_RADIANS);
    }
    GeoPoint toGeoPoint = new GeoPoint(planetModel,
        toY * DistanceUtils.DEGREES_TO_RADIANS,
        toX * DistanceUtils.DEGREES_TO_RADIANS);
    return planetModel.surfaceDistance(fromGeoPoint, toGeoPoint) * DistanceUtils.RADIANS_TO_DEGREES;
  }

  @Override
  public boolean within(Point from, double toX, double toY, double distance) {
    return (distance < distance(from, toX, toY));
  }

  @Override
  public Point pointOnBearing(Point from, double distDEG, double bearingDEG, SpatialContext ctx, Point reuse) {
    // Algorithm using Vincenty's formulae (https://en.wikipedia.org/wiki/Vincenty%27s_formulae)
    // which takes into account that planets may not be spherical.
    //Code adaptation from http://www.movable-type.co.uk/scripts/latlong-vincenty.html
    Geo3dPointShape geoFrom = (Geo3dPointShape) from;
    GeoPoint point = (GeoPoint) geoFrom.shape;
    double lat = point.getLatitude();
    double lon = point.getLongitude();
    double dist = DistanceUtils.DEGREES_TO_RADIANS * distDEG;
    double bearing = DistanceUtils.DEGREES_TO_RADIANS * bearingDEG;

    double sinα1 = Math.sin(bearing);
    double cosα1 = Math.cos(bearing);

    double tanU1 = (1 - planetModel.flattening) * Math.tan(lat);
    double cosU1 = 1 / Math.sqrt((1 + tanU1 * tanU1));
    double sinU1 = tanU1 * cosU1;

    double σ1 = Math.atan2(tanU1, cosα1);
    double sinα = cosU1 * sinα1;
    double cosSqα = 1 - sinα * sinα;
    double uSq = cosSqα * planetModel.squareRatio;// (planetModel.ab* planetModel.ab - planetModel.c*planetModel.c) / (planetModel.c*planetModel.c);
    double A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
    double B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));

    double cos2σM;
    double sinσ;
    double cosσ;
    double Δσ;

    double σ = dist / (planetModel.c * A);
    double σʹ;
    double iterations = 0;
    do {
      cos2σM = Math.cos(2 * σ1 + σ);
      sinσ = Math.sin(σ);
      cosσ = Math.cos(σ);
      Δσ = B * sinσ * (cos2σM + B / 4 * (cosσ * (-1 + 2 * cos2σM * cos2σM) -
          B / 6 * cos2σM * (-3 + 4 * sinσ * sinσ) * (-3 + 4 * cos2σM * cos2σM)));
      σʹ = σ;
      σ = dist / (planetModel.c * A) + Δσ;
    } while (Math.abs(σ - σʹ) > 1e-12 && ++iterations < 200);

    if (iterations >= 200) {
      throw new RuntimeException("Formula failed to converge");
    }

    double x = sinU1 * sinσ - cosU1 * cosσ * cosα1;
    double φ2 = Math.atan2(sinU1 * cosσ + cosU1 * sinσ * cosα1, (1 - planetModel.flattening) * Math.sqrt(sinα * sinα + x * x));
    double λ = Math.atan2(sinσ * sinα1, cosU1 * cosσ - sinU1 * sinσ * cosα1);
    double C = planetModel.flattening / 16 * cosSqα * (4 + planetModel.flattening * (4 - 3 * cosSqα));
    double L = λ - (1 - C) * planetModel.flattening * sinα *
        (σ + C * sinσ * (cos2σM + C * cosσ * (-1 + 2 * cos2σM * cos2σM)));
    double λ2 = (lon + L + 3 * Math.PI) % (2 * Math.PI) - Math.PI;  // normalise to -180..+180

    return ctx.getShapeFactory().pointXY(λ2 * DistanceUtils.RADIANS_TO_DEGREES,
        φ2 * DistanceUtils.RADIANS_TO_DEGREES);
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
