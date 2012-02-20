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

package org.apache.lucene.spatial.base.distance;

import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.shape.Point;
import org.apache.lucene.spatial.base.shape.Rectangle;

import static java.lang.Math.toRadians;

/**
 * A base class for a Distance Calculator that assumes a spherical earth model.
 * @author dsmiley
 */
public abstract class GeodesicSphereDistCalc extends AbstractDistanceCalculator {
  protected final double radius;

  public GeodesicSphereDistCalc(double radius) {
    this.radius = radius;
  }

  @Override
  public double distanceToDegrees(double distance) {
    return DistanceUtils.dist2Degrees(distance, radius);
  }

  @Override
  public double degreesToDistance(double degrees) {
    return DistanceUtils.radians2Dist(toRadians(degrees), radius);
  }

  @Override
  public Point pointOnBearing(Point from, double dist, double bearingDEG, SpatialContext ctx) {
    //TODO avoid unnecessary double[] intermediate object
    if (dist == 0)
      return from;
    double[] latLon = DistanceUtils.pointOnBearingRAD(
        toRadians(from.getY()), toRadians(from.getX()),
        DistanceUtils.dist2Radians(dist,ctx.getUnits().earthRadius()),
        toRadians(bearingDEG), null);
    return ctx.makePoint(Math.toDegrees(latLon[1]), Math.toDegrees(latLon[0]));
  }

  @Override
  public Rectangle calcBoxByDistFromPt(Point from, double distance, SpatialContext ctx) {
    assert radius == ctx.getUnits().earthRadius();
    if (distance == 0)
      return from.getBoundingBox();
    return DistanceUtils.calcBoxByDistFromPtDEG(from.getY(), from.getX(), distance, ctx);
  }

  @Override
  public double calcBoxByDistFromPtHorizAxis(Point from, double distance, SpatialContext ctx) {
    return DistanceUtils.calcBoxByDistFromPtHorizAxisDEG(from.getY(), from.getX(), distance, radius);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GeodesicSphereDistCalc that = (GeodesicSphereDistCalc) o;

    if (Double.compare(that.radius, radius) != 0) return false;

    return true;
  }

  @Override
  public int hashCode() {
    long temp = radius != +0.0d ? Double.doubleToLongBits(radius) : 0L;
    return (int) (temp ^ (temp >>> 32));
  }

  @Override
  public final double distance(Point from, double toX, double toY) {
    return distanceLatLonRAD(toRadians(from.getY()), toRadians(from.getX()), toRadians(toY), toRadians(toX)) * radius;
  }

  protected abstract double distanceLatLonRAD(double lat1, double lon1, double lat2, double lon2);

  public static class Haversine extends GeodesicSphereDistCalc {

    public Haversine(double radius) {
      super(radius);
    }

    @Override
    protected double distanceLatLonRAD(double lat1, double lon1, double lat2, double lon2) {
      return DistanceUtils.distHaversineRAD(lat1,lon1,lat2,lon2);
    }

  }

  public static class LawOfCosines extends GeodesicSphereDistCalc {

    public LawOfCosines(double radius) {
      super(radius);
    }

    @Override
    protected double distanceLatLonRAD(double lat1, double lon1, double lat2, double lon2) {
      return DistanceUtils.distLawOfCosinesRAD(lat1, lon1, lat2, lon2);
    }

  }

  public static class Vincenty extends GeodesicSphereDistCalc {
    public Vincenty(double radius) {
      super(radius);
    }

    @Override
    protected double distanceLatLonRAD(double lat1, double lon1, double lat2, double lon2) {
      return DistanceUtils.distVincentyRAD(lat1, lon1, lat2, lon2);
    }
  }
}
