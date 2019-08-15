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

import java.util.ArrayList;
import java.util.List;

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import org.apache.lucene.spatial.prefix.tree.S2ShapeFactory;
import org.apache.lucene.spatial3d.geom.GeoBBox;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoCircle;
import org.apache.lucene.spatial3d.geom.GeoCircleFactory;
import org.apache.lucene.spatial3d.geom.GeoCompositeAreaShape;
import org.apache.lucene.spatial3d.geom.GeoPath;
import org.apache.lucene.spatial3d.geom.GeoPathFactory;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPointShape;
import org.apache.lucene.spatial3d.geom.GeoPointShapeFactory;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.GeoS2ShapeFactory;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

/**
 * Geo3d implementation of {@link S2ShapeFactory}
 *
 * @lucene.experimental
 */
public class Geo3dShapeFactory implements S2ShapeFactory {

  private final boolean normWrapLongitude;
  private SpatialContext context;
  private PlanetModel planetModel;

  /**
   * Default accuracy for circles when not using the unit sphere.
   * It is equivalent to ~10m on the surface of the earth.
   */
  private static final double DEFAULT_CIRCLE_ACCURACY = 1e-4;
  private double circleAccuracy = DEFAULT_CIRCLE_ACCURACY;

  public Geo3dShapeFactory(SpatialContext context, SpatialContextFactory factory) {
    this.context = context;
    this.planetModel = ((Geo3dSpatialContextFactory) factory).planetModel;
    this.normWrapLongitude = context.isGeo() && factory.normWrapLongitude;
  }

  @Override
  public SpatialContext getSpatialContext() {
    return context;
  }

  /**
   * Set the accuracy for circles in decimal degrees. Note that accuracy has no effect
   * when the planet model is a sphere. In that case, circles are always fully precise.
   *
   * @param circleAccuracy the provided accuracy in decimal degrees.
   */
  public void setCircleAccuracy(double circleAccuracy) {
    this.circleAccuracy = circleAccuracy;
  }

  @Override
  public boolean isNormWrapLongitude() {
    return normWrapLongitude;
  }

  @Override
  public double normX(double x) {
    if (this.normWrapLongitude) {
      x = DistanceUtils.normLonDEG(x);
    }
    return x;
  }

  @Override
  public double normY(double y) {
    return y;
  }

  @Override
  public double normZ(double z) {
    return z;
  }

  @Override
  public double normDist(double distance) {
    return distance;
  }

  @Override
  public void verifyX(double x) {
    Rectangle bounds = this.context.getWorldBounds();
    if (x < bounds.getMinX() || x > bounds.getMaxX()) {
      throw new InvalidShapeException("Bad X value " + x + " is not in boundary " + bounds);
    }
  }

  @Override
  public void verifyY(double y) {
    Rectangle bounds = this.context.getWorldBounds();
    if (y < bounds.getMinY() || y > bounds.getMaxY()) {
      throw new InvalidShapeException("Bad Y value " + y + " is not in boundary " + bounds);
    }
  }

  @Override
  public void verifyZ(double v) {
  }

  @Override
  public Point pointXY(double x, double y) {
    GeoPointShape point = GeoPointShapeFactory.makeGeoPointShape(planetModel,
        y * DistanceUtils.DEGREES_TO_RADIANS,
        x * DistanceUtils.DEGREES_TO_RADIANS);
    return new Geo3dPointShape(point, context);
  }

  @Override
  public Point pointXYZ(double x, double y, double z) {
    GeoPoint point = new GeoPoint(x, y, z);
    GeoPointShape pointShape = GeoPointShapeFactory.makeGeoPointShape(planetModel,
        point.getLatitude(),
        point.getLongitude());
    return new Geo3dPointShape(pointShape, context);
    //throw new UnsupportedOperationException();
  }

  @Override
  public Rectangle rect(Point point, Point point1) {
    return rect(point.getX(), point1.getX(), point.getY(), point1.getY());
  }

  @Override
  public Rectangle rect(double minX, double maxX, double minY, double maxY) {
    GeoBBox bBox = GeoBBoxFactory.makeGeoBBox(planetModel,
        maxY * DistanceUtils.DEGREES_TO_RADIANS,
        minY * DistanceUtils.DEGREES_TO_RADIANS,
        minX * DistanceUtils.DEGREES_TO_RADIANS,
        maxX * DistanceUtils.DEGREES_TO_RADIANS);
    return new Geo3dRectangleShape(bBox, context, minX, maxX, minY, maxY);
  }

  @Override
  public Circle circle(double x, double y, double distance) {
    GeoCircle circle;
    if (planetModel.isSphere()) {
      circle = GeoCircleFactory.makeGeoCircle(planetModel,
          y * DistanceUtils.DEGREES_TO_RADIANS,
          x * DistanceUtils.DEGREES_TO_RADIANS,
          distance * DistanceUtils.DEGREES_TO_RADIANS);
    }
    else {
      //accuracy is defined as a linear distance in this class. At tiny distances, linear distance
      //can be approximated to surface distance in radians.
      circle = GeoCircleFactory.makeExactGeoCircle(planetModel,
          y * DistanceUtils.DEGREES_TO_RADIANS,
          x * DistanceUtils.DEGREES_TO_RADIANS,
          distance * DistanceUtils.DEGREES_TO_RADIANS,
          circleAccuracy * DistanceUtils.DEGREES_TO_RADIANS);

    }
    return new Geo3dCircleShape(circle, context);
  }

  @Override
  public Circle circle(Point point, double distance) {
    return circle(point.getX(), point.getY(), distance);
  }

  @Override
  @Deprecated // use a builder
  public Shape lineString(List<Point> list, double distance) {
    LineStringBuilder builder = lineString();
    for (Point point : list) {
      builder.pointXY(point.getX(), point.getY());
    }
    builder.buffer(distance);
    return builder.build();
  }

  @Override
  @Deprecated // use a builder
  public <S extends Shape> ShapeCollection<S> multiShape(List<S> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public LineStringBuilder lineString() {
    return new Geo3dLineStringBuilder();
  }

  @Override
  public PolygonBuilder polygon() {
    return new Geo3dPolygonBuilder();
  }

  @Override
  public <T extends Shape> MultiShapeBuilder<T> multiShape(Class<T> aClass) {
    return new Geo3dMultiShapeBuilder<>();
  }

  @Override
  public MultiPointBuilder multiPoint() {
    return new Geo3dMultiPointBuilder();
  }

  @Override
  public MultiLineStringBuilder multiLineString() {
    return new Geo3dMultiLineBuilder();
  }

  @Override
  public MultiPolygonBuilder multiPolygon() {
    return new Geo3dMultiPolygonBuilder();
  }

  @Override
  public Shape getS2CellShape(S2CellId cellId) {
    S2Cell cell = new S2Cell(cellId);
    GeoPoint point1 = getGeoPoint(cell.getVertexRaw(0));
    GeoPoint point2 = getGeoPoint(cell.getVertexRaw(1));
    GeoPoint point3 = getGeoPoint(cell.getVertexRaw(2));
    GeoPoint point4 = getGeoPoint(cell.getVertexRaw(3));
    return new Geo3dShape<>(GeoS2ShapeFactory.makeGeoS2Shape(planetModel, point1, point2, point3, point4), context);
  }

  private GeoPoint getGeoPoint(S2Point point) {
    return planetModel.createSurfacePoint(point.get(0), point.get(1), point.get(2));
  }

  /**
   * Geo3d implementation of {@link org.locationtech.spatial4j.shape.ShapeFactory.PointsBuilder} interface to
   * generate {@link GeoPoint}.
   *
   * @param <T> is normally this object
   */
  private class Geo3dPointBuilder<T> implements PointsBuilder<T> {

    List<GeoPoint> points = new ArrayList<>();

    @SuppressWarnings("unchecked")
    @Override
    public T pointXY(double x, double y) {
      GeoPoint point = new GeoPoint(planetModel, y * DistanceUtils.DEGREES_TO_RADIANS, x * DistanceUtils.DEGREES_TO_RADIANS);
      points.add(point);
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T pointXYZ(double x, double y, double z) {
      GeoPoint point = new GeoPoint(x, y, z);
      if (!points.contains(point)) {
        points.add(point);
      }
      return (T) this;
    }
  }

  /**
   * Geo3d implementation of {@link org.locationtech.spatial4j.shape.ShapeFactory.LineStringBuilder} to generate
   * line strings.
   */
  private class Geo3dLineStringBuilder extends Geo3dPointBuilder<LineStringBuilder> implements LineStringBuilder {

    double distance = 0;

    @Override
    public LineStringBuilder buffer(double distance) {
      this.distance = distance;
      return this;
    }

    @Override
    public Shape build() {
      GeoPath path = GeoPathFactory.makeGeoPath(planetModel, distance, points.toArray(new GeoPoint[points.size()]));
      return new Geo3dShape<>(path, context);
    }
  }

  /**
   * Geo3d implementation of {@link org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder} to generate
   * polygons.
   */
  private class Geo3dPolygonBuilder extends Geo3dPointBuilder<PolygonBuilder> implements PolygonBuilder {

    List<GeoPolygonFactory.PolygonDescription> polyHoles = new ArrayList<>();

    @Override
    public HoleBuilder hole() {
      return new Geo3dHoleBuilder();
    }

    class Geo3dHoleBuilder extends Geo3dPointBuilder<PolygonBuilder.HoleBuilder> implements PolygonBuilder.HoleBuilder {
      @Override
      public PolygonBuilder endHole() {
        polyHoles.add(new GeoPolygonFactory.PolygonDescription(points));
        return Geo3dPolygonBuilder.this;
      }
    }

    @Override
    public Shape build() {
      GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points, polyHoles);
      GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, description);
      if (polygon == null) {
        throw new InvalidShapeException("Invalid polygon, all points are coplanar");
      }
      return new Geo3dShape<>(polygon, context);
    }

    @Override
    public Shape buildOrRect() {
      return build();
    }
  }

  private class Geo3dMultiPointBuilder extends Geo3dPointBuilder<MultiPointBuilder> implements MultiPointBuilder {

    @Override
    public Shape build() {
      GeoCompositeAreaShape areaShape = new GeoCompositeAreaShape(planetModel);
      for (GeoPoint point : points) {
        GeoPointShape pointShape = GeoPointShapeFactory.makeGeoPointShape(planetModel, point.getLatitude(), point.getLongitude());
        areaShape.addShape(pointShape);
      }
      return new Geo3dShape<>(areaShape, context);
    }
  }

  /**
   * Geo3d implementation of {@link org.locationtech.spatial4j.shape.ShapeFactory.MultiLineStringBuilder} to generate
   * multi-lines
   */
  private class Geo3dMultiLineBuilder implements MultiLineStringBuilder {

    List<LineStringBuilder> builders = new ArrayList<>();

    @Override
    public LineStringBuilder lineString() {
      return new Geo3dLineStringBuilder();
    }

    @Override
    public MultiLineStringBuilder add(LineStringBuilder lineStringBuilder) {
      builders.add(lineStringBuilder);
      return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Shape build() {
      GeoCompositeAreaShape areaShape = new GeoCompositeAreaShape(planetModel);
      for (LineStringBuilder builder : builders) {
        Geo3dShape<GeoPolygon> shape = (Geo3dShape<GeoPolygon>) builder.build();
        areaShape.addShape(shape.shape);
      }
      return new Geo3dShape<>(areaShape, context);
    }
  }

  /**
   * Geo3d implementation of {@link org.locationtech.spatial4j.shape.ShapeFactory.MultiPolygonBuilder} to generate
   * multi-polygons. We have chosen to use a composite shape but
   * it might be possible to use GeoComplexPolygon.
   */
  private class Geo3dMultiPolygonBuilder implements MultiPolygonBuilder {

    List<PolygonBuilder> builders = new ArrayList<>();

    @Override
    public PolygonBuilder polygon() {
      return new Geo3dPolygonBuilder();
    }

    @Override
    public MultiPolygonBuilder add(PolygonBuilder polygonBuilder) {
      builders.add(polygonBuilder);
      return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Shape build() {
      GeoCompositeAreaShape areaShape = new GeoCompositeAreaShape(planetModel);
      for (PolygonBuilder builder : builders) {
        Geo3dShape<GeoPolygon> shape = (Geo3dShape<GeoPolygon>) builder.build();
        areaShape.addShape(shape.shape);
      }
      return new Geo3dShape<>(areaShape, context);
    }
  }

  /**
   * Geo3d implementation of {@link org.locationtech.spatial4j.shape.ShapeFactory.MultiShapeBuilder} to generate
   * geometry collections.
   *
   * @param <T> is the type of shapes.
   */
  private class Geo3dMultiShapeBuilder<T extends Shape> implements MultiShapeBuilder<T> {

    GeoCompositeAreaShape composite = new GeoCompositeAreaShape(planetModel);

    @Override
    public MultiShapeBuilder<T> add(T shape) {
      Geo3dShape<?> areaShape = (Geo3dShape<?>) shape;
      composite.addShape(areaShape.shape);
      return this;
    }

    @Override
    public Shape build() {
      return new Geo3dShape<>(composite, context);
    }
  }
}
