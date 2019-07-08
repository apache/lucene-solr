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

package org.apache.lucene.component2D;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;

/** Factory methods for creating {@link Component2D} from LatLon objects.
 *
 * @lucen.internal
 */

public class LatLonComponent2DFactory {

  /** Builds a Component2D from multi-point on the form [Lat, Lon] */
  public static Component2D create(double[]... points) {
    if (points.length == 1) {
      GeoUtils.checkLatitude(points[0][0]);
      GeoUtils.checkLongitude(points[0][1]);
      return PointComponent2D.createComponent(GeoEncodingUtils.encodeLongitude(points[0][1]), GeoEncodingUtils.encodeLatitude(points[0][0]));
    }
    Component2D components[] = new Component2D[points.length];
    for (int i = 0; i < components.length; i++) {
      GeoUtils.checkLatitude(points[i][0]);
      GeoUtils.checkLongitude(points[i][1]);
      components[i] = PointComponent2D.createComponent(GeoEncodingUtils.encodeLongitude(points[i][1]), GeoEncodingUtils.encodeLatitude(points[i][0]));
    }
    return Component2DTree.create(components);
  }

  private static final int NEGATIVE_DATELINE = GeoEncodingUtils.encodeLongitude(-180);
  private static final int POSITIVE_DATELINE = GeoEncodingUtils.encodeLongitude(180);

  /** Builds a Component2D from multi-rectangle */
  public static Component2D create(Rectangle... rectangles) {
    if (rectangles.length == 1 && rectangles[0].crossesDateline() == false) {
      int minX = GeoEncodingUtils.encodeLongitude(rectangles[0].minLon);
      int maxX = GeoEncodingUtils.encodeLongitude(rectangles[0].maxLon);
      int minY = GeoEncodingUtils.encodeLatitude(rectangles[0].minLat);
      int maxY = GeoEncodingUtils.encodeLatitude(rectangles[0].maxLat);
      return RectangleComponent2D.createComponent(minX, maxX, minY, maxY);
    }
    List<Component2D> components = new ArrayList<>();
    for (Rectangle rectangle: rectangles) {
      int minX = GeoEncodingUtils.encodeLongitude(rectangle.minLon);
      int maxX = GeoEncodingUtils.encodeLongitude(rectangle.maxLon);
      int minY = GeoEncodingUtils.encodeLatitude(rectangle.minLat);
      int maxY = GeoEncodingUtils.encodeLatitude(rectangle.maxLat);
      if (rectangle.crossesDateline()) {
        components.add(RectangleComponent2D.createComponent(minX, POSITIVE_DATELINE, minY, maxY));
        components.add(RectangleComponent2D.createComponent(NEGATIVE_DATELINE, maxX, minY, maxY));
      } else {
        components.add(RectangleComponent2D.createComponent(minX, maxX, minY, maxY));
      }
    }
    return Component2DTree.create(components.toArray(new Component2D[components.size()]));
  }

  /** Builds a Component2D from polygon */
  private static Component2D createComponent(Polygon polygon) {
    Polygon gonHoles[] = polygon.getHoles();
    Component2D holes = null;
    if (gonHoles.length > 0) {
      holes = create(gonHoles);
    }
    RectangleComponent2D box =  RectangleComponent2D.createComponent(GeoEncodingUtils.encodeLongitude(polygon.minLon),
        GeoEncodingUtils.encodeLongitude(polygon.maxLon),
        GeoEncodingUtils.encodeLatitude(polygon.minLat),
        GeoEncodingUtils.encodeLatitude(polygon.maxLat));
    return new PolygonComponent2D(encodeLongs(polygon.getPolyLons()), encodeLats(polygon.getPolyLats()), box, holes);
  }

  /** Builds a Component2D tree from multipolygon */
  public static Component2D create(Polygon... polygons) {
    if (polygons.length == 1) {
      return createComponent(polygons[0]);
    }
    Component2D components[] = new Component2D[polygons.length];
    for (int i = 0; i < components.length; i++) {
      components[i] = createComponent(polygons[i]);
    }
    return Component2DTree.create(components);
  }

  /** Builds a Component2D from line */
  private static Component2D createComponent(Line line) {
    RectangleComponent2D box = RectangleComponent2D.createComponent(GeoEncodingUtils.encodeLongitude(line.minLon),
        GeoEncodingUtils.encodeLongitude(line.maxLon),
        GeoEncodingUtils.encodeLatitude(line.minLat),
        GeoEncodingUtils.encodeLatitude(line.maxLat));
    return new LineComponent2D(encodeLongs(line.getLons()), encodeLats(line.getLats()), box);
  }

  /** Builds a Component2D tree from multiline */
  public static Component2D create(Line... lines) {
    if (lines.length == 1) {
      return createComponent(lines[0]);
    }
    Component2D components[] = new Component2D[lines.length];
    for (int i = 0; i < components.length; i++) {
      components[i] = createComponent(lines[i]);
    }
    return Component2DTree.create(components);
  }

  /** Builds a Component2D from polygon */
  private static Component2D createComponent(Object shape) {
    if (shape instanceof double[]) {
      return create((double[]) shape);
    } else if (shape instanceof Polygon) {
      return create((Polygon) shape);
    } else if (shape instanceof Line) {
      return  create((Line) shape);
    } else if (shape instanceof Rectangle) {
      return create((Rectangle) shape);
    } else {
      throw new IllegalArgumentException("Unknown shape type: " + shape.getClass());
    }
  }

  /** Builds a Component2D from an array of shape descriptors. Current descriptors supported are:
   *  {@link Polygon}, {@link Line}, {@link Rectangle} and double[] ([Lat, Lon] point).
   * */
  public static Component2D create(Object... shapes) {
    if (shapes.length == 1) {
      return createComponent(shapes[0]);
    }
    Component2D[] components = new Component2D[shapes.length];
    for (int i = 0; i < shapes.length; i ++) {
      components[i] = createComponent(shapes[i]);
    }
    return Component2DTree.createTree(components, 0, components.length - 1, true);
  }

  private static int[] encodeLongs(double[] longs) {
    int[] encoded = new int[longs.length];
    for (int i = 0; i < longs.length; i++) {
      encoded[i] = GeoEncodingUtils.encodeLongitude(longs[i]);
    }
    return encoded;
  }

  private static int[] encodeLats(double[] lats) {
    int[] encoded = new int[lats.length];
    for (int i = 0; i < lats.length; i++) {
      encoded[i] = GeoEncodingUtils.encodeLatitude(lats[i]);
    }
    return encoded;
  }
}
