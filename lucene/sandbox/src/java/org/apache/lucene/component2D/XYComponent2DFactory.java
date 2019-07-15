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

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;

/** Factory methods for creating {@link Component2D} from XY objects.
 *
 * @lucen.internal
 */

public class XYComponent2DFactory {

  /** Builds a Component2D from multi-point on the form [X,Y] */
  public static Component2D create(double[]... points) {
    if (points.length == 1) {
      return PointComponent2D.createComponent(XYEncodingUtils.encode(points[0][0]), XYEncodingUtils.encode(points[0][1]));
    }
    Component2D components[] = new Component2D[points.length];
    for (int i = 0; i < components.length; i++) {
      components[i] = PointComponent2D.createComponent(XYEncodingUtils.encode(points[i][0]), XYEncodingUtils.encode(points[i][1]));
    }
    return Component2DTree.create(components);
  }

  /** Builds a Component2D from multi-rectangle */
  public static Component2D create(XYRectangle... rectangles) {
    if (rectangles.length == 1) {
      int minX = XYEncodingUtils.encode(rectangles[0].minX);
      int maxX = XYEncodingUtils.encode(rectangles[0].maxX);
      int minY = XYEncodingUtils.encode(rectangles[0].minY);
      int maxY = XYEncodingUtils.encode(rectangles[0].maxY);
      return RectangleComponent2D.createComponent(minX, maxX, minY, maxY);
    }
    List<Component2D> components = new ArrayList<>();
    for (XYRectangle rectangle: rectangles) {
      int minX = XYEncodingUtils.encode(rectangle.minX);
      int maxX = XYEncodingUtils.encode(rectangle.maxX);
      int minY = XYEncodingUtils.encode(rectangle.minY);
      int maxY = XYEncodingUtils.encode(rectangle.maxY);
      components.add(RectangleComponent2D.createComponent(minX, maxX, minY, maxY));
    }
    return Component2DTree.create(components.toArray(new Component2D[components.size()]));
  }

  /** Builds a Component2D from polygon */
  private static Component2D createComponent(XYPolygon polygon) {
    XYPolygon gonHoles[] = polygon.getHoles();
    Component2D holes = null;
    if (gonHoles.length > 0) {
      holes = create(gonHoles);
    }
    RectangleComponent2D box =  RectangleComponent2D.createComponent(XYEncodingUtils.encode(polygon.minX),
        XYEncodingUtils.encode(polygon.maxX),
        XYEncodingUtils.encode(polygon.minY),
        XYEncodingUtils.encode(polygon.maxY));
    return new PolygonComponent2D(quantize(polygon.getPolyX()), quantize(polygon.getPolyY()), box, holes, XYShape.DECODER);
  }

  /** Builds a Component2D tree from multipolygon */
  public static Component2D create(XYPolygon... polygons) {
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
  private static Component2D createComponent(XYLine line) {
    RectangleComponent2D box = RectangleComponent2D.createComponent(XYEncodingUtils.encode(line.minX),
        XYEncodingUtils.encode(line.maxX),
        XYEncodingUtils.encode(line.minY),
        XYEncodingUtils.encode(line.maxY));
    return new LineComponent2D(quantize(line.getX()), quantize(line.getY()), box, XYShape.DECODER);
  }

  /** Builds a Component2D tree from multiline */
  public static Component2D create(XYLine... lines) {
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
    } else if (shape instanceof XYPolygon) {
      return create((XYPolygon) shape);
    } else if (shape instanceof XYLine) {
      return  create((XYLine) shape);
    } else if (shape instanceof XYRectangle) {
      return create((XYRectangle) shape);
    } else {
      throw new IllegalArgumentException("Unknown shape type: " + shape.getClass());
    }
  }

  /** Builds a Component2D from an array of shape descriptors. Current descriptors supported are:
   *  {@link XYPolygon}, {@link XYLine}, {@link XYRectangle} and double[] ([Lat, Lon] point).
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

  private static double[] quantize(double[] vars) {
    double[] encoded = new double[vars.length];
    for (int i = 0; i < vars.length; i++) {
      encoded[i] = XYEncodingUtils.decode(XYEncodingUtils.encode(vars[i]));
    }
    return encoded;
  }
}
