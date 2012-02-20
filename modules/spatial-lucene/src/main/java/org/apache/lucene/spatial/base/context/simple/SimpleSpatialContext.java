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

package org.apache.lucene.spatial.base.context.simple;

import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.distance.DistanceCalculator;
import org.apache.lucene.spatial.base.distance.DistanceUnits;
import org.apache.lucene.spatial.base.exception.InvalidShapeException;
import org.apache.lucene.spatial.base.shape.Circle;
import org.apache.lucene.spatial.base.shape.Point;
import org.apache.lucene.spatial.base.shape.Rectangle;
import org.apache.lucene.spatial.base.shape.Shape;
import org.apache.lucene.spatial.base.shape.simple.CircleImpl;
import org.apache.lucene.spatial.base.shape.simple.GeoCircleImpl;
import org.apache.lucene.spatial.base.shape.simple.PointImpl;
import org.apache.lucene.spatial.base.shape.simple.RectangleImpl;

import java.text.NumberFormat;
import java.util.Locale;

public class SimpleSpatialContext extends SpatialContext {

  public static SimpleSpatialContext GEO_KM = new SimpleSpatialContext(DistanceUnits.KILOMETERS);

  public SimpleSpatialContext(DistanceUnits units) {
    this(units, null, null);
  }

  public SimpleSpatialContext(DistanceUnits units, DistanceCalculator calculator, Rectangle worldBounds) {
    super(units, calculator, worldBounds);
  }

  @Override
  public Shape readShape(String value) throws InvalidShapeException {
    Shape s = super.readStandardShape( value );
    if( s == null ) {
      throw new InvalidShapeException( "Unable to read: "+value );
    }
    return s;
  }

  @Override
  public String toString(Shape shape) {
    if (Point.class.isInstance(shape)) {
      NumberFormat nf = NumberFormat.getInstance(Locale.US);
      nf.setGroupingUsed(false);
      nf.setMaximumFractionDigits(6);
      nf.setMinimumFractionDigits(6);
      Point point = (Point) shape;
      return nf.format(point.getX()) + " " + nf.format(point.getY());
    } else if (Rectangle.class.isInstance(shape)) {
      return writeRect((Rectangle) shape);
    }
    return shape.toString();
  }

  @Override
  public Circle makeCircle(Point point, double distance) {
    if (distance < 0)
      throw new InvalidShapeException("distance must be >= 0; got "+distance);
    if (isGeo())
      return new GeoCircleImpl( point, Math.min(distance,maxCircleDistance), this );
    else
      return new CircleImpl( point, distance, this );
  }

  @Override
  public Rectangle makeRect(double minX, double maxX, double minY, double maxY) {
    //--Normalize parameters
    if (isGeo()) {
      double delta = calcWidth(minX,maxX);
      if (delta >= 360) {
        //The only way to officially support complete longitude wrap-around is via western longitude = -180. We can't
        // support any point because 0 is undifferentiated in sign.
        minX = -180;
        maxX = 180;
      } else {
        minX = normX(minX);
        maxX = normX(maxX);
        assert Math.abs(delta - calcWidth(minX,maxX)) < 0.0001;//recompute delta; should be the same
      }
      if (minY > maxY) {
        throw new IllegalArgumentException("maxY must be >= minY");
      }
      if (minY < -90 || minY > 90 || maxY < -90 || maxY > 90)
        throw new IllegalArgumentException("minY or maxY is outside of -90 to 90 bounds. What did you mean?");
//      debatable what to do in this situation.
//      if (minY < -90) {
//        minX = -180;
//        maxX = 180;
//        maxY = Math.min(90,Math.max(maxY,-90 + (-90 - minY)));
//        minY = -90;
//      }
//      if (maxY > 90) {
//        minX = -180;
//        maxX = 180;
//        minY = Math.max(-90,Math.min(minY,90 - (maxY - 90)));
//        maxY = 90;
//      }

    } else {
      //these normalizations probably won't do anything since it's not geo but should probably call them any way.
      minX = normX(minX);
      maxX = normX(maxX);
      minY = normY(minY);
      maxY = normY(maxY);
    }
    return new RectangleImpl( minX, maxX, minY, maxY );
  }

  private double calcWidth(double minX,double maxX) {
    double w = maxX - minX;
    if (w < 0) {//only true when minX > maxX (WGS84 assumed)
      w += 360;
      assert w >= 0;
    }
    return w;
  }

  @Override
  public Point makePoint(double x, double y) {
    return new PointImpl(normX(x),normY(y));
  }
}
