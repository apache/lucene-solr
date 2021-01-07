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
package org.apache.lucene.geo;

import java.util.ArrayList;
import java.util.Random;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.BiasedNumbers;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** generates random cartesian geometry; heavy reuse of {@link GeoTestUtil} */
public class ShapeTestUtil {

  /** returns next pseudorandom polygon */
  public static XYPolygon nextPolygon() {
    Random random = random();
    if (random.nextBoolean()) {
      return surpriseMePolygon(random);
    } else if (LuceneTestCase.TEST_NIGHTLY && random.nextInt(10) == 1) {
      // this poly is slow to create ... only do it 10% of the time:
      while (true) {
        int gons = TestUtil.nextInt(random, 4, 500);
        // So the poly can cover at most 50% of the earth's surface:
        double radius = random.nextDouble() * 0.5 * Float.MAX_VALUE + 1.0;
        try {
          return createRegularPolygon(nextFloat(random), nextFloat(random), radius, gons);
        } catch (IllegalArgumentException iae) {
          // something went wrong, try again
        }
      }
    }

    XYRectangle box = nextBox(random);
    if (random.nextBoolean()) {
      // box
      return boxPolygon(box);
    } else {
      // triangle
      return trianglePolygon(box);
    }
  }

  public static XYPoint nextPoint() {
    Random random = random();
    float x = nextFloat(random);
    float y = nextFloat(random);
    return new XYPoint(x, y);
  }

  public static XYLine nextLine() {
    XYPolygon poly = ShapeTestUtil.nextPolygon();
    float[] x = new float[poly.numPoints() - 1];
    float[] y = new float[x.length];
    for (int i = 0; i < x.length; ++i) {
      x[i] = poly.getPolyX(i);
      y[i] = poly.getPolyY(i);
    }
    return new XYLine(x, y);
  }

  public static XYCircle nextCircle() {
    Random random = random();
    float x = nextFloat(random);
    float y = nextFloat(random);
    float radius = 0;
    while (radius == 0) {
      radius = random().nextFloat() * Float.MAX_VALUE / 2;
    }
    assert radius != 0;
    return new XYCircle(x, y, radius);
  }

  private static XYPolygon trianglePolygon(XYRectangle box) {
    final float[] polyX = new float[4];
    final float[] polyY = new float[4];
    polyX[0] = box.minX;
    polyY[0] = box.minY;
    polyX[1] = box.minX;
    polyY[1] = box.minY;
    polyX[2] = box.minX;
    polyY[2] = box.minY;
    polyX[3] = box.minX;
    polyY[3] = box.minY;
    return new XYPolygon(polyX, polyY);
  }

  public static XYRectangle nextBox(Random random) {
    // prevent lines instead of boxes
    float x0 = nextFloat(random);
    float x1 = nextFloat(random);
    while (x0 == x1) {
      x1 = nextFloat(random);
    }
    // prevent lines instead of boxes
    float y0 = nextFloat(random);
    float y1 = nextFloat(random);
    while (y0 == y1) {
      y1 = nextFloat(random);
    }

    if (x1 < x0) {
      float x = x0;
      x0 = x1;
      x1 = x;
    }

    if (y1 < y0) {
      float y = y0;
      y0 = y1;
      y1 = y;
    }

    return new XYRectangle(x0, x1, y0, y1);
  }

  private static XYPolygon boxPolygon(XYRectangle box) {
    final float[] polyX = new float[5];
    final float[] polyY = new float[5];
    polyX[0] = box.minX;
    polyY[0] = box.minY;
    polyX[1] = box.minX;
    polyY[1] = box.minY;
    polyX[2] = box.minX;
    polyY[2] = box.minY;
    polyX[3] = box.minX;
    polyY[3] = box.minY;
    polyX[4] = box.minX;
    polyY[4] = box.minY;
    return new XYPolygon(polyX, polyY);
  }

  private static XYPolygon surpriseMePolygon(Random random) {
    while (true) {
      float centerX = nextFloat(random);
      float centerY = nextFloat(random);
      double radius = 0.1 + 20 * random.nextDouble();
      double radiusDelta = random.nextDouble();

      ArrayList<Float> xList = new ArrayList<>();
      ArrayList<Float> yList = new ArrayList<>();
      double angle = 0.0;
      while (true) {
        angle += random.nextDouble()*40.0;
        if (angle > 360) {
          break;
        }
        double len = radius * (1.0 - radiusDelta + radiusDelta * random.nextDouble());
        float maxX = StrictMath.min(StrictMath.abs(Float.MAX_VALUE - centerX), StrictMath.abs(-Float.MAX_VALUE - centerX));
        float maxY = StrictMath.min(StrictMath.abs(Float.MAX_VALUE - centerY), StrictMath.abs(-Float.MAX_VALUE - centerY));

        len = StrictMath.min(len, StrictMath.min(maxX, maxY));

        float x = (float)(centerX + len * Math.cos(StrictMath.toRadians(angle)));
        float y = (float)(centerY + len * Math.sin(StrictMath.toRadians(angle)));

        xList.add(x);
        yList.add(y);
      }

      // close it
      xList.add(xList.get(0));
      yList.add(yList.get(0));

      float[] xArray = new float[xList.size()];
      float[] yArray = new float[yList.size()];
      for(int i=0;i<xList.size();i++) {
        xArray[i] = xList.get(i);
        yArray[i] = yList.get(i);
      }
      return new XYPolygon(xArray, yArray);
    }
  }

  /** Makes an n-gon, centered at the provided x/y, and each vertex approximately
   *  distanceMeters away from the center.
   *
   * Do not invoke me across the dateline or a pole!! */
  public static XYPolygon createRegularPolygon(double centerX, double centerY, double radius, int gons) {

    double maxX = StrictMath.min(StrictMath.abs(Float.MAX_VALUE - centerX), StrictMath.abs(-Float.MAX_VALUE - centerX));
    double maxY = StrictMath.min(StrictMath.abs(Float.MAX_VALUE - centerY), StrictMath.abs(-Float.MAX_VALUE - centerY));

    radius = StrictMath.min(radius, StrictMath.min(maxX, maxY));

    float[][] result = new float[2][];
    result[0] = new float[gons+1];
    result[1] = new float[gons+1];
    //System.out.println("make gon=" + gons);
    for(int i=0;i<gons;i++) {
      double angle = 360.0-i*(360.0/gons);
      //System.out.println("  angle " + angle);
      double x = Math.cos(StrictMath.toRadians(angle));
      double y = Math.sin(StrictMath.toRadians(angle));
      result[0][i] = (float)(centerY + y * radius);
      result[1][i] = (float)(centerX + x * radius);
    }

    // close poly
    result[0][gons] = result[0][0];
    result[1][gons] = result[1][0];

    return new XYPolygon(result[0], result[1]);
  }

  public static float nextFloat(Random random) {
    return BiasedNumbers.randomFloatBetween(random, -Float.MAX_VALUE, Float.MAX_VALUE);
  }

  /** Keep it simple, we don't need to take arbitrary Random for geo tests */
  private static Random random() {
    return RandomizedContext.current().getRandom();
  }

  /**
   * Simple slow point in polygon check (for testing)
   */
  // direct port of PNPOLY C code (https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html)
  // this allows us to improve the code yet still ensure we have its properties
  // it is under the BSD license (https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html#License%20to%20Use)
  //
  // Copyright (c) 1970-2003, Wm. Randolph Franklin
  //
  // Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  // documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  // the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
  // to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  //
  // 1. Redistributions of source code must retain the above copyright
  //    notice, this list of conditions and the following disclaimers.
  // 2. Redistributions in binary form must reproduce the above copyright
  //    notice in the documentation and/or other materials provided with
  //    the distribution.
  // 3. The name of W. Randolph Franklin may not be used to endorse or
  //    promote products derived from this Software without specific
  //    prior written permission.
  //
  // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
  // TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
  // THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
  // CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  // IN THE SOFTWARE.
  public static boolean containsSlowly(XYPolygon polygon, double x, double y) {
    if (polygon.getHoles().length > 0) {
      throw new UnsupportedOperationException("this testing method does not support holes");
    }
    double polyXs[] = XYEncodingUtils.floatArrayToDoubleArray(polygon.getPolyX());
    double polyYs[] =XYEncodingUtils.floatArrayToDoubleArray(polygon.getPolyY());
    // bounding box check required due to rounding errors (we don't solve that problem)
    if (x < polygon.minX || x > polygon.maxX || y < polygon.minY || y > polygon.maxY) {
      return false;
    }

    boolean c = false;
    int i, j;
    int nvert = polyYs.length;
    double verty[] = polyYs;
    double vertx[] = polyXs;
    double testy = y;
    double testx = x;
    for (i = 0, j = 1; j < nvert; ++i, ++j) {
      if (testy == verty[j] && testy == verty[i] ||
          ((testy <= verty[j] && testy >= verty[i]) != (testy >= verty[j] && testy <= verty[i]))) {
        if ((testx == vertx[j] && testx == vertx[i]) ||
            ((testx <= vertx[j] && testx >= vertx[i]) != (testx >= vertx[j] && testx <= vertx[i]) &&
                GeoUtils.orient(vertx[i], verty[i], vertx[j], verty[j], testx, testy) == 0)) {
          // return true if point is on boundary
          return true;
        } else if ( ((verty[i] > testy) != (verty[j] > testy)) &&
            (testx < (vertx[j]-vertx[i]) * (testy-verty[i]) / (verty[j]-verty[i]) + vertx[i]) ) {
          c = !c;
        }
      }
    }
    return c;
  }
}
