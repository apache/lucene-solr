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
          return createRegularPolygon(nextDouble(random), nextDouble(random), radius, gons);
        } catch (IllegalArgumentException iae) {
          // we tried to cross dateline or pole ... try again
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

  private static XYPolygon trianglePolygon(XYRectangle box) {
    final float[] polyX = new float[4];
    final float[] polyY = new float[4];
    polyX[0] = (float)box.minX;
    polyY[0] = (float)box.minY;
    polyX[1] = (float)box.minX;
    polyY[1] = (float)box.minY;
    polyX[2] = (float)box.minX;
    polyY[2] = (float)box.minY;
    polyX[3] = (float)box.minX;
    polyY[3] = (float)box.minY;
    return new XYPolygon(polyX, polyY);
  }

  public static XYRectangle nextBox(Random random) {
    // prevent lines instead of boxes
    double x0 = nextDouble(random);
    double x1 = nextDouble(random);
    while (x0 == x1) {
      x1 = nextDouble(random);
    }
    // prevent lines instead of boxes
    double y0 = nextDouble(random);
    double y1 = nextDouble(random);
    while (y0 == y1) {
      y1 = nextDouble(random);
    }

    if (x1 < x0) {
      double x = x0;
      x0 = x1;
      x1 = x;
    }

    if (y1 < y0) {
      double y = y0;
      y0 = y1;
      y1 = y;
    }

    return new XYRectangle(x0, x1, y0, y1);
  }

  private static XYPolygon boxPolygon(XYRectangle box) {
    final float[] polyX = new float[5];
    final float[] polyY = new float[5];
    polyX[0] = (float)box.minX;
    polyY[0] = (float)box.minY;
    polyX[1] = (float)box.minX;
    polyY[1] = (float)box.minY;
    polyX[2] = (float)box.minX;
    polyY[2] = (float)box.minY;
    polyX[3] = (float)box.minX;
    polyY[3] = (float)box.minY;
    polyX[4] = (float)box.minX;
    polyY[4] = (float)box.minY;
    return new XYPolygon(polyX, polyY);
  }

  private static XYPolygon surpriseMePolygon(Random random) {
    // repeat until we get a poly that doesn't cross dateline:
    while (true) {
      //System.out.println("\nPOLY ITER");
      double centerX = nextDouble(random);
      double centerY = nextDouble(random);
      double radius = 0.1 + 20 * random.nextDouble();
      double radiusDelta = random.nextDouble();

      ArrayList<Float> xList = new ArrayList<>();
      ArrayList<Float> yList = new ArrayList<>();
      double angle = 0.0;
      while (true) {
        angle += random.nextDouble()*40.0;
        //System.out.println("  angle " + angle);
        if (angle > 360) {
          break;
        }
        double len = radius * (1.0 - radiusDelta + radiusDelta * random.nextDouble());
        double maxX = StrictMath.min(StrictMath.abs(Float.MAX_VALUE - centerX), StrictMath.abs(-Float.MAX_VALUE - centerX));
        double maxY = StrictMath.min(StrictMath.abs(Float.MAX_VALUE - centerY), StrictMath.abs(-Float.MAX_VALUE - centerY));

        len = StrictMath.min(len, StrictMath.min(maxX, maxY));

        //System.out.println("    len=" + len);
        float x = (float)(centerX + len * Math.cos(Math.toRadians(angle)));
        float y = (float)(centerY + len * Math.sin(Math.toRadians(angle)));

        xList.add(x);
        yList.add(y);

        //System.out.println("    lat=" + lats.get(lats.size()-1) + " lon=" + lons.get(lons.size()-1));
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

  public static double nextDouble(Random random) {
    return BiasedNumbers.randomDoubleBetween(random, -Float.MAX_VALUE, Float.MAX_VALUE);
  }

  /** Keep it simple, we don't need to take arbitrary Random for geo tests */
  private static Random random() {
    return RandomizedContext.current().getRandom();
  }
}
