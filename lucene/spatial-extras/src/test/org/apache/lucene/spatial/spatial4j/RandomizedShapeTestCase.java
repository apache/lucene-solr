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

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import static org.locationtech.spatial4j.shape.SpatialRelation.CONTAINS;
import static org.locationtech.spatial4j.shape.SpatialRelation.WITHIN;

import org.apache.lucene.util.LuceneTestCase;

/**
 * A base test class with utility methods to help test shapes.
 * Extends from RandomizedTest.
 */
public abstract class RandomizedShapeTestCase extends LuceneTestCase {

  protected static final double EPS = 10e-9;

  protected SpatialContext ctx;//needs to be set ASAP

  /** Used to reduce the space of numbers to increase the likelihood that
   * random numbers become equivalent, and thus trigger different code paths.
   * Also makes some random shapes easier to manually examine.
   */
  protected final double DIVISIBLE = 2;// even coordinates; (not always used)

  protected RandomizedShapeTestCase() {
  }

  public RandomizedShapeTestCase(SpatialContext ctx) {
    this.ctx = ctx;
  }

  @SuppressWarnings("unchecked")
  public static void checkShapesImplementEquals( Class<?>[] classes ) {
    for( Class<?> clazz : classes ) {
      try {
        clazz.getDeclaredMethod( "equals", Object.class );
      } catch (Exception e) {
        fail("Shape needs to define 'equals' : " + clazz.getName());
      }
      try {
        clazz.getDeclaredMethod( "hashCode" );
      } catch (Exception e) {
        fail("Shape needs to define 'hashCode' : " + clazz.getName());
      }
    }
  }

  //These few norm methods normalize the arguments for creating a shape to
  // account for the dateline. Some tests loop past the dateline or have offsets
  // that go past it and it's easier to have them coded that way and correct for
  // it here.  These norm methods should be used when needed, not frivolously.

  protected double normX(double x) {
    return ctx.isGeo() ? DistanceUtils.normLonDEG(x) : x;
  }

  protected double normY(double y) {
    return ctx.isGeo() ? DistanceUtils.normLatDEG(y) : y;
  }

  protected Rectangle makeNormRect(double minX, double maxX, double minY, double maxY) {
    if (ctx.isGeo()) {
      if (Math.abs(maxX - minX) >= 360) {
        minX = -180;
        maxX = 180;
      } else {
        minX = DistanceUtils.normLonDEG(minX);
        maxX = DistanceUtils.normLonDEG(maxX);
      }

    } else {
      if (maxX < minX) {
        double t = minX;
        minX = maxX;
        maxX = t;
      }
      minX = boundX(minX, ctx.getWorldBounds());
      maxX = boundX(maxX, ctx.getWorldBounds());
    }
    if (maxY < minY) {
      double t = minY;
      minY = maxY;
      maxY = t;
    }
    minY = boundY(minY, ctx.getWorldBounds());
    maxY = boundY(maxY, ctx.getWorldBounds());
    return ctx.getShapeFactory().rect(minX, maxX, minY, maxY);
  }

  public static double divisible(double v, double divisible) {
    return (int) (Math.round(v / divisible) * divisible);
  }

  protected double divisible(double v) {
    return divisible(v, DIVISIBLE);
  }

  /** reset()'s p, and confines to world bounds. Might not be divisible if
   * the world bound isn't divisible too.
   */
  protected Point divisible(Point p) {
    Rectangle bounds = ctx.getWorldBounds();
    double newX = boundX( divisible(p.getX()), bounds );
    double newY = boundY( divisible(p.getY()), bounds );
    p.reset(newX, newY);
    return p;
  }

  static double boundX(double i, Rectangle bounds) {
    return bound(i, bounds.getMinX(), bounds.getMaxX());
  }

  static double boundY(double i, Rectangle bounds) {
    return bound(i, bounds.getMinY(), bounds.getMaxY());
  }

  static double bound(double i, double min, double max) {
    if (i < min) return min;
    if (i > max) return max;
    return i;
  }

  protected void assertRelation(SpatialRelation expected, Shape a, Shape b) {
    assertRelation(null, expected, a, b);
  }

  protected void assertRelation(String msg, SpatialRelation expected, Shape a, Shape b) {
    _assertIntersect(msg, expected, a, b);
    //check flipped a & b w/ transpose(), while we're at it
    _assertIntersect(msg, expected.transpose(), b, a);
  }

  private void _assertIntersect(String msg, SpatialRelation expected, Shape a, Shape b) {
    SpatialRelation sect = a.relate(b);
    if (sect == expected)
      return;
    msg = ((msg == null) ? "" : msg+"\r") + a +" intersect "+b;
    if (expected == WITHIN || expected == CONTAINS) {
      if (a.getClass().equals(b.getClass())) // they are the same shape type
        assertEquals(msg,a,b);
      else {
        //they are effectively points or lines that are the same location
        assertTrue(msg,!a.hasArea());
        assertTrue(msg,!b.hasArea());

        Rectangle aBBox = a.getBoundingBox();
        Rectangle bBBox = b.getBoundingBox();
        if (aBBox.getHeight() == 0 && bBBox.getHeight() == 0
            && (aBBox.getMaxY() == 90 && bBBox.getMaxY() == 90
            || aBBox.getMinY() == -90 && bBBox.getMinY() == -90))
          ;//== a point at the pole
        else
          assertEquals(msg, aBBox, bBBox);
      }
    } else {
      assertEquals(msg,expected,sect);//always fails
    }
  }

}
