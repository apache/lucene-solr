package org.apache.lucene.spatial.spatial4j;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.InfBufLine;
import com.spatial4j.core.shape.impl.PointImpl;

import static com.spatial4j.core.shape.SpatialRelation.CONTAINS;
import static com.spatial4j.core.shape.SpatialRelation.DISJOINT;

public abstract class RectIntersectionTestHelper<S extends Shape> extends RandomizedShapeTestCase {

  public RectIntersectionTestHelper(SpatialContext ctx) {
    super(ctx);
  }

  /** Override to return true if generateRandomShape is essentially a Rectangle. */
  protected boolean isRandomShapeRectangular() {
    return false;
  }

  protected abstract S generateRandomShape(Point nearP);

  /** shape has no area; return a point in it */
  protected abstract Point randomPointInEmptyShape(S shape);

  // Minimum distribution of relationships
  
  // Each shape has different characteristics, so we don't expect (for instance) shapes that
  // are likely to be long and thin to contain as many rectangles as those that
  // short and fat.

  /** Called once by {@link #testRelateWithRectangle()} to determine the max laps to try before failing. */
  protected int getMaxLaps() {
    return scaledRandomIntBetween(20_000, 200_000);
  }

  /** The minimum number of times we need to see each predicate in {@code laps} iterations. */
  protected int getDefaultMinimumPredicateFrequency(int maxLaps) {
    return maxLaps / 1000;
  }

  protected int getContainsMinimum(int maxLaps) {
    return getDefaultMinimumPredicateFrequency(maxLaps);
  }

  protected int getIntersectsMinimum(int maxLaps) {
    return getDefaultMinimumPredicateFrequency(maxLaps);
  }

  protected int getWithinMinimum(int maxLaps) {
    return getDefaultMinimumPredicateFrequency(maxLaps);
  }

  protected int getDisjointMinimum(int maxLaps) {
    return getDefaultMinimumPredicateFrequency(maxLaps);
  }

  protected int getBoundingMinimum(int maxLaps) {
    return getDefaultMinimumPredicateFrequency(maxLaps);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Point randomPointInOrNull(Shape shape) {
    if (!shape.hasArea()) {
      final Point pt = randomPointInEmptyShape((S) shape);
      assert shape.relate(pt).intersects() : "faulty randomPointInEmptyShape";
      return pt;
    }
    return super.randomPointInOrNull(shape);
  }

  public void testRelateWithRectangle() {
    //counters for the different intersection cases
    int i_C = 0, i_I = 0, i_W = 0, i_D = 0, i_bboxD = 0;
    int lap = 0;
    final int MAXLAPS = getMaxLaps();
    while(i_C < getContainsMinimum(MAXLAPS) || i_I < getIntersectsMinimum(MAXLAPS) || i_W < getWithinMinimum(MAXLAPS)
        || (!isRandomShapeRectangular() && i_D < getDisjointMinimum(MAXLAPS)) || i_bboxD < getBoundingMinimum(MAXLAPS)) {
      lap++;

      LogRule.clear();

      if (lap > MAXLAPS) {
        fail("Did not find enough contains/within/intersection/disjoint/bounds cases in a reasonable number" +
            " of attempts. CWIDbD: " +
            i_C + "("+getContainsMinimum(MAXLAPS)+")," +
            i_W + "("+getWithinMinimum(MAXLAPS)+")," +
            i_I + "("+getIntersectsMinimum(MAXLAPS)+")," +
            i_D + "("+getDisjointMinimum(MAXLAPS)+")," +
            i_bboxD + "("+getBoundingMinimum(MAXLAPS)+")"
            + "  Laps exceeded " + MAXLAPS);
      }

      Point nearP = randomPointIn(ctx.getWorldBounds());

      S s = generateRandomShape(nearP);

      Rectangle r = randomRectangle(s.getBoundingBox().getCenter());

      SpatialRelation ic = s.relate(r);

      LogRule.log("S-R Rel: {}, Shape {}, Rectangle {}    lap# {}", ic, s, r, lap);

      if (ic != DISJOINT) {
        assertTrue("if not disjoint then the shape's bbox shouldn't be disjoint",
            s.getBoundingBox().relate(r).intersects());
      }

      try {
        int MAX_TRIES = scaledRandomIntBetween(10, 100);
        switch (ic) {
          case CONTAINS:
            i_C++;
            for (int j = 0; j < MAX_TRIES; j++) {
              Point p = randomPointIn(r);
              assertRelation(null, CONTAINS, s, p);
            }
            break;

          case WITHIN:
            i_W++;
            for (int j = 0; j < MAX_TRIES; j++) {
              Point p = randomPointInOrNull(s);
              if (p == null) {//couldn't find a random point in shape
                break;
              }
              assertRelation(null, CONTAINS, r, p);
            }
            break;

          case DISJOINT:
            if (!s.getBoundingBox().relate(r).intersects()) {//bboxes are disjoint
              i_bboxD++;
              if (i_bboxD >= getBoundingMinimum(MAXLAPS))
                break;
            } else {
              i_D++;
            }
            for (int j = 0; j < MAX_TRIES; j++) {
              Point p = randomPointIn(r);
              assertRelation(null, DISJOINT, s, p);
            }
            break;

          case INTERSECTS:
            i_I++;
            SpatialRelation pointR = null;//set once
            Rectangle randomPointSpace = null;
            MAX_TRIES = 1000;//give many attempts
            for (int j = 0; j < MAX_TRIES; j++) {
              Point p;
              if (j < 4) {
                p = new PointImpl(0, 0, ctx);
                InfBufLine.cornerByQuadrant(r, j + 1, p);
              } else {
                if (randomPointSpace == null) {
                  if (pointR == DISJOINT) {
                    randomPointSpace = intersectRects(r,s.getBoundingBox());
                  } else {//CONTAINS
                    randomPointSpace = r;
                  }
                }
                p = randomPointIn(randomPointSpace);
              }
              SpatialRelation pointRNew = s.relate(p);
              if (pointR == null) {
                pointR = pointRNew;
              } else if (pointR != pointRNew) {
                break;
              } else if (j >= MAX_TRIES) {
                //TODO consider logging instead of failing
                fail("Tried intersection brute-force too many times without success");
              }
            }

            break;

          default: fail(""+ic);
        } // switch
      } catch (AssertionError e) {
        onAssertFail(e, s, r, ic);
      }

    } // while loop

    System.out.println("Laps: "+lap + " CWIDbD: "+i_C+","+i_W+","+i_I+","+i_D+","+i_bboxD);
  }

  protected void onAssertFail(AssertionError e, S s, Rectangle r, SpatialRelation ic) {
    throw e;
  }

  private Rectangle intersectRects(Rectangle r1, Rectangle r2) {
    assert r1.relate(r2).intersects();
    final double minX, maxX;
    if (r1.relateXRange(r2.getMinX(),r2.getMinX()).intersects()) {
      minX = r2.getMinX();
    } else {
      minX = r1.getMinX();
    }
    if (r1.relateXRange(r2.getMaxX(),r2.getMaxX()).intersects()) {
      maxX = r2.getMaxX();
    } else {
      maxX = r1.getMaxX();
    }
    final double minY, maxY;
    if (r1.relateYRange(r2.getMinY(),r2.getMinY()).intersects()) {
      minY = r2.getMinY();
    } else {
      minY = r1.getMinY();
    }
    if (r1.relateYRange(r2.getMaxY(),r2.getMaxY()).intersects()) {
      maxY = r2.getMaxY();
    } else {
      maxY = r1.getMaxY();
    }
    return ctx.makeRectangle(minX, maxX, minY, maxY);
  }

}
