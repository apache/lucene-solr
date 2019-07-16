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

import java.util.function.Function;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.PointValues;

/**
 *
 * A component2D predicate for fast computation of point in component computation.
 *
 * @lucene.internal
 */
class XYComponent2DPredicate {

  private final Component2D component;

  static final int ARITY = 64;

  final int yShift, xShift;
  final int yBase, xBase;
  final int maxYDelta, maxXDelta;
  final byte[] relations;

  private XYComponent2DPredicate(
      int yShift, int xShift,
      int yBase, int xBase,
      int maxYDelta, int maxXDelta,
      byte[] relations,
      Component2D component) {
    if (yShift < 1 || yShift > 31) {
      throw new IllegalArgumentException();
    }
    if (xShift < 1 || xShift > 31) {
      throw new IllegalArgumentException();
    }
    this.yShift = yShift;
    this.xShift = xShift;
    this.yBase = yBase;
    this.xBase = xBase;
    this.maxYDelta = maxYDelta;
    this.maxXDelta = maxXDelta;
    this.relations = relations;
    this.component = component;
  }

  /** Check whether the given point is within the considered component.
   *  NOTE: this operates directly on the encoded representation of points. */
  public boolean test(int x, int y) {
    final int y2 = ((y - XYEncodingUtils.MIN_ENC_VAL) >>> yShift);
    if (y2 < yBase || y2 >= yBase + maxYDelta) {
      return false;
    }
    int x2 = ((x - XYEncodingUtils.MIN_ENC_VAL) >>> xShift);
    if (x2 < xBase ||  x2 - xBase >= maxXDelta) {
      return false;
    }
    final int relation = relations[(y2 - yBase) * maxXDelta + (x2 - xBase)];
    if (relation == PointValues.Relation.CELL_CROSSES_QUERY.ordinal()) {
      return component.contains(x, y);
    } else {
      return relation == PointValues.Relation.CELL_INSIDE_QUERY.ordinal();
    }
  }

  private static XYComponent2DPredicate createSubBoxes(RectangleComponent2D boundingBox, Function<RectangleComponent2D, PointValues.Relation> boxToRelation, Component2D component) {
    final int minY = boundingBox.minY;
    final int maxY = boundingBox.maxY;
    final int minX = boundingBox.minX;
    final int maxX = boundingBox.maxX;

    final int yShift, xShift;
    final int yBase, xBase;
    final int maxYDelta, maxXDelta;
    {
      long minY2 = (long) minY - XYEncodingUtils.MIN_ENC_VAL;
      long maxY2 = (long) maxY - XYEncodingUtils.MIN_ENC_VAL;
      yShift = computeShift(minY2, maxY2);
      yBase = (int) (minY2 >>> yShift);
      maxYDelta = (int) (maxY2 >>> yShift) - yBase + 1;
      assert maxYDelta > 0;
    }
    {
      long minX2 = (long) minX - XYEncodingUtils.MIN_ENC_VAL;
      long maxX2 = (long) maxX - XYEncodingUtils.MIN_ENC_VAL;
      xShift = computeShift(minX2, maxX2);
      xBase = (int) (minX2 >>> xShift);
      maxXDelta = (int) (maxX2 >>> xShift) - xBase + 1;
      assert maxXDelta > 0;
    }

    final byte[] relations = new byte[maxYDelta * maxXDelta];
    for (int i = 0; i < maxYDelta; ++i) {
      for (int j = 0; j < maxXDelta; ++j) {
        final int boxMinY = ((yBase + i) << yShift) + XYEncodingUtils.MIN_ENC_VAL;
        final int boxMinX = ((xBase + j) << xShift) + XYEncodingUtils.MIN_ENC_VAL;
        final int boxMaxY = boxMinY + (1 << yShift) - 1;
        final int boxMaxX = boxMinX + (1 << xShift) - 1;
        relations[i * maxXDelta + j] = (byte) boxToRelation.apply(RectangleComponent2D.createComponent(
            boxMinX, boxMaxX < boxMinX ? XYEncodingUtils.MAX_ENC_VAL : boxMaxX,
            boxMinY, boxMaxY < boxMinY ? XYEncodingUtils.MAX_ENC_VAL : boxMaxY )).ordinal();
      }
    }

    return new XYComponent2DPredicate(
        yShift, xShift,
        yBase, xBase,
        maxYDelta, maxXDelta,
        relations, component);
  }

  /** Compute the minimum shift value so that
   * {@code (b>>>shift)-(a>>>shift)} is less that {@code ARITY}. */
  private static int computeShift(long a, long b) {
    assert a <= b;
    // We enforce a shift of at least 1 so that when we work with unsigned ints
    // by doing (lat - MIN_VALUE), the result of the shift (lat - MIN_VALUE) >>> shift
    // can be used for comparisons without particular care: the sign bit has
    // been cleared so comparisons work the same for signed and unsigned ints
    for (int shift = 1; ; ++shift) {
      final long delta = (b >>> shift) - (a >>> shift);
      if (delta >= 0 && delta < XYComponent2DPredicate.ARITY) {
        return shift;
      }
    }
  }

  /** Create a predicate that checks whether points are within a component2D.
   *  @lucene.internal */
  static XYComponent2DPredicate createComponentPredicate(Component2D component) {
    final RectangleComponent2D boundingBox = component.getBoundingBox();
    final Function<RectangleComponent2D, PointValues.Relation> boxToRelation = box -> component.relate(
        box.minX, box.maxX, box.minY, box.maxY);
    return  createSubBoxes(boundingBox, boxToRelation, component);
  }
}
