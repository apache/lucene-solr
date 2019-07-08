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

import org.apache.lucene.index.PointValues;

/**
 *
 * A component2D predicate for fast computation of point in component computation,
 *
 * @lucene.internal
 */
public class Component2DPredicate {

  private final Component2D component;

  static final int ARITY = 64;

  final int latShift, lonShift;
  final int latBase, lonBase;
  final int maxLatDelta, maxLonDelta;
  final byte[] relations;

  private Component2DPredicate(
      int latShift, int lonShift,
      int latBase, int lonBase,
      int maxLatDelta, int maxLonDelta,
      byte[] relations,
      Component2D component) {
    if (latShift < 1 || latShift > 31) {
      throw new IllegalArgumentException();
    }
    if (lonShift < 1 || lonShift > 31) {
      throw new IllegalArgumentException();
    }
    this.latShift = latShift;
    this.lonShift = lonShift;
    this.latBase = latBase;
    this.lonBase = lonBase;
    this.maxLatDelta = maxLatDelta;
    this.maxLonDelta = maxLonDelta;
    this.relations = relations;
    this.component = component;
  }

  /** Check whether the given point is within the considered component.
   *  NOTE: this operates directly on the encoded representation of points. */
  public boolean test(int x, int y) {
    final int y2 = ((y - Integer.MIN_VALUE) >>> latShift);
    if (y2 < latBase || y2 >= latBase + maxLatDelta) {
      //not sure about this but it fails in some cases for point components
      return (y2 == Integer.MAX_VALUE) ? component.contains(x, y) : false;
    }
    int x2 = ((x - Integer.MIN_VALUE) >>> lonShift);
    if (x2 < lonBase) { // wrap
      x2 += 1 << (32 - lonShift);
    }
    assert Integer.toUnsignedLong(x2) >= lonBase;
    assert x2 - lonBase >= 0;
    if (x2 - lonBase >= maxLonDelta) {
      return false;
    }

    final int relation = relations[(y2 - latBase) * maxLonDelta + (x2 - lonBase)];
    if (relation == PointValues.Relation.CELL_CROSSES_QUERY.ordinal()) {
      return component.contains(x, y);
    } else {
      return relation == PointValues.Relation.CELL_INSIDE_QUERY.ordinal();
    }
  }

  private static Component2DPredicate createSubBoxes(RectangleComponent2D boundingBox, Function<RectangleComponent2D, PointValues.Relation> boxToRelation, Component2D component) {
    final int minY = boundingBox.minY;
    final int maxY = boundingBox.maxY;
    final int minX = boundingBox.minX;
    final int maxX = boundingBox.maxX;

    final int latShift, lonShift;
    final int latBase, lonBase;
    final int maxLatDelta, maxLonDelta;
    {
      long minY2 = (long) minY - Integer.MIN_VALUE;
      long maxY2 = (long) maxY - Integer.MIN_VALUE;
      latShift = computeShift(minY2, maxY2);
      latBase = (int) (minY2 >>> latShift);
      maxLatDelta = (int) (maxY2 >>> latShift) - latBase + 1;
      assert maxLatDelta > 0;
    }
    {
      long minX2 = (long) minX - Integer.MIN_VALUE;
      long maxX2 = (long) maxX - Integer.MIN_VALUE;
      lonShift = computeShift(minX2, maxX2);
      lonBase = (int) (minX2 >>> lonShift);
      maxLonDelta = (int) (maxX2 >>> lonShift) - lonBase + 1;
      assert maxLonDelta > 0;
    }

    final byte[] relations = new byte[maxLatDelta * maxLonDelta];
    for (int i = 0; i < maxLatDelta; ++i) {
      for (int j = 0; j < maxLonDelta; ++j) {
        final int boxMinY = ((latBase + i) << latShift) + Integer.MIN_VALUE;
        final int boxMinX = ((lonBase + j) << lonShift) + Integer.MIN_VALUE;
        final int boxMaxY = boxMinY + (1 << latShift) - 1;
        final int boxMaxX = boxMinX + (1 << lonShift) - 1;

        relations[i * maxLonDelta + j] = (byte) boxToRelation.apply(RectangleComponent2D.createComponent(
            boxMinX, boxMaxX,
            boxMinY, boxMaxY)).ordinal();
      }
    }

    return new Component2DPredicate(
        latShift, lonShift,
        latBase, lonBase,
        maxLatDelta, maxLonDelta,
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
      if (delta >= 0 && delta < Component2DPredicate.ARITY) {
        return shift;
      }
    }
  }

  /** Create a predicate that checks whether points are within a component2D.
   *  @lucene.internal */
  public static Component2DPredicate createComponentPredicate(Component2D component) {
    final RectangleComponent2D boundingBox = component.getBoundingBox();
    final Function<RectangleComponent2D, PointValues.Relation> boxToRelation = box -> component.relate(
        box.minX, box.maxX, box.minY, box.maxY);
    return  createSubBoxes(boundingBox, boxToRelation, component);
  }

}
