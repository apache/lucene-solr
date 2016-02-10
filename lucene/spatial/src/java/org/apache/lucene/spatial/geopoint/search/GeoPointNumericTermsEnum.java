package org.apache.lucene.spatial.geopoint.search;

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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

/**
 * Decomposes a given {@link GeoPointMultiTermQuery} into a set of terms that represent the query criteria using
 * {@link org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding#NUMERIC} method defined by
 * {@link org.apache.lucene.analysis.NumericTokenStream}. The terms are then enumerated by the
 * {@link GeoPointTermQueryConstantScoreWrapper} and all docs whose GeoPoint fields match the prefix terms or
 * pass the {@link GeoPointMultiTermQuery.CellComparator#postFilter} criteria are returned in the resulting DocIdSet.
 *
 *  @lucene.experimental
 */
@Deprecated
final class GeoPointNumericTermsEnum extends GeoPointTermsEnum {
  private final List<Range> rangeBounds = new LinkedList<>();

  // detail level should be a factor of PRECISION_STEP limiting the depth of recursion (and number of ranges)
  private final short DETAIL_LEVEL;

  GeoPointNumericTermsEnum(final TermsEnum tenum, final GeoPointMultiTermQuery query) {
    super(tenum, query);
    DETAIL_LEVEL = (short)(((GeoEncodingUtils.BITS<<1)-this.maxShift)/2);
    computeRange(0L, (short) (((GeoEncodingUtils.BITS) << 1) - 1));
    assert rangeBounds.isEmpty() == false;
    Collections.sort(rangeBounds);
  }

  /**
   * entry point for recursively computing ranges
   */
  private final void computeRange(long term, final short shift) {
    final long split = term | (0x1L<<shift);
    assert shift < 64;
    final long upperMax;
    if (shift < 63) {
      upperMax = term | ((1L << (shift+1))-1);
    } else {
      upperMax = 0xffffffffffffffffL;
    }
    final long lowerMax = split-1;

    relateAndRecurse(term, lowerMax, shift);
    relateAndRecurse(split, upperMax, shift);
  }

  /**
   * recurse to higher level precision cells to find ranges along the space-filling curve that fall within the
   * query box
   *
   * @param start starting value on the space-filling curve for a cell at a given res
   * @param end ending value on the space-filling curve for a cell at a given res
   * @param res spatial res represented as a bit shift (MSB is lower res)
   */
  private void relateAndRecurse(final long start, final long end, final short res) {
    final double minLon = GeoEncodingUtils.mortonUnhashLon(start);
    final double minLat = GeoEncodingUtils.mortonUnhashLat(start);
    final double maxLon = GeoEncodingUtils.mortonUnhashLon(end);
    final double maxLat = GeoEncodingUtils.mortonUnhashLat(end);

    final short level = (short)((GeoEncodingUtils.BITS<<1)-res>>>1);

    // if cell is within and a factor of the precision step, or it crosses the edge of the shape add the range
    final boolean within = res % GeoPointField.PRECISION_STEP == 0 && relationImpl.cellWithin(minLon, minLat, maxLon, maxLat);
    if (within || (level == DETAIL_LEVEL && relationImpl.cellIntersectsShape(minLon, minLat, maxLon, maxLat))) {
      final short nextRes = (short)(res-1);
      if (nextRes % GeoPointField.PRECISION_STEP == 0) {
        rangeBounds.add(new Range(start, nextRes, !within));
        rangeBounds.add(new Range(start|(1L<<nextRes), nextRes, !within));
      } else {
        rangeBounds.add(new Range(start, res, !within));
      }
    } else if (level < DETAIL_LEVEL && relationImpl.cellIntersectsMBR(minLon, minLat, maxLon, maxLat)) {
      computeRange(start, (short) (res - 1));
    }
  }

  @Override
  protected final BytesRef peek() {
    rangeBounds.get(0).fillBytesRef(this.nextSubRangeBRB);
    return nextSubRangeBRB.get();
  }

  @Override
  protected void nextRange() {
    currentRange = rangeBounds.remove(0);
    super.nextRange();
  }

  @Override
  protected final BytesRef nextSeekTerm(BytesRef term) {
    while (hasNext()) {
      if (currentRange == null) {
        nextRange();
      }
      // if the new upper bound is before the term parameter, the sub-range is never a hit
      if (term != null && term.compareTo(currentCell) > 0) {
        nextRange();
        if (!rangeBounds.isEmpty()) {
          continue;
        }
      }
      // never seek backwards, so use current term if lower bound is smaller
      return (term != null && term.compareTo(currentCell) > 0) ? term : currentCell;
    }

    // no more sub-range enums available
    assert rangeBounds.isEmpty();
    return null;
  }

  @Override
  protected final boolean hasNext() {
    return rangeBounds.isEmpty() == false;
  }

  /**
   * Internal class to represent a range along the space filling curve
   */
  protected final class Range extends BaseRange {
    Range(final long lower, final short shift, boolean boundary) {
      super(lower, shift, boundary);
    }

    /**
     * Encode as a BytesRef using a reusable object. This allows us to lazily create the BytesRef (which is
     * quite expensive), only when we need it.
     */
    @Override
    protected void fillBytesRef(BytesRefBuilder result) {
      assert result != null;
      NumericUtils.longToPrefixCoded(start, shift, result);
    }
  }
}
