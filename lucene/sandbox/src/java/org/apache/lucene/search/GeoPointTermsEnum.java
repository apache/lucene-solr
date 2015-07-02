package org.apache.lucene.search;

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

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.NumericUtils;

/**
 * computes all ranges along a space-filling curve that represents
 * the given bounding box and enumerates all terms contained within those ranges
 */
class GeoPointTermsEnum extends FilteredTermsEnum {
  protected final double minLon;
  protected final double minLat;
  protected final double maxLon;
  protected final double maxLat;

  private Range currentRange;
  private BytesRef currentLowerBound, currentUpperBound;

  private final ComputedRangesAttribute rangesAtt;

  private final LinkedList<Range> rangeBounds;

  private static final short DETAIL_LEVEL = 16;

  GeoPointTermsEnum(final TermsEnum tenum, AttributeSource atts, final double minLon, final double minLat,
                    final double maxLon, final double maxLat) {
    super(tenum);
    final long rectMinHash = GeoUtils.mortonHash(minLon, minLat);
    final long rectMaxHash = GeoUtils.mortonHash(maxLon, maxLat);
    this.minLon = GeoUtils.mortonUnhashLon(rectMinHash);
    this.minLat = GeoUtils.mortonUnhashLat(rectMinHash);
    this.maxLon = GeoUtils.mortonUnhashLon(rectMaxHash);
    this.maxLat = GeoUtils.mortonUnhashLat(rectMaxHash);

    this.rangesAtt = atts.addAttribute(ComputedRangesAttribute.class);
    this.rangeBounds = rangesAtt.ranges();

    if (rangeBounds.isEmpty()) {
      computeRange(0L, (short) (((GeoUtils.BITS) << 1) - 1));
      Collections.sort(rangeBounds);
    }
  }

  /**
   * entry point for recursively computing ranges
   */
  private final void computeRange(long term, final short shift) {
    final long split = term | (0x1L<<shift);
    final long upperMax = term | ((0x1L<<(shift+1))-1);
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
    final double minLon = GeoUtils.mortonUnhashLon(start);
    final double minLat = GeoUtils.mortonUnhashLat(start);
    final double maxLon = GeoUtils.mortonUnhashLon(end);
    final double maxLat = GeoUtils.mortonUnhashLat(end);

    final short level = (short)(62-res>>>1);

    // if cell is within and a factor of the precision step, add the range
    // if cell cellCrosses

    final boolean within = res% GeoPointField.PRECISION_STEP == 0 && cellWithin(minLon, minLat, maxLon, maxLat);
    if (within || (level == DETAIL_LEVEL && cellCrosses(minLon, minLat, maxLon, maxLat))) {
      rangeBounds.add(new Range(start, end, res, level, !within));
    } else if (level <= DETAIL_LEVEL && cellIntersects(minLon, minLat, maxLon, maxLat)) {
      computeRange(start, (short)(res - 1));
    }
  }

  protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    return GeoUtils.rectCrosses(minLon, minLat, maxLon, maxLat, this.minLon, this.minLat, this.maxLon, this.maxLat);
  }

  protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    return GeoUtils.rectWithin(minLon, minLat, maxLon, maxLat, this.minLon, this.minLat, this.maxLon, this.maxLat);
  }

  protected boolean cellIntersects(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    return GeoUtils.rectIntersects(minLon, minLat, maxLon, maxLat, this.minLon, this.minLat, this.maxLon, this.maxLat);
  }

  private void nextRange() {
    currentRange = rangeBounds.removeFirst();
    currentLowerBound = currentRange.lower;
    assert currentUpperBound == null || currentUpperBound.compareTo(currentRange.lower) <= 0 :
        "The current upper bound must be <= the new lower bound";

    currentUpperBound = currentRange.upper;
  }

  @Override
  protected final BytesRef nextSeekTerm(BytesRef term) {
    while (!rangeBounds.isEmpty()) {
      if (currentRange == null) {
        nextRange();
      }

      // if the new upper bound is before the term parameter, the sub-range is never a hit
      if (term != null && term.compareTo(currentUpperBound) > 0) {
        nextRange();
        if (!rangeBounds.isEmpty()) {
          continue;
        }
      }
      // never seek backwards, so use current term if lower bound is smaller
      return (term != null && term.compareTo(currentLowerBound) > 0) ?
          term : currentLowerBound;
    }

    // no more sub-range enums available
    assert rangeBounds.isEmpty();
    currentLowerBound = currentUpperBound = null;
    return null;
  }

  /**
   * The two-phase query approach. {@link #nextSeekTerm} is called to obtain the next term that matches a numeric
   * range of the bounding box. Those terms that pass the initial range filter are then compared against the
   * decoded min/max latitude and longitude values of the bounding box only if the range is not a "boundary" range
   * (e.g., a range that straddles the boundary of the bbox).
   * @param term term for candidate document
   * @return match status
   */
  @Override
  protected AcceptStatus accept(BytesRef term) {
    // validate value is in range
    while (currentUpperBound == null || term.compareTo(currentUpperBound) > 0) {
      if (rangeBounds.isEmpty())
        return AcceptStatus.END;
      // peek next sub-range, only seek if the current term is smaller than next lower bound
      if (term.compareTo(rangeBounds.getFirst().lower) < 0)
        return AcceptStatus.NO_AND_SEEK;
      // step forward to next range without seeking, as next lower range bound is less or equal current term
      nextRange();
    }

    // final-filter boundary ranges by bounding box
    if (currentRange.boundary) {
      final long val = NumericUtils.prefixCodedToLong(term);
      final double lon = GeoUtils.mortonUnhashLon(val);
      final double lat = GeoUtils.mortonUnhashLat(val);
      if (!GeoUtils.bboxContains(lon, lat, minLon, minLat, maxLon, maxLat)) {
        return AcceptStatus.NO;
      }
    }
    return AcceptStatus.YES;
  }

  public static interface ComputedRangesAttribute extends Attribute {
    public LinkedList<Range> ranges();
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public static final class ComputedRangesAttributeImpl extends AttributeImpl implements ComputedRangesAttribute {
    public final LinkedList<Range> rangeBounds = new LinkedList();

    @Override
    public LinkedList<Range> ranges() {
      return rangeBounds;
    }

    @Override
    public void clear() {
      rangeBounds.clear();
    }

    @Override
    public int hashCode() {
      return rangeBounds.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (!(other instanceof ComputedRangesAttributeImpl))
        return false;
      return rangeBounds.equals(((ComputedRangesAttributeImpl)other).rangeBounds);
    }

    @Override
    public void copyTo(AttributeImpl target) {
      final List<Range> targetRanges = ((ComputedRangesAttribute)target).ranges();
      targetRanges.clear();
      targetRanges.addAll(rangeBounds);
    }

    @Override
    public AttributeImpl clone() {
      ComputedRangesAttributeImpl c = (ComputedRangesAttributeImpl) super.clone();;
      copyTo(c);
      return c;
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(ComputedRangesAttribute.class, "rangeBounds", rangeBounds);
    }
  }

  /**
   * Internal class to represent a range along the space filling curve
   */
  private final class Range implements Comparable<Range> {
    final BytesRef lower;
    final BytesRef upper;
    final short level;
    final boolean boundary;

    Range(final long lower, final long upper, final short res, final short level, boolean boundary) {
      this.level = level;
      this.boundary = boundary;

      BytesRefBuilder brb = new BytesRefBuilder();
      NumericUtils.longToPrefixCodedBytes(lower, boundary ? 0 : res, brb);
      this.lower = brb.get();
      NumericUtils.longToPrefixCodedBytes(upper, boundary ? 0 : res, (brb = new BytesRefBuilder()));
      this.upper = brb.get();
    }

    @Override
    public final int compareTo(Range other) {
      return this.lower.compareTo(other.lower);
    }
  }
}
