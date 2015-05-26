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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.ToStringUtils;

/** Implements a simple bounding box query on a GeoPoint field. This is inspired by
 * {@link org.apache.lucene.search.NumericRangeQuery} and is implemented using a
 * two phase approach. First, candidate terms are queried using a numeric
 * range based on the morton codes of the min and max lat/lon pairs. Terms
 * passing this initial filter are passed to a final check that verifies whether
 * the decoded lat/lon falls within (or on the boundary) of the query bounding box.
 * The value comparisons are subject to a precision tolerance defined in
 * {@value org.apache.lucene.util.GeoUtils#TOLERANCE}
 *
 * NOTES:
 *    1.  All latitude/longitude values must be in decimal degrees.
 *    2.  Complex computational geometry (e.g., dateline wrapping) is not supported
 *    3.  For more advanced GeoSpatial indexing and query operations see spatial module
 *    4.  This is well suited for small rectangles, large bounding boxes, could result
 *        in visiting every term in terms dictionary (see LUCENE-6481)
 *
 * @lucene.experimental
 */
public class GeoPointInBBoxQuery extends MultiTermQuery {
  // simple bounding box optimization - no objects used to avoid dependencies
  protected final double minLon;
  protected final double minLat;
  protected final double maxLon;
  protected final double maxLat;

  private static final short DETAIL_LEVEL = 16;

  /**
   * Constructs a new GeoBBoxQuery that will match encoded GeoPoint terms that fall within or on the boundary
   * of the bounding box defined by the input parameters
   * @param field the field name
   * @param minLon lower longitude (x) value of the bounding box
   * @param minLat lower latitude (y) value of the bounding box
   * @param maxLon upper longitude (x) value of the bounding box
   * @param maxLat upper latitude (y) value of the bounding box
   */
  public GeoPointInBBoxQuery(final String field, final double minLon, final double minLat, final double maxLon, final double maxLat) {
    super(field);
    this.minLon = minLon;
    this.minLat = minLat;
    this.maxLon = maxLon;
    this.maxLat = maxLat;
  }

  @Override @SuppressWarnings("unchecked")
  protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
    final Long min = GeoUtils.mortonHash(minLon, minLat);
    final Long max = Math.abs(GeoUtils.mortonHash(maxLon, maxLat));
    if (min != null && max != null &&  min.compareTo(max) > 0) {
      return TermsEnum.EMPTY;
    }
    return new GeoBBoxTermsEnum(terms.iterator());
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GeoPointInBBoxQuery that = (GeoPointInBBoxQuery) o;

    if (Double.compare(that.maxLat, maxLat) != 0) return false;
    if (Double.compare(that.maxLon, maxLon) != 0) return false;
    if (Double.compare(that.minLat, minLat) != 0) return false;
    if (Double.compare(that.minLon, minLon) != 0) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(minLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (!getField().equals(field)) sb.append(getField()).append(':');
    return sb.append(" Lower Left: [")
        .append(minLon)
        .append(',')
        .append(minLat)
        .append(']')
        .append(" Upper Right: [")
        .append(maxLon)
        .append(',')
        .append(maxLat)
        .append("]")
        .append(ToStringUtils.boost(getBoost()))
        .toString();
  }

  /**
   * computes all ranges along a space-filling curve that represents
   * the given bounding box and enumerates all terms contained within those ranges
   */
  protected class GeoBBoxTermsEnum extends FilteredTermsEnum {
    private Range currentRange;
    private BytesRef currentLowerBound, currentUpperBound;
    private final LinkedList<Range> rangeBounds = new LinkedList<>();

    GeoBBoxTermsEnum(final TermsEnum tenum) {
      super(tenum);
      computeRange(0L, (short) (((GeoUtils.BITS) << 1) - 1));
      Collections.sort(rangeBounds);
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
     * @return
     */
    private void relateAndRecurse(final long start, final long end, final short res) {
      final double minLon = GeoUtils.mortonUnhash(start, true);
      final double minLat = GeoUtils.mortonUnhash(start, false);
      final double maxLon = GeoUtils.mortonUnhash(end, true);
      final double maxLat = GeoUtils.mortonUnhash(end, false);

      final short level = (short)(62-res>>>1);

      final boolean within = isWithin(minLon, minLat, maxLon, maxLat);
      final boolean bboxIntersects = (within) ? true : intersects(minLon, minLat, maxLon, maxLat);

      if ((within && res%GeoPointField.PRECISION_STEP == 0) || (bboxIntersects && level == DETAIL_LEVEL)) {
        rangeBounds.add(new Range(start, end, res, level, !within));
      } else if (bboxIntersects) {
        computeRange(start, (short)(res - 1));
      }
    }

    protected boolean intersects(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoUtils.rectIntersects(minLon, minLat, maxLon, maxLat, GeoPointInBBoxQuery.this.minLon,
          GeoPointInBBoxQuery.this.minLat, GeoPointInBBoxQuery.this.maxLon, GeoPointInBBoxQuery.this.maxLat);
    }

    protected boolean isWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
      return GeoUtils.rectIsWithin(minLon, minLat, maxLon, maxLat, GeoPointInBBoxQuery.this.minLon,
          GeoPointInBBoxQuery.this.minLat, GeoPointInBBoxQuery.this.maxLon, GeoPointInBBoxQuery.this.maxLat);
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
        if (currentRange == null)
          nextRange();

        // if the new upper bound is before the term parameter, the sub-range is never a hit
        if (term != null && term.compareTo(currentUpperBound) > 0) {
          nextRange();
	        if (!rangeBounds.isEmpty())
	          continue;
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
        final double lon = GeoUtils.mortonUnhash(val, true);
        final double lat = GeoUtils.mortonUnhash(val, false);
        if (!GeoUtils.bboxContains(lon, lat, minLon, minLat, maxLon, maxLat)) {
          return AcceptStatus.NO;
        }
      }
      return AcceptStatus.YES;
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
}
