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

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;

import static org.apache.lucene.spatial.geopoint.document.GeoPointField.geoCodedToPrefixCoded;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.prefixCodedToGeoCoded;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.getPrefixCodedShift;

/**
 * Decomposes a given {@link GeoPointMultiTermQuery} into a set of terms that represent the query criteria. The terms
 * are then enumerated by the {@link GeoPointTermQueryConstantScoreWrapper} and all docs whose GeoPoint fields match
 * the prefix terms or pass the {@link GeoPointMultiTermQuery.CellComparator#postFilter} criteria are returned in the
 * resulting DocIdSet.
 *
 *  @lucene.experimental
 */
final class GeoPointTermsEnum extends FilteredTermsEnum {
  private final short maxShift;
  private final BytesRefBuilder currentCellBRB = new BytesRefBuilder();
  private final BytesRefBuilder nextSubRangeBRB = new BytesRefBuilder();
  private final GeoPointMultiTermQuery.CellComparator relationImpl;

  private short shift;    // shift mask
  private long currStart; // range start as encoded long
  private long currEnd;   // range end as encoded long

  private final Range currentRange = new Range(-1, shift, true);;
  private final Range nextRange = new Range(-1, shift, true);
  private BytesRef currentCell;

  private boolean hasNext = false;
  private boolean withinOnly = false;
  private long lastWithin;

  public GeoPointTermsEnum(final TermsEnum tenum, final GeoPointMultiTermQuery query) {
    super(tenum);
    this.maxShift = query.maxShift;
    this.relationImpl = query.cellComparator;
    // start shift at maxShift value (from computeMaxShift)
    this.shift = maxShift;
    final long mask = (1L << shift) - 1;
    this.currStart = GeoPointField.encodeLatLon(query.minLat, query.minLon) & ~mask;
    this.currEnd = currStart | mask;
  }

  private boolean within(final double minLat, final double maxLat, final double minLon, final double maxLon) {
    return relationImpl.cellWithin(minLat, maxLat, minLon, maxLon);
  }

  private boolean boundary(final double minLat, final double maxLat, final double minLon, final double maxLon) {
    return shift == maxShift && relationImpl.cellIntersectsShape(minLat, maxLat, minLon, maxLon);
  }

  private boolean nextWithin() {
    if (withinOnly == false) {
      return false;
    }
    currStart += (1L << shift);
    setNextRange(false);
    currentRange.set(nextRange);
    hasNext = true;

    withinOnly = lastWithin != currStart;
    if (withinOnly == false) advanceVariables();
    return true;
  }

  private void nextRelation() {
    double minLon = GeoPointField.decodeLongitude(currStart);
    double minLat = GeoPointField.decodeLatitude(currStart);
    double maxLon;
    double maxLat;
    boolean isWithin;
    do {
      maxLon = GeoPointField.decodeLongitude(currEnd);
      maxLat = GeoPointField.decodeLatitude(currEnd);

      isWithin = false;
      // within or a boundary
      if (boundary(minLat, maxLat, minLon, maxLon) == true) {
        isWithin = within(minLat, maxLat, minLon, maxLon);
        final int m;
        if (isWithin == false || (m = shift % GeoPointField.PRECISION_STEP) == 0) {
          setNextRange(isWithin == false);
          advanceVariables();
          break;
        } else if (shift < 54) {
          withinOnly = true;
          shift = (short)(shift - m);
          lastWithin = currEnd & ~((1L << shift) - 1);
          setNextRange(false);
          break;
        }
      }

      // within cell but not at a depth factor of PRECISION_STEP
      if (isWithin == true || (relationImpl.cellIntersectsMBR(minLat, maxLat, minLon, maxLon) == true && shift != maxShift)) {
        // descend: currStart need not change since shift handles end of range
        currEnd = currStart | (1L<<--shift) - 1;
      } else {
        advanceVariables();
        minLon = GeoPointField.decodeLongitude(currStart);
        minLat = GeoPointField.decodeLatitude(currStart);
      }
    } while(shift < 63);
  }

  private void setNextRange(final boolean boundary) {
    nextRange.start = currStart;
    nextRange.shift = shift;
    nextRange.boundary = boundary;
  }

  private void advanceVariables() {
    /** set next variables */
    long shiftMask = 1L << shift;
    // pop-up if shift bit is set
    while ( (currStart & shiftMask) == shiftMask) {
      shiftMask = 1L << ++shift;
    }
    final long shiftMOne = shiftMask - 1;
    currStart = currStart & ~shiftMOne | shiftMask;
    currEnd = currStart | shiftMOne;
  }

  protected final BytesRef peek() {
    nextRange.fillBytesRef(nextSubRangeBRB);
    return nextSubRangeBRB.get();
  }

  protected void seek(long term, short res) {
    if (term < currStart && res < maxShift) {
      throw new IllegalArgumentException("trying to seek backwards");
    } else if (term == currStart) {
      return;
    }
    shift = res;
    currStart = term;
    currEnd = currStart | ((1L<<shift)-1);
    withinOnly = false;
  }

  protected void nextRange() {
    hasNext = false;
    currentRange.fillBytesRef(currentCellBRB);
    currentCell = currentCellBRB.get();
  }

  protected final boolean hasNext() {
    if (hasNext == true || nextWithin()) {
      return true;
    }
    nextRelation();
    if (currentRange.compareTo(nextRange) != 0) {
      currentRange.set(nextRange);
      return (hasNext = true);
    }
    return false;
  }

  @Override
  protected final BytesRef nextSeekTerm(BytesRef term) {
    while (hasNext()) {
      nextRange();
      if (term == null) {
        return currentCell;
      }

      final int comparison = term.compareTo(currentCell);
      if (comparison > 0) {
        seek(prefixCodedToGeoCoded(term), (short)(64 - getPrefixCodedShift(term)));
        continue;
      }
      return currentCell;
    }

    // no more sub-range enums available
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
    // range < term or range is null
    while (currentCell == null || term.compareTo(currentCell) > 0) {
      // no more ranges, be gone
      if (hasNext() == false) {
        return AcceptStatus.END;
      }

      // peek next range, if the range > term then seek
      final int peekCompare = term.compareTo(peek());
      if (peekCompare < 0) {
        return AcceptStatus.NO_AND_SEEK;
      } else if (peekCompare > 0) {
        seek(prefixCodedToGeoCoded(term), (short)(64 - getPrefixCodedShift(term)));
      }
      nextRange();
    }
    return AcceptStatus.YES;
  }

  /**
   * Returns true if the current range term is a boundary of the query shape
   */
  public boolean boundaryTerm() {
    if (currentCell == null) {
      throw new IllegalStateException("GeoPointTermsEnum empty or not initialized");
    }
    return currentRange.boundary;
  }

  protected boolean postFilter(final double lat, final double lon) {
    return relationImpl.postFilter(lat, lon);
  }

  protected final class Range implements Comparable<Range> {
    private short shift;
    private long start;
    private boolean boundary;

    public Range(final long start, final short shift, final boolean boundary) {
      this.boundary = boundary;
      this.start = start;
      this.shift = shift;    }

    /**
     * Encode as a BytesRef using a reusable object. This allows us to lazily create the BytesRef (which is
     * quite expensive), only when we need it.
     */
    protected void fillBytesRef(BytesRefBuilder result) {
      assert result != null;
      geoCodedToPrefixCoded(start, shift, result);
    }

    @Override
    public int compareTo(Range other) {
      final int result = Short.compare(this.shift, other.shift);
      if (result == 0) {
        return Long.compare(this.start, other.start);
      }
      return result;
    }

    protected void set(Range other) {
      this.start = other.start;
      this.shift = other.shift;
      this.boundary = other.boundary;
    }
  }
}
