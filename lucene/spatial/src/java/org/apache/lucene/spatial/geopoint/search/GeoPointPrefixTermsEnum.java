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

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.util.GeoEncodingUtils;

import static org.apache.lucene.spatial.util.GeoEncodingUtils.mortonHash;
import static org.apache.lucene.spatial.util.GeoEncodingUtils.mortonUnhashLat;
import static org.apache.lucene.spatial.util.GeoEncodingUtils.mortonUnhashLon;
import static org.apache.lucene.spatial.util.GeoEncodingUtils.geoCodedToPrefixCoded;
import static org.apache.lucene.spatial.util.GeoEncodingUtils.prefixCodedToGeoCoded;
import static org.apache.lucene.spatial.util.GeoEncodingUtils.getPrefixCodedShift;

/**
 * Decomposes a given {@link GeoPointMultiTermQuery} into a set of terms that represent the query criteria using
 * {@link org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding#PREFIX} method defined by
 * {@link GeoPointField}. The terms are then enumerated by the
 * {@link GeoPointTermQueryConstantScoreWrapper} and all docs whose GeoPoint fields match the prefix terms or pass
 * the {@link GeoPointMultiTermQuery.CellComparator#postFilter} criteria are returned in the
 * resulting DocIdSet.
 *
 *  @lucene.experimental
 */
final class GeoPointPrefixTermsEnum extends GeoPointTermsEnum {
  private final long start;

  private short shift;

  // current range as long
  private long currStart;
  private long currEnd;

  private final Range nextRange = new Range(-1, shift, true);

  private boolean hasNext = false;

  private boolean withinOnly = false;
  private long lastWithin;

  public GeoPointPrefixTermsEnum(final TermsEnum tenum, final GeoPointMultiTermQuery query) {
    super(tenum, query);
    this.start = mortonHash(query.minLon, query.minLat);
    this.currentRange = new Range(0, shift, true);
    // start shift at maxShift value (from computeMaxShift)
    this.shift = maxShift;
    final long mask = (1L << shift) - 1;
    this.currStart = start & ~mask;
    this.currEnd = currStart | mask;
  }

  private boolean within(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    return relationImpl.cellWithin(minLon, minLat, maxLon, maxLat);
  }

  private boolean boundary(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    return shift == maxShift && relationImpl.cellIntersectsShape(minLon, minLat, maxLon, maxLat);
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
    double minLon = mortonUnhashLon(currStart);
    double minLat = mortonUnhashLat(currStart);
    double maxLon;
    double maxLat;
    boolean isWithin;
    do {
      maxLon = mortonUnhashLon(currEnd);
      maxLat = mortonUnhashLat(currEnd);

      // within or a boundary
      if ((isWithin = within(minLon, minLat, maxLon, maxLat) == true) || boundary(minLon, minLat, maxLon, maxLat) == true) {
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
      if (isWithin == true || (relationImpl.cellIntersectsMBR(minLon, minLat, maxLon , maxLat) == true && shift != maxShift)) {
        // descend: currStart need not change since shift handles end of range
        currEnd = currStart | (1L<<--shift) - 1;
      } else {
        advanceVariables();
        minLon = mortonUnhashLon(currStart);
        minLat = mortonUnhashLat(currStart);
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

  @Override
  protected final BytesRef peek() {
    nextRange.fillBytesRef(nextSubRangeBRB);
    return super.peek();
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

  @Override
  protected void nextRange() {
    hasNext = false;
    super.nextRange();
  }

  @Override
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
        seek(GeoEncodingUtils.prefixCodedToGeoCoded(term), (short)(64-GeoEncodingUtils.getPrefixCodedShift(term)));
        continue;
      }
      return currentCell;
    }

    // no more sub-range enums available
    return null;
  }

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

  protected final class Range extends BaseRange {
    public Range(final long start, final short res, final boolean boundary) {
      super(start, res, boundary);
    }

    @Override
    protected void fillBytesRef(BytesRefBuilder result) {
      assert result != null;
      geoCodedToPrefixCoded(start, shift, result);
    }
  }
}
