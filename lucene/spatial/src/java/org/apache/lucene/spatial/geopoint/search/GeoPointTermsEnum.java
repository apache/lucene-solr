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
package org.apache.lucene.spatial.geopoint.search;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;

import static org.apache.lucene.spatial.geopoint.document.GeoPointField.decodeLatitude;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.decodeLongitude;
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
  private final GeoPointMultiTermQuery.CellComparator relationImpl;
  private final BytesRefBuilder currentCellBRB;
  private final Range range;

  private short shift;    // shift mask
  private long start;     // range start as encoded long
  private long end;       // range end as encoded long
  private boolean hasNext = false;

  public GeoPointTermsEnum(final TermsEnum tenum, final GeoPointMultiTermQuery query) {
    super(tenum);
    this.maxShift = query.maxShift;
    this.relationImpl = query.cellComparator;
    // start shift at maxShift value (from computeMaxShift)
    this.shift = maxShift;
    final long mask = (1L << shift) - 1;
    this.start = query.minEncoded & ~mask;
    this.end = start | mask;
    this.currentCellBRB = new BytesRefBuilder();
    this.range = new Range(-1, shift, true);
  }

  private boolean nextRelation() {
    Relation relation;
    do {
      // within or a boundary
      if ((shift % GeoPointField.PRECISION_STEP) == 0 &&
          (relation = relationImpl.relate(decodeLatitude(start), decodeLatitude(end),
              decodeLongitude(start), decodeLongitude(end))) != Relation.CELL_OUTSIDE_QUERY) {
        // if at max depth or cell completely within
        if (shift == maxShift || relation == Relation.CELL_INSIDE_QUERY) {
          setRange(relation == Relation.CELL_CROSSES_QUERY);
          advanceVariables();
          return true;
        }
      }

      // within cell but not at a depth factor of PRECISION_STEP
      if (shift != maxShift && relationImpl.cellIntersectsMBR(start, end) == true) {
        // descend: start need not change since shift handles end of range
        end = start | (1L<<--shift) - 1;
      } else {
        advanceVariables();
      }
    } while(shift < 62);
    return false;
  }

  private void setRange(final boolean boundary) {
    range.start = start;
    range.shift = shift;
    range.boundary = boundary;
    hasNext = true;
  }

  private void advanceVariables() {
    /** set next variables */
    long shiftMask = 1L << shift;
    // pop-up if shift bit is set
    while ((start & shiftMask) != 0) {
      shiftMask = 1L << ++shift;
    }
    final long shiftMOne = shiftMask - 1;
    start = start & ~shiftMOne | shiftMask;
    end = start | shiftMOne;
  }

  private void seek(long term, short res) {
    if (term < start && res < maxShift) {
      throw new IllegalArgumentException("trying to seek backwards");
    } else if (term == start && res == shift) {
      return;
    }
    shift = res;
    start = term;
    end = start | ((1L<<shift)-1);
  }

  private final boolean hasNext() {
    if (hasNext == false) {
      return nextRelation();
    }
    return true;
  }

  @Override
  protected final BytesRef nextSeekTerm(BytesRef term) {
    if (hasNext() == false) {
      return null;
    }
    geoCodedToPrefixCoded(range.start, range.shift, currentCellBRB);
    hasNext = false;
    return currentCellBRB.get();
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
    final long encodedTerm = prefixCodedToGeoCoded(term);
    final short termShift = (short)(64-getPrefixCodedShift(term));
    // range < term
    while (range.compare(encodedTerm, termShift) < 0) {
      // no more ranges, be gone
      if (hasNext() == false) {
        return AcceptStatus.END;
      }

      // peek next range, if the range > term then seek
      final int peekCompare = range.compare(encodedTerm, termShift);
      if (peekCompare > 0) {
        return AcceptStatus.NO_AND_SEEK;
      } else if (peekCompare < 0) {
        seek(encodedTerm, termShift);
      }
      hasNext = false;
    }
    return AcceptStatus.YES;
  }

  /** Returns true if the current range term is a boundary of the query shape */
  protected boolean boundaryTerm() {
    if (range.start == -1) {
      throw new IllegalStateException("GeoPointTermsEnum empty or not initialized");
    }
    return range.boundary;
  }

  protected boolean postFilter(final double lat, final double lon) {
    return relationImpl.postFilter(lat, lon);
  }

  protected final class Range {
    private short shift;
    private long start;
    private boolean boundary;

    public Range(final long start, final short shift, final boolean boundary) {
      this.boundary = boundary;
      this.start = start;
      this.shift = shift;
    }

    private int compare(long encoded, short shift) {
      final int result = Long.compare(this.start, encoded);
      if (result == 0) {
        return Short.compare(shift, this.shift);
      }
      return result;
    }
  }
}
