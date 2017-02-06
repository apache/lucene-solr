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

import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;

import static org.apache.lucene.spatial.geopoint.document.GeoPointField.decodeLatitude;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.decodeLongitude;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.geoCodedToPrefixCoded;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.prefixCodedToGeoCoded;
import static org.apache.lucene.spatial.geopoint.document.GeoPointField.getPrefixCodedShift;

/**
 * Decomposes a given {@link GeoPointMultiTermQuery} into a set of terms that represent the query criteria using
 * {@link org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding#PREFIX} method defined by
 * {@link GeoPointField}. The terms are then enumerated by the
 * {@link GeoPointTermQueryConstantScoreWrapper} and all docs whose GeoPoint fields match the prefix terms or pass
 * the {@link GeoPointMultiTermQuery.CellComparator#postFilter} criteria are returned in the
 * resulting DocIdSet.
 *
 *  @lucene.experimental
 *  @deprecated Use the higher performance {@code LatLonPoint} queries instead.
 */
@Deprecated
final class GeoPointPrefixTermsEnum extends GeoPointTermsEnum {
  private short shift;

  // current range as long
  private long start;
  private long end;

  private boolean hasNext = false;

  public GeoPointPrefixTermsEnum(final TermsEnum tenum, final GeoPointMultiTermQuery query) {
    super(tenum, query);
    this.currentRange = new Range(-1, shift, true);
    // start shift at maxShift value (from computeMaxShift)
    this.shift = maxShift;
    final long mask = (1L << shift) - 1;
    this.start = query.minEncoded & ~mask;
    this.end = start | mask;
  }

  private boolean nextRelation() {
    PointValues.Relation relation;
    do {
      // within or a boundary
      if ((shift % GeoPointField.PRECISION_STEP) == 0 &&
          (relation = relationImpl.relate(decodeLatitude(start), decodeLatitude(end),
              decodeLongitude(start), decodeLongitude(end))) != PointValues.Relation.CELL_OUTSIDE_QUERY) {
        // if at max depth or cell completely within
        if (shift == maxShift || relation == PointValues.Relation.CELL_INSIDE_QUERY) {
          setRange(relation == PointValues.Relation.CELL_CROSSES_QUERY);
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
    currentRange.start = start;
    currentRange.shift = shift;
    currentRange.boundary = boundary;
    hasNext = true;
  }

  private void advanceVariables() {
    /** set next variables */
    long shiftMask = 1L << shift;
    // pop-up if shift bit is set
    while ( (start & shiftMask) == shiftMask) {
      shiftMask = 1L << ++shift;
    }
    final long shiftMOne = shiftMask - 1;
    start = start & ~shiftMOne | shiftMask;
    end = start | shiftMOne;
  }

  protected void seek(long term, short res) {
    if (term < start && res < maxShift) {
      throw new IllegalArgumentException("trying to seek backwards");
    } else if (term == start) {
      return;
    }
    shift = res;
    start = term;
    end = start | ((1L<<shift)-1);
  }

  @Override
  protected final boolean hasNext() {
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
    geoCodedToPrefixCoded(currentRange.start, currentRange.shift, currentCellBRB);
    hasNext = false;
    return currentCellBRB.get();
  }

  @Override
  protected AcceptStatus accept(BytesRef term) {
    final long encodedTerm = prefixCodedToGeoCoded(term);
    final short termShift = (short)(64-getPrefixCodedShift(term));
    // range < term or range is null
    while (currentRange.compare(encodedTerm, termShift) < 0) {
      // no more ranges, be gone
      if (hasNext() == false) {
        return AcceptStatus.END;
      }

      // peek next range, if the range > term then seek
      final int peekCompare = currentRange.compare(encodedTerm, termShift);
      if (peekCompare > 0) {
        return AcceptStatus.NO_AND_SEEK;
      } else if (peekCompare < 0) {
        seek(encodedTerm, termShift);
      }
      hasNext = false;
    }
    return AcceptStatus.YES;
  }

  @Override
  public boolean boundaryTerm() {
    if (currentRange.start == -1) {
      throw new IllegalStateException("GeoPointTermsEnum empty or not initialized");
    }
    return currentRange.boundary;
  }
}
