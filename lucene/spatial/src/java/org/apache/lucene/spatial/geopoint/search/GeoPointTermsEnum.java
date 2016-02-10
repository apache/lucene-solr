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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.geopoint.search.GeoPointMultiTermQuery.CellComparator;

/**
 * Base class for {@link GeoPointNumericTermsEnum} and {@link GeoPointPrefixTermsEnum} which compares
 * candidate GeoPointField encoded terms against terms matching the defined query criteria.
 *
 *  @lucene.experimental
 */
abstract class GeoPointTermsEnum extends FilteredTermsEnum {
  protected final short maxShift;

  protected BaseRange currentRange;
  protected BytesRef currentCell;
  protected final BytesRefBuilder currentCellBRB = new BytesRefBuilder();
  protected final BytesRefBuilder nextSubRangeBRB = new BytesRefBuilder();

  protected final CellComparator relationImpl;

  GeoPointTermsEnum(final TermsEnum tenum, final GeoPointMultiTermQuery query) {
    super(tenum);
    this.maxShift = query.maxShift;
    this.relationImpl = query.cellComparator;
  }

  static GeoPointTermsEnum newInstance(final TermsEnum terms, final GeoPointMultiTermQuery query) {
    if (query.termEncoding == TermEncoding.PREFIX) {
      return new GeoPointPrefixTermsEnum(terms, query);
    } else if (query.termEncoding == TermEncoding.NUMERIC) {
      return new GeoPointNumericTermsEnum(terms, query);
    }
    throw new IllegalArgumentException("Invalid GeoPoint TermEncoding " + query.termEncoding);
  }

  public boolean boundaryTerm() {
    if (currentCell == null) {
      throw new IllegalStateException("GeoPointTermsEnum empty or not initialized");
    }
    return currentRange.boundary;
  }

  protected BytesRef peek() {
    return nextSubRangeBRB.get();
  }

  abstract protected boolean hasNext();

  protected void nextRange() {
    currentRange.fillBytesRef(currentCellBRB);
    currentCell = currentCellBRB.get();
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
    while (currentCell == null || term.compareTo(currentCell) > 0) {
      if (hasNext() == false) {
        return AcceptStatus.END;
      }
      // peek next sub-range, only seek if the current term is smaller than next lower bound
      if (term.compareTo(peek()) < 0) {
        return AcceptStatus.NO_AND_SEEK;
      }
      // step forward to next range without seeking, as next range is less or equal current term
      nextRange();
    }

    return AcceptStatus.YES;
  }

  protected boolean postFilter(final double lon, final double lat) {
    return relationImpl.postFilter(lon, lat);
  }

  /**
   * Internal class to represent a range along the space filling curve
   */
  abstract class BaseRange implements Comparable<BaseRange> {
    protected short shift;
    protected long start;
    protected boolean boundary;

    BaseRange(final long lower, final short shift, boolean boundary) {
      this.boundary = boundary;
      this.start = lower;
      this.shift = shift;
    }

    /**
     * Encode as a BytesRef using a reusable object. This allows us to lazily create the BytesRef (which is
     * quite expensive), only when we need it.
     */
    abstract protected void fillBytesRef(BytesRefBuilder result);

    @Override
    public int compareTo(BaseRange other) {
      final int result = Short.compare(this.shift, other.shift);
      if (result == 0) {
        return Long.compare(this.start, other.start);
      }
      return result;
    }

    protected void set(BaseRange other) {
      this.start = other.start;
      this.shift = other.shift;
      this.boundary = other.boundary;
    }
  }
}
