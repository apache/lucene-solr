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
 * @deprecated Use the higher performance {@code LatLonPoint} queries instead.
 */
@Deprecated
abstract class GeoPointTermsEnum extends FilteredTermsEnum {
  protected final short maxShift;

  protected Range currentRange;
  protected BytesRef currentCell;
  protected final BytesRefBuilder currentCellBRB = new BytesRefBuilder();

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

  abstract protected boolean hasNext();

  protected boolean postFilter(final double lat, final double lon) {
    return relationImpl.postFilter(lat, lon);
  }

  /**
   * Internal class to represent a range along the space filling curve
   */
  class Range implements Comparable<Range> {
    protected short shift;
    protected long start;
    protected boolean boundary;

    Range(final long lower, final short shift, boolean boundary) {
      this.boundary = boundary;
      this.start = lower;
      this.shift = shift;
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

    protected int compare(long encoded, short shift) {
      final int result = Long.compare(this.start, encoded);
      if (result == 0) {
        return Short.compare(shift, this.shift);
      }
      return result;
    }
  }
}
