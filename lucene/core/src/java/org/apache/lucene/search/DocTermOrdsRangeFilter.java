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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * A range filter built on top of a cached multi-valued term field (in {@link FieldCache}).
 * 
 * <p>Like {@link FieldCacheRangeFilter}, this is just a specialized range query versus
 *    using a TermRangeQuery with {@link DocTermOrdsRewriteMethod}: it will only do
 *    two ordinal to term lookups.</p>
 */

public abstract class DocTermOrdsRangeFilter extends Filter {
  final String field;
  final BytesRef lowerVal;
  final BytesRef upperVal;
  final boolean includeLower;
  final boolean includeUpper;
  
  private DocTermOrdsRangeFilter(String field, BytesRef lowerVal, BytesRef upperVal, boolean includeLower, boolean includeUpper) {
    this.field = field;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
  }
  
  /** This method is implemented for each data type */
  @Override
  public abstract DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException;
  
  /**
   * Creates a BytesRef range filter using {@link FieldCache#getTermsIndex}. This works with all
   * fields containing zero or one term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static DocTermOrdsRangeFilter newBytesRefRange(String field, BytesRef lowerVal, BytesRef upperVal, boolean includeLower, boolean includeUpper) {
    return new DocTermOrdsRangeFilter(field, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        final SortedSetDocValues docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), field);
        final long lowerPoint = lowerVal == null ? -1 : docTermOrds.lookupTerm(lowerVal);
        final long upperPoint = upperVal == null ? -1 : docTermOrds.lookupTerm(upperVal);

        final long inclusiveLowerPoint, inclusiveUpperPoint;

        // Hints:
        // * binarySearchLookup returns -1, if value was null.
        // * the value is <0 if no exact hit was found, the returned value
        //   is (-(insertion point) - 1)
        if (lowerPoint == -1 && lowerVal == null) {
          inclusiveLowerPoint = 0;
        } else if (includeLower && lowerPoint >= 0) {
          inclusiveLowerPoint = lowerPoint;
        } else if (lowerPoint >= 0) {
          inclusiveLowerPoint = lowerPoint + 1;
        } else {
          inclusiveLowerPoint = Math.max(0, -lowerPoint - 1);
        }
        
        if (upperPoint == -1 && upperVal == null) {
          inclusiveUpperPoint = Long.MAX_VALUE;  
        } else if (includeUpper && upperPoint >= 0) {
          inclusiveUpperPoint = upperPoint;
        } else if (upperPoint >= 0) {
          inclusiveUpperPoint = upperPoint - 1;
        } else {
          inclusiveUpperPoint = -upperPoint - 2;
        }      

        if (inclusiveUpperPoint < 0 || inclusiveLowerPoint > inclusiveUpperPoint) {
          return DocIdSet.EMPTY_DOCIDSET;
        }
        
        assert inclusiveLowerPoint >= 0 && inclusiveUpperPoint >= 0;
        
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected final boolean matchDoc(int doc) {
            docTermOrds.setDocument(doc);
            long ord;
            while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              if (ord > inclusiveUpperPoint) {
                return false;
              } else if (ord >= inclusiveLowerPoint) {
                return true;
              }
            }
            return false;
          }
        };
      }
    };
  }
  
  @Override
  public final String toString() {
    final StringBuilder sb = new StringBuilder(field).append(":");
    return sb.append(includeLower ? '[' : '{')
      .append((lowerVal == null) ? "*" : lowerVal.toString())
      .append(" TO ")
      .append((upperVal == null) ? "*" : upperVal.toString())
      .append(includeUpper ? ']' : '}')
      .toString();
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DocTermOrdsRangeFilter)) return false;
    DocTermOrdsRangeFilter other = (DocTermOrdsRangeFilter) o;

    if (!this.field.equals(other.field)
        || this.includeLower != other.includeLower
        || this.includeUpper != other.includeUpper
    ) { return false; }
    if (this.lowerVal != null ? !this.lowerVal.equals(other.lowerVal) : other.lowerVal != null) return false;
    if (this.upperVal != null ? !this.upperVal.equals(other.upperVal) : other.upperVal != null) return false;
    return true;
  }
  
  @Override
  public final int hashCode() {
    int h = field.hashCode();
    h ^= (lowerVal != null) ? lowerVal.hashCode() : 550356204;
    h = (h << 1) | (h >>> 31);  // rotate to distinguish lower from upper
    h ^= (upperVal != null) ? upperVal.hashCode() : -1674416163;
    h ^= (includeLower ? 1549299360 : -365038026) ^ (includeUpper ? 1721088258 : 1948649653);
    return h;
  }

  /** Returns the field name for this filter */
  public String getField() { return field; }

  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesLower() { return includeLower; }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesUpper() { return includeUpper; }

  /** Returns the lower value of this range filter */
  public BytesRef getLowerVal() { return lowerVal; }

  /** Returns the upper value of this range filter */
  public BytesRef getUpperVal() { return upperVal; }
}
