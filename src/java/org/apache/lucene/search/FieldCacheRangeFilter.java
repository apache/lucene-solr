package org.apache.lucene.search;
/**
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

import org.apache.lucene.index.IndexReader;

/**
 * A range filter built on top of a cached single term field (in FieldCache).
 * 
 * FieldCacheRangeFilter builds a single cache for the field the first time it is used.
 * 
 * Each subsequent FieldCacheRangeFilter on the same field then reuses this cache,
 * even if the range itself changes. 
 * 
 * This means that FieldCacheRangeFilter is much faster (sometimes more than 100x as fast) 
 * as building a RangeFilter (or ConstantScoreRangeQuery on a RangeFilter) for each query.
 * However, if the range never changes it is slower (around 2x as slow) than building a 
 * CachingWrapperFilter on top of a single RangeFilter.
 * 
 * As with all FieldCache based functionality, FieldCacheRangeFilter is only valid for 
 * fields which contain zero or one terms for each document. Thus it works on dates, 
 * prices and other single value fields but will not work on regular text fields. It is
 * preferable to use an UN_TOKENIZED field to ensure that there is only a single term. 
 *
 * Also, collation is done at the time the FieldCache is built; to change 
 * collation you need to override the getFieldCache() method to change the underlying cache. 
 */

public class FieldCacheRangeFilter extends Filter {
  private String field;
  private String lowerVal;
  private String upperVal;
  private boolean includeLower;
  private boolean includeUpper;
  
  public FieldCacheRangeFilter(
        String field, 
        String lowerVal,
        String upperVal,
        boolean includeLower,
        boolean includeUpper) {
    this.field = field;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
  }

  public FieldCache getFieldCache() {
    return FieldCache.DEFAULT;
  }
  
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    return new RangeMultiFilterDocIdSet(getFieldCache().getStringIndex(reader, field));
  }
  
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(field);
    buffer.append(":");
    buffer.append(includeLower ? "[" : "{");
    if (null != lowerVal) {
      buffer.append(lowerVal);
    }
    buffer.append("-");
    if (null != upperVal) {
      buffer.append(upperVal);
    }
    buffer.append(includeUpper ? "]" : "}");
    return buffer.toString();
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FieldCacheRangeFilter)) return false;
    FieldCacheRangeFilter other = (FieldCacheRangeFilter) o;

    if (!this.field.equals(other.field)
        || this.includeLower != other.includeLower
        || this.includeUpper != other.includeUpper
    ) { return false; }
    if (this.lowerVal != null ? !this.lowerVal.equals(other.lowerVal) : other.lowerVal != null) return false;
    if (this.upperVal != null ? !this.upperVal.equals(other.upperVal) : other.upperVal != null) return false;
    return true;
  }
  
  public int hashCode() {
    int h = field.hashCode();
    h ^= lowerVal != null ? lowerVal.hashCode() : 550356204;
    h = (h << 1) | (h >>> 31);  // rotate to distinguish lower from upper
    h ^= (upperVal != null ? (upperVal.hashCode()) : -1674416163);
    h ^= (includeLower ? 1549299360 : -365038026)
    ^ (includeUpper ? 1721088258 : 1948649653);

    return h;
  }

  protected class RangeMultiFilterDocIdSet extends DocIdSet {
    private int inclusiveLowerPoint;
    private int inclusiveUpperPoint;
    private FieldCache.StringIndex fcsi;
    
    public RangeMultiFilterDocIdSet(FieldCache.StringIndex fcsi) {
      this.fcsi = fcsi;
      initialize();
    }
    
    private void initialize() {
      int lowerPoint = fcsi.binarySearchLookup(lowerVal);
      if (includeLower && lowerPoint >= 0) {
        inclusiveLowerPoint = lowerPoint;
      } else if (lowerPoint >= 0) {
        inclusiveLowerPoint = lowerPoint+1;
      } else {
        inclusiveLowerPoint = -lowerPoint-1;
      }
      int upperPoint = fcsi.binarySearchLookup(upperVal);
      if (includeUpper && upperPoint >= 0) {
        inclusiveUpperPoint = upperPoint;
      } else if (upperPoint >= 0) {
        inclusiveUpperPoint = upperPoint - 1;
      } else {
        inclusiveUpperPoint = -upperPoint - 2;
      }
    }
    
    public DocIdSetIterator iterator() {
      return new RangeMultiFilterIterator();
    }
    
    protected class RangeMultiFilterIterator extends DocIdSetIterator {
      private int doc = -1;
      
      public int doc() {
        return doc;
      }

      public boolean next() {
        try {
          do {
            doc++;
          } while (fcsi.order[doc] > inclusiveUpperPoint 
                   || fcsi.order[doc] < inclusiveLowerPoint);
          return true;
        } catch (ArrayIndexOutOfBoundsException e) {
          doc = Integer.MAX_VALUE;
          return false;
        }
      }

      public boolean skipTo(int target) {
        try {
          doc = target;
          while (fcsi.order[doc] > inclusiveUpperPoint 
                || fcsi.order[doc] < inclusiveLowerPoint) { 
            doc++;
          }
          return true;
        } catch (ArrayIndexOutOfBoundsException e) {
          doc = Integer.MAX_VALUE;
          return false;
        }
      }
    }
  }
}
