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
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.document.NumericField; // for javadocs

/**
 * A range filter built on top of a cached single term field (in {@link FieldCache}).
 * 
 * <p>FieldCacheRangeFilter builds a single cache for the field the first time it is used.
 * Each subsequent FieldCacheRangeFilter on the same field then reuses this cache,
 * even if the range itself changes. 
 * 
 * <p>This means that FieldCacheRangeFilter is much faster (sometimes more than 100x as fast) 
 * as building a {@link TermRangeFilter} (or {@link ConstantScoreRangeQuery} on a {@link TermRangeFilter})
 * for each query, if using a {@link #newStringRange}. However, if the range never changes it
 * is slower (around 2x as slow) than building a CachingWrapperFilter on top of a single TermRangeFilter.
 *
 * For numeric data types, this filter may be significantly faster than {@link NumericRangeFilter}.
 * Furthermore, it does not need the numeric values encoded by {@link NumericField}. But
 * it has the problem that it only works with exact one value/document (see below).
 *
 * <p>As with all {@link FieldCache} based functionality, FieldCacheRangeFilter is only valid for 
 * fields which exact one term for each document (except for {@link #newStringRange}
 * where 0 terms are also allowed). Due to a restriction of {@link FieldCache}, for numeric ranges
 * all terms that do not have a numeric value, 0 is assumed.
 *
 * <p>Thus it works on dates, prices and other single value fields but will not work on
 * regular text fields. It is preferable to use a <code>NOT_ANALYZED</code> field to ensure that
 * there is only a single term. 
 *
 * <p>This class does not have an constructor, use one of the static factory methods available,
 * that create a correct instance for different data types supported by {@link FieldCache}.
 */

public abstract class FieldCacheRangeFilter extends Filter {
  final String field;
  final FieldCache.Parser parser;
  final Object lowerVal;
  final Object upperVal;
  final boolean includeLower;
  final boolean includeUpper;
  
  private FieldCacheRangeFilter(String field, FieldCache.Parser parser, Object lowerVal, Object upperVal, boolean includeLower, boolean includeUpper) {
    this.field = field;
    this.parser = parser;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
  }
  
  /** This method is implemented for each data type */
  public abstract DocIdSet getDocIdSet(IndexReader reader) throws IOException;

  /**
   * Creates a string range query using {@link FieldCache#getStringIndex}. This works with all
   * fields containing zero or one term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newStringRange(String field, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter(field, null, lowerVal, upperVal, includeLower, includeUpper) {
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final FieldCache.StringIndex fcsi = FieldCache.DEFAULT.getStringIndex(reader, field);
        final int lowerPoint = fcsi.binarySearchLookup((String) lowerVal);
        final int upperPoint = fcsi.binarySearchLookup((String) upperVal);
        
        final int inclusiveLowerPoint, inclusiveUpperPoint;

        // Hints:
        // * binarySearchLookup returns 0, if value was null.
        // * the value is <0 if no exact hit was found, the returned value
        //   is (-(insertion point) - 1)
        if (lowerPoint == 0) {
          assert lowerVal == null;
          inclusiveLowerPoint = 1;
        } else if (includeLower && lowerPoint > 0) {
          inclusiveLowerPoint = lowerPoint;
        } else if (lowerPoint > 0) {
          inclusiveLowerPoint = lowerPoint + 1;
        } else {
          inclusiveLowerPoint = Math.max(1, -lowerPoint - 1);
        }
        
        if (upperPoint == 0) {
          assert upperVal == null;
          inclusiveUpperPoint = Integer.MAX_VALUE;  
        } else if (includeUpper && upperPoint > 0) {
          inclusiveUpperPoint = upperPoint;
        } else if (upperPoint > 0) {
          inclusiveUpperPoint = upperPoint - 1;
        } else {
          inclusiveUpperPoint = -upperPoint - 2;
        }      

        if (inclusiveUpperPoint <= 0 || inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        assert inclusiveLowerPoint > 0 && inclusiveUpperPoint > 0;
        
        // for this DocIdSet, we never need to use TermDocs,
        // because deleted docs have an order of 0 (null entry in StringIndex)
        return new FieldCacheDocIdSet(reader, false) {
          final boolean matchDoc(int doc) {
            return fcsi.order[doc] >= inclusiveLowerPoint && fcsi.order[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getBytes(IndexReader,String)}. This works with all
   * byte fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newByteRange(String field, Byte lowerVal, Byte upperVal, boolean includeLower, boolean includeUpper) {
    return newByteRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getBytes(IndexReader,String,FieldCache.ByteParser)}. This works with all
   * byte fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newByteRange(String field, FieldCache.ByteParser parser, Byte lowerVal, Byte upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final byte inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          final byte i = ((Number) lowerVal).byteValue();
          if (!includeLower && i == Byte.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = (byte) (includeLower ?  i : (i + 1));
        } else {
          inclusiveLowerPoint = Byte.MIN_VALUE;
        }
        if (upperVal != null) {
          final byte i = ((Number) upperVal).byteValue();
          if (!includeUpper && i == Byte.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = (byte) (includeUpper ? i : (i - 1));
        } else {
          inclusiveUpperPoint = Byte.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final byte[] values = FieldCache.DEFAULT.getBytes(reader, field, (FieldCache.ByteParser) parser);
        // we only request the usage of termDocs, if the range contains 0
        return new FieldCacheDocIdSet(reader, (inclusiveLowerPoint <= 0 && inclusiveUpperPoint >= 0)) {
          boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getShorts(IndexReader,String)}. This works with all
   * short fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newShortRange(String field, Short lowerVal, Short upperVal, boolean includeLower, boolean includeUpper) {
    return newShortRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getShorts(IndexReader,String,FieldCache.ShortParser)}. This works with all
   * short fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newShortRange(String field, FieldCache.ShortParser parser, Short lowerVal, Short upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final short inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          short i = ((Number) lowerVal).shortValue();
          if (!includeLower && i == Short.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = (short) (includeLower ? i : (i + 1));
        } else {
          inclusiveLowerPoint = Short.MIN_VALUE;
        }
        if (upperVal != null) {
          short i = ((Number) upperVal).shortValue();
          if (!includeUpper && i == Short.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = (short) (includeUpper ? i : (i - 1));
        } else {
          inclusiveUpperPoint = Short.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final short[] values = FieldCache.DEFAULT.getShorts(reader, field, (FieldCache.ShortParser) parser);
        // we only request the usage of termDocs, if the range contains 0
        return new FieldCacheDocIdSet(reader, (inclusiveLowerPoint <= 0 && inclusiveUpperPoint >= 0)) {
          boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getInts(IndexReader,String)}. This works with all
   * int fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newIntRange(String field, Integer lowerVal, Integer upperVal, boolean includeLower, boolean includeUpper) {
    return newIntRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getInts(IndexReader,String,FieldCache.IntParser)}. This works with all
   * int fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newIntRange(String field, FieldCache.IntParser parser, Integer lowerVal, Integer upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final int inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          int i = ((Number) lowerVal).intValue();
          if (!includeLower && i == Integer.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = includeLower ? i : (i + 1);
        } else {
          inclusiveLowerPoint = Integer.MIN_VALUE;
        }
        if (upperVal != null) {
          int i = ((Number) upperVal).intValue();
          if (!includeUpper && i == Integer.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = includeUpper ? i : (i - 1);
        } else {
          inclusiveUpperPoint = Integer.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final int[] values = FieldCache.DEFAULT.getInts(reader, field, (FieldCache.IntParser) parser);
        // we only request the usage of termDocs, if the range contains 0
        return new FieldCacheDocIdSet(reader, (inclusiveLowerPoint <= 0 && inclusiveUpperPoint >= 0)) {
          boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getLongs(IndexReader,String)}. This works with all
   * long fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newLongRange(String field, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
    return newLongRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getLongs(IndexReader,String,FieldCache.LongParser)}. This works with all
   * long fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newLongRange(String field, FieldCache.LongParser parser, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final long inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          long i = ((Number) lowerVal).longValue();
          if (!includeLower && i == Long.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = includeLower ? i : (i + 1L);
        } else {
          inclusiveLowerPoint = Long.MIN_VALUE;
        }
        if (upperVal != null) {
          long i = ((Number) upperVal).longValue();
          if (!includeUpper && i == Long.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = includeUpper ? i : (i - 1L);
        } else {
          inclusiveUpperPoint = Long.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final long[] values = FieldCache.DEFAULT.getLongs(reader, field, (FieldCache.LongParser) parser);
        // we only request the usage of termDocs, if the range contains 0
        return new FieldCacheDocIdSet(reader, (inclusiveLowerPoint <= 0L && inclusiveUpperPoint >= 0L)) {
          boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getFloats(IndexReader,String)}. This works with all
   * float fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newFloatRange(String field, Float lowerVal, Float upperVal, boolean includeLower, boolean includeUpper) {
    return newFloatRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getFloats(IndexReader,String,FieldCache.FloatParser)}. This works with all
   * float fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newFloatRange(String field, FieldCache.FloatParser parser, Float lowerVal, Float upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        // we transform the floating point numbers to sortable integers
        // using NumericUtils to easier find the next bigger/lower value
        final float inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          float f = ((Number) lowerVal).floatValue();
          if (!includeUpper && f > 0.0f && Float.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          int i = NumericUtils.floatToSortableInt(f);
          inclusiveLowerPoint = NumericUtils.sortableIntToFloat( includeLower ?  i : (i + 1) );
        } else {
          inclusiveLowerPoint = Float.NEGATIVE_INFINITY;
        }
        if (upperVal != null) {
          float f = ((Number) upperVal).floatValue();
          if (!includeUpper && f < 0.0f && Float.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          int i = NumericUtils.floatToSortableInt(f);
          inclusiveUpperPoint = NumericUtils.sortableIntToFloat( includeUpper ? i : (i - 1) );
        } else {
          inclusiveUpperPoint = Float.POSITIVE_INFINITY;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final float[] values = FieldCache.DEFAULT.getFloats(reader, field, (FieldCache.FloatParser) parser);
        // we only request the usage of termDocs, if the range contains 0
        return new FieldCacheDocIdSet(reader, (inclusiveLowerPoint <= 0.0f && inclusiveUpperPoint >= 0.0f)) {
          boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getDoubles(IndexReader,String)}. This works with all
   * double fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newDoubleRange(String field, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper) {
    return newDoubleRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range query using {@link FieldCache#getDoubles(IndexReader,String,FieldCache.DoubleParser)}. This works with all
   * double fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter newDoubleRange(String field, FieldCache.DoubleParser parser, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        // we transform the floating point numbers to sortable integers
        // using NumericUtils to easier find the next bigger/lower value
        final double inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          double f = ((Number) lowerVal).doubleValue();
          if (!includeUpper && f > 0.0 && Double.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          long i = NumericUtils.doubleToSortableLong(f);
          inclusiveLowerPoint = NumericUtils.sortableLongToDouble( includeLower ?  i : (i + 1L) );
        } else {
          inclusiveLowerPoint = Double.NEGATIVE_INFINITY;
        }
        if (upperVal != null) {
          double f = ((Number) upperVal).doubleValue();
          if (!includeUpper && f < 0.0 && Double.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          long i = NumericUtils.doubleToSortableLong(f);
          inclusiveUpperPoint = NumericUtils.sortableLongToDouble( includeUpper ? i : (i - 1L) );
        } else {
          inclusiveUpperPoint = Double.POSITIVE_INFINITY;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final double[] values = FieldCache.DEFAULT.getDoubles(reader, field, (FieldCache.DoubleParser) parser);
        // we only request the usage of termDocs, if the range contains 0
        return new FieldCacheDocIdSet(reader, (inclusiveLowerPoint <= 0.0 && inclusiveUpperPoint >= 0.0)) {
          boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  public final String toString() {
    final StringBuffer sb = new StringBuffer(field).append(":");
    return sb.append(includeLower ? '[' : '{')
      .append((lowerVal == null) ? "*" : lowerVal.toString())
      .append(" TO ")
      .append((upperVal == null) ? "*" : upperVal.toString())
      .append(includeUpper ? ']' : '}')
      .toString();
  }

  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FieldCacheRangeFilter)) return false;
    FieldCacheRangeFilter other = (FieldCacheRangeFilter) o;

    if (!this.field.equals(other.field)
        || this.includeLower != other.includeLower
        || this.includeUpper != other.includeUpper
    ) { return false; }
    if (this.lowerVal != null ? !this.lowerVal.equals(other.lowerVal) : other.lowerVal != null) return false;
    if (this.upperVal != null ? !this.upperVal.equals(other.upperVal) : other.upperVal != null) return false;
    if (this.parser != null ? !this.parser.equals(other.parser) : other.parser != null) return false;
    return true;
  }
  
  public final int hashCode() {
    int h = field.hashCode();
    h ^= (lowerVal != null) ? lowerVal.hashCode() : 550356204;
    h = (h << 1) | (h >>> 31);  // rotate to distinguish lower from upper
    h ^= (upperVal != null) ? upperVal.hashCode() : -1674416163;
    h ^= (parser != null) ? parser.hashCode() : -1572457324;
    h ^= (includeLower ? 1549299360 : -365038026) ^ (includeUpper ? 1721088258 : 1948649653);
    return h;
  }
  
  static abstract class FieldCacheDocIdSet extends DocIdSet {
    private final IndexReader reader;
    private boolean mayUseTermDocs;
  
    FieldCacheDocIdSet(IndexReader reader, boolean mayUseTermDocs) {
      this.reader = reader;
      this.mayUseTermDocs = mayUseTermDocs;
    }
  
    /** this method checks, if a doc is a hit, should throw AIOBE, when position invalid */
    abstract boolean matchDoc(int doc) throws ArrayIndexOutOfBoundsException;
    
    /** this DocIdSet is cacheable, if it works solely with FieldCache and no TermDocs */
    public boolean isCacheable() {
      return !(mayUseTermDocs && reader.hasDeletions());
    }

    public DocIdSetIterator iterator() throws IOException {
      // Synchronization needed because deleted docs BitVector
      // can change after call to hasDeletions until TermDocs creation.
      // We only use an iterator with termDocs, when this was requested (e.g. range contains 0)
      // and the index has deletions
      final TermDocs termDocs;
      synchronized(reader) {
        termDocs = isCacheable() ? null : reader.termDocs(null);
      }
      if (termDocs != null) {
        // a DocIdSetIterator using TermDocs to iterate valid docIds
        return new DocIdSetIterator() {
          private int doc = -1;
          
          /** @deprecated use {@link #nextDoc()} instead. */
          public boolean next() throws IOException {
            return nextDoc() != NO_MORE_DOCS;
          }
          
          /** @deprecated use {@link #advance(int)} instead. */
          public boolean skipTo(int target) throws IOException {
            return advance(target) != NO_MORE_DOCS;
          }

          /** @deprecated use {@link #docID()} instead. */
          public int doc() {
            return termDocs.doc();
          }
          
          public int docID() {
            return doc;
          }
          
          public int nextDoc() throws IOException {
            do {
              if (!termDocs.next())
                return doc = NO_MORE_DOCS;
            } while (!matchDoc(doc = termDocs.doc()));
            return doc;
          }
          
          public int advance(int target) throws IOException {
            if (!termDocs.skipTo(target))
              return doc = NO_MORE_DOCS;
            while (!matchDoc(doc = termDocs.doc())) { 
              if (!termDocs.next())
                return doc = NO_MORE_DOCS;
            }
            return doc;
          }
        };
      } else {
        // a DocIdSetIterator generating docIds by incrementing a variable -
        // this one can be used if there are no deletions are on the index
        return new DocIdSetIterator() {
          private int doc = -1;
          
          /** @deprecated use {@link #nextDoc()} instead. */
          public boolean next() throws IOException {
            return nextDoc() != NO_MORE_DOCS;
          }
          
          /** @deprecated use {@link #advance(int)} instead. */
          public boolean skipTo(int target) throws IOException {
            return advance(target) != NO_MORE_DOCS;
          }

          /** @deprecated use {@link #docID()} instead. */
          public int doc() {
            return doc;
          }

          public int docID() {
            return doc;
          }
          
          public int nextDoc() {
            try {
              do {
                doc++;
              } while (!matchDoc(doc));
              return doc;
            } catch (ArrayIndexOutOfBoundsException e) {
              return doc = NO_MORE_DOCS;
            }
          }
          
          public int advance(int target) {
            try {
              doc = target;
              while (!matchDoc(doc)) { 
                doc++;
              }
              return doc;
            } catch (ArrayIndexOutOfBoundsException e) {
              return doc = NO_MORE_DOCS;
            }
          }
        };
      }
    }
  }

}
