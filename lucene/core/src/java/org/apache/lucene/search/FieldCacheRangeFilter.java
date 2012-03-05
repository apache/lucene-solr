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

import org.apache.lucene.index.AtomicReader; // for javadocs
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.document.IntField; // for javadocs
import org.apache.lucene.document.FloatField; // for javadocs
import org.apache.lucene.document.LongField; // for javadocs
import org.apache.lucene.document.DoubleField; // for javadocs
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * A range filter built on top of a cached single term field (in {@link FieldCache}).
 * 
 * <p>{@code FieldCacheRangeFilter} builds a single cache for the field the first time it is used.
 * Each subsequent {@code FieldCacheRangeFilter} on the same field then reuses this cache,
 * even if the range itself changes. 
 * 
 * <p>This means that {@code FieldCacheRangeFilter} is much faster (sometimes more than 100x as fast) 
 * as building a {@link TermRangeFilter}, if using a {@link #newStringRange}.
 * However, if the range never changes it is slower (around 2x as slow) than building
 * a CachingWrapperFilter on top of a single {@link TermRangeFilter}.
 *
 * For numeric data types, this filter may be significantly faster than {@link NumericRangeFilter}.
 * Furthermore, it does not need the numeric values encoded
 * by {@link IntField}, {@link FloatField}, {@link
 * LongField} or {@link DoubleField}. But
 * it has the problem that it only works with exact one value/document (see below).
 *
 * <p>As with all {@link FieldCache} based functionality, {@code FieldCacheRangeFilter} is only valid for 
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

public abstract class FieldCacheRangeFilter<T> extends Filter {
  final String field;
  final FieldCache.Parser parser;
  final T lowerVal;
  final T upperVal;
  final boolean includeLower;
  final boolean includeUpper;
  
  private FieldCacheRangeFilter(String field, FieldCache.Parser parser, T lowerVal, T upperVal, boolean includeLower, boolean includeUpper) {
    this.field = field;
    this.parser = parser;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
  }
  
  /** This method is implemented for each data type */
  @Override
  public abstract DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException;

  /**
   * Creates a string range filter using {@link FieldCache#getTermsIndex}. This works with all
   * fields containing zero or one term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<String> newStringRange(String field, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter<String>(field, null, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        final FieldCache.DocTermsIndex fcsi = FieldCache.DEFAULT.getTermsIndex(context.reader(), field);
        final BytesRef spare = new BytesRef();
        final int lowerPoint = fcsi.binarySearchLookup(lowerVal == null ? null : new BytesRef(lowerVal), spare);
        final int upperPoint = fcsi.binarySearchLookup(upperVal == null ? null : new BytesRef(upperVal), spare);

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
        
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected final boolean matchDoc(int doc) {
            final int docOrd = fcsi.getOrd(doc);
            return docOrd >= inclusiveLowerPoint && docOrd <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getBytes(AtomicReader,String,boolean)}. This works with all
   * byte fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Byte> newByteRange(String field, Byte lowerVal, Byte upperVal, boolean includeLower, boolean includeUpper) {
    return newByteRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getBytes(AtomicReader,String,FieldCache.ByteParser,boolean)}. This works with all
   * byte fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Byte> newByteRange(String field, FieldCache.ByteParser parser, Byte lowerVal, Byte upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter<Byte>(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        final byte inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          final byte i = lowerVal.byteValue();
          if (!includeLower && i == Byte.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = (byte) (includeLower ?  i : (i + 1));
        } else {
          inclusiveLowerPoint = Byte.MIN_VALUE;
        }
        if (upperVal != null) {
          final byte i = upperVal.byteValue();
          if (!includeUpper && i == Byte.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = (byte) (includeUpper ? i : (i - 1));
        } else {
          inclusiveUpperPoint = Byte.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final byte[] values = FieldCache.DEFAULT.getBytes(context.reader(), field, (FieldCache.ByteParser) parser, false);
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getShorts(AtomicReader,String,boolean)}. This works with all
   * short fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Short> newShortRange(String field, Short lowerVal, Short upperVal, boolean includeLower, boolean includeUpper) {
    return newShortRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getShorts(AtomicReader,String,FieldCache.ShortParser,boolean)}. This works with all
   * short fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Short> newShortRange(String field, FieldCache.ShortParser parser, Short lowerVal, Short upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter<Short>(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        final short inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          short i = lowerVal.shortValue();
          if (!includeLower && i == Short.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = (short) (includeLower ? i : (i + 1));
        } else {
          inclusiveLowerPoint = Short.MIN_VALUE;
        }
        if (upperVal != null) {
          short i = upperVal.shortValue();
          if (!includeUpper && i == Short.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = (short) (includeUpper ? i : (i - 1));
        } else {
          inclusiveUpperPoint = Short.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final short[] values = FieldCache.DEFAULT.getShorts(context.reader(), field, (FieldCache.ShortParser) parser, false);
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getInts(AtomicReader,String,boolean)}. This works with all
   * int fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Integer> newIntRange(String field, Integer lowerVal, Integer upperVal, boolean includeLower, boolean includeUpper) {
    return newIntRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getInts(AtomicReader,String,FieldCache.IntParser,boolean)}. This works with all
   * int fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Integer> newIntRange(String field, FieldCache.IntParser parser, Integer lowerVal, Integer upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter<Integer>(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        final int inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          int i = lowerVal.intValue();
          if (!includeLower && i == Integer.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = includeLower ? i : (i + 1);
        } else {
          inclusiveLowerPoint = Integer.MIN_VALUE;
        }
        if (upperVal != null) {
          int i = upperVal.intValue();
          if (!includeUpper && i == Integer.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = includeUpper ? i : (i - 1);
        } else {
          inclusiveUpperPoint = Integer.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final int[] values = FieldCache.DEFAULT.getInts(context.reader(), field, (FieldCache.IntParser) parser, false);
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getLongs(AtomicReader,String,boolean)}. This works with all
   * long fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Long> newLongRange(String field, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
    return newLongRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getLongs(AtomicReader,String,FieldCache.LongParser,boolean)}. This works with all
   * long fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Long> newLongRange(String field, FieldCache.LongParser parser, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter<Long>(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        final long inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          long i = lowerVal.longValue();
          if (!includeLower && i == Long.MAX_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveLowerPoint = includeLower ? i : (i + 1L);
        } else {
          inclusiveLowerPoint = Long.MIN_VALUE;
        }
        if (upperVal != null) {
          long i = upperVal.longValue();
          if (!includeUpper && i == Long.MIN_VALUE)
            return DocIdSet.EMPTY_DOCIDSET;
          inclusiveUpperPoint = includeUpper ? i : (i - 1L);
        } else {
          inclusiveUpperPoint = Long.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final long[] values = FieldCache.DEFAULT.getLongs(context.reader(), field, (FieldCache.LongParser) parser, false);
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getFloats(AtomicReader,String,boolean)}. This works with all
   * float fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Float> newFloatRange(String field, Float lowerVal, Float upperVal, boolean includeLower, boolean includeUpper) {
    return newFloatRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getFloats(AtomicReader,String,FieldCache.FloatParser,boolean)}. This works with all
   * float fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Float> newFloatRange(String field, FieldCache.FloatParser parser, Float lowerVal, Float upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter<Float>(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        // we transform the floating point numbers to sortable integers
        // using NumericUtils to easier find the next bigger/lower value
        final float inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          float f = lowerVal.floatValue();
          if (!includeUpper && f > 0.0f && Float.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          int i = NumericUtils.floatToSortableInt(f);
          inclusiveLowerPoint = NumericUtils.sortableIntToFloat( includeLower ?  i : (i + 1) );
        } else {
          inclusiveLowerPoint = Float.NEGATIVE_INFINITY;
        }
        if (upperVal != null) {
          float f = upperVal.floatValue();
          if (!includeUpper && f < 0.0f && Float.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          int i = NumericUtils.floatToSortableInt(f);
          inclusiveUpperPoint = NumericUtils.sortableIntToFloat( includeUpper ? i : (i - 1) );
        } else {
          inclusiveUpperPoint = Float.POSITIVE_INFINITY;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final float[] values = FieldCache.DEFAULT.getFloats(context.reader(), field, (FieldCache.FloatParser) parser, false);
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getDoubles(AtomicReader,String,boolean)}. This works with all
   * double fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Double> newDoubleRange(String field, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper) {
    return newDoubleRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
  }
  
  /**
   * Creates a numeric range filter using {@link FieldCache#getDoubles(AtomicReader,String,FieldCache.DoubleParser,boolean)}. This works with all
   * double fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static FieldCacheRangeFilter<Double> newDoubleRange(String field, FieldCache.DoubleParser parser, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper) {
    return new FieldCacheRangeFilter<Double>(field, parser, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        // we transform the floating point numbers to sortable integers
        // using NumericUtils to easier find the next bigger/lower value
        final double inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          double f = lowerVal.doubleValue();
          if (!includeUpper && f > 0.0 && Double.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          long i = NumericUtils.doubleToSortableLong(f);
          inclusiveLowerPoint = NumericUtils.sortableLongToDouble( includeLower ?  i : (i + 1L) );
        } else {
          inclusiveLowerPoint = Double.NEGATIVE_INFINITY;
        }
        if (upperVal != null) {
          double f = upperVal.doubleValue();
          if (!includeUpper && f < 0.0 && Double.isInfinite(f))
            return DocIdSet.EMPTY_DOCIDSET;
          long i = NumericUtils.doubleToSortableLong(f);
          inclusiveUpperPoint = NumericUtils.sortableLongToDouble( includeUpper ? i : (i - 1L) );
        } else {
          inclusiveUpperPoint = Double.POSITIVE_INFINITY;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return DocIdSet.EMPTY_DOCIDSET;
        
        final double[] values = FieldCache.DEFAULT.getDoubles(context.reader(), field, (FieldCache.DoubleParser) parser, false);
        // ignore deleted docs if range doesn't contain 0
        return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
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
  @SuppressWarnings({"unchecked","rawtypes"})
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
  
  @Override
  public final int hashCode() {
    int h = field.hashCode();
    h ^= (lowerVal != null) ? lowerVal.hashCode() : 550356204;
    h = (h << 1) | (h >>> 31);  // rotate to distinguish lower from upper
    h ^= (upperVal != null) ? upperVal.hashCode() : -1674416163;
    h ^= (parser != null) ? parser.hashCode() : -1572457324;
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
  public T getLowerVal() { return lowerVal; }

  /** Returns the upper value of this range filter */
  public T getUpperVal() { return upperVal; }
  
  /** Returns the current numeric parser ({@code null} for {@code T} is {@code String}} */
  public FieldCache.Parser getParser() { return parser; }
}
