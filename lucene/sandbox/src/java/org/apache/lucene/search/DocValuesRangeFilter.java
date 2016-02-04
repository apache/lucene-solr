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
package org.apache.lucene.search;
import java.io.IOException;

import org.apache.lucene.document.DoubleField; // for javadocs
import org.apache.lucene.document.FloatField; // for javadocs
import org.apache.lucene.document.IntField; // for javadocs
import org.apache.lucene.document.LongField; // for javadocs
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * A range filter built on top of numeric doc values field 
 * (from {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)}).
 * 
 * <p>{@code DocValuesRangeFilter} builds a single cache for the field the first time it is used.
 * Each subsequent {@code DocValuesRangeFilter} on the same field then reuses this cache,
 * even if the range itself changes. 
 * 
 * <p>This means that {@code DocValuesRangeFilter} is much faster (sometimes more than 100x as fast) 
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
 * <p>As with all {@link org.apache.lucene.index.LeafReader#getNumericDocValues} based functionality, 
 * {@code DocValuesRangeFilter} is only valid for 
 * fields which exact one term for each document (except for {@link #newStringRange}
 * where 0 terms are also allowed). Due to historical reasons, for numeric ranges
 * all terms that do not have a numeric value, 0 is assumed.
 *
 * <p>Thus it works on dates, prices and other single value fields but will not work on
 * regular text fields. It is preferable to use a <code>NOT_ANALYZED</code> field to ensure that
 * there is only a single term. 
 *
 * <p>This class does not have an constructor, use one of the static factory methods available,
 * that create a correct instance for different data types.
 *
 * @deprecated Use {@link DocValuesRangeQuery} instead
 *
 * @lucene.experimental
 */
// TODO: use docsWithField to handle empty properly
@Deprecated
public abstract class DocValuesRangeFilter<T> extends Filter {
  final String field;
  final T lowerVal;
  final T upperVal;
  final boolean includeLower;
  final boolean includeUpper;
  
  private DocValuesRangeFilter(String field, T lowerVal, T upperVal, boolean includeLower, boolean includeUpper) {
    this.field = field;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
  }
  
  /** This method is implemented for each data type */
  @Override
  public abstract DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException;

  /**
   * Creates a string range filter using {@link org.apache.lucene.index.LeafReader#getSortedDocValues(String)}. This works with all
   * fields containing zero or one term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static DocValuesRangeFilter<String> newStringRange(String field, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeFilter<String>(field, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final SortedDocValues fcsi = DocValues.getSorted(context.reader(), field);
        final int lowerPoint = lowerVal == null ? -1 : fcsi.lookupTerm(new BytesRef(lowerVal));
        final int upperPoint = upperVal == null ? -1 : fcsi.lookupTerm(new BytesRef(upperVal));

        final int inclusiveLowerPoint, inclusiveUpperPoint;

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
          inclusiveUpperPoint = Integer.MAX_VALUE;  
        } else if (includeUpper && upperPoint >= 0) {
          inclusiveUpperPoint = upperPoint;
        } else if (upperPoint >= 0) {
          inclusiveUpperPoint = upperPoint - 1;
        } else {
          inclusiveUpperPoint = -upperPoint - 2;
        }      

        if (inclusiveUpperPoint < 0 || inclusiveLowerPoint > inclusiveUpperPoint) {
          return null;
        }
        
        assert inclusiveLowerPoint >= 0 && inclusiveUpperPoint >= 0;
        
        return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
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
   * Creates a BytesRef range filter using {@link org.apache.lucene.index.LeafReader#getSortedDocValues(String)}. This works with all
   * fields containing zero or one term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  // TODO: bogus that newStringRange doesnt share this code... generics hell
  public static DocValuesRangeFilter<BytesRef> newBytesRefRange(String field, BytesRef lowerVal, BytesRef upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeFilter<BytesRef>(field, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final SortedDocValues fcsi = DocValues.getSorted(context.reader(), field);
        final int lowerPoint = lowerVal == null ? -1 : fcsi.lookupTerm(lowerVal);
        final int upperPoint = upperVal == null ? -1 : fcsi.lookupTerm(upperVal);

        final int inclusiveLowerPoint, inclusiveUpperPoint;

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
          inclusiveUpperPoint = Integer.MAX_VALUE;  
        } else if (includeUpper && upperPoint >= 0) {
          inclusiveUpperPoint = upperPoint;
        } else if (upperPoint >= 0) {
          inclusiveUpperPoint = upperPoint - 1;
        } else {
          inclusiveUpperPoint = -upperPoint - 2;
        }      

        if (inclusiveUpperPoint < 0 || inclusiveLowerPoint > inclusiveUpperPoint) {
          return null;
        }
        
        assert inclusiveLowerPoint >= 0 && inclusiveUpperPoint >= 0;
        
        return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
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
   * Creates a numeric range filter using {@link org.apache.lucene.index.LeafReader#getSortedDocValues(String)}. This works with all
   * int fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static DocValuesRangeFilter<Integer> newIntRange(String field, Integer lowerVal, Integer upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeFilter<Integer>(field, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final int inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          int i = lowerVal.intValue();
          if (!includeLower && i == Integer.MAX_VALUE)
            return null;
          inclusiveLowerPoint = includeLower ? i : (i + 1);
        } else {
          inclusiveLowerPoint = Integer.MIN_VALUE;
        }
        if (upperVal != null) {
          int i = upperVal.intValue();
          if (!includeUpper && i == Integer.MIN_VALUE)
            return null;
          inclusiveUpperPoint = includeUpper ? i : (i - 1);
        } else {
          inclusiveUpperPoint = Integer.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return null;
        
        final NumericDocValues values = DocValues.getNumeric(context.reader(), field);
        return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            final int value = (int) values.get(doc);
            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)}. This works with all
   * long fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static DocValuesRangeFilter<Long> newLongRange(String field, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeFilter<Long>(field, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final long inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          long i = lowerVal.longValue();
          if (!includeLower && i == Long.MAX_VALUE)
            return null;
          inclusiveLowerPoint = includeLower ? i : (i + 1L);
        } else {
          inclusiveLowerPoint = Long.MIN_VALUE;
        }
        if (upperVal != null) {
          long i = upperVal.longValue();
          if (!includeUpper && i == Long.MIN_VALUE)
            return null;
          inclusiveUpperPoint = includeUpper ? i : (i - 1L);
        } else {
          inclusiveUpperPoint = Long.MAX_VALUE;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return null;
        
        final NumericDocValues values = DocValues.getNumeric(context.reader(), field);
        return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            final long value = values.get(doc);
            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)}. This works with all
   * float fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static DocValuesRangeFilter<Float> newFloatRange(String field, Float lowerVal, Float upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeFilter<Float>(field, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        // we transform the floating point numbers to sortable integers
        // using NumericUtils to easier find the next bigger/lower value
        final float inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          float f = lowerVal.floatValue();
          if (!includeUpper && f > 0.0f && Float.isInfinite(f))
            return null;
          int i = NumericUtils.floatToSortableInt(f);
          inclusiveLowerPoint = NumericUtils.sortableIntToFloat( includeLower ?  i : (i + 1) );
        } else {
          inclusiveLowerPoint = Float.NEGATIVE_INFINITY;
        }
        if (upperVal != null) {
          float f = upperVal.floatValue();
          if (!includeUpper && f < 0.0f && Float.isInfinite(f))
            return null;
          int i = NumericUtils.floatToSortableInt(f);
          inclusiveUpperPoint = NumericUtils.sortableIntToFloat( includeUpper ? i : (i - 1) );
        } else {
          inclusiveUpperPoint = Float.POSITIVE_INFINITY;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return null;
        
        final NumericDocValues values = DocValues.getNumeric(context.reader(), field);
        return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            final float value = Float.intBitsToFloat((int)values.get(doc));
            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  /**
   * Creates a numeric range filter using {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)}. This works with all
   * double fields containing exactly one numeric term in the field. The range can be half-open by setting one
   * of the values to <code>null</code>.
   */
  public static DocValuesRangeFilter<Double> newDoubleRange(String field, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeFilter<Double>(field, lowerVal, upperVal, includeLower, includeUpper) {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        // we transform the floating point numbers to sortable integers
        // using NumericUtils to easier find the next bigger/lower value
        final double inclusiveLowerPoint, inclusiveUpperPoint;
        if (lowerVal != null) {
          double f = lowerVal.doubleValue();
          if (!includeUpper && f > 0.0 && Double.isInfinite(f))
            return null;
          long i = NumericUtils.doubleToSortableLong(f);
          inclusiveLowerPoint = NumericUtils.sortableLongToDouble( includeLower ?  i : (i + 1L) );
        } else {
          inclusiveLowerPoint = Double.NEGATIVE_INFINITY;
        }
        if (upperVal != null) {
          double f = upperVal.doubleValue();
          if (!includeUpper && f < 0.0 && Double.isInfinite(f))
            return null;
          long i = NumericUtils.doubleToSortableLong(f);
          inclusiveUpperPoint = NumericUtils.sortableLongToDouble( includeUpper ? i : (i - 1L) );
        } else {
          inclusiveUpperPoint = Double.POSITIVE_INFINITY;
        }
        
        if (inclusiveLowerPoint > inclusiveUpperPoint)
          return null;
        
        final NumericDocValues values = DocValues.getNumeric(context.reader(), field);
        // ignore deleted docs if range doesn't contain 0
        return new DocValuesDocIdSet(context.reader().maxDoc(), acceptDocs) {
          @Override
          protected boolean matchDoc(int doc) {
            final double value = Double.longBitsToDouble(values.get(doc));
            return value >= inclusiveLowerPoint && value <= inclusiveUpperPoint;
          }
        };
      }
    };
  }
  
  @Override
  public final String toString(String defaultField) {
    final StringBuilder sb = new StringBuilder(field).append(":");
    return sb.append(includeLower ? '[' : '{')
      .append((lowerVal == null) ? "*" : lowerVal.toString())
      .append(" TO ")
      .append((upperVal == null) ? "*" : upperVal.toString())
      .append(includeUpper ? ']' : '}')
      .toString();
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (super.equals(o) == false) return false;
    DocValuesRangeFilter other = (DocValuesRangeFilter) o;

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
    int h = super.hashCode();
    h = 31 * h + field.hashCode();
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
  public T getLowerVal() { return lowerVal; }

  /** Returns the upper value of this range filter */
  public T getUpperVal() { return upperVal; }
}
