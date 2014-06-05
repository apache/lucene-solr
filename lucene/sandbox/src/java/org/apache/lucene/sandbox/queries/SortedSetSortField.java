package org.apache.lucene.sandbox.queries;

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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/** 
 * SortField for {@link SortedSetDocValues}.
 * <p>
 * A SortedSetDocValues contains multiple values for a field, so sorting with
 * this technique "selects" a value as the representative sort value for the document.
 * <p>
 * By default, the minimum value in the set is selected as the sort value, but
 * this can be customized. Selectors other than the default do have some limitations
 * (see below) to ensure that all selections happen in constant-time for performance.
 * <p>
 * Like sorting by string, this also supports sorting missing values as first or last,
 * via {@link #setMissingValue(Object)}.
 * <p>
 * Limitations:
 * <ul>
 *   <li>Fields containing {@link Integer#MAX_VALUE} or more unique values
 *       are unsupported.
 *   <li>Selectors other than the default ({@link Selector#MIN}) require 
 *       optional codec support. However several codecs provided by Lucene, 
 *       including the current default codec, support this.
 * </ul>
 */
public class SortedSetSortField extends SortField {
  
  /** Selects a value from the document's set to use as the sort value */
  public static enum Selector {
    /** 
     * Selects the minimum value in the set 
     */
    MIN,
    /** 
     * Selects the maximum value in the set 
     */
    MAX,
    /** 
     * Selects the middle value in the set.
     * <p>
     * If the set has an even number of values, the lower of the middle two is chosen.
     */
    MIDDLE_MIN,
    /** 
     * Selects the middle value in the set.
     * <p>
     * If the set has an even number of values, the higher of the middle two is chosen
     */
    MIDDLE_MAX
  }
  
  private final Selector selector;
  
  /**
   * Creates a sort, possibly in reverse, by the minimum value in the set 
   * for the document.
   * @param field Name of field to sort by.  Must not be null.
   * @param reverse True if natural order should be reversed.
   */
  public SortedSetSortField(String field, boolean reverse) {
    this(field, reverse, Selector.MIN);
  }

  /**
   * Creates a sort, possibly in reverse, specifying how the sort value from 
   * the document's set is selected.
   * @param field Name of field to sort by.  Must not be null.
   * @param reverse True if natural order should be reversed.
   * @param selector custom selector for choosing the sort value from the set.
   * <p>
   * NOTE: selectors other than {@link Selector#MIN} require optional codec support.
   */
  public SortedSetSortField(String field, boolean reverse, Selector selector) {
    super(field, SortField.Type.CUSTOM, reverse);
    if (selector == null) {
      throw new NullPointerException();
    }
    this.selector = selector;
  }
  
  /** Returns the selector in use for this sort */
  public Selector getSelector() {
    return selector;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + selector.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    SortedSetSortField other = (SortedSetSortField) obj;
    if (selector != other.selector) return false;
    return true;
  }
  
  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<sortedset" + ": \"").append(getField()).append("\">");
    if (getReverse()) buffer.append('!');
    if (missingValue != null) {
      buffer.append(" missingValue=");
      buffer.append(missingValue);
    }
    buffer.append(" selector=");
    buffer.append(selector);

    return buffer.toString();
  }

  /**
   * Set how missing values (the empty set) are sorted.
   * <p>
   * Note that this must be {@link #STRING_FIRST} or {@link #STRING_LAST}.
   */
  @Override
  public void setMissingValue(Object missingValue) {
    if (missingValue != STRING_FIRST && missingValue != STRING_LAST) {
      throw new IllegalArgumentException("For SORTED_SET type, missing value must be either STRING_FIRST or STRING_LAST");
    }
    this.missingValue = missingValue;
  }
  
  @Override
  public FieldComparator<?> getComparator(int numHits, int sortPos) throws IOException {
    return new FieldComparator.TermOrdValComparator(numHits, getField(), missingValue == STRING_LAST) {
      @Override
      protected SortedDocValues getSortedDocValues(AtomicReaderContext context, String field) throws IOException {
        SortedSetDocValues sortedSet = FieldCache.DEFAULT.getDocTermOrds(context.reader(), field);
        
        if (sortedSet.getValueCount() >= Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("fields containing more than " + (Integer.MAX_VALUE-1) + " unique terms are unsupported");
        }
        
        SortedDocValues singleton = DocValues.unwrapSingleton(sortedSet);
        if (singleton != null) {
          // it's actually single-valued in practice, but indexed as multi-valued,
          // so just sort on the underlying single-valued dv directly.
          // regardless of selector type, this optimization is safe!
          return singleton;
        } else if (selector == Selector.MIN) {
          return new MinValue(sortedSet);
        } else {
          if (sortedSet instanceof RandomAccessOrds == false) {
            throw new UnsupportedOperationException("codec does not support random access ordinals, cannot use selector: " + selector);
          }
          RandomAccessOrds randomOrds = (RandomAccessOrds) sortedSet;
          switch(selector) {
            case MAX: return new MaxValue(randomOrds);
            case MIDDLE_MIN: return new MiddleMinValue(randomOrds);
            case MIDDLE_MAX: return new MiddleMaxValue(randomOrds);
            case MIN: 
            default: 
              throw new AssertionError();
          }
        }
      }
    };
  }
  
  /** Wraps a SortedSetDocValues and returns the first ordinal (min) */
  static class MinValue extends SortedDocValues {
    final SortedSetDocValues in;
    
    MinValue(SortedSetDocValues in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      return (int) in.nextOrd();
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }

    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
  
  /** Wraps a SortedSetDocValues and returns the last ordinal (max) */
  static class MaxValue extends SortedDocValues {
    final RandomAccessOrds in;
    
    MaxValue(RandomAccessOrds in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      final int count = in.cardinality();
      if (count == 0) {
        return -1;
      } else {
        return (int) in.ordAt(count-1);
      }
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }
    
    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
  
  /** Wraps a SortedSetDocValues and returns the middle ordinal (or min of the two) */
  static class MiddleMinValue extends SortedDocValues {
    final RandomAccessOrds in;
    
    MiddleMinValue(RandomAccessOrds in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      final int count = in.cardinality();
      if (count == 0) {
        return -1;
      } else {
        return (int) in.ordAt((count-1) >>> 1);
      }
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }
    
    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
  
  /** Wraps a SortedSetDocValues and returns the middle ordinal (or max of the two) */
  static class MiddleMaxValue extends SortedDocValues {
    final RandomAccessOrds in;
    
    MiddleMaxValue(RandomAccessOrds in) {
      this.in = in;
    }

    @Override
    public int getOrd(int docID) {
      in.setDocument(docID);
      final int count = in.cardinality();
      if (count == 0) {
        return -1;
      } else {
        return (int) in.ordAt(count >>> 1);
      }
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return (int) in.getValueCount();
    }
    
    @Override
    public int lookupTerm(BytesRef key) {
      return (int) in.lookupTerm(key);
    }
  }
}
