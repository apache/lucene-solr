package org.apache.lucene.search.trie;

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
import java.util.Date;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.OpenBitSet;

/**
 * Implementation of a Lucene {@link Filter} that implements trie-based range filtering.
 * This filter depends on a specific structure of terms in the index that can only be created
 * by {@link TrieUtils} methods.
 * For more information, how the algorithm works, see the package description {@link org.apache.lucene.search.trie}.
 */
public final class TrieRangeFilter extends Filter {

  /**
   * Universal constructor (expert use only): Uses already trie-converted min/max values.
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeFilter(final String field, String min, String max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    if (min==null && max==null) throw new IllegalArgumentException("The min and max values cannot be both null.");
    this.trieVariant=variant;
    this.field=field.intern();
    // just for toString()
    this.minUnconverted=min;
    this.maxUnconverted=max;
    this.minInclusive=minInclusive;
    this.maxInclusive=maxInclusive;
    // encode bounds
    this.min=(min==null) ? trieVariant.TRIE_CODED_NUMERIC_MIN : (
      minInclusive ? min : variant.incrementTrieCoded(min)
    );
    this.max=(max==null) ? trieVariant.TRIE_CODED_NUMERIC_MAX : (
      maxInclusive ? max : variant.decrementTrieCoded(max)
    );
  }

  /**
   * Generates a trie filter using the supplied field with range bounds in numeric form (double).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeFilter(final String field, final Double min, final Double max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    this(
      field,
      (min==null) ? null : variant.doubleToTrieCoded(min.doubleValue()),
      (max==null) ? null : variant.doubleToTrieCoded(max.doubleValue()),
      minInclusive,
      maxInclusive,
      variant
    );
    this.minUnconverted=min;
    this.maxUnconverted=max;
  }

  /**
   * Generates a trie filter using the supplied field with range bounds in date/time form.
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeFilter(final String field, final Date min, final Date max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    this(
      field,
      (min==null) ? null : variant.dateToTrieCoded(min),
      (max==null) ? null : variant.dateToTrieCoded(max),
      minInclusive,
      maxInclusive,
      variant
    );
    this.minUnconverted=min;
    this.maxUnconverted=max;
  }

  /**
   * Generates a trie filter using the supplied field with range bounds in integer form (long).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeFilter(final String field, final Long min, final Long max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    this(
      field,
      (min==null) ? null : variant.longToTrieCoded(min.longValue()),
      (max==null) ? null : variant.longToTrieCoded(max.longValue()),
      minInclusive,
      maxInclusive,
      variant
    );
    this.minUnconverted=min;
    this.maxUnconverted=max;
  }

  //@Override
  public String toString() {
    return toString(null);
  }

  public String toString(final String field) {
    final StringBuffer sb=new StringBuffer();
    if (!this.field.equals(field)) sb.append(this.field).append(':');
    return sb.append(minInclusive ? '[' : '{')
      .append((minUnconverted==null) ? "*" : minUnconverted.toString())
      .append(" TO ")
      .append((maxUnconverted==null) ? "*" : maxUnconverted.toString())
      .append(maxInclusive ? ']' : '}').toString();
  }

  /**
   * Two instances are equal if they have the same trie-encoded range bounds, same field, and same variant.
   * If one of the instances uses an exclusive lower bound, it is equal to a range with inclusive bound,
   * when the inclusive lower bound is equal to the incremented exclusive lower bound of the other one.
   * The same applys for the upper bound in other direction.
   */
  //@Override
  public final boolean equals(final Object o) {
    if (o instanceof TrieRangeFilter) {
      TrieRangeFilter q=(TrieRangeFilter)o;
      // trieVariants are singleton per type, so no equals needed.
      return (field==q.field && min.equals(q.min) && max.equals(q.max) && trieVariant==q.trieVariant);
    } else return false;
  }

  //@Override
  public final int hashCode() {
    // the hash code uses from the variant only the number of bits, as this is unique for the variant
    return field.hashCode()+(min.hashCode()^0x14fa55fb)+(max.hashCode()^0x733fa5fe)+(trieVariant.TRIE_BITS^0x64365465);
  }
  
  /** prints the String in hexadecimal \\u notation (for debugging of <code>setBits()</code>) */
  private String stringToHexDigits(final String s) {
    StringBuffer sb=new StringBuffer(s.length()*3);
    for (int i=0,c=s.length(); i<c; i++) {
      char ch=s.charAt(i);
      sb.append("\\u").append(Integer.toHexString((int)ch));
    }
    return sb.toString();
  }

  /** Marks documents in a specific range. Code borrowed from original RangeFilter and simplified (and returns number of terms) */
  private int setBits(final IndexReader reader, final TermDocs termDocs, final OpenBitSet bits, String lowerTerm, String upperTerm) throws IOException {
    //System.out.println(stringToHexDigits(lowerTerm)+" TO "+stringToHexDigits(upperTerm));
    int count=0,len=lowerTerm.length();
    final String field;
    if (len<trieVariant.TRIE_CODED_LENGTH) {
      // lower precision value is in helper field
      field=(this.field + trieVariant.LOWER_PRECISION_FIELD_NAME_SUFFIX).intern();
      // add padding before lower precision values to group them
      lowerTerm=new StringBuffer(len+1).append((char)(trieVariant.TRIE_CODED_PADDING_START+len)).append(lowerTerm).toString();
      upperTerm=new StringBuffer(len+1).append((char)(trieVariant.TRIE_CODED_PADDING_START+len)).append(upperTerm).toString();
      // length is longer by 1 char because of padding
      len++;
    } else {
      // full precision value is in original field
      field=this.field;
    }
    final TermEnum enumerator = reader.terms(new Term(field, lowerTerm));
    try {
      do {
        final Term term = enumerator.term();
        if (term!=null && term.field()==field) {
          // break out when upperTerm reached or length of term is different
          final String t=term.text();
          if (len!=t.length() || t.compareTo(upperTerm)>0) break;
          // we have a good term, find the docs
          count++;
          termDocs.seek(enumerator);
          while (termDocs.next()) bits.set(termDocs.doc());
        } else break;
      } while (enumerator.next());
    } finally {
      enumerator.close();
    }
    return count;
  }

  /** Splits range recursively (and returns number of terms) */
  private int splitRange(
    final IndexReader reader, final TermDocs termDocs, final OpenBitSet bits,
    final String min, final boolean lowerBoundOpen, final String max, final boolean upperBoundOpen
  ) throws IOException {
    int count=0;
    final int length=min.length();
    final String minShort=lowerBoundOpen ? min.substring(0,length-1) : trieVariant.incrementTrieCoded(min.substring(0,length-1));
    final String maxShort=upperBoundOpen ? max.substring(0,length-1) : trieVariant.decrementTrieCoded(max.substring(0,length-1));

    if (length==1 || minShort.compareTo(maxShort)>=0) {
      // we are in the lowest precision or the current precision is not existent
      count+=setBits(reader, termDocs, bits, min, max);
    } else {
      // Avoid too much seeking: first go deeper into lower precision
      // (in IndexReader's TermEnum these terms are earlier).
      // Do this only, if the current length is not trieVariant.TRIE_CODED_LENGTH (not full precision),
      // because terms from the highest prec come before all lower prec terms
      // (because the field name is ordered before the suffixed one).
      if (length!=trieVariant.TRIE_CODED_LENGTH) count+=splitRange(
        reader,termDocs,bits,
        minShort,lowerBoundOpen,
        maxShort,upperBoundOpen
      );
      // Avoid too much seeking: set bits for lower part of current (higher) precision.
      // These terms come later in IndexReader's TermEnum.
      if (!lowerBoundOpen) {
        count+=setBits(reader, termDocs, bits, min, trieVariant.decrementTrieCoded(minShort+trieVariant.TRIE_CODED_SYMBOL_MIN));
      }
      // Avoid too much seeking: set bits for upper part of current precision.
      // These terms come later in IndexReader's TermEnum.
      if (!upperBoundOpen) {
        count+=setBits(reader, termDocs, bits, trieVariant.incrementTrieCoded(maxShort+trieVariant.TRIE_CODED_SYMBOL_MAX), max);
      }
      // If the first step (see above) was not done (because length==trieVariant.TRIE_CODED_LENGTH) we do it now.
      if (length==trieVariant.TRIE_CODED_LENGTH) count+=splitRange(
        reader,termDocs,bits,
        minShort,lowerBoundOpen,
        maxShort,upperBoundOpen
      );
    }
    return count;
  }

  /**
   * Returns a DocIdSet that provides the documents which should be permitted or prohibited in search results.
   */
  //@Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    if (min.compareTo(max) > 0) {
      // shortcut: if min>max, no docs will match!
      lastNumberOfTerms=0;
      return DocIdSet.EMPTY_DOCIDSET;
    } else {
      final OpenBitSet bits = new OpenBitSet(reader.maxDoc());
      final TermDocs termDocs = reader.termDocs();
      try {
        lastNumberOfTerms=splitRange(
          reader,termDocs,bits,
          min,trieVariant.TRIE_CODED_NUMERIC_MIN.equals(min),
          max,trieVariant.TRIE_CODED_NUMERIC_MAX.equals(max)
        );
      } finally {
        termDocs.close();
      }
      return bits;
    }
  }
  
  /**
   * EXPERT: Return the number of terms visited during the last execution of {@link #getDocIdSet}.
   * This may be used for performance comparisons of different trie variants and their effectiveness.
   * This method is not thread safe, be sure to only call it when no query is running!
   * @throws IllegalStateException if {@link #getDocIdSet} was not yet executed.
   */
  public int getLastNumberOfTerms() {
    if (lastNumberOfTerms < 0) throw new IllegalStateException();
    return lastNumberOfTerms;
  }

  // members
  private final String field,min,max;
  private final TrieUtils trieVariant;
  private final boolean minInclusive,maxInclusive;
  private Object minUnconverted,maxUnconverted;
  private int lastNumberOfTerms=-1;
}
