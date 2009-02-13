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
import java.util.Arrays;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.ToStringUtils;


abstract class AbstractTrieRangeFilter extends Filter {

  AbstractTrieRangeFilter(final String[] fields, final int precisionStep,
    Number min, Number max, final boolean minInclusive, final boolean maxInclusive
  ) {
    this.fields=(String[])fields.clone();
    this.precisionStep=precisionStep;
    this.min=min;
    this.max=max;
    this.minInclusive=minInclusive;
    this.maxInclusive=maxInclusive;
  }

  //@Override
  public String toString() {
    return toString(null);
  }

  public String toString(final String field) {
    final StringBuffer sb=new StringBuffer();
    if (!this.fields[0].equals(field)) sb.append(this.fields[0]).append(':');
    return sb.append(minInclusive ? '[' : '{')
      .append((min==null) ? "*" : min.toString())
      .append(" TO ")
      .append((max==null) ? "*" : max.toString())
      .append(maxInclusive ? ']' : '}').toString();
  }

  //@Override
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (o==null) return false;
    if (this.getClass().equals(o.getClass())) {
      AbstractTrieRangeFilter q=(AbstractTrieRangeFilter)o;
      return (
        Arrays.equals(fields,q.fields) &&
        (q.min == null ? min == null : q.min.equals(min)) &&
        (q.max == null ? max == null : q.max.equals(max)) &&
        minInclusive==q.minInclusive &&
        maxInclusive==q.maxInclusive &&
        precisionStep==q.precisionStep
      );
    }
    return false;
  }

  //@Override
  public final int hashCode() {
    int hash=Arrays.asList(fields).hashCode()+(precisionStep^0x64365465);
    if (min!=null) hash += min.hashCode()^0x14fa55fb;
    if (max!=null) hash += max.hashCode()^0x733fa5fe;
    return hash+
      (Boolean.valueOf(minInclusive).hashCode()^0x14fa55fb)+
      (Boolean.valueOf(maxInclusive).hashCode()^0x733fa5fe);
  }
  
  /**
   * Expert: Return the number of terms visited during the last execution of {@link #getDocIdSet}.
   * This may be used for performance comparisons of different trie variants and their effectiveness.
   * This method is not thread safe, be sure to only call it when no query is running!
   * @throws IllegalStateException if {@link #getDocIdSet} was not yet executed.
   */
  public int getLastNumberOfTerms() {
    if (lastNumberOfTerms < 0) throw new IllegalStateException();
    return lastNumberOfTerms;
  }
  
  void resetLastNumberOfTerms() {
    lastNumberOfTerms=0;
  }

  /** Returns this range filter as a query.
   * Using this method, it is possible to create a Query using <code>new {Long|Int}TrieRangeFilter(....).asQuery()</code>.
   * This is a synonym for wrapping with a {@link ConstantScoreQuery},
   * but this query returns a better <code>toString()</code> variant.
   */
  public Query asQuery() {
    return new ConstantScoreQuery(this) {
    
      /** this instance return a nicer String variant than the original {@link ConstantScoreQuery} */
      //@Override
      public String toString(final String field) {
        // return a more convenient representation of this query than ConstantScoreQuery does:
        return ((AbstractTrieRangeFilter) filter).toString(field)+ToStringUtils.boost(getBoost());
      }

    };
  }
  
  void fillBits(
    final IndexReader reader,
    final OpenBitSet bits, final TermDocs termDocs,
    String field,
    final String lowerTerm, final String upperTerm
  ) throws IOException {
    final int len=lowerTerm.length();
    assert upperTerm.length()==len;
    field=field.intern();
    
    // find the docs
    final TermEnum enumerator = reader.terms(new Term(field, lowerTerm));
    try {
      do {
        final Term term = enumerator.term();
        if (term!=null && term.field()==field) {
          // break out when upperTerm reached or length of term is different
          final String t=term.text();
          if (len!=t.length() || t.compareTo(upperTerm)>0) break;
          // we have a good term, find the docs
          lastNumberOfTerms++;
          termDocs.seek(enumerator);
          while (termDocs.next()) bits.set(termDocs.doc());
        } else break;
      } while (enumerator.next());
    } finally {
      enumerator.close();
    }
  }
  
  // members
  final String[] fields;
  final int precisionStep;
  final Number min,max;
  final boolean minInclusive,maxInclusive;
  
  private int lastNumberOfTerms=-1;
}
