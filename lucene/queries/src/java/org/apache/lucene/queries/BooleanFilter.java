package org.apache.lucene.queries;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * A container Filter that allows Boolean composition of Filters.
 * Filters are allocated into one of three logical constructs;
 * SHOULD, MUST NOT, MUST
 * The results Filter BitSet is constructed as follows:
 * SHOULD Filters are OR'd together
 * The resulting Filter is NOT'd with the NOT Filters
 * The resulting Filter is AND'd with the MUST Filters
 */
public class BooleanFilter extends Filter implements Iterable<FilterClause> {

  private final List<FilterClause> clauses = new ArrayList<FilterClause>();

  /**
   * Returns the a DocIdSetIterator representing the Boolean composition
   * of the filters that have been added.
   */
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    FixedBitSet res = null;
    final AtomicReader reader = context.reader();
    
    boolean hasShouldClauses = false;
    for (final FilterClause fc : clauses) {
      if (fc.getOccur() == Occur.SHOULD) {
        hasShouldClauses = true;
        final DocIdSetIterator disi = getDISI(fc.getFilter(), context);
        if (disi == null) continue;
        if (res == null) {
          res = new FixedBitSet(reader.maxDoc());
        }
        res.or(disi);
      }
    }
    if (hasShouldClauses && res == null)
      return DocIdSet.EMPTY_DOCIDSET;
    
    for (final FilterClause fc : clauses) {
      if (fc.getOccur() == Occur.MUST_NOT) {
        if (res == null) {
          assert !hasShouldClauses;
          res = new FixedBitSet(reader.maxDoc());
          res.set(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
        }
        final DocIdSetIterator disi = getDISI(fc.getFilter(), context);
        if (disi != null) {
          res.andNot(disi);
        }
      }
    }
    
    for (final FilterClause fc : clauses) {
      if (fc.getOccur() == Occur.MUST) {
        final DocIdSetIterator disi = getDISI(fc.getFilter(), context);
        if (disi == null) {
          return DocIdSet.EMPTY_DOCIDSET; // no documents can match
        }
        if (res == null) {
          res = new FixedBitSet(reader.maxDoc());
          res.or(disi);
        } else {
          res.and(disi);
        }
      }
    }

    return res != null ? BitsFilteredDocIdSet.wrap(res, acceptDocs) : DocIdSet.EMPTY_DOCIDSET;
  }

  private static DocIdSetIterator getDISI(Filter filter, AtomicReaderContext context)
      throws IOException {
    // we dont pass acceptDocs, we will filter at the end using an additional filter
    final DocIdSet set = filter.getDocIdSet(context, null);
    return (set == null || set == DocIdSet.EMPTY_DOCIDSET) ? null : set.iterator();
  }

  /**
  * Adds a new FilterClause to the Boolean Filter container
  * @param filterClause A FilterClause object containing a Filter and an Occur parameter
  */
  public void add(FilterClause filterClause) {
    clauses.add(filterClause);
  }
  
  public final void add(Filter filter, Occur occur) {
    add(new FilterClause(filter, occur));
  }
  
  /**
  * Returns the list of clauses
  */
  public List<FilterClause> clauses() {
    return clauses;
  }
  
  /** Returns an iterator on the clauses in this query. It implements the {@link Iterable} interface to
   * make it possible to do:
   * <pre class="prettyprint">for (FilterClause clause : booleanFilter) {}</pre>
   */
  @Override
  public final Iterator<FilterClause> iterator() {
    return clauses().iterator();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if ((obj == null) || (obj.getClass() != this.getClass())) {
      return false;
    }

    final BooleanFilter other = (BooleanFilter)obj;
    return clauses.equals(other.clauses);
  }

  @Override
  public int hashCode() {
    return 657153718 ^ clauses.hashCode();
  }
  
  /** Prints a user-readable version of this Filter. */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("BooleanFilter(");
    final int minLen = buffer.length();
    for (final FilterClause c : clauses) {
      if (buffer.length() > minLen) {
        buffer.append(' ');
      }
      buffer.append(c);
    }
    return buffer.append(')').toString();
  }
}
