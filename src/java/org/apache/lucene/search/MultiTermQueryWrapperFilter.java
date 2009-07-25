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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;
import java.util.BitSet;

/**
 * A wrapper for {@link MultiTermQuery}, that exposes its
 * functionality as a {@link Filter}.
 * <P>
 * <code>MultiTermQueryWrapperFilter</code> is not designed to
 * be used by itself. Normally you subclass it to provide a Filter
 * counterpart for a {@link MultiTermQuery} subclass.
 * <P>
 * For example, {@link TermRangeFilter} and {@link PrefixFilter} extend
 * <code>MultiTermQueryWrapperFilter</code>.
 * This class also provides the functionality behind
 * {@link MultiTermQuery#CONSTANT_SCORE_FILTER_REWRITE};
 * this is why it is not abstract.
 */
public class MultiTermQueryWrapperFilter extends Filter {
    
  protected final MultiTermQuery query;

  /**
   * Wrap a {@link MultiTermQuery} as a Filter.
   */
  protected MultiTermQueryWrapperFilter(MultiTermQuery query) {
      this.query = query;
  }
  
  //@Override
  public String toString() {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString();
  }

  //@Override
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (o==null) return false;
    if (this.getClass().equals(o.getClass())) {
      return this.query.equals( ((MultiTermQueryWrapperFilter)o).query );
    }
    return false;
  }

  //@Override
  public final int hashCode() {
    return query.hashCode();
  }
  
  /**
   * Expert: Return the number of unique terms visited during execution of the filter.
   * If there are many of them, you may consider using another filter type
   * or optimize your total term count in index.
   * <p>This method is not thread safe, be sure to only call it when no filter is running!
   * If you re-use the same filter instance for another
   * search, be sure to first reset the term counter
   * with {@link #clearTotalNumberOfTerms}.
   * @see #clearTotalNumberOfTerms
   */
  public int getTotalNumberOfTerms() {
    return query.getTotalNumberOfTerms();
  }
  
  /**
   * Expert: Resets the counting of unique terms.
   * Do this before executing the filter.
   * @see #getTotalNumberOfTerms
   */
  public void clearTotalNumberOfTerms() {
    query.clearTotalNumberOfTerms();
  }

  abstract class TermGenerator {
    public void generate(IndexReader reader, TermEnum enumerator) throws IOException {
      final int[] docs = new int[32];
      final int[] freqs = new int[32];
      TermDocs termDocs = reader.termDocs();
      try {
        int termCount = 0;
        do {
          Term term = enumerator.term();
          if (term == null)
            break;
          termCount++;
          termDocs.seek(term);
          while (true) {
            final int count = termDocs.read(docs, freqs);
            if (count != 0) {
              for(int i=0;i<count;i++) {
                handleDoc(docs[i]);
              }
            } else {
              break;
            }
          }
        } while (enumerator.next());

        query.incTotalNumberOfTerms(termCount);

      } finally {
        termDocs.close();
      }
    }
    abstract public void handleDoc(int doc);
  }
  
  /**
   * Returns a BitSet with true for documents which should be
   * permitted in search results, and false for those that should
   * not.
   * @deprecated Use {@link #getDocIdSet(IndexReader)} instead.
   */
  //@Override
  public BitSet bits(IndexReader reader) throws IOException {
    final TermEnum enumerator = query.getEnum(reader);
    try {
      final BitSet bitSet = new BitSet(reader.maxDoc());
      new TermGenerator() {
        public void handleDoc(int doc) {
          bitSet.set(doc);
        }
      }.generate(reader, enumerator);
      return bitSet;
    } finally {
      enumerator.close();
    }
  }

  /**
   * Returns a DocIdSet with documents that should be
   * permitted in search results.
   */
  //@Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    final TermEnum enumerator = query.getEnum(reader);
    try {
      // if current term in enum is null, the enum is empty -> shortcut
      if (enumerator.term() == null)
        return DocIdSet.EMPTY_DOCIDSET;
      // else fill into a OpenBitSet
      final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
      new TermGenerator() {
        public void handleDoc(int doc) {
          bitSet.set(doc);
        }
      }.generate(reader, enumerator);
      return bitSet;
    } finally {
      enumerator.close();
    }
  }

}
