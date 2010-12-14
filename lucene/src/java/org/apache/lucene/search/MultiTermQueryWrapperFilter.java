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
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.BulkPostingsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Bits;

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
public class MultiTermQueryWrapperFilter<Q extends MultiTermQuery> extends Filter {
    
  protected final Q query;

  /**
   * Wrap a {@link MultiTermQuery} as a Filter.
   */
  protected MultiTermQueryWrapperFilter(Q query) {
      this.query = query;
  }
  
  @Override
  public String toString() {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString();
  }

  @Override
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (o==null) return false;
    if (this.getClass().equals(o.getClass())) {
      return this.query.equals( ((MultiTermQueryWrapperFilter)o).query );
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return query.hashCode();
  }

  /** Returns the field name for this query */
  public final String getField() { return query.getField(); }
  
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
  
  /**
   * Returns a DocIdSet with documents that should be permitted in search
   * results.
   */
  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    final Fields fields = MultiFields.getFields(reader);
    if (fields == null) {
      // reader has no fields
      return DocIdSet.EMPTY_DOCIDSET;
    }

    final Terms terms = fields.terms(query.field);
    if (terms == null) {
      // field does not exist
      return DocIdSet.EMPTY_DOCIDSET;
    }

    final TermsEnum termsEnum = query.getTermsEnum(terms);
    assert termsEnum != null;
    if (termsEnum.next() != null) {
      // fill into a OpenBitSet
      final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
      int termCount = 0;
      final Bits delDocs = MultiFields.getDeletedDocs(reader);
      BulkPostingsEnum postingsEnum = null;
      do {
        termCount++;
        postingsEnum = termsEnum.bulkPostings(postingsEnum, false, false);
        final int docFreq = termsEnum.docFreq();
        final BulkPostingsEnum.BlockReader docDeltasReader = postingsEnum.getDocDeltasReader();
        final int[] docDeltas = docDeltasReader.getBuffer();
        int offset = docDeltasReader.offset();
        int limit = docDeltasReader.end();
        if (offset >= limit) {
          limit = docDeltasReader.fill();
        }
        int count = 0;
        int doc = 0;
        while (count < docFreq) {
          if (offset >= limit) {
            offset = 0;
            limit = docDeltasReader.fill();
          }
          doc += docDeltas[offset++];
          count++;
          if (delDocs == null || !delDocs.get(doc)) {
            bitSet.set(doc);
          }
        }
      } while (termsEnum.next() != null);

      query.incTotalNumberOfTerms(termCount);
      return bitSet;
    } else {
      return DocIdSet.EMPTY_DOCIDSET;
    }
  }
}
