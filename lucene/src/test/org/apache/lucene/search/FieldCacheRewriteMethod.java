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
import java.util.Comparator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

/**
 * Rewrites MultiTermQueries into a filter, using the FieldCache for term enumeration.
 * <p>
 * WARNING: This is only appropriate for single-valued unanalyzed fields. Additionally, for 
 * most queries this method is actually SLOWER than using the default CONSTANT_SCORE_AUTO 
 * in MultiTermQuery. This method is only faster than other methods for certain queries,
 * such as ones that enumerate many terms.
 * 
 * @lucene.experimental
 */
public final class FieldCacheRewriteMethod extends MultiTermQuery.RewriteMethod {
  
  @Override
  public Query rewrite(IndexReader reader, MultiTermQuery query) {
    Query result = new ConstantScoreQuery(new MultiTermQueryFieldCacheWrapperFilter(query));
    result.setBoost(query.getBoost());
    return result;
  }
  
  static class MultiTermQueryFieldCacheWrapperFilter extends Filter {
    
    protected final MultiTermQuery query;
    
    /**
     * Wrap a {@link MultiTermQuery} as a Filter.
     */
    protected MultiTermQueryFieldCacheWrapperFilter(MultiTermQuery query) {
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
        return this.query.equals( ((MultiTermQueryFieldCacheWrapperFilter)o).query );
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
    public DocIdSet getDocIdSet(AtomicReaderContext context) throws IOException {
      final FieldCache.DocTermsIndex fcsi = FieldCache.DEFAULT.getTermsIndex(context.reader, query.field);
      // Cannot use FixedBitSet because we require long index (ord):
      final OpenBitSet termSet = new OpenBitSet(fcsi.numOrd());
      TermsEnum termsEnum = query.getTermsEnum(new Terms() {
        
        @Override
        public Comparator<BytesRef> getComparator() throws IOException {
          return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
        
        @Override
        public TermsEnum iterator() throws IOException {
          return fcsi.getTermsEnum();
        }

        @Override
        public long getSumTotalTermFreq() {
          return -1;
        }

        @Override
        public long getSumDocFreq() throws IOException {
          return -1;
        }

        @Override
        public int getDocCount() throws IOException {
          return -1;
        }

        @Override
        public long getUniqueTermCount() throws IOException {
          return -1;
        }
      });
      
      assert termsEnum != null;
      if (termsEnum.next() != null) {
        // fill into a OpenBitSet
        int termCount = 0;
        do {
          long ord = termsEnum.ord();
          if (ord > 0) {
            termSet.set(ord);
            termCount++;
          }
        } while (termsEnum.next() != null);
        
        query.incTotalNumberOfTerms(termCount);
      } else {
        return DocIdSet.EMPTY_DOCIDSET;
      }
      
      return new FieldCacheRangeFilter.FieldCacheDocIdSet(context.reader, true) {
        @Override
        boolean matchDoc(int doc) throws ArrayIndexOutOfBoundsException {
          return termSet.get(fcsi.getOrd(doc));
        }
      };
    }
  }
}
