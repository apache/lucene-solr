package org.apache.lucene.search;

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
import java.util.Comparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

/**
 * Rewrites MultiTermQueries into a filter, using the FieldCache for term enumeration.
 * <p>
 * This can be used to perform these queries against an unindexed docvalues field.
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
     * Returns a DocIdSet with documents that should be permitted in search
     * results.
     */
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      final SortedDocValues fcsi = FieldCache.DEFAULT.getTermsIndex(context.reader(), query.field);
      // Cannot use FixedBitSet because we require long index (ord):
      final OpenBitSet termSet = new OpenBitSet(fcsi.getValueCount());
      TermsEnum termsEnum = query.getTermsEnum(new Terms() {
        
        @Override
        public Comparator<BytesRef> getComparator() {
          return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
        
        @Override
        public TermsEnum iterator(TermsEnum reuse) {
          return fcsi.termsEnum();
        }

        @Override
        public long getSumTotalTermFreq() {
          return -1;
        }

        @Override
        public long getSumDocFreq() {
          return -1;
        }

        @Override
        public int getDocCount() {
          return -1;
        }

        @Override
        public long size() {
          return -1;
        }

        @Override
        public boolean hasOffsets() {
          return false;
        }

        @Override
        public boolean hasPositions() {
          return false;
        }
        
        @Override
        public boolean hasPayloads() {
          return false;
        }
      });
      
      assert termsEnum != null;
      if (termsEnum.next() != null) {
        // fill into a OpenBitSet
        do {
          long ord = termsEnum.ord();
          if (ord >= 0) {
            termSet.set(ord);
          }
        } while (termsEnum.next() != null);
      } else {
        return DocIdSet.EMPTY_DOCIDSET;
      }
      
      return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
        @Override
        protected final boolean matchDoc(int doc) throws ArrayIndexOutOfBoundsException {
          int ord = fcsi.getOrd(doc);
          if (ord == -1) {
            return false;
          }
          return termSet.get(ord);
        }
      };
    }
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    return 641;
  }
}
