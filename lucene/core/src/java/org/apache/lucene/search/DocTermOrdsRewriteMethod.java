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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

/**
 * Rewrites MultiTermQueries into a filter, using DocTermOrds for term enumeration.
 * <p>
 * This can be used to perform these queries against an unindexed docvalues field.
 * @lucene.experimental
 */
public final class DocTermOrdsRewriteMethod extends MultiTermQuery.RewriteMethod {
  
  @Override
  public Query rewrite(IndexReader reader, MultiTermQuery query) {
    Query result = new ConstantScoreQuery(new MultiTermQueryDocTermOrdsWrapperFilter(query));
    result.setBoost(query.getBoost());
    return result;
  }
  
  static class MultiTermQueryDocTermOrdsWrapperFilter extends Filter {
    
    protected final MultiTermQuery query;
    
    /**
     * Wrap a {@link MultiTermQuery} as a Filter.
     */
    protected MultiTermQueryDocTermOrdsWrapperFilter(MultiTermQuery query) {
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
        return this.query.equals( ((MultiTermQueryDocTermOrdsWrapperFilter)o).query );
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
      final SortedSetDocValues docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), query.field);
      // Cannot use FixedBitSet because we require long index (ord):
      final OpenBitSet termSet = new OpenBitSet(docTermOrds.getValueCount());
      TermsEnum termsEnum = query.getTermsEnum(new Terms() {
        
        @Override
        public Comparator<BytesRef> getComparator() {
          return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
        
        @Override
        public TermsEnum iterator(TermsEnum reuse) {
          return docTermOrds.termsEnum();
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
          termSet.set(termsEnum.ord());
        } while (termsEnum.next() != null);
      } else {
        return DocIdSet.EMPTY_DOCIDSET;
      }
      
      return new FieldCacheDocIdSet(context.reader().maxDoc(), acceptDocs) {
        @Override
        protected final boolean matchDoc(int doc) throws ArrayIndexOutOfBoundsException {
          docTermOrds.setDocument(doc);
          long ord;
          // TODO: we could track max bit set and early terminate (since they come in sorted order)
          while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            if (termSet.get(ord)) {
              return true;
            }
          }
          return false;
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
    return 877;
  }
}
