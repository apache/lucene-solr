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

package org.apache.solr.search.join;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.Hash;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class HashRangeQuery extends Query {

  protected final String field;
  protected final int lower;
  protected final int upper;

  public static final String CACHE_KEY_PREFIX = "hash_";

  public HashRangeQuery(String field, int lower, int upper) {
    this.field = field;
    this.lower = lower;
    this.upper = upper;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public boolean isCacheable(LeafReaderContext context) {
        return DocValues.isCacheable(context, field);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        SortedDocValues docValues = context.reader().getSortedDocValues(field);
        int[] cache = getCache(context);

        TwoPhaseIterator iterator = new TwoPhaseIterator(docValues) {
          @Override
          public boolean matches() throws IOException {
            int hash = cache != null ? cache[docValues.docID()] : hash(docValues);
            return hash >= lower && hash <= upper;
          }

          @Override
          public float matchCost() {
            return cache != null ? 2 : 100;
          }
        };

        return new ConstantScoreScorer(this, boost, scoreMode, iterator);
      }

      private int[] getCache(LeafReaderContext context) throws IOException {
        IndexReader.CacheHelper cacheHelper = context.reader().getReaderCacheHelper();
        if (cacheHelper == null) {
          return null;
        }
        @SuppressWarnings("unchecked")
        final SolrCache<IndexReader.CacheKey, int[]> cache =
                ((SolrIndexSearcher) searcher).getCache(CACHE_KEY_PREFIX + field);
        if (cache == null) {
          return null;
        }

        IndexReader.CacheKey cacheKey = cacheHelper.getKey();
        synchronized (cacheKey) {
          int[] hashes = cache.get(cacheKey);
          if (hashes == null) {
            hashes = new int[context.reader().maxDoc()];
            SortedDocValues docValues = context.reader().getSortedDocValues(field);
            int doc;
            while ((doc = docValues.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              hashes[doc] = hash(docValues);
            }
            cache.put(cacheKey, hashes);
          }
          return hashes;
        }
      }

      private int hash(SortedDocValues docValues) throws IOException {
        //TODO maybe cache hashCode if same ord as prev doc to save lookupOrd?
        BytesRef bytesRef = docValues.lookupOrd(docValues.ordValue());
        return Hash.murmurhash3_x86_32(bytesRef.bytes, bytesRef.offset, bytesRef.length, 0);
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public String toString(String field) {
    return String.format(Locale.ROOT, "{!hash_range f=%s l=%d u=%d}", this.field, lower, upper);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
            equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(HashRangeQuery other) {
    return Objects.equals(field, other.field) &&
            Objects.equals(lower, other.lower) &&
            Objects.equals(upper, other.upper);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + Objects.hashCode(field);
    result = prime * result + Objects.hashCode(lower);
    result = prime * result + Objects.hashCode(upper);
    return result;
  }
}
