package org.apache.lucene.search.join;

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

import static org.apache.lucene.search.DocIdSet.EMPTY;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;

/** A {@link CachingWrapperFilter} that caches sets using a {@link FixedBitSet},
 *  as required for joins. */
public final class FixedBitSetCachingWrapperFilter extends CachingWrapperFilter {

  /** Sole constructor, see {@link CachingWrapperFilter#CachingWrapperFilter(Filter)}. */
  public FixedBitSetCachingWrapperFilter(Filter filter) {
    super(filter);
  }

  @Override
  protected DocIdSet docIdSetToCache(DocIdSet docIdSet, AtomicReader reader)
      throws IOException {
    if (docIdSet == null) {
      return EMPTY;
    } else if (docIdSet instanceof FixedBitSet) {
      // this is different from CachingWrapperFilter: even when the DocIdSet is
      // cacheable, we convert it to a FixedBitSet since we require all the
      // cached filters to be FixedBitSets
      return docIdSet;
    } else {
      final DocIdSetIterator it = docIdSet.iterator();
      if (it == null) {
        return EMPTY;
      } else {
        final FixedBitSet copy = new FixedBitSet(reader.maxDoc());
        copy.or(it);
        return copy;
      }
    }
  }

}
