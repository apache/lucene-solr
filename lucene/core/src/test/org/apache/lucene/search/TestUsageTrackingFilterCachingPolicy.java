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

import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RoaringDocIdSet;

public class TestUsageTrackingFilterCachingPolicy extends LuceneTestCase {

  public void testCheapToCache() {
    assertTrue(UsageTrackingFilterCachingPolicy.isCheapToCache(null));
    assertTrue(UsageTrackingFilterCachingPolicy.isCheapToCache(DocIdSet.EMPTY));
    assertTrue(UsageTrackingFilterCachingPolicy.isCheapToCache(new RoaringDocIdSet.Builder(5).add(3).build()));
    assertFalse(UsageTrackingFilterCachingPolicy.isCheapToCache(new DocValuesDocIdSet(5, null) {
      @Override
      protected boolean matchDoc(int doc) {
        return false;
      }
    }));
  }

  public void testCostlyFilter() {
    assertTrue(UsageTrackingFilterCachingPolicy.isCostly(new QueryWrapperFilter(new PrefixQuery(new Term("field", "prefix")))));
    assertTrue(UsageTrackingFilterCachingPolicy.isCostly(new QueryWrapperFilter(NumericRangeQuery.newIntRange("intField", 8, 1, 1000, true, true))));
    assertFalse(UsageTrackingFilterCachingPolicy.isCostly(new QueryWrapperFilter(new TermQuery(new Term("field", "value")))));
  }

}
