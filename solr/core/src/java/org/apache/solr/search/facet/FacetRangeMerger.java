
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

package org.apache.solr.search.facet;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.SimpleOrderedMap;

public class FacetRangeMerger extends FacetRequestSortedMerger<FacetRange> {
  FacetBucket beforeBucket;
  FacetBucket afterBucket;
  FacetBucket betweenBucket;

  public FacetRangeMerger(FacetRange freq) {
    super(freq);
  }

  @Override
  FacetMerger createFacetMerger(String key, Object val) {
    return super.createFacetMerger(key, val);
  }

  @Override
  public void merge(Object facetResult, Context mcontext) {
    merge((SimpleOrderedMap) facetResult , mcontext);
  }

  @Override
  public void sortBuckets() {
    // TODO: mincount>0 will mess up order?
    sortedBuckets = new ArrayList<>( buckets.values() );
  }

  @Override
  public void finish(Context mcontext) {
    // nothing to do
  }

  public void merge(SimpleOrderedMap facetResult, Context mcontext) {
    boolean all = freq.others.contains(FacetParams.FacetRangeOther.ALL);

    if (all || freq.others.contains(FacetParams.FacetRangeOther.BEFORE)) {
      Object o = facetResult.get("before");
      if (o != null) {
        if (beforeBucket == null) {
          beforeBucket = newBucket(null, mcontext);
        }
        beforeBucket.mergeBucket((SimpleOrderedMap)o, mcontext);
      }
    }

    if (all || freq.others.contains(FacetParams.FacetRangeOther.AFTER)) {
      Object o = facetResult.get("after");
      if (o != null) {
        if (afterBucket == null) {
          afterBucket = newBucket(null, mcontext);
        }
        afterBucket.mergeBucket((SimpleOrderedMap)o , mcontext);
      }
    }

    if (all || freq.others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
      Object o = facetResult.get("between");
      if (o != null) {
        if (betweenBucket == null) {
          betweenBucket = newBucket(null, mcontext);
        }
        betweenBucket.mergeBucket((SimpleOrderedMap)o , mcontext);
      }
    }

    List<SimpleOrderedMap> bucketList = (List<SimpleOrderedMap>) facetResult.get("buckets");
    mergeBucketList(bucketList , mcontext);
  }


  @Override
  public Object getMergedResult() {
    // TODO: use sortedBuckets
    SimpleOrderedMap result = new SimpleOrderedMap(4);

    List<SimpleOrderedMap> resultBuckets = new ArrayList<>(buckets.size());

    for (FacetBucket bucket : buckets.values()) {
       if (bucket.getCount() < freq.mincount) {
         continue;
       }
      resultBuckets.add( bucket.getMergedBucket() );
    }

    result.add("buckets", resultBuckets);

    if (beforeBucket != null) {
      result.add("before", beforeBucket.getMergedBucket());
    }
    if (afterBucket != null) {
      result.add("after", afterBucket.getMergedBucket());
    }
    if (betweenBucket != null) {
      result.add("between", betweenBucket.getMergedBucket());
    }
    return result;

  }
}
