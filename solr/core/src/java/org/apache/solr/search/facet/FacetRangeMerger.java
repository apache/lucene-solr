
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.SimpleOrderedMap;

public class FacetRangeMerger extends FacetRequestSortedMerger<FacetRange> {
  FacetBucket beforeBucket;
  FacetBucket afterBucket;
  FacetBucket betweenBucket;
  Object actual_end = null;

  public FacetRangeMerger(FacetRange freq) {
    super(freq);
  }

  @Override
  public void merge(Object facetResult, Context mcontext) {
    super.merge(facetResult, mcontext);
    merge((SimpleOrderedMap) facetResult , mcontext);
  }

  @Override
  public void sortBuckets(final FacetRequest.FacetSort sort) {
    // regardless of sort or mincount, every shard returns a consistent set of buckets which are already in the correct order
    sortedBuckets = new ArrayList<>( buckets.values() );
  }

  @Override
  public void finish(Context mcontext) {
    // nothing to do
  }
  
  @Override
  Map<String, Object> getRefinementSpecial(Context mcontext, Map<String, Object> refinement, Collection<String> tagsWithPartial) {
    if (!tagsWithPartial.isEmpty()) {
      // Since 'other' buckets will always be included, we only need to worry about subfacets being partial.

      refinement = getRefinementSpecial(mcontext, refinement, tagsWithPartial, beforeBucket, FacetParams.FacetRangeOther.BEFORE.toString());
      refinement = getRefinementSpecial(mcontext, refinement, tagsWithPartial, afterBucket, FacetParams.FacetRangeOther.AFTER.toString());
      refinement = getRefinementSpecial(mcontext, refinement, tagsWithPartial, betweenBucket, FacetParams.FacetRangeOther.BETWEEN.toString());

      // if we need an actual end to compute either of these buckets,
      // and it's been returned to us by at least one shard
      // send it back as part of the refinement request
      if ( (!freq.hardend) &&
           actual_end != null &&
           refinement != null &&
           (refinement.containsKey(FacetParams.FacetRangeOther.AFTER.toString()) ||
            refinement.containsKey(FacetParams.FacetRangeOther.BETWEEN.toString())) ) {
        refinement.put("_actual_end", actual_end);
      }
    }
    return refinement;
  }
  
  private Map<String, Object> getRefinementSpecial(Context mcontext, Map<String, Object> refinement, Collection<String> tagsWithPartial, FacetBucket bucket, String label) {
    if (null == bucket) {
      return refinement;
    }
    Map<String, Object> bucketRefinement = bucket.getRefinement(mcontext, tagsWithPartial);
    if (bucketRefinement != null) {
      refinement = refinement == null ? new HashMap<>(2) : refinement;
      refinement.put(label, bucketRefinement);
    }
    return refinement;
  }
  
  public void merge(@SuppressWarnings("rawtypes") SimpleOrderedMap facetResult, Context mcontext) {
    boolean all = freq.others.contains(FacetParams.FacetRangeOther.ALL);

    if (all || freq.others.contains(FacetParams.FacetRangeOther.BEFORE)) {
      Object o = facetResult.get(FacetParams.FacetRangeOther.BEFORE.toString());
      if (o != null) {
        if (beforeBucket == null) {
          beforeBucket = newBucket(null, mcontext);
        }
        beforeBucket.mergeBucket((SimpleOrderedMap)o, mcontext);
      }
    }

    if (all || freq.others.contains(FacetParams.FacetRangeOther.AFTER)) {
      Object o = facetResult.get(FacetParams.FacetRangeOther.AFTER.toString());
      if (o != null) {
        if (afterBucket == null) {
          afterBucket = newBucket(null, mcontext);
        }
        afterBucket.mergeBucket((SimpleOrderedMap)o , mcontext);
      }
    }

    if (all || freq.others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
      Object o = facetResult.get(FacetParams.FacetRangeOther.BETWEEN.toString());
      if (o != null) {
        if (betweenBucket == null) {
          betweenBucket = newBucket(null, mcontext);
        }
        betweenBucket.mergeBucket((SimpleOrderedMap)o , mcontext);
      }
    }

    Object shard_actual_end = facetResult.get(FacetRange.ACTUAL_END_JSON_KEY);
    if (null != shard_actual_end) {
      if (null == actual_end) {
        actual_end = shard_actual_end;
      } else {
        assert actual_end.equals(shard_actual_end) : actual_end + " != " + shard_actual_end;
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<SimpleOrderedMap> bucketList = (List<SimpleOrderedMap>) facetResult.get("buckets");
    mergeBucketList(bucketList , mcontext);
  }


  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
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
      result.add(FacetParams.FacetRangeOther.BEFORE.toString(), beforeBucket.getMergedBucket());
    }
    if (afterBucket != null) {
      result.add(FacetParams.FacetRangeOther.AFTER.toString(), afterBucket.getMergedBucket());
    }
    if (betweenBucket != null) {
      result.add(FacetParams.FacetRangeOther.BETWEEN.toString(), betweenBucket.getMergedBucket());
    }
    return result;

  }
}
