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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.util.SimpleOrderedMap;


// TODO: refactor more out to base class
public class FacetFieldMerger extends FacetRequestSortedMerger<FacetField> {
  FacetBucket missingBucket;
  FacetBucket allBuckets;
  FacetMerger numBuckets;
  int[] numReturnedPerShard;

  // LinkedHashMap<Object,FacetBucket> buckets = new LinkedHashMap<>();
  // List<FacetBucket> sortedBuckets;
  int numReturnedBuckets; // the number of buckets in the bucket lists returned from all of the shards


  public FacetFieldMerger(FacetField freq) {
    super(freq);
  }

  @Override
  public void merge(Object facetResult, Context mcontext) {
    if (numReturnedPerShard == null) {
      numReturnedPerShard = new int[mcontext.numShards];
    }
    merge((SimpleOrderedMap)facetResult, mcontext);
  }

  protected void merge(SimpleOrderedMap facetResult, Context mcontext) {
    if (freq.missing) {
      Object o = facetResult.get("missing");
      if (o != null) {
        if (missingBucket == null) {
          missingBucket = newBucket(null, mcontext);
        }
        missingBucket.mergeBucket((SimpleOrderedMap)o , mcontext);
      }
    }

    if (freq.allBuckets) {
      Object o = facetResult.get("allBuckets");
      if (o != null) {
        if (allBuckets == null) {
          allBuckets = newBucket(null, mcontext);
        }
        allBuckets.mergeBucket((SimpleOrderedMap)o , mcontext);
      }
    }

    List<SimpleOrderedMap> bucketList = (List<SimpleOrderedMap>) facetResult.get("buckets");
    numReturnedPerShard[mcontext.shardNum] = bucketList.size();
    numReturnedBuckets += bucketList.size();
    mergeBucketList(bucketList , mcontext);

    if (freq.numBuckets) {
      Object nb = facetResult.get("numBuckets");
      if (nb != null) {
        if (numBuckets == null) {
          numBuckets = new FacetNumBucketsMerger();
        }
        numBuckets.merge(nb , mcontext);
      }
    }

  }




  @Override
  public Object getMergedResult() {
    SimpleOrderedMap result = new SimpleOrderedMap();

    if (numBuckets != null) {
      int removed = 0;
      if (freq.mincount > 1) {
        for (FacetBucket bucket : buckets.values()) {
          if (bucket.count < freq.mincount) removed++;
        }
      }
      result.add("numBuckets", ((Number)numBuckets.getMergedResult()).longValue() - removed);

      // TODO: we can further increase this estimate.
      // If not sorting by count, use a simple ratio to scale
      // If sorting by count desc, then add up the highest_possible_missing_count from each shard
    }

    sortBuckets();

    long first = freq.offset;
    long end = freq.limit >=0 ? first + (int) freq.limit : Integer.MAX_VALUE;
    long last = Math.min(sortedBuckets.size(), end);

    List<SimpleOrderedMap> resultBuckets = new ArrayList<>(Math.max(0, (int)(last - first)));

    /** this only works if there are no filters (like mincount)
    for (int i=first; i<last; i++) {
      FacetBucket bucket = sortedBuckets.get(i);
      resultBuckets.add( bucket.getMergedBucket() );
    }
    ***/

    // TODO: change effective offsets + limits at shards...

    int off = (int)freq.offset;
    int lim = freq.limit >= 0 ? (int)freq.limit : Integer.MAX_VALUE;
    for (FacetBucket bucket : sortedBuckets) {
      if (bucket.getCount() < freq.mincount) {
        continue;
      }

      if (off > 0) {
        --off;
        continue;
      }

      if (resultBuckets.size() >= lim) {
        break;
      }

      resultBuckets.add( bucket.getMergedBucket() );
    }


    result.add("buckets", resultBuckets);
    if (missingBucket != null) {
      result.add("missing", missingBucket.getMergedBucket());
    }
    if (allBuckets != null) {
      result.add("allBuckets", allBuckets.getMergedBucket());
    }

    return result;
  }


  @Override
  public void finish(Context mcontext) {
    // TODO: check refine of subs?
    // TODO: call subs each time with a shard/shardnum that is missing a bucket at this level?
    // or pass a bit vector of shards w/ value???

    // build up data structure and only then call the context (or whatever) to do the refinement?
    // basically , only do at the top-level facet?
  }



  private class FacetNumBucketsMerger extends FacetMerger {
    long sumBuckets;
    long shardsMissingSum;
    long shardsTruncatedSum;
    Set<Object> values;

    @Override
    public void merge(Object facetResult, Context mcontext) {
      SimpleOrderedMap map = (SimpleOrderedMap)facetResult;
      long numBuckets = ((Number)map.get("numBuckets")).longValue();
      sumBuckets += numBuckets;

      List vals = (List)map.get("vals");
      if (vals != null) {
        if (values == null) {
          values = new HashSet<>(vals.size()*4);
        }
        values.addAll(vals);
        if (numBuckets > values.size()) {
          shardsTruncatedSum += numBuckets - values.size();
        }
      } else {
        shardsMissingSum += numBuckets;
      }
    }

    @Override
    public void finish(Context mcontext) {
      // nothing to do
    }

    @Override
    public Object getMergedResult() {
      long exactCount = values == null ? 0 : values.size();
      return exactCount + shardsMissingSum + shardsTruncatedSum;
      // TODO: reduce count by (at least) number of buckets that fail to hit mincount (after merging)
      // that should make things match for most of the small tests at least
    }
  }
}
