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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.JavaBinDecoder;
import org.apache.solr.common.util.SimpleOrderedMap;


// TODO: refactor more out to base class
public class FacetFieldMerger extends FacetRequestSortedMerger<FacetField> {
  FacetBucket missingBucket;
  FacetBucket allBuckets;
  FacetMerger numBuckets;
  int[] numReturnedPerShard;  // TODO: this is currently unused?

  // LinkedHashMap<Object,FacetBucket> buckets = new LinkedHashMap<>();
  // List<FacetBucket> sortedBuckets;
  int numReturnedBuckets; // the number of buckets in the bucket lists returned from all of the shards


  public FacetFieldMerger(FacetField freq) {
    super(freq);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void merge(Object facetResult, Context mcontext) {
    super.merge(facetResult, mcontext);
    if (numReturnedPerShard == null) {
      numReturnedPerShard = new int[mcontext.numShards];
    }
    merge((SimpleOrderedMap)facetResult, mcontext);
  }

  protected void merge(@SuppressWarnings("rawtypes") SimpleOrderedMap facetResult, Context mcontext) {
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


    @SuppressWarnings({"unchecked", "rawtypes"})
    List<SimpleOrderedMap> bucketList = (List<SimpleOrderedMap>) facetResult.get("buckets");
    numReturnedPerShard[mcontext.shardNum] = bucketList.size();
    numReturnedBuckets += bucketList.size();
    mergeBucketList(bucketList , mcontext);

    if (freq.numBuckets) {
      Object nb = facetResult.get("numBuckets");
      if (nb != null) {
        if (numBuckets == null) {
          numBuckets = new HLLAgg("hll_merger").createFacetMerger(nb);
        }
        numBuckets.merge(nb , mcontext);
      }
    }

  }




  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object getMergedResult() {
    SimpleOrderedMap result = new SimpleOrderedMap();

    if (numBuckets != null) {
      result.add("numBuckets", ((Number)numBuckets.getMergedResult()).longValue());
    }

    sortBuckets(freq.sort);

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

    boolean refine = freq.refine != null && freq.refine != FacetRequest.RefineMethod.NONE;

    int off = (int)freq.offset;
    int lim = freq.limit >= 0 ? (int)freq.limit : Integer.MAX_VALUE;
    for (FacetBucket bucket : sortedBuckets) {
      if (bucket.getCount() < freq.mincount) {
        continue;
      }
      if (refine && !isBucketComplete(bucket,mcontext)) {
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
  public Object getPrototype() {
    return null;
  }

  @Override
  public void readState(JavaBinDecoder codec, Context mcontext) throws IOException {
    if (codec.readBoolean()) {
      missingBucket = newBucket(null, mcontext);
      missingBucket.readState(codec, mcontext);
    } else {
      missingBucket = null;
    }
    if (codec.readBoolean()) {
      allBuckets = newBucket(null, mcontext);
      allBuckets.readState(codec, mcontext);
    } else {
      allBuckets = null;
    }
    if (codec.readBoolean()) {
      numBuckets = new HLLAgg("hll_merger").createFacetMerger(0L);
      numBuckets.readState(codec, mcontext);
    } else {
      numBuckets = null;
    }
    super.readState(codec, mcontext);
  }

  @Override
  public void writeState(JavaBinCodec codec) throws IOException {
    codec.writeBoolean(missingBucket != null);
    if (missingBucket != null) {
      missingBucket.writeState(codec);
    }
    codec.writeBoolean(allBuckets != null);
    if (allBuckets != null) {
      allBuckets.writeState(codec);
    }
    codec.writeBoolean(numBuckets != null);
    if (numBuckets != null) {
      numBuckets.writeState(codec);
    }
    super.writeState(codec);
  }

  @Override
  public void finish(Context mcontext) {
    // TODO: check refine of subs?
    // TODO: call subs each time with a shard/shardnum that is missing a bucket at this level?
    // or pass a bit vector of shards w/ value???

    // build up data structure and only then call the context (or whatever) to do the refinement?
    // basically , only do at the top-level facet?
  }

  @Override
  Map<String, Object> getRefinementSpecial(Context mcontext, Map<String, Object> refinement, Collection<String> tagsWithPartial) {
    if (!tagsWithPartial.isEmpty()) {
      // Since special buckets missing and allBuckets themselves will always be included, we only need to worry about subfacets being partial.
      if (freq.missing) {
        refinement = getRefinementSpecial(mcontext, refinement, tagsWithPartial, missingBucket, "missing");
      }
      /** allBuckets does not execute sub-facets because we don't change the domain.  We may need refinement info in the future though for stats.
      if (freq.allBuckets) {
        refinement = getRefinementSpecial(mcontext, refinement, tagsWithPartial, allBuckets, "allBuckets");
      }
       **/
    }
    return refinement;
  }

  private Map<String, Object> getRefinementSpecial(Context mcontext, Map<String, Object> refinement, Collection<String> tagsWithPartial, FacetBucket bucket, String label) {
    // boolean prev = mcontext.setBucketWasMissing(true); // the special buckets should have the same "missing" status as this facet, so no need to set it again
    Map<String, Object> bucketRefinement = bucket.getRefinement(mcontext, tagsWithPartial);
    if (bucketRefinement != null) {
      refinement = refinement == null ? new HashMap<>(2) : refinement;
      refinement.put(label, bucketRefinement);
    }
    return refinement;
  }
}
