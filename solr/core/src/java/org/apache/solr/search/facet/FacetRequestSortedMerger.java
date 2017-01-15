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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.SimpleOrderedMap;

// base class for facets that create a list of buckets that can be sorted
abstract class FacetRequestSortedMerger<FacetRequestT extends FacetRequestSorted> extends FacetBucketMerger<FacetRequestT> {
  LinkedHashMap<Object,FacetBucket> buckets = new LinkedHashMap<>();
  List<FacetBucket> sortedBuckets;

  public FacetRequestSortedMerger(FacetRequestT freq) {
    super(freq);
  }

  private static class SortVal implements Comparable<SortVal> {
    FacetBucket bucket;
    FacetSortableMerger merger;  // make this class inner and access merger , direction in parent?
    FacetRequest.SortDirection direction;

    @Override
    public int compareTo(SortVal o) {
      int c = -merger.compareTo(o.merger, direction) * direction.getMultiplier();
      return c == 0 ? bucket.bucketValue.compareTo(o.bucket.bucketValue) : c;
    }
  }

  public void mergeBucketList(List<SimpleOrderedMap> bucketList, Context mcontext) {
    for (SimpleOrderedMap bucketRes : bucketList) {
      Comparable bucketVal = (Comparable)bucketRes.get("val");
      FacetBucket bucket = buckets.get(bucketVal);
      if (bucket == null) {
        bucket = newBucket(bucketVal, mcontext);
        buckets.put(bucketVal, bucket);
      }
      bucket.mergeBucket( bucketRes , mcontext );
    }
  }

  public void sortBuckets() {
    sortedBuckets = new ArrayList<>( buckets.values() );

    Comparator<FacetBucket> comparator = null;

    final FacetRequest.SortDirection direction = freq.sortDirection;
    final int sortMul = direction.getMultiplier();

    if ("count".equals(freq.sortVariable)) {
      comparator = (o1, o2) -> {
        int v = -Long.compare(o1.count, o2.count) * sortMul;
        return v == 0 ? o1.bucketValue.compareTo(o2.bucketValue) : v;
      };
      Collections.sort(sortedBuckets, comparator);
    } else if ("index".equals(freq.sortVariable)) {
      comparator = (o1, o2) -> -o1.bucketValue.compareTo(o2.bucketValue) * sortMul;
      Collections.sort(sortedBuckets, comparator);
    } else {
      final String key = freq.sortVariable;

      /**
       final FacetSortableMerger[] arr = new FacetSortableMerger[buckets.size()];
       final int[] index = new int[arr.length];
       int start = 0;
       int nullStart = index.length;
       int i=0;
       for (FacetBucket bucket : buckets.values()) {
       FacetMerger merger = bucket.getExistingMerger(key);
       if (merger == null) {
       index[--nullStart] = i;
       }
       if (merger != null) {
       arr[start] = (FacetSortableMerger)merger;
       index[start] = i;
       start++;
       }
       i++;
       }

       PrimUtils.sort(0, nullStart, index, new PrimUtils.IntComparator() {
      @Override
      public int compare(int a, int b) {
      return arr[index[a]].compareTo(arr[index[b]], direction);
      }
      });
       **/


      List<SortVal> lst = new ArrayList<>(buckets.size());
      List<FacetBucket> nulls = new ArrayList<>(buckets.size()>>1);
      for (int i=0; i<sortedBuckets.size(); i++) {
        FacetBucket bucket = sortedBuckets.get(i);
        FacetMerger merger = bucket.getExistingMerger(key);
        if (merger == null) {
          nulls.add(bucket);
        }
        if (merger != null) {
          SortVal sv = new SortVal();
          sv.bucket = bucket;
          sv.merger = (FacetSortableMerger)merger;
          sv.direction = direction;
          // sv.pos = i;  // if we need position in the future...
          lst.add(sv);
        }
      }
      Collections.sort(lst);
      Collections.sort(nulls, (o1, o2) -> o1.bucketValue.compareTo(o2.bucketValue));

      ArrayList<FacetBucket> out = new ArrayList<>(buckets.size());
      for (SortVal sv : lst) {
        out.add( sv.bucket );
      }
      out.addAll(nulls);
      sortedBuckets = out;
    }
  }


  @Override
  public Map<String, Object> getRefinement(Context mcontext) {
    // step 1) If this facet request has refining, then we need to fully request top buckets that were not seen by this shard.
    // step 2) If this facet does not have refining, but some sub-facets do, we need to check/recurse those sub-facets in *every* top bucket.
    // A combination of the two is possible and makes step 2 redundant for any buckets we fully requested in step 1.

    Map<String,Object> refinement = null;

    Collection<String> tags = mcontext.getSubsWithRefinement(freq);
    if (tags.isEmpty() && !freq.doRefine()) {
      // we don't have refining, and neither do our subs
      return null;
    }

    // Tags for sub facets that have partial facets somewhere in their children.
    // If we are missing a bucket for this shard, we'll need to get the specific buckets that need refining.
    Collection<String> tagsWithPartial = mcontext.getSubsWithPartial(freq);

    boolean thisMissing = mcontext.bucketWasMissing(); // Was this whole facet missing (i.e. inside a bucket that was missing)?

    // TODO: add information in sub-shard response about dropped buckets (i.e. not all returned due to limit)
    // If we know we've seen all the buckets from a shard, then we don't have to add to leafBuckets or missingBuckets, only skipBuckets
    boolean isCommandPartial = freq.returnsPartial();
    boolean returnedAllBuckets = !isCommandPartial && !thisMissing;  // did the shard return all of the possible buckets?

    if (returnedAllBuckets && tags.isEmpty() && tagsWithPartial.isEmpty()) {
      // this facet returned all possible buckets, and there were no sub-facets with partial results
      // and sub-facets that require refining
      return null;
    }


    int num = freq.limit < 0 ? Integer.MAX_VALUE : (int)(freq.offset + freq.limit);
    int numBucketsToCheck = Math.min(buckets.size(), num);

    Collection<FacetBucket> bucketList;
    if (buckets.size() < num) {
      // no need to sort
      // todo: but we may need to filter.... simplify by always sorting?
      bucketList = buckets.values();
    } else {
      // only sort once
      if (sortedBuckets == null) {
        sortBuckets();  // todo: make sure this filters buckets as well
      }
      bucketList = sortedBuckets;
    }

    ArrayList<Object> leafBuckets = null;    // "_l" missing buckets specified by bucket value only (no need to specify anything further)
    ArrayList<Object> missingBuckets = null; // "_m" missing buckets that need to specify values for partial facets.. each entry is [bucketval, subs]
    ArrayList<Object> skipBuckets = null;    // "_s" present buckets that we need to recurse into because children facets have refinement requirements. each entry is [bucketval, subs]

    for (FacetBucket bucket : bucketList) {
      if (numBucketsToCheck-- <= 0) break;
      // if this bucket is missing,
      assert thisMissing == false || thisMissing == true && mcontext.getShardFlag(bucket.bucketNumber) == false;
      boolean saw = !thisMissing && mcontext.getShardFlag(bucket.bucketNumber);
      if (!saw) {
        // we didn't see the bucket for this shard
        Map<String,Object> bucketRefinement = null;

        // find facets that we need to fill in buckets for
        if (!tagsWithPartial.isEmpty()) {
          boolean prev = mcontext.setBucketWasMissing(true);
          bucketRefinement = bucket.getRefinement(mcontext, tagsWithPartial);
          mcontext.setBucketWasMissing(prev);

          if (bucketRefinement != null) {
            if (missingBuckets==null) missingBuckets = new ArrayList<>();
            missingBuckets.add( Arrays.asList(bucket.bucketValue, bucketRefinement) );
          }
        }

        // if we didn't add to "_m" (missing), then we should add to "_l" (leaf missing)
        if (bucketRefinement == null) {
          if (leafBuckets == null) leafBuckets = new ArrayList<>();
          leafBuckets.add(bucket.bucketValue);
        }

      } else if (!tags.isEmpty()) {
        // we had this bucket, but we need to recurse to certain children that have refinements
        Map<String,Object> bucketRefinement = bucket.getRefinement(mcontext, tagsWithPartial);
        if (bucketRefinement != null) {
          if (skipBuckets == null) skipBuckets = new ArrayList<>();
          skipBuckets.add( Arrays.asList(bucket.bucketValue, bucketRefinement) );
        }
      }

    }

    // TODO: what if we don't need to refine any variable buckets, but we do need to contribute to numBuckets, missing, allBuckets, etc...
    // because we were "missing".  That will be handled at a higher level (i.e. we'll be in someone's missing bucket?)
    // TODO: test with a sub-facet with a limit of 0 and something like a missing bucket
    if (leafBuckets != null || missingBuckets != null || skipBuckets != null) {
      refinement = new HashMap<>(3);
      if (leafBuckets != null) refinement.put("_l",leafBuckets);
      if (missingBuckets != null) refinement.put("_m", missingBuckets);
      if (skipBuckets != null) refinement.put("_s", skipBuckets);
    }

    return refinement;
  }


}
