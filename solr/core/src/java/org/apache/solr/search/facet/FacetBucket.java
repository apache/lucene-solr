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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.util.SimpleOrderedMap;

public class FacetBucket {
  final FacetBucketMerger parent;
  final Comparable bucketValue;
  final int bucketNumber;  // this is just for internal correlation (the first bucket created is bucket 0, the next bucket 1, across all field buckets)

  long count;
  Map<String, FacetMerger> subs;

  public FacetBucket(FacetBucketMerger parent, Comparable bucketValue, FacetMerger.Context mcontext) {
    this.parent = parent;
    this.bucketValue = bucketValue;
    this.bucketNumber = mcontext.getNewBucketNumber(); // TODO: we don't need bucket numbers for all buckets...
  }

  public long getCount() {
    return count;
  }

  /** returns the existing merger for the given key, or null if none yet exists */
  FacetMerger getExistingMerger(String key) {
    if (subs == null) return null;
    return subs.get(key);
  }

  private FacetMerger getMerger(String key, Object prototype) {
    FacetMerger merger = null;
    if (subs != null) {
      merger = subs.get(key);
      if (merger != null) return merger;
    }

    merger = parent.createFacetMerger(key, prototype);

    if (merger != null) {
      if (subs == null) {
        subs = new HashMap<>();
      }
      subs.put(key, merger);
    }

    return merger;
  }

  public void mergeBucket(SimpleOrderedMap bucket, FacetMerger.Context mcontext) {
    // todo: for refinements, we want to recurse, but not re-do stats for intermediate buckets

    mcontext.setShardFlag(bucketNumber);

    // drive merging off the received bucket?
    for (int i=0; i<bucket.size(); i++) {
      String key = bucket.getName(i);
      Object val = bucket.getVal(i);
      if ("count".equals(key)) {
        count += ((Number)val).longValue();
        continue;
      }
      if ("val".equals(key)) {
        // this is taken care of at a higher level...
        continue;
      }

      FacetMerger merger = getMerger(key, val);

      if (merger != null) {
        merger.merge( val , mcontext );
      }
    }
  }


  public SimpleOrderedMap getMergedBucket() {
    SimpleOrderedMap out = new SimpleOrderedMap( (subs == null ? 0 : subs.size()) + 2 );
    if (bucketValue != null) {
      out.add("val", bucketValue);
    }
    out.add("count", count);
    if (subs != null) {
      for (Map.Entry<String,FacetMerger> mergerEntry : subs.entrySet()) {
        FacetMerger subMerger = mergerEntry.getValue();
        out.add(mergerEntry.getKey(), subMerger.getMergedResult());
      }
    }

    return out;
  }

  public Map<String, Object> getRefinement(FacetMerger.Context mcontext, Collection<String> refineTags) {
    if (subs == null) {
      return null;
    }
    Map<String,Object> refinement = null;
    for (String tag : refineTags) {
      FacetMerger subMerger = subs.get(tag);
      if (subMerger != null) {
        Map<String,Object> subRef = subMerger.getRefinement(mcontext);
        if (subRef != null) {
          if (refinement == null) {
            refinement = new HashMap<>(refineTags.size());
          }
          refinement.put(tag, subRef);
        }
      }
    }
    return refinement;
  }

  public Map<String, Object> getRefinement2(FacetMerger.Context mcontext, Collection<String> refineTags) {
    // TODO - partial results should turn off refining!!!

    boolean parentMissing = mcontext.bucketWasMissing();

    // TODO: this is a redundant check for many types of facets... only do on field faceting
    if (!parentMissing) {
      // if parent bucket wasn't missing, check if this bucket was.
      // this really only needs checking on certain buckets... (like terms facet)
      boolean sawThisBucket = mcontext.getShardFlag(bucketNumber);
      if (!sawThisBucket) {
        mcontext.setBucketWasMissing(true);
      }
    } else {
      // if parent bucket was missing, then we should be too
      assert !mcontext.getShardFlag(bucketNumber);
    }

    Map<String,Object> refinement = null;

    if (!mcontext.bucketWasMissing()) {
      // this is just a pass-through bucket... see if there is anything to do at all
      if (subs == null || refineTags.isEmpty()) {
        return null;
      }
    } else {
      // for missing bucket, go over all sub-facts
      refineTags = null;
      refinement = new HashMap<>(4);
      if (bucketValue != null) {
        refinement.put("_v", bucketValue);
      }
      refinement.put("_m",1);
    }

    // TODO: listing things like sub-facets that have no field facets are redundant
    // (we only need facet that have variable values)

    for (Map.Entry<String,FacetMerger> sub : subs.entrySet()) {
      if (refineTags != null && !refineTags.contains(sub.getKey())) {
        continue;
      }
      Map<String,Object> subRef = sub.getValue().getRefinement(mcontext);
      if (subRef != null) {
        if (refinement == null) {
          refinement = new HashMap<>(4);
        }
        refinement.put(sub.getKey(), subRef);
      }
    }


    // reset the "bucketMissing" flag on the way back out.
    mcontext.setBucketWasMissing(parentMissing);
    return refinement;
  }

}
