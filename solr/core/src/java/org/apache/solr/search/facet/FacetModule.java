package org.apache.solr.search.facet;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FacetModule extends SearchComponent {
  public static Logger log = LoggerFactory.getLogger(FacetModule.class);

  public static final String COMPONENT_NAME = "facet_module";

  // Ensure these don't overlap with other PURPOSE flags in ShardRequest
  // The largest current flag in ShardRequest is 0x00002000
  // We'll put our bits in the middle to avoid future ones in ShardRequest and
  // custom ones that may start at the top.
  public final static int PURPOSE_GET_JSON_FACETS      = 0x00100000;
  public final static int PURPOSE_REFINE_JSON_FACETS   = 0x00200000;

  // Internal information passed down from the top level to shards for distributed faceting.
  private final static String FACET_STATE = "_facet_";


  public FacetComponentState getFacetComponentState(ResponseBuilder rb) {
    // TODO: put a map on ResponseBuilder?
    // rb.componentInfo.get(FacetComponentState.class);
    return (FacetComponentState) rb.req.getContext().get(FacetComponentState.class);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // if this is null, faceting is not enabled
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    boolean isShard = rb.req.getParams().getBool(ShardParams.IS_SHARD, false);

    FacetContext fcontext = new FacetContext();
    fcontext.base = rb.getResults().docSet;
    fcontext.req = rb.req;
    fcontext.searcher = rb.req.getSearcher();
    fcontext.qcontext = QueryContext.newContext(fcontext.searcher);
    if (isShard) {
      fcontext.flags |= FacetContext.IS_SHARD;
    }

    FacetProcessor fproc = facetState.facetRequest.createFacetProcessor(fcontext);
    fproc.process();
    rb.rsp.add("facets", fproc.getResponse());
  }


  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    Map<String,Object> json = rb.req.getJSON();
    Map<String,Object> jsonFacet = null;
    if (json == null) {
      int version = rb.req.getParams().getInt("facet.version",1);
      if (version <= 1) return;
      boolean facetsEnabled = rb.req.getParams().getBool(FacetParams.FACET, false);
      if (!facetsEnabled) return;
      jsonFacet = new LegacyFacet(rb.req.getParams()).getLegacy();
    } else {
      jsonFacet = (Map<String, Object>) json.get("facet");
    }
    if (jsonFacet == null) return;

    SolrParams params = rb.req.getParams();

    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    if (isShard) {
      String jfacet = params.get(FACET_STATE);
      if (jfacet == null) {
        // if this is a shard request, but there is no facet state, then don't do anything.
        return;
      }
    }

    // At this point, we know we need to do something.  Create and save the state.
    rb.setNeedDocSet(true);

    // Parse the facet in the prepare phase?
    FacetParser parser = new FacetTopParser(rb.req);
    FacetRequest facetRequest = null;
    try {
      facetRequest = parser.parse(jsonFacet);
    } catch (SyntaxError syntaxError) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
    }

    FacetComponentState fcState = new FacetComponentState();
    fcState.rb = rb;
    fcState.isShard = isShard;
    fcState.facetCommands = jsonFacet;
    fcState.facetRequest = facetRequest;

    rb.req.getContext().put(FacetComponentState.class, fcState);
  }



  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return ResponseBuilder.STAGE_DONE;

    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,ShardRequest sreq) {
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      sreq.purpose |= FacetModule.PURPOSE_GET_JSON_FACETS;
      sreq.params.set(FACET_STATE, "{}");
    } else {
      // turn off faceting on other requests
      sreq.params.remove("json.facet");
      sreq.params.remove(FACET_STATE);
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    for (ShardResponse shardRsp : sreq.responses) {
      SolrResponse rsp = shardRsp.getSolrResponse();
      NamedList<Object> top = rsp.getResponse();
      Object facet = top.get("facets");
      if (facet == null) continue;
      if (facetState.merger == null) {
        facetState.merger = facetState.facetRequest.createFacetMerger(facet);
      }
      facetState.merger.merge(facet);
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;

    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    if (facetState.merger != null) {
      rb.rsp.add("facets", facetState.merger.getMergedResult());
    }
  }

  @Override
  public String getDescription() {
    return "Facet Module";
  }

  @Override
  public String getSource() {
    return null;
  }

}


class FacetComponentState {
  ResponseBuilder rb;
  Map<String,Object> facetCommands;
  FacetRequest facetRequest;
  boolean isShard;

  //
  // Only used for distributed search
  //
  FacetMerger merger;
}

//
// The FacetMerger code is in the prototype stage, and this is the reason that
// many implementations are all in this file.  They can be moved to separate
// files after the interfaces are locked down more.
//

class FacetMerger {
  public void merge(Object facetResult) {

  }

  public Object getMergedResult() {
    return null; // TODO
  }
}


abstract class FacetSortableMerger extends FacetMerger {
  public void prepareSort() {
  }

  /** Return the normal comparison sort order.  The sort direction is only to be used in special circumstances (such as making NaN sort
   * last regardless of sort order.)  Normal sorters do not need to pay attention to direction.
   */
  public abstract int compareTo(FacetSortableMerger other, FacetField.SortDirection direction);
}

abstract class FacetDoubleMerger extends FacetSortableMerger {
  @Override
  public abstract void merge(Object facetResult);

  protected abstract double getDouble();

  @Override
  public Object getMergedResult() {
    return getDouble();
  }


  @Override
  public int compareTo(FacetSortableMerger other, FacetField.SortDirection direction) {
    return compare(getDouble(), ((FacetDoubleMerger)other).getDouble(), direction);
  }


  public static int compare(double a, double b, FacetField.SortDirection direction) {
    if (a < b) return -1;
    if (a > b) return 1;

    if (a != a) {  // a==NaN
      if (b != b) {
        return 0;  // both NaN
      }
      return -1 * direction.getMultiplier();  // asc==-1, so this will put NaN at end of sort
    }

    if (b != b) { // b is NaN so a is greater
      return 1 * direction.getMultiplier();  // if sorting asc, make a less so NaN is at end
    }

    // consider +-0 to be equal
    return 0;
  }
}





class FacetLongMerger extends FacetSortableMerger {
  long val;

  @Override
  public void merge(Object facetResult) {
    val += ((Number)facetResult).longValue();
  }

  @Override
  public Object getMergedResult() {
    return val;
  }

  @Override
  public int compareTo(FacetSortableMerger other, FacetField.SortDirection direction) {
    return Long.compare(val, ((FacetLongMerger)other).val);
  }
}


// base class for facets that create buckets (and can hence have sub-facets)
class FacetBucketMerger<FacetRequestT extends FacetRequest> extends FacetMerger {
  FacetRequestT freq;

  public FacetBucketMerger(FacetRequestT freq) {
    this.freq = freq;
  }

  /** Bucketval is the representative value for the bucket.  Only applicable to terms and range queries to distinguish buckets. */
  FacetBucket newBucket(Comparable bucketVal) {
    return new FacetBucket(this, bucketVal);
  }

  // do subs...

  // callback stuff for buckets?
  // passing object gives us a chance to specialize based on value
  FacetMerger createFacetMerger(String key, Object val) {
    FacetRequest sub = freq.getSubFacets().get(key);
    if (sub != null) {
      return sub.createFacetMerger(val);
    }

    AggValueSource subStat = freq.getFacetStats().get(key);
    if (subStat != null) {
      return subStat.createFacetMerger(val);
    }

    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no merger for key=" + key + " , val=" + val);
  }
}

class FacetQueryMerger extends FacetBucketMerger<FacetQuery> {
  FacetBucket bucket;

  public FacetQueryMerger(FacetQuery freq) {
    super(freq);
  }

  @Override
  public void merge(Object facet) {
    if (bucket == null) {
      bucket = newBucket(null);
    }
    bucket.mergeBucket((SimpleOrderedMap) facet);
  }

  @Override
  public Object getMergedResult() {
    return bucket.getMergedBucket();
  }
}



class FacetBucket {
  FacetBucketMerger parent;
  Comparable bucketValue;
  long count;
  Map<String, FacetMerger> subs;

  public FacetBucket(FacetBucketMerger parent, Comparable bucketValue) {
    this.parent = parent;
    this.bucketValue = bucketValue;
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

  public void mergeBucket(SimpleOrderedMap bucket) {
    // todo: for refinements, we want to recurse, but not re-do stats for intermediate buckets

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
        merger.merge( val );
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
}



class FacetFieldMerger extends FacetBucketMerger<FacetField> {
  FacetBucket missingBucket;
  FacetBucket allBuckets;
  FacetMerger numBuckets;

  LinkedHashMap<Object,FacetBucket> buckets = new LinkedHashMap<Object,FacetBucket>();
  List<FacetBucket> sortedBuckets;
  int numReturnedBuckets; // the number of buckets in the bucket lists returned from all of the shards

  private static class SortVal implements Comparable<SortVal> {
    FacetBucket bucket;
    FacetSortableMerger merger;
    FacetField.SortDirection direction;

    @Override
    public int compareTo(SortVal o) {
      int c = -merger.compareTo(o.merger, direction) * direction.getMultiplier();
      return c == 0 ? bucket.bucketValue.compareTo(o.bucket.bucketValue) : c;
    }
  }

  public FacetFieldMerger(FacetField freq) {
    super(freq);
  }

  @Override
  public void merge(Object facetResult) {
    merge((SimpleOrderedMap)facetResult);
  }

  public void merge(SimpleOrderedMap facetResult) {
    if (freq.missing) {
      Object o = facetResult.get("missing");
      if (o != null) {
        if (missingBucket == null) {
          missingBucket = newBucket(null);
        }
        missingBucket.mergeBucket((SimpleOrderedMap)o);
      }
    }

    if (freq.allBuckets) {
      Object o = facetResult.get("allBuckets");
      if (o != null) {
        if (allBuckets == null) {
          allBuckets = newBucket(null);
        }
        allBuckets.mergeBucket((SimpleOrderedMap)o);
      }
    }

    List<SimpleOrderedMap> bucketList = (List<SimpleOrderedMap>) facetResult.get("buckets");
    numReturnedBuckets += bucketList.size();
    mergeBucketList(bucketList);

    if (freq.numBuckets) {
      Object nb = facetResult.get("numBuckets");
      if (nb != null) {
        if (numBuckets == null) {
          numBuckets = new FacetNumBucketsMerger();
        }
        numBuckets.merge(nb);
      }
    }

  }

  public void mergeBucketList(List<SimpleOrderedMap> bucketList) {
    for (SimpleOrderedMap bucketRes : bucketList) {
      Comparable bucketVal = (Comparable)bucketRes.get("val");
      FacetBucket bucket = buckets.get(bucketVal);
      if (bucket == null) {
        bucket = newBucket(bucketVal);
        buckets.put(bucketVal, bucket);
      }
      bucket.mergeBucket( bucketRes );
    }
  }

  public void sortBuckets() {
    sortedBuckets = new ArrayList<>( buckets.values() );

    Comparator<FacetBucket> comparator = null;

    final FacetField.SortDirection direction = freq.sortDirection;
    final int sortMul = direction.getMultiplier();

    if ("count".equals(freq.sortVariable)) {
      comparator = new Comparator<FacetBucket>() {
        @Override
        public int compare(FacetBucket o1, FacetBucket o2) {
          int v = -Long.compare(o1.count, o2.count) * sortMul;
          return v == 0 ? o1.bucketValue.compareTo(o2.bucketValue) : v;
        }
      };
      Collections.sort(sortedBuckets, comparator);
    } else if ("index".equals(freq.sortVariable)) {
      comparator = new Comparator<FacetBucket>() {
        @Override
        public int compare(FacetBucket o1, FacetBucket o2) {
          return -o1.bucketValue.compareTo(o2.bucketValue) * sortMul;
        }
      };
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

      // timsort may do better here given that the lists may be partially sorted.

      List<SortVal> lst = new ArrayList<SortVal>(buckets.size());
      List<FacetBucket> nulls = new ArrayList<FacetBucket>(buckets.size()>>1);
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
      Collections.sort(nulls, new Comparator<FacetBucket>() {
        @Override
        public int compare(FacetBucket o1, FacetBucket o2) {
          return o1.bucketValue.compareTo(o2.bucketValue);
        }
      });

      ArrayList<FacetBucket> out = new ArrayList<>(buckets.size());
      for (SortVal sv : lst) {
        out.add( sv.bucket );
      }
      out.addAll(nulls);
      sortedBuckets = out;
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

    int first = (int)freq.offset;
    int end = freq.limit >=0 ? first + (int) freq.limit : Integer.MAX_VALUE;
    int last = Math.min(sortedBuckets.size(), end);

    List<SimpleOrderedMap> resultBuckets = new ArrayList<>(Math.max(0, (last - first)));

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


  private class FacetNumBucketsMerger extends FacetMerger {
    long sumBuckets;
    long shardsMissingSum;
    long shardsTruncatedSum;
    Set<Object> values;

    @Override
    public void merge(Object facetResult) {
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
    public Object getMergedResult() {
      long exactCount = values == null ? 0 : values.size();
      return exactCount + shardsMissingSum + shardsTruncatedSum;
      // TODO: reduce count by (at least) number of buckets that fail to hit mincount (after merging)
      // that should make things match for most of the small tests at least
    }
  }
}


class FacetRangeMerger extends FacetBucketMerger<FacetRange> {
  FacetBucket beforeBucket;
  FacetBucket afterBucket;
  FacetBucket betweenBucket;

  LinkedHashMap<Object, FacetBucket> buckets = new LinkedHashMap<Object, FacetBucket>();


  public FacetRangeMerger(FacetRange freq) {
    super(freq);
  }

  @Override
  FacetBucket newBucket(Comparable bucketVal) {
    return super.newBucket(bucketVal);
  }

  @Override
  FacetMerger createFacetMerger(String key, Object val) {
    return super.createFacetMerger(key, val);
  }

  @Override
  public void merge(Object facetResult) {
    merge((SimpleOrderedMap) facetResult);
  }

  public void merge(SimpleOrderedMap facetResult) {
    boolean all = freq.others.contains(FacetParams.FacetRangeOther.ALL);

    if (all || freq.others.contains(FacetParams.FacetRangeOther.BEFORE)) {
      Object o = facetResult.get("before");
      if (o != null) {
        if (beforeBucket == null) {
          beforeBucket = newBucket(null);
        }
        beforeBucket.mergeBucket((SimpleOrderedMap)o);
      }
    }

    if (all || freq.others.contains(FacetParams.FacetRangeOther.AFTER)) {
      Object o = facetResult.get("after");
      if (o != null) {
        if (afterBucket == null) {
          afterBucket = newBucket(null);
        }
        afterBucket.mergeBucket((SimpleOrderedMap)o);
      }
    }

    if (all || freq.others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
      Object o = facetResult.get("between");
      if (o != null) {
        if (betweenBucket == null) {
          betweenBucket = newBucket(null);
        }
        betweenBucket.mergeBucket((SimpleOrderedMap)o);
      }
    }

    List<SimpleOrderedMap> bucketList = (List<SimpleOrderedMap>) facetResult.get("buckets");
    mergeBucketList(bucketList);
  }

  public void mergeBucketList(List<SimpleOrderedMap> bucketList) {
    for (SimpleOrderedMap bucketRes : bucketList) {
      Comparable bucketVal = (Comparable)bucketRes.get("val");
      FacetBucket bucket = buckets.get(bucketVal);
      if (bucket == null) {
        bucket = newBucket(bucketVal);
        buckets.put(bucketVal, bucket);
      }
      bucket.mergeBucket( bucketRes );
    }
  }

  @Override
  public Object getMergedResult() {
    SimpleOrderedMap result = new SimpleOrderedMap(4);

    List<SimpleOrderedMap> resultBuckets = new ArrayList<>(buckets.size());
    // TODO: if we implement mincount for ranges, we'll need to sort buckets (see FacetFieldMerger)

    for (FacetBucket bucket : buckets.values()) {
      /***
       if (bucket.getCount() < freq.mincount) {
       continue;
       }
       ***/
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
