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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
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
import org.apache.solr.util.RTimer;
import org.noggit.JSONUtil;

public class FacetModule extends SearchComponent {

  public static final String COMPONENT_NAME = "facet_module";

  // Ensure these don't overlap with other PURPOSE flags in ShardRequest
  // The largest current flag in ShardRequest is 0x00002000
  // We'll put our bits in the middle to avoid future ones in ShardRequest and
  // custom ones that may start at the top.
  public final static int PURPOSE_GET_JSON_FACETS      = 0x00100000;
  public final static int PURPOSE_REFINE_JSON_FACETS   = 0x00200000;

  // Internal information passed down from the top level to shards for distributed faceting.
  private final static String FACET_STATE = "_facet_";
  private final static String FACET_REFINE = "refine";


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
    if (rb.isDebug()) {
      FacetDebugInfo fdebug = new FacetDebugInfo();
      fcontext.setDebugInfo(fdebug);
      fdebug.setReqDescription(facetState.facetRequest.getFacetDescription());
      fdebug.setProcessor(fproc.getClass().getSimpleName());
     
      final RTimer timer = new RTimer();
      fproc.process();
      long timeElapsed = (long) timer.getTime();
      fdebug.setElapse(timeElapsed);
      fdebug.putInfoItem("domainSize", (long)fcontext.base.size());
      rb.req.getContext().put("FacetDebugInfo", fdebug);
    } else {
      fproc.process();
    }
    
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


  private void clearFaceting(List<ShardRequest> outgoing) {
    // turn off faceting for requests not marked as being for faceting refinements
    for (ShardRequest sreq : outgoing) {
      if ((sreq.purpose & PURPOSE_REFINE_JSON_FACETS) != 0) continue;
      sreq.params.remove("json.facet");  // this just saves space... the presence of FACET_STATE really control the faceting
      sreq.params.remove(FACET_STATE);
    }
  }


  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return ResponseBuilder.STAGE_DONE;

    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) {
      return ResponseBuilder.STAGE_DONE;
    }

    // Check if there are any refinements possible
    if (facetState.mcontext.getSubsWithRefinement(facetState.facetRequest).isEmpty()) {
      clearFaceting(rb.outgoing);
      return ResponseBuilder.STAGE_DONE;
    }

    // Overlap facet refinement requests (those shards that we need a count
    // for particular facet values from), where possible, with
    // the requests to get fields (because we know that is the
    // only other required phase).
    // We do this in distributedProcess so we can look at all of the
    // requests in the outgoing queue at once.

    assert rb.shards.length == facetState.mcontext.numShards;
    for (String shard : rb.shards) {
      facetState.mcontext.setShard(shard);

      // shard-specific refinement
      Map<String,Object> refinement = facetState.merger.getRefinement(facetState.mcontext);
      if (refinement == null) continue;

      boolean newRequest = false;
      ShardRequest shardsRefineRequest = null;

      // try to find a request that is already going out to that shard.
      // If nshards becomes too great, we may want to move to hashing for
      // better scalability.
      for (ShardRequest sreq : rb.outgoing) {
        if ( (sreq.purpose & (ShardRequest.PURPOSE_GET_FIELDS|ShardRequest.PURPOSE_REFINE_FACETS|ShardRequest.PURPOSE_REFINE_PIVOT_FACETS)) != 0
            && sreq.shards != null
            && sreq.shards.length == 1
            && sreq.shards[0].equals(shard))
        {
          shardsRefineRequest = sreq;
          break;
        }
      }

      if (shardsRefineRequest == null) {
        // we didn't find any other suitable requests going out to that shard,
        // so create one ourselves.
        newRequest = true;
        shardsRefineRequest = new ShardRequest();
        shardsRefineRequest.shards = new String[] { shard };
        shardsRefineRequest.params = new ModifiableSolrParams(rb.req.getParams());
        // don't request any documents
        shardsRefineRequest.params.remove(CommonParams.START);
        shardsRefineRequest.params.set(CommonParams.ROWS, "0");
        shardsRefineRequest.params.set(CommonParams.ROWS, "0");
        shardsRefineRequest.params.set(FacetParams.FACET, false);
      }

      shardsRefineRequest.purpose |= PURPOSE_REFINE_JSON_FACETS;

      Map<String,Object> fstate = new HashMap<>(1);
      fstate.put(FACET_REFINE, refinement);
      String fstateString = JSONUtil.toJSON(fstate);
      shardsRefineRequest.params.add(FACET_STATE, fstateString);

      if (newRequest) {
        rb.addRequest(this, shardsRefineRequest);
      }
    }

    // clearFaceting(rb.outgoing);
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,ShardRequest sreq) {
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      sreq.purpose |= FacetModule.PURPOSE_GET_JSON_FACETS;
      sreq.params.set(FACET_STATE, "{}"); // The presence of FACET_STATE (_facet_) turns on json faceting
    } else {
      // turn off faceting on other requests
      /*** distributedProcess will need to use other requests for refinement
      sreq.params.remove("json.facet");  // this just saves space... the presence of FACET_STATE really control the faceting
      sreq.params.remove(FACET_STATE);
       **/
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    for (ShardResponse shardRsp : sreq.responses) {
      SolrResponse rsp = shardRsp.getSolrResponse();
      NamedList<Object> top = rsp.getResponse();
      if (top == null) continue; // shards.tolerant=true will cause this to happen on exceptions/errors
      Object facet = top.get("facets");
      if (facet == null) continue;
      if (facetState.merger == null) {
        facetState.merger = facetState.facetRequest.createFacetMerger(facet);
        facetState.mcontext = new FacetMerger.Context( sreq.responses.size() );
      }
      facetState.mcontext.root = facet;
      facetState.mcontext.newShard(shardRsp.getShard());
      facetState.merger.merge(facet , facetState.mcontext);
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;

    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    if (facetState.merger != null) {
      // TODO: merge any refinements
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
  FacetMerger.Context mcontext;
}

// base class for facet functions that can be used in a sort
abstract class FacetSortableMerger extends FacetMerger {
  public void prepareSort() {
  }

  @Override
  public void finish(Context mcontext) {
    // nothing to do for simple stats...
  }

  /** Return the normal comparison sort order.  The sort direction is only to be used in special circumstances (such as making NaN sort
   * last regardless of sort order.)  Normal sorters do not need to pay attention to direction.
   */
  public abstract int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction);
}

abstract class FacetDoubleMerger extends FacetSortableMerger {
  @Override
  public abstract void merge(Object facetResult, Context mcontext);

  protected abstract double getDouble();

  @Override
  public Object getMergedResult() {
    return getDouble();
  }


  @Override
  public int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction) {
    return compare(getDouble(), ((FacetDoubleMerger)other).getDouble(), direction);
  }


  public static int compare(double a, double b, FacetRequest.SortDirection direction) {
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
  public void merge(Object facetResult, Context mcontext) {
    val += ((Number)facetResult).longValue();
  }

  @Override
  public Object getMergedResult() {
    return val;
  }

  @Override
  public int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction) {
    return Long.compare(val, ((FacetLongMerger)other).val);
  }
}


// base class for facets that create buckets (and can hence have sub-facets)
abstract class FacetBucketMerger<FacetRequestT extends FacetRequest> extends FacetMerger {
  FacetRequestT freq;

  public FacetBucketMerger(FacetRequestT freq) {
    this.freq = freq;
  }

  /** Bucketval is the representative value for the bucket.  Only applicable to terms and range queries to distinguish buckets. */
  FacetBucket newBucket(Comparable bucketVal, Context mcontext) {
    return new FacetBucket(this, bucketVal, mcontext);
  }

  @Override
  public Map<String, Object> getRefinement(Context mcontext) {
    Collection<String> refineTags = mcontext.getSubsWithRefinement(freq);
    return null; // FIXME
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
  public void merge(Object facet, Context mcontext) {
    if (bucket == null) {
      bucket = newBucket(null, mcontext);
    }
    bucket.mergeBucket((SimpleOrderedMap) facet, mcontext);
  }

  @Override
  public Map<String, Object> getRefinement(Context mcontext) {
    Collection<String> tags;
    if (mcontext.bucketWasMissing()) {
      // if this bucket was missing, we need to get all subfacets that have partials (that need to list values for refinement)
      tags = mcontext.getSubsWithPartial(freq);
    } else {
      tags = mcontext.getSubsWithRefinement(freq);
    }

    Map<String,Object> refinement = bucket.getRefinement(mcontext, tags);

    return refinement;
  }


  @Override
  public void finish(Context mcontext) {
    // FIXME we need to propagate!!!
  }

  @Override
  public Object getMergedResult() {
    return bucket.getMergedBucket();
  }
}



