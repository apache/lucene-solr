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
import java.util.Date;
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
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QueryContext;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import static org.apache.solr.common.util.Utils.fromJSONString;

public class FacetModule extends SearchComponent {

  public static final String COMPONENT_NAME = "facet_module";

  // Ensure these don't overlap with other PURPOSE flags in ShardRequest
  // The largest current flag in ShardRequest is 0x00002000
  // We'll put our bits in the middle to avoid future ones in ShardRequest and
  // custom ones that may start at the top.
  public final static int PURPOSE_GET_JSON_FACETS = 0x00100000;
  public final static int PURPOSE_REFINE_JSON_FACETS = 0x00200000;

  // Internal information passed down from the top level to shards for distributed faceting.
  private final static String FACET_INFO = "_facet_";
  private final static String FACET_REFINE = "refine";


  public FacetComponentState getFacetComponentState(ResponseBuilder rb) {
    // TODO: put a map on ResponseBuilder?
    // rb.componentInfo.get(FacetComponentState.class);
    return (FacetComponentState) rb.req.getContext().get(FacetComponentState.class);
  }


  @Override
  @SuppressWarnings({"unchecked"})
  public void prepare(ResponseBuilder rb) throws IOException {
    Map<String, Object> json = rb.req.getJSON();
    Map<String, Object> jsonFacet = null;
    if (json == null) {
      int version = rb.req.getParams().getInt("facet.version", 1);
      if (version <= 1) return;
      boolean facetsEnabled = rb.req.getParams().getBool(FacetParams.FACET, false);
      if (!facetsEnabled) return;
      jsonFacet = new LegacyFacet(rb.req.getParams()).getLegacy();
    } else {
      Object jsonObj = json.get("facet");
      if (jsonObj instanceof Map) {
        jsonFacet = (Map<String, Object>) jsonObj;
      } else if (jsonObj != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Expected Map for 'facet', received " + jsonObj.getClass().getSimpleName() + "=" + jsonObj);
      }
    }
    if (jsonFacet == null) return;

    SolrParams params = rb.req.getParams();

    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    @SuppressWarnings({"unchecked"})
    Map<String, Object> facetInfo = null;
    if (isShard) {
      String jfacet = params.get(FACET_INFO);
      if (jfacet == null) {
        // if this is a shard request, but there is no _facet_ info, then don't do anything.
        return;
      }
      facetInfo = (Map<String, Object>) fromJSONString(jfacet);
    }

    // At this point, we know we need to do something.  Create and save the state.
    rb.setNeedDocSet(true);

    // Parse the facet in the prepare phase?
    FacetRequest facetRequest = FacetRequest.parse(rb.req, jsonFacet);

    FacetComponentState fcState = new FacetComponentState();
    fcState.rb = rb;
    fcState.isShard = isShard;
    fcState.facetInfo = facetInfo;
    fcState.facetCommands = jsonFacet;
    fcState.facetRequest = facetRequest;

    rb.req.getContext().put(FacetComponentState.class, fcState);
  }


  @Override
  @SuppressWarnings({"unchecked"})
  public void process(ResponseBuilder rb) throws IOException {
    // if this is null, faceting is not enabled
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    boolean cache = rb.req.getParams().getBool(CommonParams.CACHE, true);
    boolean isShard = rb.req.getParams().getBool(ShardParams.IS_SHARD, false);

    FacetContext fcontext = new FacetContext();
    fcontext.base = rb.getResults().docSet;
    fcontext.req = rb.req;
    fcontext.searcher = rb.req.getSearcher();
    fcontext.qcontext = QueryContext.newContext(fcontext.searcher);
    fcontext.cache = cache;
    if (isShard) {
      fcontext.flags |= FacetContext.IS_SHARD;
      fcontext.facetInfo = facetState.facetInfo.isEmpty() ? null : (Map<String, Object>) facetState.facetInfo.get(FACET_REFINE);
      if (fcontext.facetInfo != null) {
        fcontext.flags |= FacetContext.IS_REFINEMENT;
        fcontext.flags |= FacetContext.SKIP_FACET; // the root bucket should have been received from all shards previously
      }
    }
    if (rb.isDebug()) {
      FacetDebugInfo fdebug = new FacetDebugInfo();
      fcontext.setDebugInfo(fdebug);
      rb.req.getContext().put("FacetDebugInfo", fdebug);
    }

    Object results = facetState.facetRequest.process(fcontext);
    // ExitableDirectory timeout causes absent "facets"
    rb.rsp.add("facets", results);
  }


  private void clearFaceting(List<ShardRequest> outgoing) {
    // turn off faceting for requests not marked as being for faceting refinements
    for (ShardRequest sreq : outgoing) {
      if ((sreq.purpose & PURPOSE_REFINE_JSON_FACETS) != 0) continue;
      sreq.params.remove("json.facet");  // this just saves space... the presence of FACET_INFO is enough to control the faceting
      sreq.params.remove(FACET_INFO);
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
    if ((facetState.mcontext == null) || facetState.mcontext.getSubsWithRefinement(facetState.facetRequest).isEmpty()) {
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
      Map<String, Object> refinement = facetState.merger.getRefinement(facetState.mcontext);
      if (refinement == null) continue;

      boolean newRequest = false;
      ShardRequest shardsRefineRequest = null;

      // try to find a request that is already going out to that shard.
      // If nshards becomes too great, we may want to move to hashing for
      // better scalability.
      for (ShardRequest sreq : rb.outgoing) {
        if ((sreq.purpose & (ShardRequest.PURPOSE_GET_FIELDS | ShardRequest.PURPOSE_REFINE_FACETS | ShardRequest.PURPOSE_REFINE_PIVOT_FACETS)) != 0
            && sreq.shards != null
            && sreq.shards.length == 1
            && sreq.shards[0].equals(shard)) {
          shardsRefineRequest = sreq;
          break;
        }
      }

      if (shardsRefineRequest == null) {
        // we didn't find any other suitable requests going out to that shard,
        // so create one ourselves.
        newRequest = true;
        shardsRefineRequest = new ShardRequest();
        shardsRefineRequest.shards = new String[]{shard};
        shardsRefineRequest.params = new ModifiableSolrParams(rb.req.getParams());
        // don't request any documents
        shardsRefineRequest.params.remove(CommonParams.START);
        shardsRefineRequest.params.set(CommonParams.ROWS, "0");
        shardsRefineRequest.params.set(FacetParams.FACET, false);
      }

      shardsRefineRequest.purpose |= PURPOSE_REFINE_JSON_FACETS;

      Map<String, Object> finfo = new HashMap<>(1);
      finfo.put(FACET_REFINE, refinement);

      // String finfoStr = JSONUtil.toJSON(finfo, -1);  // this doesn't handle formatting of Date objects the way we want
      CharArr out = new CharArr();
      JSONWriter jsonWriter = new JSONWriter(out, -1) {
        @Override
        public void handleUnknownClass(Object o) {
          // handle date formatting correctly
          if (o instanceof Date) {
            String s = ((Date) o).toInstant().toString();
            writeString(s);
            return;
          }
          super.handleUnknownClass(o);
        }
      };
      jsonWriter.write(finfo);
      String finfoStr = out.toString();
      // System.err.println("##################### REFINE=" + finfoStr);
      shardsRefineRequest.params.add(FACET_INFO, finfoStr);

      if (newRequest) {
        rb.addRequest(this, shardsRefineRequest);
      }
    }

    // clearFaceting(rb.outgoing);
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    FacetComponentState facetState = getFacetComponentState(rb);
    if (facetState == null) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      sreq.purpose |= FacetModule.PURPOSE_GET_JSON_FACETS;
      sreq.params.set(FACET_INFO, "{}"); // The presence of FACET_INFO (_facet_) turns on json faceting
    } else {
      // turn off faceting on other requests
      /*** distributedProcess will need to use other requests for refinement
       sreq.params.remove("json.facet");  // this just saves space... the presence of FACET_INFO really control the faceting
       sreq.params.remove(FACET_INFO);
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
      if (facet == null) {
        @SuppressWarnings("rawtypes") SimpleOrderedMap shardResponseHeader = (SimpleOrderedMap) rsp.getResponse().get("responseHeader");
        if (Boolean.TRUE.equals(shardResponseHeader.getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
          rb.rsp.getResponseHeader().asShallowMap().put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
        }
        continue;
      }
      if (facetState.merger == null) {
        facetState.merger = facetState.facetRequest.createFacetMerger(facet);
        facetState.mcontext = new FacetMerger.Context(sreq.responses.size());
      }

      if ((sreq.purpose & PURPOSE_REFINE_JSON_FACETS) != 0) {
        // System.err.println("REFINE FACET RESULT FROM SHARD = " + facet);
        // call merge again with a diff flag set on the context???
        facetState.mcontext.root = facet;
        facetState.mcontext.setShard(shardRsp.getShard());  // TODO: roll newShard into setShard?
        facetState.merger.merge(facet, facetState.mcontext);
        return;
      }

      // System.err.println("MERGING FACET RESULT FROM SHARD = " + facet);
      facetState.mcontext.root = facet;
      facetState.mcontext.newShard(shardRsp.getShard());
      facetState.merger.merge(facet, facetState.mcontext);
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
  public Category getCategory() {
    return Category.QUERY;
  }


  // TODO: perhaps factor out some sort of root/parent facet object that doesn't depend
// on stuff like ResponseBuilder, but contains request parameters,
// root filter lists (for filter exclusions), etc?
  class FacetComponentState {
    ResponseBuilder rb;
    Map<String, Object> facetCommands;
    FacetRequest facetRequest;
    boolean isShard;
    Map<String, Object> facetInfo; // _facet_ param: contains out-of-band facet info, mainly for refinement requests

    //
    // Only used for distributed search
    //
    FacetMerger merger;
    FacetMerger.Context mcontext;
  }

  // base class for facet functions that can be used in a sort
  abstract static class FacetSortableMerger extends FacetMerger {
    public void prepareSort() {
    }

    @Override
    public void finish(Context mcontext) {
      // nothing to do for simple stats...
    }

    /**
     * Return the normal comparison sort order.  The sort direction is only to be used in special circumstances (such as making NaN sort
     * last regardless of sort order.)  Normal sorters do not need to pay attention to direction.
     */
    public abstract int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction);
  }

  abstract static class FacetDoubleMerger extends FacetSortableMerger {
    @Override
    public abstract void merge(Object facetResult, Context mcontext);

    protected abstract double getDouble();

    @Override
    public Object getMergedResult() {
      return getDouble();
    }


    @Override
    public int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction) {
      return compare(getDouble(), ((FacetDoubleMerger) other).getDouble(), direction);
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

  static class FacetLongMerger extends FacetSortableMerger {
    long val;

    @Override
    public void merge(Object facetResult, Context mcontext) {
      val += ((Number) facetResult).longValue();
    }

    @Override
    public Object getMergedResult() {
      return val;
    }

    @Override
    public int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction) {
      return Long.compare(val, ((FacetLongMerger) other).val);
    }
  }


  // base class for facets that create buckets (and can hence have sub-facets)
  abstract static class FacetBucketMerger<FacetRequestT extends FacetRequest> extends FacetMerger {
    FacetRequestT freq;

    public FacetBucketMerger(FacetRequestT freq) {
      this.freq = freq;
    }

    /**
     * Bucketval is the representative value for the bucket.  Only applicable to terms and range queries to distinguish buckets.
     */
    FacetBucket newBucket(@SuppressWarnings("rawtypes") Comparable bucketVal, Context mcontext) {
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


  static class FacetQueryMerger extends FacetBucketMerger<FacetQuery> {
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

      Map<String, Object> refinement = bucket.getRefinement(mcontext, tags);

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
}



