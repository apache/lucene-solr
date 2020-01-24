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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.facet.FacetDebugInfo;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.util.SolrPluginUtils;

import static org.apache.solr.common.params.CommonParams.FQ;
import static org.apache.solr.common.params.CommonParams.JSON;

/**
 * Adds debugging information to a request.
 * 
 *
 * @since solr 1.3
 */
public class DebugComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "debug";
  
  /**
   * A counter to ensure that no RID is equal, even if they fall in the same millisecond
   */
  private static final AtomicLong ridCounter = new AtomicLong();
  
  /**
   * Map containing all the possible stages as key and
   * the corresponding readable purpose as value
   */
  private static final Map<Integer, String> stages;

  static {
      Map<Integer, String> map = new TreeMap<>();
      map.put(ResponseBuilder.STAGE_START, "START");
      map.put(ResponseBuilder.STAGE_PARSE_QUERY, "PARSE_QUERY");
      map.put(ResponseBuilder.STAGE_TOP_GROUPS, "TOP_GROUPS");
      map.put(ResponseBuilder.STAGE_EXECUTE_QUERY, "EXECUTE_QUERY");
      map.put(ResponseBuilder.STAGE_GET_FIELDS, "GET_FIELDS");
      map.put(ResponseBuilder.STAGE_DONE, "DONE");
      stages = Collections.unmodifiableMap(map);
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    if(rb.isDebugTrack() && rb.isDistrib) {
      rb.setNeedDocList(true);
      doDebugTrack(rb);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    if( rb.isDebug() ) {
      SolrQueryRequest req = rb.req;
      StatsCache statsCache = req.getSearcher().getStatsCache();
      req.getContext().put(SolrIndexSearcher.STATS_SOURCE, statsCache.get(req));
      DocList results = null;
      //some internal grouping requests won't have results value set
      if(rb.getResults() != null) {
        results = rb.getResults().docList;
      }

      NamedList stdinfo = SolrPluginUtils.doStandardDebug( rb.req,
          rb.getQueryString(), rb.wrap(rb.getQuery()), results, rb.isDebugQuery(), rb.isDebugResults());
      
      NamedList info = rb.getDebugInfo();
      if( info == null ) {
        rb.setDebugInfo( stdinfo );
        info = stdinfo;
      }
      else {
        info.addAll( stdinfo );
      }

      FacetDebugInfo fdebug = (FacetDebugInfo)(rb.req.getContext().get("FacetDebugInfo"));
      if (fdebug != null) {
        info.add("facet-trace", fdebug.getFacetDebugInfo());
      }

      fdebug = (FacetDebugInfo)(rb.req.getContext().get("FacetDebugInfo-nonJson"));
      if (fdebug != null) {
        info.add("facet-debug", fdebug.getFacetDebugInfo());
      }
      
      if (rb.req.getJSON() != null) {
        info.add(JSON, rb.req.getJSON());
      }

      if (rb.isDebugQuery() && rb.getQparser() != null) {
        rb.getQparser().addDebugInfo(rb.getDebugInfo());
      }
      
      if (null != rb.getDebugInfo() ) {
        if (rb.isDebugQuery() && null != rb.getFilters() ) {
          info.add("filter_queries",rb.req.getParams().getParams(FQ));
          List<String> fqs = new ArrayList<>(rb.getFilters().size());
          for (Query fq : rb.getFilters()) {
            fqs.add(QueryParsing.toString(fq, rb.req.getSchema()));
          }
          info.add("parsed_filter_queries",fqs);
        }
        
        // Add this directly here?
        rb.rsp.add("debug", rb.getDebugInfo() );
      }
    }
  }


  private void doDebugTrack(ResponseBuilder rb) {
    String rid = getRequestId(rb.req);
    rb.addDebug(rid, "track", CommonParams.REQUEST_ID);//to see it in the response
    rb.rsp.addToLog(CommonParams.REQUEST_ID, rid); //to see it in the logs of the landing core
    
  }

  public static String getRequestId(SolrQueryRequest req) {
    String rid = req.getParams().get(CommonParams.REQUEST_ID);
    if(rid == null || "".equals(rid)) {
      rid = generateRid(req);
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.add(CommonParams.REQUEST_ID, rid);//add rid to the request so that shards see it
      req.setParams(params);
    }
    return rid;
  }

  @SuppressForbidden(reason = "Need currentTimeMillis, only used for naming")
  private static String generateRid(SolrQueryRequest req) {
    String hostName = req.getCore().getCoreContainer().getHostName();
    return hostName + "-" + req.getCore().getName() + "-" + System.currentTimeMillis() + "-" + ridCounter.getAndIncrement();
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.isDebug()) return;
    
    // Turn on debug to get explain only when retrieving fields
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_DEBUG;
      // always distribute the latest version of global stats
      sreq.purpose |= ShardRequest.PURPOSE_SET_TERM_STATS;
      StatsCache statsCache = rb.req.getSearcher().getStatsCache();
      statsCache.sendGlobalStats(rb, sreq);

      if (rb.isDebugAll()) {
        sreq.params.set(CommonParams.DEBUG_QUERY, "true");
      } else {
        if (rb.isDebugQuery()){
          sreq.params.add(CommonParams.DEBUG, CommonParams.QUERY);
        }
        if (rb.isDebugResults()){
          sreq.params.add(CommonParams.DEBUG, CommonParams.RESULTS);
        }
      }
    } else {
      sreq.params.set(CommonParams.DEBUG_QUERY, "false");
      sreq.params.set(CommonParams.DEBUG, "false");
    }
    if (rb.isDebugTimings()) {
      sreq.params.add(CommonParams.DEBUG, CommonParams.TIMING);
    } 
    if (rb.isDebugTrack()) {
      sreq.params.add(CommonParams.DEBUG, CommonParams.TRACK);
      sreq.params.set(CommonParams.REQUEST_ID, rb.req.getParams().get(CommonParams.REQUEST_ID));
      sreq.params.set(CommonParams.REQUEST_PURPOSE, SolrPluginUtils.getRequestPurpose(sreq.purpose));
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (rb.isDebugTrack() && rb.isDistrib && !rb.finished.isEmpty()) {
      @SuppressWarnings("unchecked")
      NamedList<Object> stageList = (NamedList<Object>) ((NamedList<Object>)rb.getDebugInfo().get("track")).get(stages.get(rb.stage));
      if(stageList == null) {
        stageList = new SimpleOrderedMap<>();
        rb.addDebug(stageList, "track", stages.get(rb.stage));
      }
      for(ShardResponse response: sreq.responses) {
        stageList.add(response.getShard(), getTrackResponse(response));
      }
    }
  }

  private final static Set<String> EXCLUDE_SET = Set.of("explain");

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.isDebug() && rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      NamedList<Object> info = rb.getDebugInfo();
      NamedList<Object> explain = new SimpleOrderedMap<>();

      Map.Entry<String, Object>[]  arr =  new NamedList.NamedListEntry[rb.resultIds.size()];
      // Will be set to true if there is at least one response with PURPOSE_GET_DEBUG
      boolean hasGetDebugResponses = false;

      for (ShardRequest sreq : rb.finished) {
        for (ShardResponse srsp : sreq.responses) {
          if (srsp.getException() != null) {
            // can't expect the debug content if there was an exception for this request
            // this should only happen when using shards.tolerant=true
            continue;
          }
          NamedList sdebug = (NamedList)srsp.getSolrResponse().getResponse().get("debug");
          info = (NamedList)merge(sdebug, info, EXCLUDE_SET);
          if ((sreq.purpose & ShardRequest.PURPOSE_GET_DEBUG) != 0) {
            hasGetDebugResponses = true;
            if (rb.isDebugResults()) {
              NamedList sexplain = (NamedList)sdebug.get("explain");
              SolrPluginUtils.copyNamedListIntoArrayByDocPosInResponse(sexplain, rb.resultIds, arr);
            }
          }
        }
      }

      if (rb.isDebugResults()) {
         explain = SolrPluginUtils.removeNulls(arr, new SimpleOrderedMap<>());
      }

      if (!hasGetDebugResponses) {
        if (info == null) {
          info = new SimpleOrderedMap<>();
        }
        // No responses were received from shards. Show local query info.
        SolrPluginUtils.doStandardQueryDebug(
                rb.req, rb.getQueryString(),  rb.wrap(rb.getQuery()), rb.isDebugQuery(), info);
        if (rb.isDebugQuery() && rb.getQparser() != null) {
          rb.getQparser().addDebugInfo(info);
        }
      }
      if (rb.isDebugResults()) {
        int idx = info.indexOf("explain",0);
        if (idx>=0) {
          info.setVal(idx, explain);
        } else {
          info.add("explain", explain);
        }
      }

      rb.setDebugInfo(info);
      rb.rsp.add("debug", rb.getDebugInfo() );
    }
    
  }


  private NamedList<String> getTrackResponse(ShardResponse shardResponse) {
    NamedList<String> namedList = new SimpleOrderedMap<>();
    if (shardResponse.getException() != null) {
      namedList.add("Exception", shardResponse.getException().getMessage());
      return namedList;
    }
    NamedList<Object> responseNL = shardResponse.getSolrResponse().getResponse();
    @SuppressWarnings("unchecked")
    NamedList<Object> responseHeader = (NamedList<Object>)responseNL.get("responseHeader");
    if(responseHeader != null) {
      namedList.add("QTime", responseHeader.get("QTime").toString());
    }
    namedList.add("ElapsedTime", String.valueOf(shardResponse.getSolrResponse().getElapsedTime()));
    namedList.add("RequestPurpose", shardResponse.getShardRequest().params.get(CommonParams.REQUEST_PURPOSE));
    SolrDocumentList docList = (SolrDocumentList)shardResponse.getSolrResponse().getResponse().get("response");
    if(docList != null) {
      namedList.add("NumFound", String.valueOf(docList.getNumFound()));
    }
    namedList.add("Response", String.valueOf(responseNL));
    return namedList;
  }

  protected Object merge(Object source, Object dest, Set<String> exclude) {
    if (source == null) return dest;
    if (dest == null) {
      if (source instanceof NamedList) {
        dest = source instanceof SimpleOrderedMap ? new SimpleOrderedMap() : new NamedList();
      } else {
        return source;
      }
    } else {

      if (dest instanceof Collection) {
        // merge as Set
        if (!(dest instanceof Set)) {
          dest = new LinkedHashSet<>((Collection<?>) dest);
        }
        if (source instanceof Collection) {
          ((Collection)dest).addAll((Collection)source);
        } else {
          ((Collection)dest).add(source);
        }
        return dest;
      } else if (source instanceof Number) {
        if (dest instanceof Number) {
          if (source instanceof Double || dest instanceof Double) {
            return ((Number)source).doubleValue() + ((Number)dest).doubleValue();
          }
          return ((Number)source).longValue() + ((Number)dest).longValue();
        }
        // fall through
      } else if (source instanceof String) {
        if (source.equals(dest)) {
          return dest;
        }
        // fall through
      }
    }


    if (source instanceof NamedList && dest instanceof NamedList) {
      NamedList<Object> tmp = new NamedList<>();
      @SuppressWarnings("unchecked")
      NamedList<Object> sl = (NamedList<Object>)source;
      @SuppressWarnings("unchecked")
      NamedList<Object> dl = (NamedList<Object>)dest;
      for (int i=0; i<sl.size(); i++) {
        String skey = sl.getName(i);
        if (exclude.contains(skey)) continue;
        Object sval = sl.getVal(i);
        int didx = -1;

        // optimize case where elements are in same position
        if (i < dl.size()) {
          String dkey = dl.getName(i);
          if (skey == dkey || (skey!=null && skey.equals(dkey))) {
            didx = i;
          }
        }

        if (didx == -1) {
          didx = dl.indexOf(skey, 0);
        }

        if (didx == -1) {
          tmp.add(skey, merge(sval, null, Collections.emptySet()));
        } else {
          dl.setVal(didx, merge(sval, dl.getVal(didx), Collections.emptySet()));
        }
      }
      dl.addAll(tmp);
      return dl;
    }

    // only add to list if JSON is different
    if (source.equals(dest)) return source;

    // merge unlike elements in a list
    List<Object> t = new ArrayList<>();
    t.add(dest);
    t.add(source);
    return t;
  }


  
  /////////////////////////////////////////////
  ///  SolrInfoBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Debug Information";
  }

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }
}
