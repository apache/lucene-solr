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
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.facet.FacetHeatmap;
import org.apache.solr.search.facet.FacetMerger;
import org.apache.solr.search.facet.FacetRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A 2D spatial faceting summary of a rectangular region. Used by {@link org.apache.solr.handler.component.FacetComponent}
 * and {@link org.apache.solr.request.SimpleFacets}.
 * @see FacetHeatmap
 */
public class SpatialHeatmapFacets {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  //underneath facet_counts we put this here:
  public static final String RESPONSE_KEY = "facet_heatmaps";

  /** Called by {@link org.apache.solr.request.SimpleFacets} to compute heatmap facets. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static NamedList<Object> getHeatmapForField(String fieldKey, String fieldName, ResponseBuilder rb, SolrParams params, DocSet docSet) throws IOException {
    final FacetRequest facetRequest = createHeatmapRequest(fieldKey, fieldName, rb, params);
    return (NamedList) facetRequest.process(rb.req, docSet);
  }

  private static FacetRequest createHeatmapRequest(String fieldKey, String fieldName, ResponseBuilder rb, SolrParams params) {
    Map<String, Object> jsonFacet = new HashMap<>();
    jsonFacet.put("type", "heatmap");
    jsonFacet.put("field", fieldName);
    // jsonFacets has typed values, unlike SolrParams which is all string
    jsonFacet.put(FacetHeatmap.GEOM_PARAM, params.getFieldParam(fieldKey, FacetParams.FACET_HEATMAP_GEOM));
    jsonFacet.put(FacetHeatmap.LEVEL_PARAM, params.getFieldInt(fieldKey, FacetParams.FACET_HEATMAP_LEVEL));
    jsonFacet.put(FacetHeatmap.DIST_ERR_PCT_PARAM, params.getFieldDouble(fieldKey, FacetParams.FACET_HEATMAP_DIST_ERR_PCT));
    jsonFacet.put(FacetHeatmap.DIST_ERR_PARAM, params.getFieldDouble(fieldKey, FacetParams.FACET_HEATMAP_DIST_ERR));
    jsonFacet.put(FacetHeatmap.MAX_CELLS_PARAM, params.getFieldInt(fieldKey, FacetParams.FACET_HEATMAP_MAX_CELLS));
    jsonFacet.put(FacetHeatmap.FORMAT_PARAM, params.getFieldParam(fieldKey, FacetParams.FACET_HEATMAP_FORMAT));

    return FacetRequest.parseOneFacetReq(rb.req, jsonFacet);
  }

  //
  // Distributed Support
  //

  /** Parses request to "HeatmapFacet" instances. */
  public static LinkedHashMap<String,HeatmapFacet> distribParse(SolrParams params, ResponseBuilder rb) {
    final LinkedHashMap<String, HeatmapFacet> heatmapFacets = new LinkedHashMap<>();
    final String[] heatmapFields = params.getParams(FacetParams.FACET_HEATMAP);
    if (heatmapFields != null) {
      for (String heatmapField : heatmapFields) {
        HeatmapFacet facet = new HeatmapFacet(rb, heatmapField);
        heatmapFacets.put(facet.getKey(), facet);
      }
    }
    return heatmapFacets;
  }

  /** Called by FacetComponent's impl of
   * {@link org.apache.solr.handler.component.SearchComponent#modifyRequest(ResponseBuilder, SearchComponent, ShardRequest)}. */
  public static void distribModifyRequest(ShardRequest sreq, LinkedHashMap<String, HeatmapFacet> heatmapFacets) {
    // Set the format to PNG because it's compressed and it's the only format we have code to read at the moment.
    // We re-write the facet.heatmap list with PNG format in local-params where it has highest precedence.

    //Remove existing heatmap field param vals; we will rewrite
    sreq.params.remove(FacetParams.FACET_HEATMAP);
    for (HeatmapFacet facet : heatmapFacets.values()) {
      //add heatmap field param
      ModifiableSolrParams newLocalParams = new ModifiableSolrParams();
      if (facet.localParams != null) {
        newLocalParams.add(facet.localParams);
      }
      // Set format to PNG; it's the only one we parse
      newLocalParams.set(FacetParams.FACET_HEATMAP_FORMAT, FacetHeatmap.FORMAT_PNG);
      sreq.params.add(FacetParams.FACET_HEATMAP,
          newLocalParams.toLocalParamsString() + facet.facetOn);
    }
  }

  /** Called by FacetComponent.countFacets which is in turn called by FC's impl of
   * {@link org.apache.solr.handler.component.SearchComponent#handleResponses(ResponseBuilder, ShardRequest)}. */
  @SuppressWarnings("unchecked")
  public static void distribHandleResponse(LinkedHashMap<String, HeatmapFacet> heatmapFacets, @SuppressWarnings({"rawtypes"})NamedList srsp_facet_counts) {
    NamedList<NamedList<Object>> facet_heatmaps = (NamedList<NamedList<Object>>) srsp_facet_counts.get(RESPONSE_KEY);
    if (facet_heatmaps == null) {
      return;
    }
    // (should the caller handle the above logic?  Arguably yes.)
    for (Map.Entry<String, NamedList<Object>> entry : facet_heatmaps) {
      String fieldKey = entry.getKey();
      NamedList<Object> shardNamedList = entry.getValue();
      final HeatmapFacet facet = heatmapFacets.get(fieldKey);
      if (facet == null) {
        log.error("received heatmap for field/key {} that we weren't expecting", fieldKey);
        continue;
      }
      facet.jsonFacetMerger.merge(shardNamedList, null);//merge context not needed (null)
    }
  }


  /** Called by FacetComponent's impl of
   * {@link org.apache.solr.handler.component.SearchComponent#finishStage(ResponseBuilder)}. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static NamedList distribFinish(LinkedHashMap<String, HeatmapFacet> heatmapInfos, ResponseBuilder rb) {
    NamedList<NamedList<Object>> result = new SimpleOrderedMap<>();
    for (Map.Entry<String, HeatmapFacet> entry : heatmapInfos.entrySet()) {
      final HeatmapFacet facet = entry.getValue();
      result.add(entry.getKey(), (NamedList<Object>) facet.jsonFacetMerger.getMergedResult());
    }
    return result;
  }

  /** Goes in {@link org.apache.solr.handler.component.FacetComponent.FacetInfo#heatmapFacets}, created by
   * {@link #distribParse(org.apache.solr.common.params.SolrParams, ResponseBuilder)}. */
  public static class HeatmapFacet extends FacetComponent.FacetBase {
    //note: 'public' following-suit with FacetBase & existing subclasses... though should this really be?

    public FacetMerger jsonFacetMerger;

    public HeatmapFacet(ResponseBuilder rb, String facetStr) {
      super(rb, FacetParams.FACET_HEATMAP, facetStr);
      //note: logic in super (FacetBase) is partially redundant with SimpleFacet.parseParams :-(
      final SolrParams params = SolrParams.wrapDefaults(localParams, rb.req.getParams());
      final FacetRequest heatmapRequest = createHeatmapRequest(getKey(), facetOn, rb, params);
      jsonFacetMerger = heatmapRequest.createFacetMerger(null);
    }
  }

  // Note: originally there was a lot more code here but it migrated to the JSON Facet API as "FacetHeatmap"

}
