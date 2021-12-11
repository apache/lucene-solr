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

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.PivotListEntry;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Processes all Pivot facet logic for a single node -- both non-distrib, and per-shard
 */
public class PivotFacetProcessor extends SimpleFacets
{
  public static final String QUERY = "query";
  public static final String RANGE = "range";
  protected SolrParams params;
    
  public PivotFacetProcessor(SolrQueryRequest req, DocSet docs, SolrParams params, ResponseBuilder rb) {
    super(req, docs, params, rb);
    this.params = params;
  }
  
  /**
   * Processes all of the specified {@link FacetParams#FACET_PIVOT} strings, generating 
   * a complete response tree for each pivot.  The values in this response will either
   * be the complete tree of fields and values for the specified pivot in the local index, 
   * or the requested refinements if the pivot params include the {@link PivotFacet#REFINE_PARAM}
   */
  public SimpleOrderedMap<List<NamedList<Object>>> process(String[] pivots) throws IOException {
    if (!rb.doFacets || pivots == null) 
      return null;
    
    // rb._statsInfo may be null if stats=false, ie: refine requests
    // if that's the case, but we need to refine w/stats, then we'll lazy init our 
    // own instance of StatsInfo
    StatsInfo statsInfo = rb._statsInfo;

    SimpleOrderedMap<List<NamedList<Object>>> pivotResponse = new SimpleOrderedMap<>();
    for (String pivotList : pivots) {
      final ParsedParams parsed;
      
      try {
        parsed = this.parseParams(FacetParams.FACET_PIVOT, pivotList);
      } catch (SyntaxError e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
      List<String> pivotFields = StrUtils.splitSmart(parsed.facetValue, ",", true);
      if( pivotFields.size() < 1 ) {
        throw new SolrException( ErrorCode.BAD_REQUEST,
                                 "Pivot Facet needs at least one field name: " + pivotList);
      } else {
        SolrIndexSearcher searcher = rb.req.getSearcher();
        for (String fieldName : pivotFields) {
          SchemaField sfield = searcher.getSchema().getField(fieldName);
          if (sfield == null) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "\"" + fieldName + "\" is not a valid field name in pivot: " + pivotList);
          }
        }
      } 

      // start by assuming no local params...

      String refineKey = null; // no local => no refinement
      List<StatsField> statsFields = Collections.emptyList(); // no local => no stats
      List<FacetComponent.FacetBase> facetQueries = Collections.emptyList();
      List<RangeFacetRequest> facetRanges = Collections.emptyList();
      if (null != parsed.localParams) {
        // we might be refining..
        refineKey = parsed.localParams.get(PivotFacet.REFINE_PARAM);
        
        String statsLocalParam = parsed.localParams.get(StatsParams.STATS);
        if (null != refineKey
            && null != statsLocalParam
            && null == statsInfo) {
          // we are refining and need to compute stats, 
          // but stats component hasn't inited StatsInfo (because we
          // don't need/want top level stats when refining) so we lazy init
          // our own copy of StatsInfo
          statsInfo = new StatsInfo(rb);
        }
        statsFields = getTaggedStatsFields(statsInfo, statsLocalParam);

        try {
          FacetComponent.FacetContext facetContext = FacetComponent.FacetContext.getFacetContext(req);

          String taggedQueries = parsed.localParams.get(QUERY);
          if (StringUtils.isEmpty(taggedQueries))  {
            facetQueries = Collections.emptyList();
          } else  {
            List<String> localParamValue = StrUtils.splitSmart(taggedQueries, ',');
            if (localParamValue.size() > 1) {
              String msg = QUERY + " local param of " + FacetParams.FACET_PIVOT +
                  "may not include tags separated by a comma - please use a common tag on all " +
                  FacetParams.FACET_QUERY + " params you wish to compute under this pivot";
              throw new SolrException(ErrorCode.BAD_REQUEST, msg);
            }
            taggedQueries = localParamValue.get(0);
            facetQueries = facetContext.getQueryFacetsForTag(taggedQueries);
          }

          String taggedRanges = parsed.localParams.get(RANGE);
          if (StringUtils.isEmpty(taggedRanges)) {
            facetRanges = Collections.emptyList();
          } else  {
            List<String> localParamValue = StrUtils.splitSmart(taggedRanges, ',');
            if (localParamValue.size() > 1) {
              String msg = RANGE + " local param of " + FacetParams.FACET_PIVOT +
                  "may not include tags separated by a comma - please use a common tag on all " +
                  FacetParams.FACET_RANGE + " params you wish to compute under this pivot";
              throw new SolrException(ErrorCode.BAD_REQUEST, msg);
            }
            taggedRanges = localParamValue.get(0);
            facetRanges = facetContext.getRangeFacetRequestsForTag(taggedRanges);
          }
        } catch (IllegalStateException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Faceting context not set, cannot calculate pivot values");
        }
      }

      if (null != refineKey) {
        String[] refinementValuesByField 
          = params.getParams(PivotFacet.REFINE_PARAM + refineKey);

        for(String refinements : refinementValuesByField){
          pivotResponse.addAll(processSingle(pivotFields, refinements, statsFields, parsed, facetQueries, facetRanges));
        }
      } else{
        pivotResponse.addAll(processSingle(pivotFields, null, statsFields, parsed, facetQueries, facetRanges));
      }
    }
    return pivotResponse;
  }

  /**
   * Process a single branch of refinement values for a specific pivot
   * @param pivotFields the ordered list of fields in this pivot
   * @param refinements the comma separate list of refinement values corresponding to each field in the pivot, or null if there are no refinements
   * @param statsFields List of {@link StatsField} instances to compute for each pivot value
   * @param facetQueries the list of facet queries hung under this pivot
   * @param facetRanges the list of facet ranges hung under this pivot
   */
  private SimpleOrderedMap<List<NamedList<Object>>> processSingle
  (List<String> pivotFields,
   String refinements,
   List<StatsField> statsFields,
   final ParsedParams parsed,
   List<FacetComponent.FacetBase> facetQueries,
   List<RangeFacetRequest> facetRanges) throws IOException {

    SolrIndexSearcher searcher = rb.req.getSearcher();
    SimpleOrderedMap<List<NamedList<Object>>> pivotResponse = new SimpleOrderedMap<>();

    String field = pivotFields.get(0);
    SchemaField sfield = searcher.getSchema().getField(field);
      
    Deque<String> fnames = new LinkedList<>();
    for( int i = pivotFields.size()-1; i>1; i-- ) {
      fnames.push( pivotFields.get(i) );
    }
    
    NamedList<Integer> facetCounts;
    Deque<String> vnames = new LinkedList<>();

    if (null != refinements) {
      // All values, split by the field they should go to
      List<String> refinementValuesByField
        = PivotFacetHelper.decodeRefinementValuePath(refinements);

      for( int i=refinementValuesByField.size()-1; i>0; i-- ) {
        vnames.push(refinementValuesByField.get(i));//Only for [1] and on
      }

      String firstFieldsValues = refinementValuesByField.get(0);

      facetCounts = new NamedList<>();
      facetCounts.add(firstFieldsValues,
                      getSubsetSize(parsed.docs, sfield, firstFieldsValues));
    } else {
      // no refinements needed
      facetCounts = this.getTermCountsForPivots(field, parsed);
    }
    
    if(pivotFields.size() > 1) {
      String subField = pivotFields.get(1);
      pivotResponse.add(parsed.key,
                        doPivots(facetCounts, field, subField, fnames, vnames, parsed, statsFields, facetQueries, facetRanges));
    } else {
      pivotResponse.add(parsed.key, doPivots(facetCounts, field, null, fnames, vnames, parsed, statsFields, facetQueries, facetRanges));
    }
    return pivotResponse;
  }
  
  /**
   * returns the {@link StatsField} instances that should be computed for a pivot
   * based on the 'stats' local params used.
   *
   * @return A list of StatsFields to compute for this pivot, or the empty list if none
   */
  private static List<StatsField> getTaggedStatsFields(StatsInfo statsInfo,
                                                       String statsLocalParam) {
    if (null == statsLocalParam || null == statsInfo) {
      return Collections.emptyList();
    }
    
    List<StatsField> fields = new ArrayList<>(7);
    List<String> statsAr = StrUtils.splitSmart(statsLocalParam, ',');

    // TODO: for now, we only support a single tag name - we reserve using 
    // ',' as a possible delimiter for logic related to only computing stats
    // at certain levels -- see SOLR-6663
    if (1 < statsAr.size()) {
      String msg = StatsParams.STATS + " local param of " + FacetParams.FACET_PIVOT + 
        "may not include tags separated by a comma - please use a common tag on all " + 
        StatsParams.STATS_FIELD + " params you wish to compute under this pivot";
      throw new SolrException(ErrorCode.BAD_REQUEST, msg);
    }

    for(String stat : statsAr) {
      fields.addAll(statsInfo.getStatsFieldsByTag(stat));
    }
    return fields;
  }

  /**
   * Recursive function to compute all the pivot counts for the values under the specified field
   */
  protected List<NamedList<Object>> doPivots(NamedList<Integer> superFacets,
                                             String field, String subField,
                                             Deque<String> fnames, Deque<String> vnames,
                                             ParsedParams parsed, List<StatsField> statsFields,
                                             List<FacetComponent.FacetBase> facetQueries, List<RangeFacetRequest> facetRanges)
      throws IOException {

    boolean isShard = rb.req.getParams().getBool(ShardParams.IS_SHARD, false);

    SolrIndexSearcher searcher = rb.req.getSearcher();
    // TODO: optimize to avoid converting to an external string and then having to convert back to internal below
    SchemaField sfield = searcher.getSchema().getField(field);
    FieldType ftype = sfield.getType();

    String nextField = fnames.poll();

    // re-usable BytesRefBuilder for conversion of term values to Objects
    BytesRefBuilder termval = new BytesRefBuilder(); 

    List<NamedList<Object>> values = new ArrayList<>( superFacets.size() );
    for (Map.Entry<String, Integer> kv : superFacets) {
      // Only sub-facet if parent facet has positive count - still may not be any values for the sub-field though
      if (kv.getValue() >= getMinCountForField(field)) {  
        final String fieldValue = kv.getKey();
        final int pivotCount = kv.getValue();

        SimpleOrderedMap<Object> pivot = new SimpleOrderedMap<>();
        pivot.add( "field", field );
        if (null == fieldValue) {
          pivot.add( "value", null );
        } else {
          ftype.readableToIndexed(fieldValue, termval);
          pivot.add( "value", ftype.toObject(sfield, termval.get()) );
        }
        pivot.add( "count", pivotCount );

        final DocSet subset = getSubset(parsed.docs, sfield, fieldValue);
        
        addPivotQueriesAndRanges(pivot, params, subset, facetQueries, facetRanges);

        if( subField != null )  {
          NamedList<Integer> facetCounts;
          if(!vnames.isEmpty()){
            String val = vnames.pop();
            facetCounts = new NamedList<>();
            facetCounts.add(val, getSubsetSize(subset,
                                               searcher.getSchema().getField(subField),
                                               val));
          } else {
            facetCounts = this.getTermCountsForPivots(subField, parsed.withDocs(subset));
          }

          if (facetCounts.size() >= 1) {
            pivot.add( "pivot", doPivots( facetCounts, subField, nextField, fnames, vnames, parsed.withDocs(subset), statsFields, facetQueries, facetRanges) );
          }
        }
        if ((isShard || 0 < pivotCount) && ! statsFields.isEmpty()) {
          Map<String, StatsValues> stv = new LinkedHashMap<>();
          for (StatsField statsField : statsFields) {
            stv.put(statsField.getOutputKey(), statsField.computeLocalStatsValues(subset));
          }
          pivot.add("stats", StatsComponent.convertToResponse(stv));
        }
        values.add( pivot );
      }

    }
    // put the field back on the list
    fnames.push( nextField );
    return values;
  }
  
  /**
   * Given a base docset, computes the size of the subset of documents corresponding to the specified pivotValue
   *
   * @param base the set of documents to evaluate relative to
   * @param field the field type used by the pivotValue
   * @param pivotValue String representation of the value, may be null (ie: "missing")
   */
  private int getSubsetSize(DocSet base, SchemaField field, String pivotValue) throws IOException {
    FieldType ft = field.getType();
    if ( null == pivotValue ) {
      Query query = ft.getRangeQuery(null, field, null, null, false, false);
      DocSet hasVal = searcher.getDocSet(query);
      return base.andNotSize(hasVal);
    } else {
      Query query = ft.getFieldTermQuery(null, field, pivotValue);
      return searcher.numDocs(query, base);
    }
  }

  /**
   * Given a base docset, computes the subset of documents corresponding to the specified pivotValue
   *
   * @param base the set of documents to evaluate relative to
   * @param field the field type used by the pivotValue
   * @param pivotValue String representation of the value, may be null (ie: "missing")
   */
  private DocSet getSubset(DocSet base, SchemaField field, String pivotValue) throws IOException {
    FieldType ft = field.getType();
    if ( null == pivotValue ) {
      Query query = ft.getRangeQuery(null, field, null, null, false, false);
      DocSet hasVal = searcher.getDocSet(query);
      return base.andNot(hasVal);
    } else {
      Query query = ft.getFieldTermQuery(null, field, pivotValue);
      return searcher.getDocSet(query, base);
    }
  }

  /**
   * Add facet.queries and facet.ranges to the pivot response if needed
   * 
   * @param pivot
   *          Pivot in which to inject additional data
   * @param params
   *          Query parameters.
   * @param docs
   *          DocSet of the current pivot to use for computing sub-counts
   * @param facetQueries
   *          Tagged facet queries should have to be included, must not be null
   * @param facetRanges
   *          Taged facet ranges should have to be included, must not be null
   * @throws IOException
   *           If searcher has issues finding numDocs.
   */
  protected void addPivotQueriesAndRanges(NamedList<Object> pivot, SolrParams params, DocSet docs,
                                          List<FacetComponent.FacetBase> facetQueries,
                                          List<RangeFacetRequest> facetRanges) throws IOException {
    assert null != facetQueries;
    assert null != facetRanges;
    
    if ( ! facetQueries.isEmpty()) {
      SimpleFacets facets = new SimpleFacets(req, docs, params);
      NamedList<Integer> res = new SimpleOrderedMap<>();
      for (FacetComponent.FacetBase facetQuery : facetQueries) {
        try {
          ParsedParams parsed = getParsedParams(params, docs, facetQuery);
          facets.getFacetQueryCount(parsed, res);
        } catch (SyntaxError e) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
                                  "Invalid " + FacetParams.FACET_QUERY + " (" + facetQuery.facetStr +
                                  ") cause: " + e.getMessage(), e);
        }
      }
      pivot.add(PivotListEntry.QUERIES.getName(), res);
    }
    if ( ! facetRanges.isEmpty()) {
      RangeFacetProcessor rangeFacetProcessor = new RangeFacetProcessor(req, docs, params, null);
      NamedList<Object> resOuter = new SimpleOrderedMap<>();
      for (RangeFacetRequest rangeFacet : facetRanges) {
        try {
          rangeFacetProcessor.getFacetRangeCounts(rangeFacet, resOuter);
        } catch (SyntaxError e) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
                                  "Invalid " + FacetParams.FACET_RANGE + " (" + rangeFacet.facetStr +
                                  ") cause: " + e.getMessage(), e);
        }
      }
      pivot.add(PivotListEntry.RANGES.getName(), resOuter);
    }
  }

  private ParsedParams getParsedParams(SolrParams params, DocSet docs, FacetComponent.FacetBase facet) {
    SolrParams wrapped = SolrParams.wrapDefaults(facet.localParams, global);
    SolrParams required = new RequiredSolrParams(params);
    return new ParsedParams(facet.localParams, wrapped, required, facet.facetOn, docs, facet.getKey(), facet.getTags(), -1);
  }

  private int getMinCountForField(String fieldname){
    return params.getFieldInt(fieldname, FacetParams.FACET_PIVOT_MINCOUNT, 1);
  }
  
}
