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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.CursorMark;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;
import org.apache.solr.util.RTimer;

/**
 * This class is experimental and will be changing in the future.
 *
 *
 * @since solr 1.3
 */
public class ResponseBuilder
{
  public SolrQueryRequest req;
  public SolrQueryResponse rsp;
  public boolean doHighlights;
  public boolean doFacets;
  public boolean doExpand;
  public boolean doStats;
  public boolean doTerms;
  public boolean doAnalytics;
  public MergeStrategy mergeFieldHandler;

  private boolean needDocList = false;
  private boolean needDocSet = false;
  private int fieldFlags = 0;
  //private boolean debug = false;
  private boolean debugTimings, debugQuery, debugResults, debugTrack;

  private QParser qparser = null;
  private String queryString = null;
  private Query query = null;
  private List<Query> filters = null;
  private SortSpec sortSpec = null;
  private GroupingSpecification groupingSpec;
  private CursorMark cursorMark;
  private CursorMark nextCursorMark;

  private List<MergeStrategy> mergeStrategies;
  private RankQuery rankQuery;


  private DocListAndSet results = null;
  private NamedList<Object> debugInfo = null;
  private RTimer timer = null;

  private Query highlightQuery = null;

  public List<SearchComponent> components;

  SolrRequestInfo requestInfo;

  public ResponseBuilder(SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components)
  {
    this.req = req;
    this.rsp = rsp;
    this.components = components;
    this.requestInfo = SolrRequestInfo.getRequestInfo();
  }

  //////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////
  //// Distributed Search section
  //////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////

  public static final String FIELD_SORT_VALUES = "fsv";
  public static final String SHARDS = "shards";
  public static final String IDS = "ids";

  /**
   * public static final String NUMDOCS = "nd";
   * public static final String DOCFREQS = "tdf";
   * public static final String TERMS = "terms";
   * public static final String EXTRACT_QUERY_TERMS = "eqt";
   * public static final String LOCAL_SHARD = "local";
   * public static final String DOC_QUERY = "dq";
   * *
   */

  public static int STAGE_START = 0;
  public static int STAGE_PARSE_QUERY = 1000;
  public static int STAGE_TOP_GROUPS = 1500;
  public static int STAGE_EXECUTE_QUERY = 2000;
  public static int STAGE_GET_FIELDS = 3000;
  public static int STAGE_DONE = Integer.MAX_VALUE;

  public int stage;  // What stage is this current request at?

  //The address of the Shard
  boolean isDistrib; // is this a distributed search?
  public String[] shards;
  public String[] slices; // the optional logical ids of the shards
  public int shards_rows = -1;
  public int shards_start = -1;
  public List<ShardRequest> outgoing;  // requests to be sent
  public List<ShardRequest> finished;  // requests that have received responses from all shards
  public String shortCircuitedURL;

  /**
   * This function will return true if this was a distributed search request.
   */
  public boolean isDistributed() {
    return this.isDistrib;
  }

  public int getShardNum(String shard) {
    for (int i = 0; i < shards.length; i++) {
      if (shards[i] == shard || shards[i].equals(shard)) return i;
    }
    return -1;
  }

  public void addRequest(SearchComponent me, ShardRequest sreq) {
    outgoing.add(sreq);
    if ((sreq.purpose & ShardRequest.PURPOSE_PRIVATE) == 0) {
      // if this isn't a private request, let other components modify it.
      for (SearchComponent component : components) {
        if (component != me) {
          component.modifyRequest(this, me, sreq);
        }
      }
    }
  }

  public Map<Object, ShardDoc> resultIds;
  // Maps uniqueKeyValue to ShardDoc, which may be used to
  // determine order of the doc or uniqueKey in the final
  // returned sequence.
  // Only valid after STAGE_EXECUTE_QUERY has completed.

  public boolean onePassDistributedQuery;

  public FacetComponent.FacetInfo _facetInfo;
  /* private... components that don't own these shouldn't use them */
  SolrDocumentList _responseDocs;
  StatsInfo _statsInfo;
  TermsComponent.TermsHelper _termsHelper;
  SimpleOrderedMap<List<NamedList<Object>>> _pivots;
  Object _analyticsRequestManager;
  boolean _isOlapAnalytics;

  // Context fields for grouping
  public final Map<String, Collection<SearchGroup<BytesRef>>> mergedSearchGroups = new HashMap<>();
  public final Map<String, Integer> mergedGroupCounts = new HashMap<>();
  public final Map<String, Map<SearchGroup<BytesRef>, Set<String>>> searchGroupToShards = new HashMap<>();
  public final Map<String, TopGroups<BytesRef>> mergedTopGroups = new HashMap<>();
  public final Map<String, QueryCommandResult> mergedQueryCommandResults = new HashMap<>();
  public final Map<Object, SolrDocument> retrievedDocuments = new HashMap<>();
  public int totalHitCount; // Hit count used when distributed grouping is performed.
  // Used for timeAllowed parameter. First phase elapsed time is subtracted from the time allowed for the second phase.
  public int firstPhaseElapsedTime;

  /**
   * Utility function to add debugging info.  This will make sure a valid
   * debugInfo exists before adding to it.
   */
  public void addDebugInfo( String name, Object val )
  {
    if( debugInfo == null ) {
      debugInfo = new SimpleOrderedMap<>();
    }
    debugInfo.add( name, val );
  }

  public void addDebug(Object val, String... path) {
    if( debugInfo == null ) {
      debugInfo = new SimpleOrderedMap<>();
    }

    NamedList<Object> target = debugInfo;
    for (int i=0; i<path.length-1; i++) {
      String elem = path[i];
      @SuppressWarnings({"unchecked"})
      NamedList<Object> newTarget = (NamedList<Object>)debugInfo.get(elem);
      if (newTarget == null) {
        newTarget = new SimpleOrderedMap<>();
        target.add(elem, newTarget);
      }
      target = newTarget;
    }

    target.add(path[path.length-1], val);
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------

  public boolean isDebug() {
    return debugQuery || debugTimings || debugResults || debugTrack;
  }

  /**
   *
   * @return true if all debugging options are on
   */
  public boolean isDebugAll(){
    return debugQuery && debugTimings && debugResults && debugTrack;
  }

  public void setDebug(boolean dbg){
    debugQuery = dbg;
    debugTimings = dbg;
    debugResults = dbg;
    debugTrack = dbg;
  }

  public void addMergeStrategy(MergeStrategy mergeStrategy) {
    if(mergeStrategies == null) {
      mergeStrategies = new ArrayList<>();
    }

    mergeStrategies.add(mergeStrategy);
  }

  public List<MergeStrategy> getMergeStrategies() {
    return this.mergeStrategies;
  }

  public RankQuery getRankQuery() {
    return rankQuery;
  }

  public void setRankQuery(RankQuery rankQuery) {
    this.rankQuery = rankQuery;
  }

  public void setResponseDocs(SolrDocumentList _responseDocs) {
    this._responseDocs = _responseDocs;
  }
  
  public SolrDocumentList getResponseDocs() {
    return this._responseDocs;
  }

  public boolean isDebugTrack() {
    return debugTrack;
  }

  public void setDebugTrack(boolean debugTrack) {
    this.debugTrack = debugTrack;
  }

  public boolean isDebugTimings() {
    return debugTimings;
  }

  public void setDebugTimings(boolean debugTimings) {
    this.debugTimings = debugTimings;
  }

  public boolean isDebugQuery() {
    return debugQuery;
  }

  public void setDebugQuery(boolean debugQuery) {
    this.debugQuery = debugQuery;
  }

  public boolean isDebugResults() {
    return debugResults;
  }

  public void setDebugResults(boolean debugResults) {
    this.debugResults = debugResults;
  }

  public NamedList<Object> getDebugInfo() {
    return debugInfo;
  }

  public void setDebugInfo(NamedList<Object> debugInfo) {
    this.debugInfo = debugInfo;
  }

  public int getFieldFlags() {
    return fieldFlags;
  }

  public void setFieldFlags(int fieldFlags) {
    this.fieldFlags = fieldFlags;
  }

  public List<Query> getFilters() {
    return filters;
  }

  public void setFilters(List<Query> filters) {
    this.filters = filters;
  }

  public Query getHighlightQuery() {
    return highlightQuery;
  }

  public void setHighlightQuery(Query highlightQuery) {
    this.highlightQuery = highlightQuery;
  }

  public boolean isNeedDocList() {
    return needDocList;
  }

  public void setNeedDocList(boolean needDocList) {
    this.needDocList = needDocList;
  }

  public boolean isNeedDocSet() {
    return needDocSet;
  }

  public void setNeedDocSet(boolean needDocSet) {
    this.needDocSet = needDocSet;
  }

  public QParser getQparser() {
    return qparser;
  }

  public void setQparser(QParser qparser) {
    this.qparser = qparser;
  }

  public String getQueryString() {
    return queryString;
  }

  public void setQueryString(String qstr) {
    this.queryString = qstr;
  }

  public Query getQuery() {
    return query;
  }

  public void setQuery(Query query) {
    this.query = query;
  }

  public DocListAndSet getResults() {
    return results;
  }

  public void setResults(DocListAndSet results) {
    this.results = results;
  }

  public SortSpec getSortSpec() {
    return sortSpec;
  }

  public void setSortSpec(SortSpec sortSpec) {
    this.sortSpec = sortSpec;
  }

  public GroupingSpecification getGroupingSpec() {
    return groupingSpec;
  }

  public void setGroupingSpec(GroupingSpecification groupingSpec) {
    this.groupingSpec = groupingSpec;
  }

  public boolean grouping() {
    return groupingSpec != null;
  }

  public RTimer getTimer() {
    return timer;
  }

  public void setTimer(RTimer timer) {
    this.timer = timer;
  }

  /**
   * Creates a SolrIndexSearcher.QueryCommand from this
   * ResponseBuilder.  TimeAllowed is left unset.
   */
  public QueryCommand createQueryCommand() {
    QueryCommand cmd = new QueryCommand();
    cmd.setQuery(wrap(getQuery()))
            .setFilterList(getFilters())
            .setSort(getSortSpec().getSort())
            .setOffset(getSortSpec().getOffset())
            .setLen(getSortSpec().getCount())
            .setFlags(getFieldFlags())
            .setNeedDocSet(isNeedDocSet())
            .setCursorMark(getCursorMark());
    return cmd;
  }

  /** Calls {@link RankQuery#wrap(Query)} if there's a rank query, otherwise just returns the query. */
  public Query wrap(Query q) {
    if(this.rankQuery != null) {
      return this.rankQuery.wrap(q);
    } else {
      return q;
    }
  }

  /**
   * Sets results from a SolrIndexSearcher.QueryResult.
   */
  public void setResult(QueryResult result) {
    setResults(result.getDocListAndSet());
    if (result.isPartialResults()) {
      rsp.getResponseHeader().asShallowMap()
          .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
      if(getResults() != null && getResults().docList==null) {
        getResults().docList = new DocSlice(0, 0, new int[] {}, new float[] {}, 0, 0, TotalHits.Relation.EQUAL_TO);
      }
    }
    final Boolean segmentTerminatedEarly = result.getSegmentTerminatedEarly();
    if (segmentTerminatedEarly != null) {
      rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, segmentTerminatedEarly);
    }
    if (null != cursorMark) {
      assert null != result.getNextCursorMark() : "using cursor but no next cursor set";
      this.setNextCursorMark(result.getNextCursorMark());
    }
  }
  
  public long getNumberDocumentsFound() {
    if (_responseDocs == null) {
      return 0;
    }
    return _responseDocs.getNumFound();
  }

  public CursorMark getCursorMark() {
    return cursorMark;
  }
  public void setCursorMark(CursorMark cursorMark) {
    this.cursorMark = cursorMark;
  }

  public CursorMark getNextCursorMark() {
    return nextCursorMark;
  }
  public void setNextCursorMark(CursorMark nextCursorMark) {
    this.nextCursorMark = nextCursorMark;
  }

  public void setAnalytics(boolean doAnalytics) {
    this.doAnalytics = doAnalytics;
  }

  public boolean isAnalytics() {
    return this.doAnalytics;
  }

  public void setAnalyticsRequestManager(Object analyticsRequestManager) {
    this._analyticsRequestManager = analyticsRequestManager;
  }

  public Object getAnalyticsRequestManager() {
    return this._analyticsRequestManager;
  }

  public void setOlapAnalytics(boolean isOlapAnalytics) {
    this._isOlapAnalytics = isOlapAnalytics;
  }

  public boolean isOlapAnalytics() {
    return this._isOlapAnalytics;
  }
}
