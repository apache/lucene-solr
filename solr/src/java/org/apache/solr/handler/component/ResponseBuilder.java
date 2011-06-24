/**
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

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RTimer;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SolrIndexSearcher;

import java.util.List;
import java.util.Map;

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
  public boolean doStats;
  public boolean doTerms;

  private boolean needDocList = false;
  private boolean needDocSet = false;
  private int fieldFlags = 0;
  //private boolean debug = false;
  private boolean debugTimings, debugQuery, debugResults;

  private QParser qparser = null;
  private String queryString = null;
  private Query query = null;
  private List<Query> filters = null;
  private SortSpec sortSpec = null;

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

  public GlobalCollectionStat globalCollectionStat;

  Map<Object, ShardDoc> resultIds;
  // Maps uniqueKeyValue to ShardDoc, which may be used to
  // determine order of the doc or uniqueKey in the final
  // returned sequence.
  // Only valid after STAGE_EXECUTE_QUERY has completed.


  public FacetComponent.FacetInfo _facetInfo;
  /* private... components that don't own these shouldn't use them */
  SolrDocumentList _responseDocs;
  StatsInfo _statsInfo;
  TermsComponent.TermsHelper _termsHelper;
  SimpleOrderedMap<List<NamedList<Object>>> _pivots;

  /**
   * Utility function to add debugging info.  This will make sure a valid
   * debugInfo exists before adding to it.
   */
  public void addDebugInfo( String name, Object val )
  {
    if( debugInfo == null ) {
      debugInfo = new SimpleOrderedMap<Object>();
    }
    debugInfo.add( name, val );
  }

  public void addDebug(Object val, String... path) {
    if( debugInfo == null ) {
      debugInfo = new SimpleOrderedMap<Object>();
    }

    NamedList<Object> target = debugInfo;
    for (int i=0; i<path.length-1; i++) {
      String elem = path[i];
      NamedList<Object> newTarget = (NamedList<Object>)debugInfo.get(elem);
      if (newTarget == null) {
        newTarget = new SimpleOrderedMap<Object>();
        target.add(elem, newTarget);
      }
      target = newTarget;
    }

    target.add(path[path.length-1], val);
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------

  public boolean isDebug() {
    return debugQuery || debugTimings || debugResults;
  }

  /**
   *
   * @return true if all debugging options are on
   */
  public boolean isDebugAll(){
    return debugQuery && debugTimings && debugResults;
  }
  
  public void setDebug(boolean dbg){
    debugQuery = dbg;
    debugTimings = dbg;
    debugResults = dbg;
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

  public void setSortSpec(SortSpec sort) {
    this.sortSpec = sort;
  }

  public RTimer getTimer() {
    return timer;
  }

  public void setTimer(RTimer timer) {
    this.timer = timer;
  }


  public static class GlobalCollectionStat {
    public final long numDocs;

    public final Map<String, Long> dfMap;

    public GlobalCollectionStat(int numDocs, Map<String, Long> dfMap) {
      this.numDocs = numDocs;
      this.dfMap = dfMap;
    }
  }

  /**
   * Creates a SolrIndexSearcher.QueryCommand from this
   * ResponseBuilder.  TimeAllowed is left unset.
   */
  public SolrIndexSearcher.QueryCommand getQueryCommand() {
    SolrIndexSearcher.QueryCommand cmd = new SolrIndexSearcher.QueryCommand();
    cmd.setQuery(getQuery())
            .setFilterList(getFilters())
            .setSort(getSortSpec().getSort())
            .setOffset(getSortSpec().getOffset())
            .setLen(getSortSpec().getCount())
            .setFlags(getFieldFlags())
            .setNeedDocSet(isNeedDocSet());
    return cmd;
  }

  /**
   * Sets results from a SolrIndexSearcher.QueryResult.
   */
  public void setResult(SolrIndexSearcher.QueryResult result) {
    setResults(result.getDocListAndSet());
    if (result.isPartialResults()) {
      rsp.getResponseHeader().add("partialResults", Boolean.TRUE);
    }
  }
}
