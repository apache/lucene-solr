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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SortableTextField;
import org.apache.solr.search.CursorMark;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SortSpecParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.grouping.CommandHandler;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.search.grouping.distributed.ShardRequestFactory;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.command.QueryCommand.Builder;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommand;
import org.apache.solr.search.grouping.distributed.command.TopGroupsFieldCommand;
import org.apache.solr.search.grouping.distributed.requestfactory.SearchGroupsRequestFactory;
import org.apache.solr.search.grouping.distributed.requestfactory.StoredFieldsShardRequestFactory;
import org.apache.solr.search.grouping.distributed.requestfactory.TopGroupsShardRequestFactory;
import org.apache.solr.search.grouping.distributed.responseprocessor.SearchGroupShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.responseprocessor.StoredFieldsShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.responseprocessor.TopGroupsShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.shardresultserializer.SearchGroupsResultTransformer;
import org.apache.solr.search.grouping.distributed.shardresultserializer.TopGroupsResultTransformer;
import org.apache.solr.search.grouping.endresulttransformer.EndResultTransformer;
import org.apache.solr.search.grouping.endresulttransformer.GroupedEndResultTransformer;
import org.apache.solr.search.grouping.endresulttransformer.MainEndResultTransformer;
import org.apache.solr.search.grouping.endresulttransformer.SimpleEndResultTransformer;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO!
 * 
 *
 * @since solr 1.3
 */
public class QueryComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "query";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }
    SolrQueryResponse rsp = rb.rsp;

    // Set field flags    
    ReturnFields returnFields = new SolrReturnFields( req );
    rsp.setReturnFields( returnFields );
    int flags = 0;
    if (returnFields.wantsScore()) {
      flags |= SolrIndexSearcher.GET_SCORES;
    }
    rb.setFieldFlags( flags );

    String defType = params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE);

    // get it from the response builder to give a different component a chance
    // to set it.
    String queryString = rb.getQueryString();
    if (queryString == null) {
      // this is the normal way it's set.
      queryString = params.get( CommonParams.Q );
      rb.setQueryString(queryString);
    }

    try {
      QParser parser = QParser.getParser(rb.getQueryString(), defType, req);
      Query q = parser.getQuery();
      if (q == null) {
        // normalize a null query to a query that matches nothing
        q = new MatchNoDocsQuery();
      }

      rb.setQuery( q );

      String rankQueryString = rb.req.getParams().get(CommonParams.RQ);
      if(rankQueryString != null) {
        QParser rqparser = QParser.getParser(rankQueryString, req);
        Query rq = rqparser.getQuery();
        if(rq instanceof RankQuery) {
          RankQuery rankQuery = (RankQuery)rq;
          rb.setRankQuery(rankQuery);
          MergeStrategy mergeStrategy = rankQuery.getMergeStrategy();
          if(mergeStrategy != null) {
            rb.addMergeStrategy(mergeStrategy);
            if(mergeStrategy.handlesMergeFields()) {
              rb.mergeFieldHandler = mergeStrategy;
            }
          }
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"rq parameter must be a RankQuery");
        }
      }

      rb.setSortSpec( parser.getSortSpec(true) );
      rb.setQparser(parser);

      final String cursorStr = rb.req.getParams().get(CursorMarkParams.CURSOR_MARK_PARAM);
      if (null != cursorStr) {
        final CursorMark cursorMark = new CursorMark(rb.req.getSchema(),
                                                     rb.getSortSpec());
        cursorMark.parseSerializedTotem(cursorStr);
        rb.setCursorMark(cursorMark);
      }

      String[] fqs = req.getParams().getParams(CommonParams.FQ);
      if (fqs!=null && fqs.length!=0) {
        List<Query> filters = rb.getFilters();
        // if filters already exists, make a copy instead of modifying the original
        filters = filters == null ? new ArrayList<>(fqs.length) : new ArrayList<>(filters);
        for (String fq : fqs) {
          if (fq != null && fq.trim().length()!=0) {
            QParser fqp = QParser.getParser(fq, req);
            fqp.setIsFilter(true);
            filters.add(fqp.getQuery());
          }
        }
        // only set the filters if they are not empty otherwise
        // fq=&someotherParam= will trigger all docs filter for every request 
        // if filter cache is disabled
        if (!filters.isEmpty()) {
          rb.setFilters( filters );
        }
      }
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    if (params.getBool(GroupParams.GROUP, false)) {
      prepareGrouping(rb);
    } else {
      //Validate only in case of non-grouping search.
      if(rb.getSortSpec().getCount() < 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'rows' parameter cannot be negative");
      }
    }

    //Input validation.
    if (rb.getSortSpec().getOffset() < 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'start' parameter cannot be negative");
    }
  }

  protected void prepareGrouping(ResponseBuilder rb) throws IOException {

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    if (null != rb.getCursorMark()) {
      // It's hard to imagine, conceptually, what it would mean to combine
      // grouping with a cursor - so for now we just don't allow the combination at all
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not use Grouping with " +
                              CursorMarkParams.CURSOR_MARK_PARAM);
    }

    SolrIndexSearcher searcher = rb.req.getSearcher();
    GroupingSpecification groupingSpec = new GroupingSpecification();
    rb.setGroupingSpec(groupingSpec);

    final SortSpec sortSpec = rb.getSortSpec();

    //TODO: move weighting of sort
    final SortSpec groupSortSpec = searcher.weightSortSpec(sortSpec, Sort.RELEVANCE);

    String withinGroupSortStr = params.get(GroupParams.GROUP_SORT);
    //TODO: move weighting of sort
    final SortSpec withinGroupSortSpec;
    if (withinGroupSortStr != null) {
      SortSpec parsedWithinGroupSortSpec = SortSpecParsing.parseSortSpec(withinGroupSortStr, req);
      withinGroupSortSpec = searcher.weightSortSpec(parsedWithinGroupSortSpec, Sort.RELEVANCE);
    } else {
      withinGroupSortSpec = new SortSpec(
          groupSortSpec.getSort(),
          groupSortSpec.getSchemaFields(),
          groupSortSpec.getCount(),
          groupSortSpec.getOffset());
    }
    withinGroupSortSpec.setOffset(params.getInt(GroupParams.GROUP_OFFSET, 0));
    withinGroupSortSpec.setCount(params.getInt(GroupParams.GROUP_LIMIT, 1));

    groupingSpec.setWithinGroupSortSpec(withinGroupSortSpec);
    groupingSpec.setGroupSortSpec(groupSortSpec);

    String formatStr = params.get(GroupParams.GROUP_FORMAT, Grouping.Format.grouped.name());
    Grouping.Format responseFormat;
    try {
       responseFormat = Grouping.Format.valueOf(formatStr);
    } catch (IllegalArgumentException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "Illegal %s parameter", GroupParams.GROUP_FORMAT));
    }
    groupingSpec.setResponseFormat(responseFormat);

    // See SOLR-12249. Disallow grouping on text fields that are not SortableText in cloud mode
    if (req.getCore().getCoreContainer().isZooKeeperAware()) {
      IndexSchema schema = rb.req.getSchema();
      String[] fields = params.getParams(GroupParams.GROUP_FIELD);
      if (fields != null) {
        for (String field : fields) {
          SchemaField schemaField = schema.getField(field);
          if (schemaField.getType().isTokenized() && (schemaField.getType() instanceof SortableTextField) == false) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT,
                "Sorting on a tokenized field that is not a SortableTextField is not supported in cloud mode."));
          }
        }
      }
    }

    groupingSpec.setFields(params.getParams(GroupParams.GROUP_FIELD));
    groupingSpec.setQueries(params.getParams(GroupParams.GROUP_QUERY));
    groupingSpec.setFunctions(params.getParams(GroupParams.GROUP_FUNC));
    groupingSpec.setIncludeGroupCount(params.getBool(GroupParams.GROUP_TOTAL_COUNT, false));
    groupingSpec.setMain(params.getBool(GroupParams.GROUP_MAIN, false));
    groupingSpec.setNeedScore((rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0);
    groupingSpec.setTruncateGroups(params.getBool(GroupParams.GROUP_TRUNCATE, false));

    // when group.format=grouped then, validate group.offset
    // for group.main=true and group.format=simple, start value is used instead of group.offset
    // and start is already validate above for negative values
    if (!(groupingSpec.isMain() || groupingSpec.getResponseFormat() == Grouping.Format.simple) &&
        groupingSpec.getWithinGroupSortSpec().getOffset() < 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'group.offset' parameter cannot be negative");
    }
  }



  /**
   * Actually run the query
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    log.debug("process: {}", rb.req.getParams());
  
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }

    SolrIndexSearcher searcher = req.getSearcher();
    StatsCache statsCache = searcher.getStatsCache();
    
    int purpose = params.getInt(ShardParams.SHARDS_PURPOSE, ShardRequest.PURPOSE_GET_TOP_IDS);
    if ((purpose & ShardRequest.PURPOSE_GET_TERM_STATS) != 0) {
      statsCache.returnLocalStats(rb, searcher);
      return;
    }
    // check if we need to update the local copy of global dfs
    if ((purpose & ShardRequest.PURPOSE_SET_TERM_STATS) != 0) {
      // retrieve from request and update local cache
      statsCache.receiveGlobalStats(req);
    }

    // Optional: This could also be implemented by the top-level searcher sending
    // a filter that lists the ids... that would be transparent to
    // the request handler, but would be more expensive (and would preserve score
    // too if desired).
    if (doProcessSearchByIds(rb)) {
      return;
    }

    // -1 as flag if not set.
    long timeAllowed = params.getLong(CommonParams.TIME_ALLOWED, -1L);
    if (null != rb.getCursorMark() && 0 < timeAllowed) {
      // fundamentally incompatible
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not search using both " +
                              CursorMarkParams.CURSOR_MARK_PARAM + " and " + CommonParams.TIME_ALLOWED);
    }

    QueryCommand cmd = rb.createQueryCommand();
    cmd.setTimeAllowed(timeAllowed);

    req.getContext().put(SolrIndexSearcher.STATS_SOURCE, statsCache.get(req));
    
    QueryResult result = new QueryResult();

    cmd.setSegmentTerminateEarly(params.getBool(CommonParams.SEGMENT_TERMINATE_EARLY, CommonParams.SEGMENT_TERMINATE_EARLY_DEFAULT));
    if (cmd.getSegmentTerminateEarly()) {
      result.setSegmentTerminatedEarly(Boolean.FALSE);
    }

    //
    // grouping / field collapsing
    //
    GroupingSpecification groupingSpec = rb.getGroupingSpec();
    if (groupingSpec != null) {
      cmd.setSegmentTerminateEarly(false); // not supported, silently ignore any segmentTerminateEarly flag
      try {
        if (params.getBool(GroupParams.GROUP_DISTRIBUTED_FIRST, false)) {
          doProcessGroupedDistributedSearchFirstPhase(rb, cmd, result);
          return;
        } else if (params.getBool(GroupParams.GROUP_DISTRIBUTED_SECOND, false)) {
          doProcessGroupedDistributedSearchSecondPhase(rb, cmd, result);
          return;
        }

        doProcessGroupedSearch(rb, cmd, result);
        return;
      } catch (SyntaxError e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    // normal search result
    doProcessUngroupedSearch(rb, cmd, result);
  }

  protected void doFieldSortValues(ResponseBuilder rb, SolrIndexSearcher searcher) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    // The query cache doesn't currently store sort field values, and SolrIndexSearcher doesn't
    // currently have an option to return sort field values.  Because of this, we
    // take the documents given and re-derive the sort values.
    //
    // TODO: See SOLR-5595
    boolean fsv = req.getParams().getBool(ResponseBuilder.FIELD_SORT_VALUES,false);
    if(fsv){
      try {
      NamedList<Object[]> sortVals = new NamedList<>(); // order is important for the sort fields
      IndexReaderContext topReaderContext = searcher.getTopReaderContext();
      List<LeafReaderContext> leaves = topReaderContext.leaves();
      LeafReaderContext currentLeaf = null;
      if (leaves.size()==1) {
        // if there is a single segment, use that subReader and avoid looking up each time
        currentLeaf = leaves.get(0);
        leaves=null;
      }

      final DocList docs = rb.getResults().docList;

      // sort ids from lowest to highest so we can access them in order
      int nDocs = docs.size();
      final long[] sortedIds = new long[nDocs];
      final float[] scores = new float[nDocs]; // doc scores, parallel to sortedIds
      DocIterator it = docs.iterator();
      for (int i=0; i<nDocs; i++) {
        sortedIds[i] = (((long)it.nextDoc()) << 32) | i;
        scores[i] = docs.hasScores() ? it.score() : Float.NaN;
      }

      // sort ids and scores together
      new InPlaceMergeSorter() {
        @Override
        protected void swap(int i, int j) {
          long tmpId = sortedIds[i];
          float tmpScore = scores[i];
          sortedIds[i] = sortedIds[j];
          scores[i] = scores[j];
          sortedIds[j] = tmpId;
          scores[j] = tmpScore;
        }

        @Override
        protected int compare(int i, int j) {
          return Long.compare(sortedIds[i], sortedIds[j]);
        }
      }.sort(0, sortedIds.length);

      SortSpec sortSpec = rb.getSortSpec();
      Sort sort = searcher.weightSort(sortSpec.getSort());
      SortField[] sortFields = sort==null ? new SortField[]{SortField.FIELD_SCORE} : sort.getSort();
      List<SchemaField> schemaFields = sortSpec.getSchemaFields();

      for (int fld = 0; fld < schemaFields.size(); fld++) {
        SchemaField schemaField = schemaFields.get(fld);
        FieldType ft = null == schemaField? null : schemaField.getType();
        SortField sortField = sortFields[fld];

        SortField.Type type = sortField.getType();
        // :TODO: would be simpler to always serialize every position of SortField[]
        if (type==SortField.Type.SCORE || type==SortField.Type.DOC) continue;

        FieldComparator<?> comparator = sortField.getComparator(1,0);
        LeafFieldComparator leafComparator = null;
        Object[] vals = new Object[nDocs];

        int lastIdx = -1;
        int idx = 0;

        for (int i = 0; i < sortedIds.length; ++i) {
          long idAndPos = sortedIds[i];
          float score = scores[i];
          int doc = (int)(idAndPos >>> 32);
          int position = (int)idAndPos;

          if (leaves != null) {
            idx = ReaderUtil.subIndex(doc, leaves);
            currentLeaf = leaves.get(idx);
            if (idx != lastIdx) {
              // we switched segments.  invalidate leafComparator.
              lastIdx = idx;
              leafComparator = null;
            }
          }

          if (leafComparator == null) {
            leafComparator = comparator.getLeafComparator(currentLeaf);
          }

          doc -= currentLeaf.docBase;  // adjust for what segment this is in
          leafComparator.setScorer(new ScoreAndDoc(doc, score));
          leafComparator.copy(0, doc);
          Object val = comparator.value(0);
          if (null != ft) val = ft.marshalSortValue(val);
          vals[position] = val;
        }

        sortVals.add(sortField.getField(), vals);
      }
      rsp.add("sort_values", sortVals);
    }catch(ExitableDirectoryReader.ExitingReaderException x) {
      // it's hard to understand where we stopped, so yield nothing
      // search handler will flag partial results
      rsp.add("sort_values",new NamedList<>() );
      throw x;
    }
    }
  }

  protected void doPrefetch(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    //pre-fetch returned documents
    if (!req.getParams().getBool(ShardParams.IS_SHARD,false) && rb.getResults().docList != null && rb.getResults().docList.size()<=50) {
      SolrPluginUtils.optimizePreFetchDocs(rb, rb.getResults().docList, rb.getQuery(), req, rsp);
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (rb.grouping()) {
      return groupedDistributedProcess(rb);
    } else {
      return regularDistributedProcess(rb);
    }
  }

  protected int groupedDistributedProcess(ResponseBuilder rb) {
    int nextStage = ResponseBuilder.STAGE_DONE;
    ShardRequestFactory shardRequestFactory = null;

    if (rb.stage < ResponseBuilder.STAGE_PARSE_QUERY) {
      nextStage = ResponseBuilder.STAGE_PARSE_QUERY;
    } else if (rb.stage == ResponseBuilder.STAGE_PARSE_QUERY) {
      createDistributedStats(rb);
      nextStage = ResponseBuilder.STAGE_TOP_GROUPS;
    } else if (rb.stage < ResponseBuilder.STAGE_TOP_GROUPS) {
      nextStage = ResponseBuilder.STAGE_TOP_GROUPS;
    } else if (rb.stage == ResponseBuilder.STAGE_TOP_GROUPS) {
      shardRequestFactory = new SearchGroupsRequestFactory();
      nextStage = ResponseBuilder.STAGE_EXECUTE_QUERY;
    } else if (rb.stage < ResponseBuilder.STAGE_EXECUTE_QUERY) {
      nextStage = ResponseBuilder.STAGE_EXECUTE_QUERY;
    } else if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      shardRequestFactory = new TopGroupsShardRequestFactory();
      nextStage = ResponseBuilder.STAGE_GET_FIELDS;
    } else if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
      nextStage = ResponseBuilder.STAGE_GET_FIELDS;
    } else if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      shardRequestFactory = new StoredFieldsShardRequestFactory();
      nextStage = ResponseBuilder.STAGE_DONE;
    }

    if (shardRequestFactory != null) {
      for (ShardRequest shardRequest : shardRequestFactory.constructRequest(rb)) {
        rb.addRequest(this, shardRequest);
      }
    }
    return nextStage;
  }

  protected int regularDistributedProcess(ResponseBuilder rb) {
    if (rb.stage < ResponseBuilder.STAGE_PARSE_QUERY)
      return ResponseBuilder.STAGE_PARSE_QUERY;
    if (rb.stage == ResponseBuilder.STAGE_PARSE_QUERY) {
      createDistributedStats(rb);
      return ResponseBuilder.STAGE_EXECUTE_QUERY;
    }
    if (rb.stage < ResponseBuilder.STAGE_EXECUTE_QUERY) return ResponseBuilder.STAGE_EXECUTE_QUERY;
    if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      createMainQuery(rb);
      return ResponseBuilder.STAGE_GET_FIELDS;
    }
    if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS) return ResponseBuilder.STAGE_GET_FIELDS;
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS && !rb.onePassDistributedQuery) {
      createRetrieveDocs(rb);
      return ResponseBuilder.STAGE_DONE;
    }
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (rb.grouping()) {
      handleGroupedResponses(rb, sreq);
    } else {
      handleRegularResponses(rb, sreq);
    }
  }

  protected void handleGroupedResponses(ResponseBuilder rb, ShardRequest sreq) {
    ShardResponseProcessor responseProcessor = null;
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_GROUPS) != 0) {
      responseProcessor = new SearchGroupShardResponseProcessor();
    } else if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      responseProcessor = new TopGroupsShardResponseProcessor();
    } else if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      responseProcessor = new StoredFieldsShardResponseProcessor();
    }

    if (responseProcessor != null) {
      responseProcessor.process(rb, sreq);
    }
  }

  protected void handleRegularResponses(ResponseBuilder rb, ShardRequest sreq) {
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      mergeIds(rb, sreq);
    }

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TERM_STATS) != 0) {
      updateStats(rb, sreq);
    }

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      returnFields(rb, sreq);
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) {
      return;
    }
    if (rb.grouping()) {
      groupedFinishStage(rb);
    } else {
      regularFinishStage(rb);
    }
  }

  protected static final EndResultTransformer MAIN_END_RESULT_TRANSFORMER = new MainEndResultTransformer();
  protected static final EndResultTransformer SIMPLE_END_RESULT_TRANSFORMER = new SimpleEndResultTransformer();

  @SuppressWarnings("unchecked")
  protected void groupedFinishStage(final ResponseBuilder rb) {
    // To have same response as non-distributed request.
    GroupingSpecification groupSpec = rb.getGroupingSpec();
    if (rb.mergedTopGroups.isEmpty()) {
      for (String field : groupSpec.getFields()) {
        rb.mergedTopGroups.put(field, new TopGroups(null, null, 0, 0, new GroupDocs[]{}, Float.NaN));
      }
      rb.resultIds = new HashMap<>();
    }

    EndResultTransformer.SolrDocumentSource solrDocumentSource = doc -> {
      ShardDoc solrDoc = (ShardDoc) doc;
      return rb.retrievedDocuments.get(solrDoc.id);
    };
    EndResultTransformer endResultTransformer;
    if (groupSpec.isMain()) {
      endResultTransformer = MAIN_END_RESULT_TRANSFORMER;
    } else if (Grouping.Format.grouped == groupSpec.getResponseFormat()) {
      endResultTransformer = new GroupedEndResultTransformer(rb.req.getSearcher());
    } else if (Grouping.Format.simple == groupSpec.getResponseFormat() && !groupSpec.isMain()) {
      endResultTransformer = SIMPLE_END_RESULT_TRANSFORMER;
    } else {
      return;
    }
    Map<String, Object> combinedMap = new LinkedHashMap<>();
    combinedMap.putAll(rb.mergedTopGroups);
    combinedMap.putAll(rb.mergedQueryCommandResults);
    endResultTransformer.transform(combinedMap, rb, solrDocumentSource);
  }

  protected void regularFinishStage(ResponseBuilder rb) {
    // We may not have been able to retrieve all the docs due to an
    // index change.  Remove any null documents.
    for (Iterator<SolrDocument> iter = rb.getResponseDocs().iterator(); iter.hasNext();) {
      if (iter.next() == null) {
        iter.remove();
        rb.getResponseDocs().setNumFound(rb.getResponseDocs().getNumFound()-1);
      }
    }

    rb.rsp.addResponse(rb.getResponseDocs());
    if (null != rb.getNextCursorMark()) {
      rb.rsp.add(CursorMarkParams.CURSOR_MARK_NEXT,
                 rb.getNextCursorMark().getSerializedTotem());
    }
  }

  protected void createDistributedStats(ResponseBuilder rb) {
    StatsCache cache = rb.req.getSearcher().getStatsCache();
    if ( (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES)!=0 || rb.getSortSpec().includesScore()) {
      ShardRequest sreq = cache.retrieveStatsRequest(rb);
      if (sreq != null) {
        rb.addRequest(this, sreq);
      }
    }
  }

  protected void updateStats(ResponseBuilder rb, ShardRequest sreq) {
    StatsCache cache = rb.req.getSearcher().getStatsCache();
    cache.mergeToGlobalStats(rb.req, sreq.responses);
  }

  protected void createMainQuery(ResponseBuilder rb) {
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_TOP_IDS;

    String keyFieldName = rb.req.getSchema().getUniqueKeyField().getName();

    // one-pass algorithm if only id and score fields are requested, but not if fl=score since that's the same as fl=*,score
    ReturnFields fields = rb.rsp.getReturnFields();

    // distrib.singlePass=true forces a one-pass query regardless of requested fields
    boolean distribSinglePass = rb.req.getParams().getBool(ShardParams.DISTRIB_SINGLE_PASS, false);

    if(distribSinglePass || (fields != null && fields.wantsField(keyFieldName)
        && fields.getRequestedFieldNames() != null  
        && (!fields.hasPatternMatching() && Arrays.asList(keyFieldName, "score").containsAll(fields.getRequestedFieldNames())))) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_FIELDS;
      rb.onePassDistributedQuery = true;
    }

    sreq.params = new ModifiableSolrParams(rb.req.getParams());
    // TODO: base on current params or original params?

    // don't pass through any shards param
    sreq.params.remove(ShardParams.SHARDS);

    // set the start (offset) to 0 for each shard request so we can properly merge
    // results from the start.
    if(rb.shards_start > -1) {
      // if the client set shards.start set this explicitly
      sreq.params.set(CommonParams.START,rb.shards_start);
    } else {
      sreq.params.set(CommonParams.START, "0");
    }
    // TODO: should we even use the SortSpec?  That's obtained from the QParser, and
    // perhaps we shouldn't attempt to parse the query at this level?
    // Alternate Idea: instead of specifying all these things at the upper level,
    // we could just specify that this is a shard request.
    if(rb.shards_rows > -1) {
      // if the client set shards.rows set this explicity
      sreq.params.set(CommonParams.ROWS,rb.shards_rows);
    } else {
      // what if rows<0 as it is allowed for grouped request??
      sreq.params.set(CommonParams.ROWS, rb.getSortSpec().getOffset() + rb.getSortSpec().getCount());
    }

    sreq.params.set(ResponseBuilder.FIELD_SORT_VALUES,"true");

    boolean shardQueryIncludeScore = (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0 || rb.getSortSpec().includesScore();
    StringBuilder additionalFL = new StringBuilder();
    boolean additionalAdded = false;
    if (distribSinglePass)  {
      String[] fls = rb.req.getParams().getParams(CommonParams.FL);
      if (fls != null && fls.length > 0 && (fls.length != 1 || !fls[0].isEmpty())) {
        // If the outer request contains actual FL's use them...
        sreq.params.set(CommonParams.FL, fls);
        if (!fields.wantsField(keyFieldName))  {
          additionalAdded = addFL(additionalFL, keyFieldName, additionalAdded);
        }
      } else {
        // ... else we need to explicitly ask for all fields, because we are going to add
        // additional fields below
        sreq.params.set(CommonParams.FL, "*");
      }
      if (!fields.wantsScore() && shardQueryIncludeScore) {
        additionalAdded = addFL(additionalFL, "score", additionalAdded);
      }
    } else {
      // reset so that only unique key is requested in shard requests
      sreq.params.set(CommonParams.FL, rb.req.getSchema().getUniqueKeyField().getName());
      if (shardQueryIncludeScore) {
        additionalAdded = addFL(additionalFL, "score", additionalAdded);
      }
    }

    // TODO: should this really sendGlobalDfs if just includeScore?

    if (shardQueryIncludeScore || rb.isDebug()) {
      StatsCache statsCache = rb.req.getSearcher().getStatsCache();
      sreq.purpose |= ShardRequest.PURPOSE_SET_TERM_STATS;
      statsCache.sendGlobalStats(rb, sreq);
    }

    if (additionalAdded) sreq.params.add(CommonParams.FL, additionalFL.toString());

    rb.addRequest(this, sreq);
  }
  
  protected boolean addFL(StringBuilder fl, String field, boolean additionalAdded) {
    if (additionalAdded) fl.append(",");
    fl.append(field);
    return true;
  }

  protected void mergeIds(ResponseBuilder rb, ShardRequest sreq) {
      List<MergeStrategy> mergeStrategies = rb.getMergeStrategies();
      if(mergeStrategies != null) {
        Collections.sort(mergeStrategies, MergeStrategy.MERGE_COMP);
        boolean idsMerged = false;
        for(MergeStrategy mergeStrategy : mergeStrategies) {
          mergeStrategy.merge(rb, sreq);
          if(mergeStrategy.mergesIds()) {
            idsMerged = true;
          }
        }

        if(idsMerged) {
          return; //ids were merged above so return.
        }
      }

      SortSpec ss = rb.getSortSpec();
      Sort sort = ss.getSort();

      SortField[] sortFields = null;
      if(sort != null) sortFields = sort.getSort();
      else {
        sortFields = new SortField[]{SortField.FIELD_SCORE};
      }
 
      IndexSchema schema = rb.req.getSchema();
      SchemaField uniqueKeyField = schema.getUniqueKeyField();


      // id to shard mapping, to eliminate any accidental dups
      HashMap<Object,String> uniqueDoc = new HashMap<>();

      // Merge the docs via a priority queue so we don't have to sort *all* of the
      // documents... we only need to order the top (rows+start)
      final ShardFieldSortedHitQueue queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount(), rb.req.getSearcher());

      NamedList<Object> shardInfo = null;
      if(rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
        shardInfo = new SimpleOrderedMap<>();
        rb.rsp.getValues().add(ShardParams.SHARDS_INFO,shardInfo);
      }
      
      long numFound = 0;
      Float maxScore=null;
      boolean thereArePartialResults = false;
      Boolean segmentTerminatedEarly = null;
      for (ShardResponse srsp : sreq.responses) {
        SolrDocumentList docs = null;
        NamedList<?> responseHeader = null;

        if(shardInfo!=null) {
          SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
          
          if (srsp.getException() != null) {
            Throwable t = srsp.getException();
            if(t instanceof SolrServerException) {
              t = ((SolrServerException)t).getCause();
            }
            nl.add("error", t.toString() );
            StringWriter trace = new StringWriter();
            t.printStackTrace(new PrintWriter(trace));
            nl.add("trace", trace.toString() );
            if (srsp.getShardAddress() != null) {
              nl.add("shardAddress", srsp.getShardAddress());
            }
          }
          else {
            responseHeader = (NamedList<?>)srsp.getSolrResponse().getResponse().get("responseHeader");
            final Object rhste = responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
            if (rhste != null) {
              nl.add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, rhste);
            }
            docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
            nl.add("numFound", docs.getNumFound());
            nl.add("maxScore", docs.getMaxScore());
            nl.add("shardAddress", srsp.getShardAddress());
          }
          if(srsp.getSolrResponse()!=null) {
            nl.add("time", srsp.getSolrResponse().getElapsedTime());
          }

          shardInfo.add(srsp.getShard(), nl);
        }
        // now that we've added the shard info, let's only proceed if we have no error.
        if (srsp.getException() != null) {
          thereArePartialResults = true;
          continue;
        }

        if (docs == null) { // could have been initialized in the shards info block above
          docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
        }
        
        if (responseHeader == null) { // could have been initialized in the shards info block above
          responseHeader = (NamedList<?>)srsp.getSolrResponse().getResponse().get("responseHeader");
        }

        final boolean thisResponseIsPartial;
        thisResponseIsPartial = Boolean.TRUE.equals(responseHeader.getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
        thereArePartialResults |= thisResponseIsPartial;
        
        if (!Boolean.TRUE.equals(segmentTerminatedEarly)) {
          final Object ste = responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
          if (Boolean.TRUE.equals(ste)) {
            segmentTerminatedEarly = Boolean.TRUE;
          } else if (Boolean.FALSE.equals(ste)) {
            segmentTerminatedEarly = Boolean.FALSE;
          }
        }
        
        // calculate global maxScore and numDocsFound
        if (docs.getMaxScore() != null) {
          maxScore = maxScore==null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
        }
        numFound += docs.getNumFound();

        NamedList sortFieldValues = (NamedList)(srsp.getSolrResponse().getResponse().get("sort_values"));
        if (sortFieldValues.size()==0 && // we bypass merging this response only if it's partial itself
                            thisResponseIsPartial) { // but not the previous one!!
          continue; //fsv timeout yields empty sort_vlaues
        }
        NamedList unmarshalledSortFieldValues = unmarshalSortValues(ss, sortFieldValues, schema);

        // go through every doc in this response, construct a ShardDoc, and
        // put it in the priority queue so it can be ordered.
        for (int i=0; i<docs.size(); i++) {
          SolrDocument doc = docs.get(i);
          Object id = doc.getFieldValue(uniqueKeyField.getName());

          String prevShard = uniqueDoc.put(id, srsp.getShard());
          if (prevShard != null) {
            // duplicate detected
            numFound--;

            // For now, just always use the first encountered since we can't currently
            // remove the previous one added to the priority queue.  If we switched
            // to the Java5 PriorityQueue, this would be easier.
            continue;
            // make which duplicate is used deterministic based on shard
            // if (prevShard.compareTo(srsp.shard) >= 0) {
            //  TODO: remove previous from priority queue
            //  continue;
            // }
          }

          ShardDoc shardDoc = new ShardDoc();
          shardDoc.id = id;
          shardDoc.shard = srsp.getShard();
          shardDoc.orderInShard = i;
          Object scoreObj = doc.getFieldValue("score");
          if (scoreObj != null) {
            if (scoreObj instanceof String) {
              shardDoc.score = Float.parseFloat((String)scoreObj);
            } else {
              shardDoc.score = (Float)scoreObj;
            }
          }

          shardDoc.sortFieldValues = unmarshalledSortFieldValues;

          queue.insertWithOverflow(shardDoc);
        } // end for-each-doc-in-response
      } // end for-each-response
      
      // The queue now has 0 -> queuesize docs, where queuesize <= start + rows
      // So we want to pop the last documents off the queue to get
      // the docs offset -> queuesize
      int resultSize = queue.size() - ss.getOffset();
      resultSize = Math.max(0, resultSize);  // there may not be any docs in range

      Map<Object,ShardDoc> resultIds = new HashMap<>();
      for (int i=resultSize-1; i>=0; i--) {
        ShardDoc shardDoc = queue.pop();
        shardDoc.positionInResponse = i;
        // Need the toString() for correlation with other lists that must
        // be strings (like keys in highlighting, explain, etc)
        resultIds.put(shardDoc.id.toString(), shardDoc);
      }

      // Add hits for distributed requests
      // https://issues.apache.org/jira/browse/SOLR-3518
      rb.rsp.addToLog("hits", numFound);

      SolrDocumentList responseDocs = new SolrDocumentList();
      if (maxScore!=null) responseDocs.setMaxScore(maxScore);
      responseDocs.setNumFound(numFound);
      responseDocs.setStart(ss.getOffset());
      // size appropriately
      for (int i=0; i<resultSize; i++) responseDocs.add(null);

      // save these results in a private area so we can access them
      // again when retrieving stored fields.
      // TODO: use ResponseBuilder (w/ comments) or the request context?
      rb.resultIds = resultIds;
      rb.setResponseDocs(responseDocs);

      populateNextCursorMarkFromMergedShards(rb);

      if (thereArePartialResults) {
         rb.rsp.getResponseHeader().asShallowMap()
                   .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
      }
      if (segmentTerminatedEarly != null) {
        final Object existingSegmentTerminatedEarly = rb.rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
        if (existingSegmentTerminatedEarly == null) {
          rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, segmentTerminatedEarly);
        } else if (!Boolean.TRUE.equals(existingSegmentTerminatedEarly) && Boolean.TRUE.equals(segmentTerminatedEarly)) {
          rb.rsp.getResponseHeader().remove(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
          rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, segmentTerminatedEarly);
        }
      }
  }

  /**
   * Inspects the state of the {@link ResponseBuilder} and populates the next 
   * {@link ResponseBuilder#setNextCursorMark} as appropriate based on the merged 
   * sort values from individual shards
   *
   * @param rb A <code>ResponseBuilder</code> that already contains merged 
   *           <code>ShardDocs</code> in <code>resultIds</code>, may or may not be 
   *           part of a Cursor based request (method will NOOP if not needed)
   */
  protected void populateNextCursorMarkFromMergedShards(ResponseBuilder rb) {

    final CursorMark lastCursorMark = rb.getCursorMark();
    if (null == lastCursorMark) {
      // Not a cursor based request
      return; // NOOP
    }

    assert null != rb.resultIds : "resultIds was not set in ResponseBuilder";

    Collection<ShardDoc> docsOnThisPage = rb.resultIds.values();

    if (0 == docsOnThisPage.size()) {
      // nothing more matching query, re-use existing totem so user can "resume" 
      // search later if it makes sense for this sort.
      rb.setNextCursorMark(lastCursorMark);
      return;
    }

    ShardDoc lastDoc = null;
    // ShardDoc and rb.resultIds are weird structures to work with...
    for (ShardDoc eachDoc : docsOnThisPage) {
      if (null == lastDoc || lastDoc.positionInResponse  < eachDoc.positionInResponse) {
        lastDoc = eachDoc;
      }
    }
    SortField[] sortFields = lastCursorMark.getSortSpec().getSort().getSort();
    List<Object> nextCursorMarkValues = new ArrayList<>(sortFields.length);
    for (SortField sf : sortFields) {
      if (sf.getType().equals(SortField.Type.SCORE)) {
        nextCursorMarkValues.add(lastDoc.score);
      } else {
        assert null != sf.getField() : "SortField has null field";
        List<Object> fieldVals = (List<Object>) lastDoc.sortFieldValues.get(sf.getField());
        nextCursorMarkValues.add(fieldVals.get(lastDoc.orderInShard));
      }
    }
    CursorMark nextCursorMark = lastCursorMark.createNext(nextCursorMarkValues);
    assert null != nextCursorMark : "null nextCursorMark";
    rb.setNextCursorMark(nextCursorMark);
  }

  protected NamedList unmarshalSortValues(SortSpec sortSpec, 
                                        NamedList sortFieldValues, 
                                        IndexSchema schema) {
    NamedList unmarshalledSortValsPerField = new NamedList();

    if (0 == sortFieldValues.size()) return unmarshalledSortValsPerField;
    
    List<SchemaField> schemaFields = sortSpec.getSchemaFields();
    SortField[] sortFields = sortSpec.getSort().getSort();

    int marshalledFieldNum = 0;
    for (int sortFieldNum = 0; sortFieldNum < sortFields.length; sortFieldNum++) {
      final SortField sortField = sortFields[sortFieldNum];
      final SortField.Type type = sortField.getType();

      // :TODO: would be simpler to always serialize every position of SortField[]
      if (type==SortField.Type.SCORE || type==SortField.Type.DOC) continue;

      final String sortFieldName = sortField.getField();
      final String valueFieldName = sortFieldValues.getName(marshalledFieldNum);
      assert sortFieldName.equals(valueFieldName)
        : "sortFieldValues name key does not match expected SortField.getField";

      List sortVals = (List)sortFieldValues.getVal(marshalledFieldNum);

      final SchemaField schemaField = schemaFields.get(sortFieldNum);
      if (null == schemaField) {
        unmarshalledSortValsPerField.add(sortField.getField(), sortVals);
      } else {
        FieldType fieldType = schemaField.getType();
        List unmarshalledSortVals = new ArrayList();
        for (Object sortVal : sortVals) {
          unmarshalledSortVals.add(fieldType.unmarshalSortValue(sortVal));
        }
        unmarshalledSortValsPerField.add(sortField.getField(), unmarshalledSortVals);
      }
      marshalledFieldNum++;
    }
    return unmarshalledSortValsPerField;
  }

  protected void createRetrieveDocs(ResponseBuilder rb) {

    // TODO: in a system with nTiers > 2, we could be passed "ids" here
    // unless those requests always go to the final destination shard

    // for each shard, collect the documents for that shard.
    HashMap<String, Collection<ShardDoc>> shardMap = new HashMap<>();
    for (ShardDoc sdoc : rb.resultIds.values()) {
      Collection<ShardDoc> shardDocs = shardMap.get(sdoc.shard);
      if (shardDocs == null) {
        shardDocs = new ArrayList<>();
        shardMap.put(sdoc.shard, shardDocs);
      }
      shardDocs.add(sdoc);
    }

    SchemaField uniqueField = rb.req.getSchema().getUniqueKeyField();

    // Now create a request for each shard to retrieve the stored fields
    for (Collection<ShardDoc> shardDocs : shardMap.values()) {
      ShardRequest sreq = new ShardRequest();
      sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;

      sreq.shards = new String[] {shardDocs.iterator().next().shard};

      sreq.params = new ModifiableSolrParams();

      // add original params
      sreq.params.add( rb.req.getParams());

      // no need for a sort, we already have order
      sreq.params.remove(CommonParams.SORT);
      sreq.params.remove(CursorMarkParams.CURSOR_MARK_PARAM);

      // we already have the field sort values
      sreq.params.remove(ResponseBuilder.FIELD_SORT_VALUES);

      if(!rb.rsp.getReturnFields().wantsField(uniqueField.getName())) {
        sreq.params.add(CommonParams.FL, uniqueField.getName());
      }
    
      ArrayList<String> ids = new ArrayList<>(shardDocs.size());
      for (ShardDoc shardDoc : shardDocs) {
        // TODO: depending on the type, we may need more tha a simple toString()?
        ids.add(shardDoc.id.toString());
      }
      sreq.params.add(ShardParams.IDS, StrUtils.join(ids, ','));

      rb.addRequest(this, sreq);
    }

  }


  protected void returnFields(ResponseBuilder rb, ShardRequest sreq) {
    // Keep in mind that this could also be a shard in a multi-tiered system.
    // TODO: if a multi-tiered system, it seems like some requests
    // could/should bypass middlemen (like retrieving stored fields)
    // TODO: merge fsv to if requested

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      boolean returnScores = (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0;

      String keyFieldName = rb.req.getSchema().getUniqueKeyField().getName();
      boolean removeKeyField = !rb.rsp.getReturnFields().wantsField(keyFieldName);
      if (rb.rsp.getReturnFields().getFieldRenames().get(keyFieldName) != null) {
        // if id was renamed we need to use the new name
        keyFieldName = rb.rsp.getReturnFields().getFieldRenames().get(keyFieldName);
      }

      for (ShardResponse srsp : sreq.responses) {
        if (srsp.getException() != null) {
          // Don't try to get the documents if there was an exception in the shard
          if(rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
            @SuppressWarnings("unchecked")
            NamedList<Object> shardInfo = (NamedList<Object>) rb.rsp.getValues().get(ShardParams.SHARDS_INFO);
            @SuppressWarnings("unchecked")
            SimpleOrderedMap<Object> nl = (SimpleOrderedMap<Object>) shardInfo.get(srsp.getShard());
            if (nl.get("error") == null) {
              // Add the error to the shards info section if it wasn't added before
              Throwable t = srsp.getException();
              if(t instanceof SolrServerException) {
                t = ((SolrServerException)t).getCause();
              }
              nl.add("error", t.toString() );
              StringWriter trace = new StringWriter();
              t.printStackTrace(new PrintWriter(trace));
              nl.add("trace", trace.toString() );
            }
          }
          
          continue;
        }
        {
          NamedList<?> responseHeader = (NamedList<?>)srsp.getSolrResponse().getResponse().get("responseHeader");
          if (Boolean.TRUE.equals(responseHeader.getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
            rb.rsp.getResponseHeader().asShallowMap()
               .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
          }
        }
        SolrDocumentList docs = (SolrDocumentList) srsp.getSolrResponse().getResponse().get("response");
        for (SolrDocument doc : docs) {
          Object id = doc.getFieldValue(keyFieldName);
          ShardDoc sdoc = rb.resultIds.get(id.toString());
          if (sdoc != null) {
            if (returnScores) {
              doc.setField("score", sdoc.score);
            } else {
              // Score might have been added (in createMainQuery) to shard-requests (and therefore in shard-response-docs)
              // Remove score if the outer request did not ask for it returned
              doc.remove("score");
            }
            if (removeKeyField) {
              doc.removeFields(keyFieldName);
            }
            rb.getResponseDocs().set(sdoc.positionInResponse, doc);
          }
        }
      }
    }
  }

  /////////////////////////////////////////////
  ///  SolrInfoBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "query";
  }

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }

  private boolean doProcessSearchByIds(ResponseBuilder rb) throws IOException {

    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;

    SolrParams params = req.getParams();

    String ids = params.get(ShardParams.IDS);
    if (ids == null) {
      return false;
    }

    SolrIndexSearcher searcher = req.getSearcher();
    IndexSchema schema = searcher.getSchema();
    SchemaField idField = schema.getUniqueKeyField();
    List<String> idArr = StrUtils.splitSmart(ids, ",", true);
    int[] luceneIds = new int[idArr.size()];
    int docs = 0;
    if (idField.getType().isPointField()) {
      for (int i=0; i<idArr.size(); i++) {
        int id = searcher.search(
            idField.getType().getFieldQuery(null, idField, idArr.get(i)), 1).scoreDocs[0].doc;
        if (id >= 0) {
          luceneIds[docs++] = id;
        }
      }
    } else {
      for (int i=0; i<idArr.size(); i++) {
        int id = searcher.getFirstMatch(
            new Term(idField.getName(), idField.getType().toInternal(idArr.get(i))));
        if (id >= 0)
          luceneIds[docs++] = id;
      }
    }

    DocListAndSet res = new DocListAndSet();
    res.docList = new DocSlice(0, docs, luceneIds, null, docs, 0);
    if (rb.isNeedDocSet()) {
      // TODO: create a cache for this!
      List<Query> queries = new ArrayList<>();
      queries.add(rb.getQuery());
      List<Query> filters = rb.getFilters();
      if (filters != null) queries.addAll(filters);
      res.docSet = searcher.getDocSet(queries);
    }
    rb.setResults(res);

    ResultContext ctx = new BasicResultContext(rb);
    rsp.addResponse(ctx);
    return true;
  }

  private void doProcessGroupedDistributedSearchFirstPhase(ResponseBuilder rb, QueryCommand cmd, QueryResult result) throws IOException {

    GroupingSpecification groupingSpec = rb.getGroupingSpec();
    assert null != groupingSpec : "GroupingSpecification is null";

    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;

    SolrIndexSearcher searcher = req.getSearcher();
    IndexSchema schema = searcher.getSchema();

    CommandHandler.Builder topsGroupsActionBuilder = new CommandHandler.Builder()
        .setQueryCommand(cmd)
        .setNeedDocSet(false) // Order matters here
        .setIncludeHitCount(true)
        .setSearcher(searcher);

    for (String field : groupingSpec.getFields()) {
      topsGroupsActionBuilder.addCommandField(new SearchGroupsFieldCommand.Builder()
          .setField(schema.getField(field))
          .setGroupSort(groupingSpec.getGroupSortSpec().getSort())
          .setTopNGroups(cmd.getOffset() + cmd.getLen())
          .setIncludeGroupCount(groupingSpec.isIncludeGroupCount())
          .build()
      );
    }

    CommandHandler commandHandler = topsGroupsActionBuilder.build();
    commandHandler.execute();
    SearchGroupsResultTransformer serializer = new SearchGroupsResultTransformer(searcher);

    rsp.add("firstPhase", commandHandler.processResult(result, serializer));
    rsp.add("totalHitCount", commandHandler.getTotalHitCount());
    rb.setResult(result);
  }

  private void doProcessGroupedDistributedSearchSecondPhase(ResponseBuilder rb, QueryCommand cmd, QueryResult result) throws IOException, SyntaxError {

    GroupingSpecification groupingSpec = rb.getGroupingSpec();
    assert null != groupingSpec : "GroupingSpecification is null";

    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;

    SolrParams params = req.getParams();

    SolrIndexSearcher searcher = req.getSearcher();
    IndexSchema schema = searcher.getSchema();

    boolean needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;

    CommandHandler.Builder secondPhaseBuilder = new CommandHandler.Builder()
        .setQueryCommand(cmd)
        .setTruncateGroups(groupingSpec.isTruncateGroups() && groupingSpec.getFields().length > 0)
        .setSearcher(searcher);

    SortSpec withinGroupSortSpec = groupingSpec.getWithinGroupSortSpec();
    int docsToCollect = Grouping.getMax(withinGroupSortSpec.getOffset(), withinGroupSortSpec.getCount(), searcher.maxDoc());
    docsToCollect = Math.max(docsToCollect, 1);

    for (String field : groupingSpec.getFields()) {
      SchemaField schemaField = schema.getField(field);
      String[] topGroupsParam = params.getParams(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + field);
      if (topGroupsParam == null) {
        topGroupsParam = new String[0];
      }

      List<SearchGroup<BytesRef>> topGroups = new ArrayList<>(topGroupsParam.length);
      for (String topGroup : topGroupsParam) {
        SearchGroup<BytesRef> searchGroup = new SearchGroup<>();
        if (!topGroup.equals(TopGroupsShardRequestFactory.GROUP_NULL_VALUE)) {
          BytesRefBuilder builder = new BytesRefBuilder();
          schemaField.getType().readableToIndexed(topGroup, builder);
          searchGroup.groupValue = builder.get();
        }
        topGroups.add(searchGroup);
      }

      secondPhaseBuilder.addCommandField(
          new TopGroupsFieldCommand.Builder()
              .setQuery(cmd.getQuery())
              .setField(schemaField)
              .setGroupSort(groupingSpec.getGroupSortSpec().getSort())
              .setSortWithinGroup(withinGroupSortSpec.getSort())
              .setFirstPhaseGroups(topGroups)
              .setMaxDocPerGroup(docsToCollect)
              .setNeedScores(needScores)
              .setNeedMaxScore(needScores)
              .build()
      );
    }

    SortSpec groupSortSpec = groupingSpec.getGroupSortSpec();
    // use start and rows for group.format=simple and group.main=true
    if (rb.getGroupingSpec().getResponseFormat() == Grouping.Format.simple || rb.getGroupingSpec().isMain()) {
      // would this ever be negative, as shardRequest sets rows to offset+limit
      int limit = groupSortSpec.getCount();
      docsToCollect = limit >= 0? limit + groupSortSpec.getOffset() : Integer.MAX_VALUE;
    }
    for (String query : groupingSpec.getQueries()) {
      secondPhaseBuilder.addCommandField(new Builder()
          .setDocsToCollect(docsToCollect)
          .setSort(groupSortSpec.getSort())
          .setQuery(query, rb.req)
          .setDocSet(searcher)
          .setMainQuery(rb.getQuery())
          .setNeedScores(needScores)
          .build()
      );
    }

    CommandHandler commandHandler = secondPhaseBuilder.build();
    commandHandler.execute();
    TopGroupsResultTransformer serializer = new TopGroupsResultTransformer(rb);
    rsp.add("secondPhase", commandHandler.processResult(result, serializer));
    rb.setResult(result);
  }

  private void doProcessGroupedSearch(ResponseBuilder rb, QueryCommand cmd, QueryResult result) throws IOException, SyntaxError {

    GroupingSpecification groupingSpec = rb.getGroupingSpec();
    assert null != groupingSpec : "GroupingSpecification is null";

    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;

    SolrParams params = req.getParams();

    SolrIndexSearcher searcher = req.getSearcher();

    int maxDocsPercentageToCache = params.getInt(GroupParams.GROUP_CACHE_PERCENTAGE, 0);
    boolean cacheSecondPassSearch = maxDocsPercentageToCache >= 1 && maxDocsPercentageToCache <= 100;
    Grouping.TotalCount defaultTotalCount = groupingSpec.isIncludeGroupCount() ?
        Grouping.TotalCount.grouped : Grouping.TotalCount.ungrouped;
    int limitDefault = cmd.getLen(); // this is normally from "rows"
    Grouping grouping =
        new Grouping(searcher, result, cmd, cacheSecondPassSearch, maxDocsPercentageToCache, groupingSpec.isMain());

    SortSpec withinGroupSortSpec = groupingSpec.getWithinGroupSortSpec();
    grouping.setGroupSort(groupingSpec.getGroupSortSpec().getSort())
        .setWithinGroupSort(withinGroupSortSpec.getSort())
        .setDefaultFormat(groupingSpec.getResponseFormat())
        .setLimitDefault(limitDefault)
        .setDefaultTotalCount(defaultTotalCount)
        .setDocsPerGroupDefault(withinGroupSortSpec.getCount())
        .setGroupOffsetDefault(withinGroupSortSpec.getOffset())
        .setGetGroupedDocSet(groupingSpec.isTruncateGroups());

    if (groupingSpec.getFields() != null) {
      for (String field : groupingSpec.getFields()) {
        grouping.addFieldCommand(field, rb.req);
      }
    }

    if (groupingSpec.getFunctions() != null) {
      for (String groupByStr : groupingSpec.getFunctions()) {
        grouping.addFunctionCommand(groupByStr, rb.req);
      }
    }

    if (groupingSpec.getQueries() != null) {
      for (String groupByStr : groupingSpec.getQueries()) {
        grouping.addQueryCommand(groupByStr, rb.req);
      }
    }

    if( rb.isNeedDocList() || rb.isDebug() ){
      // we need a single list of the returned docs
      cmd.setFlags(SolrIndexSearcher.GET_DOCLIST);
    }

    grouping.execute();
    if (grouping.isSignalCacheWarning()) {
      rsp.add(
          "cacheWarning",
          String.format(Locale.ROOT, "Cache limit of %d percent relative to maxdoc has exceeded. Please increase cache size or disable caching.", maxDocsPercentageToCache)
      );
    }
    rb.setResult(result);

    if (grouping.mainResult != null) {
      ResultContext ctx = new BasicResultContext(rb, grouping.mainResult);
      rsp.addResponse(ctx);
      rsp.getToLog().add("hits", grouping.mainResult.matches());
    } else if (!grouping.getCommands().isEmpty()) { // Can never be empty since grouping.execute() checks for this.
      rsp.add("grouped", result.groupedResults);
      rsp.getToLog().add("hits", grouping.getCommands().get(0).getMatches());
    }
  }

  private void doProcessUngroupedSearch(ResponseBuilder rb, QueryCommand cmd, QueryResult result) throws IOException {

    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;

    SolrIndexSearcher searcher = req.getSearcher();

    searcher.search(result, cmd);
    rb.setResult(result);

    ResultContext ctx = new BasicResultContext(rb);
    rsp.addResponse(ctx);
    rsp.getToLog().add("hits", rb.getResults()==null || rb.getResults().docList==null ? 0 : rb.getResults().docList.matches());

    if ( ! rb.req.getParams().getBool(ShardParams.IS_SHARD,false) ) {
      if (null != rb.getNextCursorMark()) {
        rb.rsp.add(CursorMarkParams.CURSOR_MARK_NEXT,
                   rb.getNextCursorMark().getSerializedTotem());
      }
    }

    if(rb.mergeFieldHandler != null) {
      rb.mergeFieldHandler.handleMergeFields(rb, searcher);
    } else {
      doFieldSortValues(rb, searcher);
    }

    doPrefetch(rb);
  }

  /**
   * Fake scorer for a single document
   *
   * TODO: when SOLR-5595 is fixed, this wont be needed, as we dont need to recompute sort values here from the comparator
   */
  protected static class ScoreAndDoc extends Scorable {
    final int docid;
    final float score;

    ScoreAndDoc(int docid, float score) {
      this.docid = docid;
      this.score = score;
    }

    @Override
    public int docID() {
      return docid;
    }

    @Override
    public float score() throws IOException {
      return score;
    }
  }
}
