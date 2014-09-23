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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.CursorMark;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.grouping.CommandHandler;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.search.grouping.distributed.ShardRequestFactory;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.command.QueryCommand;
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
import org.apache.solr.util.SolrPluginUtils;
import java.util.Collections;

/**
 * TODO!
 * 
 *
 * @since solr 1.3
 */
public class QueryComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "query";

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
        q = new BooleanQuery();
      }

      rb.setQuery( q );

      String rankQueryString = rb.req.getParams().get(CommonParams.RQ);
      if(rankQueryString != null) {
        QParser rqparser = QParser.getParser(rankQueryString, defType, req);
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

      rb.setSortSpec( parser.getSort(true) );
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
        filters = filters == null ? new ArrayList<Query>(fqs.length) : new ArrayList<>(filters);
        for (String fq : fqs) {
          if (fq != null && fq.trim().length()!=0) {
            QParser fqp = QParser.getParser(fq, null, req);
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
    }
  }

  private void prepareGrouping(ResponseBuilder rb) throws IOException {

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    if (null != rb.getCursorMark()) {
      // It's hard to imagine, conceptually, what it would mean to combine
      // grouping with a cursor - so for now we just don't allow the combination at all
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not use Grouping with " +
                              CursorMarkParams.CURSOR_MARK_PARAM);
    }

    SolrIndexSearcher.QueryCommand cmd = rb.getQueryCommand();
    SolrIndexSearcher searcher = rb.req.getSearcher();
    GroupingSpecification groupingSpec = new GroupingSpecification();
    rb.setGroupingSpec(groupingSpec);

    //TODO: move weighting of sort
    Sort groupSort = searcher.weightSort(cmd.getSort());
    if (groupSort == null) {
      groupSort = Sort.RELEVANCE;
    }

    // groupSort defaults to sort
    String groupSortStr = params.get(GroupParams.GROUP_SORT);
    //TODO: move weighting of sort
    Sort sortWithinGroup = groupSortStr == null ?  groupSort : searcher.weightSort(QueryParsing.parseSortSpec(groupSortStr, req).getSort());
    if (sortWithinGroup == null) {
      sortWithinGroup = Sort.RELEVANCE;
    }

    groupingSpec.setSortWithinGroup(sortWithinGroup);
    groupingSpec.setGroupSort(groupSort);

    String formatStr = params.get(GroupParams.GROUP_FORMAT, Grouping.Format.grouped.name());
    Grouping.Format responseFormat;
    try {
       responseFormat = Grouping.Format.valueOf(formatStr);
    } catch (IllegalArgumentException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "Illegal %s parameter", GroupParams.GROUP_FORMAT));
    }
    groupingSpec.setResponseFormat(responseFormat);

    groupingSpec.setFields(params.getParams(GroupParams.GROUP_FIELD));
    groupingSpec.setQueries(params.getParams(GroupParams.GROUP_QUERY));
    groupingSpec.setFunctions(params.getParams(GroupParams.GROUP_FUNC));
    groupingSpec.setGroupOffset(params.getInt(GroupParams.GROUP_OFFSET, 0));
    groupingSpec.setGroupLimit(params.getInt(GroupParams.GROUP_LIMIT, 1));
    groupingSpec.setOffset(rb.getSortSpec().getOffset());
    groupingSpec.setLimit(rb.getSortSpec().getCount());
    groupingSpec.setIncludeGroupCount(params.getBool(GroupParams.GROUP_TOTAL_COUNT, false));
    groupingSpec.setMain(params.getBool(GroupParams.GROUP_MAIN, false));
    groupingSpec.setNeedScore((cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0);
    groupingSpec.setTruncateGroups(params.getBool(GroupParams.GROUP_TRUNCATE, false));
  }



  /**
   * Actually run the query
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();
    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }
    SolrIndexSearcher searcher = req.getSearcher();

    if (rb.getQueryCommand().getOffset() < 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'start' parameter cannot be negative");
    }

    // -1 as flag if not set.
    long timeAllowed = (long)params.getInt( CommonParams.TIME_ALLOWED, -1 );
    if (null != rb.getCursorMark() && 0 < timeAllowed) {
      // fundementally incompatible
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not search using both " +
                              CursorMarkParams.CURSOR_MARK_PARAM + " and " + CommonParams.TIME_ALLOWED);
    }

    // Optional: This could also be implemented by the top-level searcher sending
    // a filter that lists the ids... that would be transparent to
    // the request handler, but would be more expensive (and would preserve score
    // too if desired).
    String ids = params.get(ShardParams.IDS);
    if (ids != null) {
      SchemaField idField = searcher.getSchema().getUniqueKeyField();
      List<String> idArr = StrUtils.splitSmart(ids, ",", true);
      int[] luceneIds = new int[idArr.size()];
      int docs = 0;
      for (int i=0; i<idArr.size(); i++) {
        int id = req.getSearcher().getFirstMatch(
                new Term(idField.getName(), idField.getType().toInternal(idArr.get(i))));
        if (id >= 0)
          luceneIds[docs++] = id;
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

      ResultContext ctx = new ResultContext();
      ctx.docs = rb.getResults().docList;
      ctx.query = null; // anything?
      rsp.add("response", ctx);
      return;
    }

    SolrIndexSearcher.QueryCommand cmd = rb.getQueryCommand();
    cmd.setTimeAllowed(timeAllowed);
    SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();

    //
    // grouping / field collapsing
    //
    GroupingSpecification groupingSpec = rb.getGroupingSpec();
    if (groupingSpec != null) {
      try {
        boolean needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;
        if (params.getBool(GroupParams.GROUP_DISTRIBUTED_FIRST, false)) {
          CommandHandler.Builder topsGroupsActionBuilder = new CommandHandler.Builder()
              .setQueryCommand(cmd)
              .setNeedDocSet(false) // Order matters here
              .setIncludeHitCount(true)
              .setSearcher(searcher);

          for (String field : groupingSpec.getFields()) {
            topsGroupsActionBuilder.addCommandField(new SearchGroupsFieldCommand.Builder()
                .setField(searcher.getSchema().getField(field))
                .setGroupSort(groupingSpec.getGroupSort())
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
          return;
        } else if (params.getBool(GroupParams.GROUP_DISTRIBUTED_SECOND, false)) {
          CommandHandler.Builder secondPhaseBuilder = new CommandHandler.Builder()
              .setQueryCommand(cmd)
              .setTruncateGroups(groupingSpec.isTruncateGroups() && groupingSpec.getFields().length > 0)
              .setSearcher(searcher);

          for (String field : groupingSpec.getFields()) {
            String[] topGroupsParam = params.getParams(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + field);
            if (topGroupsParam == null) {
              topGroupsParam = new String[0];
            }

            List<SearchGroup<BytesRef>> topGroups = new ArrayList<>(topGroupsParam.length);
            for (String topGroup : topGroupsParam) {
              SearchGroup<BytesRef> searchGroup = new SearchGroup<>();
              if (!topGroup.equals(TopGroupsShardRequestFactory.GROUP_NULL_VALUE)) {
                searchGroup.groupValue = new BytesRef(searcher.getSchema().getField(field).getType().readableToIndexed(topGroup));
              }
              topGroups.add(searchGroup);
            }

            secondPhaseBuilder.addCommandField(
                new TopGroupsFieldCommand.Builder()
                    .setField(searcher.getSchema().getField(field))
                    .setGroupSort(groupingSpec.getGroupSort())
                    .setSortWithinGroup(groupingSpec.getSortWithinGroup())
                    .setFirstPhaseGroups(topGroups)
                    .setMaxDocPerGroup(groupingSpec.getGroupOffset() + groupingSpec.getGroupLimit())
                    .setNeedScores(needScores)
                    .setNeedMaxScore(needScores)
                    .build()
            );
          }

          for (String query : groupingSpec.getQueries()) {
            secondPhaseBuilder.addCommandField(new QueryCommand.Builder()
                .setDocsToCollect(groupingSpec.getOffset() + groupingSpec.getLimit())
                .setSort(groupingSpec.getGroupSort())
                .setQuery(query, rb.req)
                .setDocSet(searcher)
                .build()
            );
          }

          CommandHandler commandHandler = secondPhaseBuilder.build();
          commandHandler.execute();
          TopGroupsResultTransformer serializer = new TopGroupsResultTransformer(rb);
          rsp.add("secondPhase", commandHandler.processResult(result, serializer));
          rb.setResult(result);
          return;
        }

        int maxDocsPercentageToCache = params.getInt(GroupParams.GROUP_CACHE_PERCENTAGE, 0);
        boolean cacheSecondPassSearch = maxDocsPercentageToCache >= 1 && maxDocsPercentageToCache <= 100;
        Grouping.TotalCount defaultTotalCount = groupingSpec.isIncludeGroupCount() ?
            Grouping.TotalCount.grouped : Grouping.TotalCount.ungrouped;
        int limitDefault = cmd.getLen(); // this is normally from "rows"
        Grouping grouping =
            new Grouping(searcher, result, cmd, cacheSecondPassSearch, maxDocsPercentageToCache, groupingSpec.isMain());
        grouping.setSort(groupingSpec.getGroupSort())
            .setGroupSort(groupingSpec.getSortWithinGroup())
            .setDefaultFormat(groupingSpec.getResponseFormat())
            .setLimitDefault(limitDefault)
            .setDefaultTotalCount(defaultTotalCount)
            .setDocsPerGroupDefault(groupingSpec.getGroupLimit())
            .setGroupOffsetDefault(groupingSpec.getGroupOffset())
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

        if (rb.doHighlights || rb.isDebug() || params.getBool(MoreLikeThisParams.MLT, false)) {
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
          ResultContext ctx = new ResultContext();
          ctx.docs = grouping.mainResult;
          ctx.query = null; // TODO? add the query?
          rsp.add("response", ctx);
          rsp.getToLog().add("hits", grouping.mainResult.matches());
        } else if (!grouping.getCommands().isEmpty()) { // Can never be empty since grouping.execute() checks for this.
          rsp.add("grouped", result.groupedResults);
          rsp.getToLog().add("hits", grouping.getCommands().get(0).getMatches());
        }
        return;
      } catch (SyntaxError e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    // normal search result
    searcher.search(result,cmd);
    rb.setResult( result );

    ResultContext ctx = new ResultContext();
    ctx.docs = rb.getResults().docList;
    ctx.query = rb.getQuery();
    rsp.add("response", ctx);
    rsp.getToLog().add("hits", rb.getResults().docList.matches());

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
      NamedList<Object[]> sortVals = new NamedList<>(); // order is important for the sort fields
      IndexReaderContext topReaderContext = searcher.getTopReaderContext();
      List<LeafReaderContext> leaves = topReaderContext.leaves();
      LeafReaderContext currentLeaf = null;
      if (leaves.size()==1) {
        // if there is a single segment, use that subReader and avoid looking up each time
        currentLeaf = leaves.get(0);
        leaves=null;
      }

      DocList docList = rb.getResults().docList;

      // sort ids from lowest to highest so we can access them in order
      int nDocs = docList.size();
      final long[] sortedIds = new long[nDocs];
      final float[] scores = new float[nDocs]; // doc scores, parallel to sortedIds
      DocList docs = rb.getResults().docList;
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

        FieldComparator comparator = null;
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
              // we switched segments.  invalidate comparator.
              comparator = null;
            }
          }

          if (comparator == null) {
            comparator = sortField.getComparator(1,0);
            comparator = comparator.setNextReader(currentLeaf);
          }

          doc -= currentLeaf.docBase;  // adjust for what segment this is in
          comparator.setScorer(new FakeScorer(doc, score));
          comparator.copy(0, doc);
          Object val = comparator.value(0);
          if (null != ft) val = ft.marshalSortValue(val);
          vals[position] = val;
        }

        sortVals.add(sortField.getField(), vals);
      }

      rsp.add("sort_values", sortVals);
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

  private int groupedDistributedProcess(ResponseBuilder rb) {
    int nextStage = ResponseBuilder.STAGE_DONE;
    ShardRequestFactory shardRequestFactory = null;

    if (rb.stage < ResponseBuilder.STAGE_PARSE_QUERY) {
      nextStage = ResponseBuilder.STAGE_PARSE_QUERY;
    } else if (rb.stage == ResponseBuilder.STAGE_PARSE_QUERY) {
      createDistributedIdf(rb);
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

  private int regularDistributedProcess(ResponseBuilder rb) {
    if (rb.stage < ResponseBuilder.STAGE_PARSE_QUERY)
      return ResponseBuilder.STAGE_PARSE_QUERY;
    if (rb.stage == ResponseBuilder.STAGE_PARSE_QUERY) {
      createDistributedIdf(rb);
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

  private void handleGroupedResponses(ResponseBuilder rb, ShardRequest sreq) {
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

  private void handleRegularResponses(ResponseBuilder rb, ShardRequest sreq) {
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      mergeIds(rb, sreq);
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

  private static final EndResultTransformer MAIN_END_RESULT_TRANSFORMER = new MainEndResultTransformer();
  private static final EndResultTransformer SIMPLE_END_RESULT_TRANSFORMER = new SimpleEndResultTransformer();

  @SuppressWarnings("unchecked")
  private void groupedFinishStage(final ResponseBuilder rb) {
    // To have same response as non-distributed request.
    GroupingSpecification groupSpec = rb.getGroupingSpec();
    if (rb.mergedTopGroups.isEmpty()) {
      for (String field : groupSpec.getFields()) {
        rb.mergedTopGroups.put(field, new TopGroups(null, null, 0, 0, new GroupDocs[]{}, Float.NaN));
      }
      rb.resultIds = new HashMap<>();
    }

    EndResultTransformer.SolrDocumentSource solrDocumentSource = new EndResultTransformer.SolrDocumentSource() {

      @Override
      public SolrDocument retrieve(ScoreDoc doc) {
        ShardDoc solrDoc = (ShardDoc) doc;
        return rb.retrievedDocuments.get(solrDoc.id);
      }

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

  private void regularFinishStage(ResponseBuilder rb) {
    // We may not have been able to retrieve all the docs due to an
    // index change.  Remove any null documents.
    for (Iterator<SolrDocument> iter = rb._responseDocs.iterator(); iter.hasNext();) {
      if (iter.next() == null) {
        iter.remove();
        rb._responseDocs.setNumFound(rb._responseDocs.getNumFound()-1);
      }
    }

    rb.rsp.add("response", rb._responseDocs);
    if (null != rb.getNextCursorMark()) {
      rb.rsp.add(CursorMarkParams.CURSOR_MARK_NEXT,
                 rb.getNextCursorMark().getSerializedTotem());
    }
  }

  private void createDistributedIdf(ResponseBuilder rb) {
    // TODO
  }

  private void createMainQuery(ResponseBuilder rb) {
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_TOP_IDS;

    String keyFieldName = rb.req.getSchema().getUniqueKeyField().getName();

    // one-pass algorithm if only id and score fields are requested, but not if fl=score since that's the same as fl=*,score
    ReturnFields fields = rb.rsp.getReturnFields();

    // distrib.singlePass=true forces a one-pass query regardless of requested fields
    boolean distribSinglePass = rb.req.getParams().getBool(ShardParams.DISTRIB_SINGLE_PASS, false);

    if(distribSinglePass || (fields != null && fields.wantsField(keyFieldName)
        && fields.getRequestedFieldNames() != null && Arrays.asList(keyFieldName, "score").containsAll(fields.getRequestedFieldNames()))) {
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
      sreq.params.set(CommonParams.ROWS, rb.getSortSpec().getOffset() + rb.getSortSpec().getCount());
    }

    sreq.params.set(ResponseBuilder.FIELD_SORT_VALUES,"true");

    boolean shardQueryIncludeScore = (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0 || rb.getSortSpec().includesScore();
    if (distribSinglePass) {
      String fl = rb.req.getParams().get(CommonParams.FL);
      if (fl == null) {
        if (fields.getRequestedFieldNames() == null && fields.wantsAllFields()) {
          fl = "*";
        } else  {
          fl = "";
          for (String s : fields.getRequestedFieldNames()) {
            fl += s + ",";
          }
        }
      }
      if (!fields.wantsField(keyFieldName))  {
        // the user has not requested the unique key but
        // we still need to add it otherwise mergeIds can't work
        if (fl.endsWith(",")) {
          fl += keyFieldName;
        } else  {
          fl += "," + keyFieldName;
        }
      }
      sreq.params.set(CommonParams.FL, updateFl(fl, shardQueryIncludeScore));
    } else {
      // in this first phase, request only the unique key field and any fields needed for merging.
      if (shardQueryIncludeScore) {
        sreq.params.set(CommonParams.FL, keyFieldName + ",score");
      } else {
        sreq.params.set(CommonParams.FL, keyFieldName);
      }
    }

    rb.addRequest(this, sreq);
  }


  String updateFl(String originalFields, boolean includeScoreIfMissing) {
    if (includeScoreIfMissing && !scorePattern.matcher(originalFields).find()) {
      return originalFields + ",score";
    } else {
      return originalFields;
    }
  }

  private static final Pattern scorePattern = Pattern.compile("\\bscore\\b");


  private void mergeIds(ResponseBuilder rb, ShardRequest sreq) {
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
      ShardFieldSortedHitQueue queue;
      queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount(), rb.req.getSearcher());

      NamedList<Object> shardInfo = null;
      if(rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
        shardInfo = new SimpleOrderedMap<>();
        rb.rsp.getValues().add(ShardParams.SHARDS_INFO,shardInfo);
      }
      
      long numFound = 0;
      Float maxScore=null;
      boolean partialResults = false;
      for (ShardResponse srsp : sreq.responses) {
        SolrDocumentList docs = null;

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
          partialResults = true;
          continue;
        }

        if (docs == null) { // could have been initialized in the shards info block above
          docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
        }
        
        NamedList<?> responseHeader = (NamedList<?>)srsp.getSolrResponse().getResponse().get("responseHeader");
        if (responseHeader != null && Boolean.TRUE.equals(responseHeader.get("partialResults"))) {
          partialResults = true;
        }
        
        // calculate global maxScore and numDocsFound
        if (docs.getMaxScore() != null) {
          maxScore = maxScore==null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
        }
        numFound += docs.getNumFound();

        NamedList sortFieldValues = (NamedList)(srsp.getSolrResponse().getResponse().get("sort_values"));
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
      rb._responseDocs = responseDocs;

      populateNextCursorMarkFromMergedShards(rb);

      if (partialResults) {
        if(rb.rsp.getResponseHeader().get("partialResults") == null) {
          rb.rsp.getResponseHeader().add("partialResults", Boolean.TRUE);
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
  private void populateNextCursorMarkFromMergedShards(ResponseBuilder rb) {

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

  private NamedList unmarshalSortValues(SortSpec sortSpec, 
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

  private void createRetrieveDocs(ResponseBuilder rb) {

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


  private void returnFields(ResponseBuilder rb, ShardRequest sreq) {
    // Keep in mind that this could also be a shard in a multi-tiered system.
    // TODO: if a multi-tiered system, it seems like some requests
    // could/should bypass middlemen (like retrieving stored fields)
    // TODO: merge fsv to if requested

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      boolean returnScores = (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0;

      String keyFieldName = rb.req.getSchema().getUniqueKeyField().getName();
      boolean removeKeyField = !rb.rsp.getReturnFields().wantsField(keyFieldName);

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
        SolrDocumentList docs = (SolrDocumentList) srsp.getSolrResponse().getResponse().get("response");

        for (SolrDocument doc : docs) {
          Object id = doc.getFieldValue(keyFieldName);
          ShardDoc sdoc = rb.resultIds.get(id.toString());
          if (sdoc != null) {
            if (returnScores) {
              doc.setField("score", sdoc.score);
            }
            if (removeKeyField) {
              doc.removeFields(keyFieldName);
            }
            rb._responseDocs.set(sdoc.positionInResponse, doc);
          }
        }
      }
    }
  }

  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "query";
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  /**
   * Fake scorer for a single document
   *
   * TODO: when SOLR-5595 is fixed, this wont be needed, as we dont need to recompute sort values here from the comparator
   */
  private static class FakeScorer extends Scorer {
    final int docid;
    final float score;

    FakeScorer(int docid, float score) {
      super(null);
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

    @Override
    public int freq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return 1;
    }

    @Override
    public Weight getWeight() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      throw new UnsupportedOperationException();
    }
  }
}
