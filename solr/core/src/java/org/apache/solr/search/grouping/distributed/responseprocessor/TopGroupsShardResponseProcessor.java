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
package org.apache.solr.search.grouping.distributed.responseprocessor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;
import org.apache.solr.search.grouping.distributed.shardresultserializer.TopGroupsResultTransformer;

/**
 * Concrete implementation for merging {@link TopGroups} instances from shard responses.
 */
public class TopGroupsShardResponseProcessor implements ShardResponseProcessor {

  @Override
  @SuppressWarnings("unchecked")
  public void process(ResponseBuilder rb, ShardRequest shardRequest) {
    Sort groupSort = rb.getGroupingSpec().getGroupSortSpec().getSort();
    String[] fields = rb.getGroupingSpec().getFields();
    String[] queries = rb.getGroupingSpec().getQueries();
    SortSpec withinGroupSortSpec = rb.getGroupingSpec().getWithinGroupSortSpec();
    Sort withinGroupSort = withinGroupSortSpec.getSort();
    assert withinGroupSort != null;

    boolean simpleOrMain = rb.getGroupingSpec().getResponseFormat() == Grouping.Format.simple ||
        rb.getGroupingSpec().isMain();

    // If group.format=simple group.offset doesn't make sense
    int groupOffsetDefault;
    if (simpleOrMain) {
      groupOffsetDefault = 0;
    } else {
      groupOffsetDefault = withinGroupSortSpec.getOffset();
    }
    int docsPerGroupDefault = withinGroupSortSpec.getCount();

    Map<String, List<TopGroups<BytesRef>>> commandTopGroups = new HashMap<>();
    for (String field : fields) {
      commandTopGroups.put(field, new ArrayList<>());
    }

    Map<String, List<QueryCommandResult>> commandTopDocs = new HashMap<>();
    for (String query : queries) {
      commandTopDocs.put(query, new ArrayList<>());
    }

    TopGroupsResultTransformer serializer = new TopGroupsResultTransformer(rb);

    NamedList<Object> shardInfo = null;
    if (rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
      shardInfo = new SimpleOrderedMap<>();
      rb.rsp.getValues().add(ShardParams.SHARDS_INFO, shardInfo);
    }

    for (ShardResponse srsp : shardRequest.responses) {
      SimpleOrderedMap<Object> individualShardInfo = null;
      if (shardInfo != null) {
        individualShardInfo = new SimpleOrderedMap<>();

        if (srsp.getException() != null) {
          Throwable t = srsp.getException();
          if (t instanceof SolrServerException && ((SolrServerException) t).getCause() != null) {
            t = ((SolrServerException) t).getCause();
          }
          individualShardInfo.add("error", t.toString());
          StringWriter trace = new StringWriter();
          t.printStackTrace(new PrintWriter(trace));
          individualShardInfo.add("trace", trace.toString());
        } else {
          // summary for successful shard response is added down below
        }
        if (srsp.getSolrResponse() != null) {
          individualShardInfo.add("time", srsp.getSolrResponse().getElapsedTime());
        }
        if (srsp.getShardAddress() != null) {
          individualShardInfo.add("shardAddress", srsp.getShardAddress());
        }
        shardInfo.add(srsp.getShard(), individualShardInfo);
      }
      if (ShardParams.getShardsTolerantAsBool(rb.req.getParams()) && srsp.getException() != null) {
        rb.rsp.getResponseHeader().asShallowMap().put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
        continue; // continue if there was an error and we're tolerant.  
      }
      @SuppressWarnings({"rawtypes"})
      NamedList<NamedList> secondPhaseResult = (NamedList<NamedList>) srsp.getSolrResponse().getResponse().get("secondPhase");
      if(secondPhaseResult == null)
        continue;
      Map<String, ?> result = serializer.transformToNative(secondPhaseResult, groupSort, withinGroupSort, srsp.getShard());
      int numFound = 0;
      float maxScore = Float.NaN;
      for (Map.Entry<String, List<TopGroups<BytesRef>>> entry : commandTopGroups.entrySet()) {
        TopGroups<BytesRef> topGroups = (TopGroups<BytesRef>) result.get(entry.getKey());
        if (topGroups == null) {
          continue;
        }
        if (individualShardInfo != null) { // keep track of this when shards.info=true
          numFound += topGroups.totalHitCount;
          if (Float.isNaN(maxScore) || topGroups.maxScore > maxScore) maxScore = topGroups.maxScore;
        }
        entry.getValue().add(topGroups);
      }
      for (String query : queries) {
        QueryCommandResult queryCommandResult = (QueryCommandResult) result.get(query);
        if (individualShardInfo != null) { // keep track of this when shards.info=true
          numFound += queryCommandResult.getMatches();
          float thisMax = queryCommandResult.getMaxScore();
          if (Float.isNaN(maxScore) || thisMax > maxScore) maxScore = thisMax;
        }
        commandTopDocs.get(query).add(queryCommandResult);
      }
      if (individualShardInfo != null) { // when shards.info=true
        individualShardInfo.add("numFound", numFound);
        individualShardInfo.add("maxScore", maxScore);
      }
    }
    for (Map.Entry<String, List<TopGroups<BytesRef>>> entry : commandTopGroups.entrySet()) {
      List<TopGroups<BytesRef>> topGroups = entry.getValue();
      if (topGroups.isEmpty()) {
        continue;
      }

      @SuppressWarnings({"rawtypes"})
      TopGroups<BytesRef>[] topGroupsArr = new TopGroups[topGroups.size()];
      int docsPerGroup = docsPerGroupDefault;
      if (docsPerGroup < 0) {
        docsPerGroup = 0;
        for (@SuppressWarnings({"rawtypes"})TopGroups subTopGroups : topGroups) {
          docsPerGroup += subTopGroups.totalGroupedHitCount;
        }
      }
      rb.mergedTopGroups.put(entry.getKey(), TopGroups.merge(topGroups.toArray(topGroupsArr), groupSort, withinGroupSort, groupOffsetDefault, docsPerGroup, TopGroups.ScoreMergeMode.None));
    }

    // calculate topN and start for group.query
    int topN = docsPerGroupDefault >= 0? docsPerGroupDefault: Integer.MAX_VALUE;
    int start = groupOffsetDefault;
    if (simpleOrMain) {
      // use start and rows here
      start = rb.getGroupingSpec().getGroupSortSpec().getOffset();
      int limit = rb.getGroupingSpec().getGroupSortSpec().getCount();
      topN = limit >= 0? limit: Integer.MAX_VALUE;
    }

    for (Map.Entry<String, List<QueryCommandResult>> entry : commandTopDocs.entrySet()) {
      List<QueryCommandResult> queryCommandResults = entry.getValue();
      List<TopDocs> topDocs = new ArrayList<>(queryCommandResults.size());
      int mergedMatches = 0;
      float maxScore = Float.NaN;
      for (QueryCommandResult queryCommandResult : queryCommandResults) {
        TopDocs thisTopDocs = queryCommandResult.getTopDocs();
        topDocs.add(thisTopDocs);
        mergedMatches += queryCommandResult.getMatches();
        if (thisTopDocs.scoreDocs.length > 0) {
          float thisMaxScore = queryCommandResult.getMaxScore();
          if (Float.isNaN(maxScore) || thisMaxScore > maxScore) {
            maxScore = thisMaxScore;
          }
        }
      }

      final TopDocs mergedTopDocs;
      if (withinGroupSort.equals(Sort.RELEVANCE)) {
        mergedTopDocs = TopDocs.merge(
            start, topN, topDocs.toArray(new TopDocs[topDocs.size()]), true);
      } else {
        mergedTopDocs = TopDocs.merge(
            withinGroupSort, start, topN, topDocs.toArray(new TopFieldDocs[topDocs.size()]), true);
      }
      rb.mergedQueryCommandResults.put(entry.getKey(), new QueryCommandResult(mergedTopDocs, mergedMatches, maxScore));
    }
    fillResultIds(rb);
  }

  /**
   * Fill the {@link ResponseBuilder}'s <code>resultIds</code> field.
   * @param rb the response builder
   */
  static void fillResultIds(ResponseBuilder rb) {
    Map<Object, ShardDoc> resultIds = new HashMap<>();
    int i = 0;
    for (TopGroups<BytesRef> topGroups : rb.mergedTopGroups.values()) {
      for (GroupDocs<BytesRef> group : topGroups.groups) {
        for (ScoreDoc scoreDoc : group.scoreDocs) {
          ShardDoc solrDoc = (ShardDoc) scoreDoc;
          // Include the first if there are duplicate IDs
          if ( ! resultIds.containsKey(solrDoc.id)) {
            solrDoc.positionInResponse = i++;
            resultIds.put(solrDoc.id, solrDoc);
          }
        }
      }
    }
    for (QueryCommandResult queryCommandResult : rb.mergedQueryCommandResults.values()) {
      for (ScoreDoc scoreDoc : queryCommandResult.getTopDocs().scoreDocs) {
        ShardDoc solrDoc = (ShardDoc) scoreDoc;
        solrDoc.positionInResponse = i++;
        resultIds.put(solrDoc.id, solrDoc);
      }
    }

    rb.resultIds = resultIds;
  }
}
