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
package org.apache.solr.search.grouping.endresulttransformer;

import java.util.Map;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;

/**
 * Implementation of {@link EndResultTransformer} that transforms the grouped result into a single flat list.
 */
public class SimpleEndResultTransformer implements EndResultTransformer {

  @Override
  public void transform(Map<String, ?> result, ResponseBuilder rb, SolrDocumentSource solrDocumentSource) {
    NamedList<Object> commands = new SimpleOrderedMap<>();
    for (Map.Entry<String, ?> entry : result.entrySet()) {
      Object value = entry.getValue();
      if (TopGroups.class.isInstance(value)) {
        @SuppressWarnings("unchecked")
        TopGroups<BytesRef> topGroups = (TopGroups<BytesRef>) value;
        NamedList<Object> command = new SimpleOrderedMap<>();
        command.add("matches", rb.totalHitCount);
        if (topGroups.totalGroupCount != null) {
          command.add("ngroups", topGroups.totalGroupCount);
        }
        SolrDocumentList docList = new SolrDocumentList();
        docList.setStart(rb.getGroupingSpec().getGroupSortSpec().getOffset());
        docList.setNumFound(topGroups.totalHitCount);

        float maxScore = Float.NEGATIVE_INFINITY;
        for (GroupDocs<BytesRef> group : topGroups.groups) {
          for (ScoreDoc scoreDoc : group.scoreDocs) {
            if (maxScore < scoreDoc.score) {
              maxScore = scoreDoc.score;
            }
            SolrDocument solrDocument = solrDocumentSource.retrieve(scoreDoc);
            if (solrDocument != null) {
              docList.add(solrDocument);
            }
          }
        }
        if (maxScore != Float.NEGATIVE_INFINITY) {
          docList.setMaxScore(maxScore);
        }
        command.add("doclist", docList);
        commands.add(entry.getKey(), command);
      } else if (value instanceof QueryCommandResult) {
        QueryCommandResult queryCommandResult = (QueryCommandResult) value;
        NamedList<Object> command = new SimpleOrderedMap<>();
        command.add("matches", queryCommandResult.getMatches());

        TopDocs topDocs = queryCommandResult.getTopDocs();
        SolrDocumentList docList = new SolrDocumentList();
        docList.setStart(rb.getGroupingSpec().getGroupSortSpec().getOffset());
        docList.setNumFound(topDocs.totalHits.value);

        if (!Float.isNaN(queryCommandResult.getMaxScore())) {
          docList.setMaxScore(queryCommandResult.getMaxScore());
        }
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
          SolrDocument solrDocument = solrDocumentSource.retrieve(scoreDoc);
          if (solrDocument != null) {
            docList.add(solrDocument);
          }
        }
        command.add("doclist", docList);
        commands.add(entry.getKey(), command);
      }
    }

    rb.rsp.add("grouped", commands);
  }
}
