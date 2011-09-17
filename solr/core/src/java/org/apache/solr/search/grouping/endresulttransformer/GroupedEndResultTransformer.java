package org.apache.solr.search.grouping.endresulttransformer;

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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class GroupedEndResultTransformer implements EndResultTransformer {

  private final SolrIndexSearcher searcher;

  public GroupedEndResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  public void transform(Map<String, ?> result, SolrQueryResponse response, GroupingSpecification groupingSpecification, SolrDocumentSource solrDocumentSource) {
    NamedList<Object> commands = new NamedList<Object>();
    for (Map.Entry<String, ?> entry : result.entrySet()) {
      Object value = entry.getValue();
      if (TopGroups.class.isInstance(value)) {
        @SuppressWarnings("unchecked")
        TopGroups<BytesRef> topGroups = (TopGroups<BytesRef>) value;
        NamedList<Object> command = new SimpleOrderedMap<Object>();
        command.add("matches", topGroups.totalHitCount);
        if (topGroups.totalGroupCount != null) {
          command.add("ngroups", topGroups.totalGroupCount);
        }

        List<NamedList> groups = new ArrayList<NamedList>();
        SchemaField groupField = searcher.getSchema().getField(entry.getKey());
        FieldType groupFieldType = groupField.getType();
        for (GroupDocs<BytesRef> group : topGroups.groups) {
          SimpleOrderedMap<Object> groupResult = new SimpleOrderedMap<Object>();
          if (group.groupValue != null) {
            groupResult.add(
                "groupValue", groupFieldType.toObject(groupField.createField(group.groupValue.utf8ToString(), 0.0f))
            );
          } else {
            groupResult.add("groupValue", null);
          }
          SolrDocumentList docList = new SolrDocumentList();
          docList.setNumFound(group.totalHits);
          if (!Float.isNaN(group.maxScore)) {
            docList.setMaxScore(group.maxScore);
          }
          docList.setStart(groupingSpecification.getGroupOffset());
          for (ScoreDoc scoreDoc : group.scoreDocs) {
            docList.add(solrDocumentSource.retrieve(scoreDoc));
          }
          groupResult.add("doclist", docList);
          groups.add(groupResult);
        }
        command.add("groups", groups);
        commands.add(entry.getKey(), command);
      } else if (QueryCommandResult.class.isInstance(value)) {
        QueryCommandResult queryCommandResult = (QueryCommandResult) value;
        NamedList<Object> command = new SimpleOrderedMap<Object>();
        command.add("matches", queryCommandResult.getMatches());
        SolrDocumentList docList = new SolrDocumentList();
        docList.setNumFound(queryCommandResult.getTopDocs().totalHits);
        if (!Float.isNaN(queryCommandResult.getTopDocs().getMaxScore())) {
          docList.setMaxScore(queryCommandResult.getTopDocs().getMaxScore());
        }
        docList.setStart(groupingSpecification.getGroupOffset());
        for (ScoreDoc scoreDoc :queryCommandResult.getTopDocs().scoreDocs){
          docList.add(solrDocumentSource.retrieve(scoreDoc));
        }
        command.add("doclist", docList);
        commands.add(entry.getKey(), command);
      }
    }
    response.add("grouped", commands);
  }

}
