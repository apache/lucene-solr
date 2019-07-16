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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;

/**
 * Implementation of {@link EndResultTransformer} that keeps each grouped result separate in the final response.
 */
public class GroupedEndResultTransformer implements EndResultTransformer {

  private final SolrIndexSearcher searcher;

  public GroupedEndResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  @Override
  public void transform(Map<String, ?> result, ResponseBuilder rb, SolrDocumentSource solrDocumentSource) {
    NamedList<Object> commands = new SimpleOrderedMap<>();
    SortSpec withinGroupSortSpec = rb.getGroupingSpec().getWithinGroupSortSpec();
    for (Map.Entry<String, ?> entry : result.entrySet()) {
      Object value = entry.getValue();
      if (TopGroups.class.isInstance(value)) {
        @SuppressWarnings("unchecked")
        TopGroups<BytesRef> topGroups = (TopGroups<BytesRef>) value;
        NamedList<Object> command = new SimpleOrderedMap<>();
        command.add("matches", rb.totalHitCount);
        Integer totalGroupCount = rb.mergedGroupCounts.get(entry.getKey());
        if (totalGroupCount != null) {
          command.add("ngroups", totalGroupCount);
        }

        List<NamedList> groups = new ArrayList<>();
        SchemaField groupField = searcher.getSchema().getField(entry.getKey());
        FieldType groupFieldType = groupField.getType();
        for (GroupDocs<BytesRef> group : topGroups.groups) {
          SimpleOrderedMap<Object> groupResult = new SimpleOrderedMap<>();
          if (group.groupValue != null) {
            // use createFields so that fields having doc values are also supported
            List<IndexableField> fields = groupField.createFields(group.groupValue.utf8ToString());
            if (CollectionUtils.isNotEmpty(fields)) {
              groupResult.add("groupValue", groupFieldType.toObject(fields.get(0)));
            } else {
              throw new SolrException(ErrorCode.INVALID_STATE,
                  "Couldn't create schema field for grouping, group value: " + group.groupValue.utf8ToString()
                  + ", field: " + groupField);
            }
          } else {
            groupResult.add("groupValue", null);
          }
          SolrDocumentList docList = new SolrDocumentList();
          assert group.totalHits.relation == TotalHits.Relation.EQUAL_TO;
          docList.setNumFound(group.totalHits.value);
          if (!Float.isNaN(group.maxScore)) {
            docList.setMaxScore(group.maxScore);
          }
          docList.setStart(withinGroupSortSpec.getOffset());
          retrieveAndAdd(docList, solrDocumentSource, group.scoreDocs);
          groupResult.add("doclist", docList);
          groups.add(groupResult);
        }
        command.add("groups", groups);
        commands.add(entry.getKey(), command);
      } else if (QueryCommandResult.class.isInstance(value)) {
        QueryCommandResult queryCommandResult = (QueryCommandResult) value;
        NamedList<Object> command = new SimpleOrderedMap<>();
        command.add("matches", queryCommandResult.getMatches());
        SolrDocumentList docList = new SolrDocumentList();
        TopDocs topDocs = queryCommandResult.getTopDocs();
        assert topDocs.totalHits.relation == TotalHits.Relation.EQUAL_TO;
        docList.setNumFound(topDocs.totalHits.value);
        if (!Float.isNaN(queryCommandResult.getMaxScore())) {
          docList.setMaxScore(queryCommandResult.getMaxScore());
        }
        docList.setStart(withinGroupSortSpec.getOffset());
        retrieveAndAdd(docList, solrDocumentSource, queryCommandResult.getTopDocs().scoreDocs);
        command.add("doclist", docList);
        commands.add(entry.getKey(), command);
      }
    }
    rb.rsp.add("grouped", commands);
  }

  private static void retrieveAndAdd(SolrDocumentList solrDocumentList, SolrDocumentSource solrDocumentSource, ScoreDoc[] scoreDocs) {
    for (ScoreDoc scoreDoc : scoreDocs) {
      SolrDocument solrDocument = solrDocumentSource.retrieve(scoreDoc);
      if (solrDocument != null) {
        solrDocumentList.add(solrDocument);
      }
    }
  }

}
