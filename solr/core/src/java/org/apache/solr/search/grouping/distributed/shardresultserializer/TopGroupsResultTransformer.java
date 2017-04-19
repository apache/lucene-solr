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
package org.apache.solr.search.grouping.distributed.shardresultserializer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.distributed.command.QueryCommand;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;
import org.apache.solr.search.grouping.distributed.command.TopGroupsFieldCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

/**
 * Implementation for transforming {@link TopGroups} and {@link TopDocs} into a {@link NamedList} structure and
 * visa versa.
 */
public class TopGroupsResultTransformer implements ShardResultTransformer<List<Command>, Map<String, ?>> {

  private final ResponseBuilder rb;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TopGroupsResultTransformer(ResponseBuilder rb) {
    this.rb = rb;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NamedList transform(List<Command> data) throws IOException {
    NamedList<NamedList> result = new NamedList<>();
    final IndexSchema schema = rb.req.getSearcher().getSchema();
    for (Command command : data) {
      NamedList commandResult;
      if (TopGroupsFieldCommand.class.isInstance(command)) {
        TopGroupsFieldCommand fieldCommand = (TopGroupsFieldCommand) command;
        SchemaField groupField = schema.getField(fieldCommand.getKey());
        commandResult = serializeTopGroups(fieldCommand.result(), groupField);
      } else if (QueryCommand.class.isInstance(command)) {
        QueryCommand queryCommand = (QueryCommand) command;
        commandResult = serializeTopDocs(queryCommand.result());
      } else {
        commandResult = null;
      }

      result.add(command.getKey(), commandResult);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
    public Map<String, ?> transformToNative(NamedList<NamedList> shardResponse, SortSpec groupSortSpec, SortSpec withinGroupSortSpec, String shard) {
    Map<String, Object> result = new HashMap<>();

    final IndexSchema schema = rb.req.getSearcher().getSchema();
    final Sort groupSort = groupSortSpec.getSort();
    final Sort withinGroupSort = withinGroupSortSpec.getSort();

    for (Map.Entry<String, NamedList> entry : shardResponse) {
      String key = entry.getKey();
      NamedList commandResult = entry.getValue();
      Integer totalGroupedHitCount = (Integer) commandResult.get("totalGroupedHitCount");
      Integer totalHits = (Integer) commandResult.get("totalHits");
      if (totalHits != null) {
        Integer matches = (Integer) commandResult.get("matches");
        Float maxScore = (Float) commandResult.get("maxScore");
        if (maxScore == null) {
          maxScore = Float.NaN;
        }

        @SuppressWarnings("unchecked")
        List<NamedList<Object>> documents = (List<NamedList<Object>>) commandResult.get("documents");
        ScoreDoc[] scoreDocs = transformToNativeShardDoc(documents, groupSortSpec, shard, schema);
        final TopDocs topDocs;
        if (withinGroupSort.equals(Sort.RELEVANCE)) {
          topDocs = new TopDocs(totalHits, scoreDocs, maxScore);
        } else {
          topDocs = new TopFieldDocs(totalHits, scoreDocs, withinGroupSort.getSort(), maxScore);
        }
        result.put(key, new QueryCommandResult(topDocs, matches));
        continue;
      }

      Integer totalHitCount = (Integer) commandResult.get("totalHitCount");

      List<GroupDocs<BytesRef>> groupDocs = new ArrayList<>();
      for (int i = 2; i < commandResult.size(); i++) {
        String groupValue = commandResult.getName(i);
        @SuppressWarnings("unchecked")
        NamedList<Object> groupResult = (NamedList<Object>) commandResult.getVal(i);
        Integer totalGroupHits = (Integer) groupResult.get("totalHits");
        Float maxScore = (Float) groupResult.get("maxScore");
        if (maxScore == null) {
          maxScore = Float.NaN;
        }

        @SuppressWarnings("unchecked")
        List<NamedList<Object>> documents = (List<NamedList<Object>>) groupResult.get("documents");
        ScoreDoc[] scoreDocs = transformToNativeShardDoc(documents, withinGroupSortSpec, shard, schema);

        BytesRef groupValueRef = groupValue != null ? new BytesRef(groupValue) : null;
        groupDocs.add(new GroupDocs<>(Float.NaN, maxScore, totalGroupHits, scoreDocs, groupValueRef, null));
      }

      @SuppressWarnings("unchecked")
      GroupDocs<BytesRef>[] groupDocsArr = groupDocs.toArray(new GroupDocs[groupDocs.size()]);
      TopGroups<BytesRef> topGroups = new TopGroups<>(
           groupSort.getSort(), withinGroupSort.getSort(), totalHitCount, totalGroupedHitCount, groupDocsArr, Float.NaN
      );

      result.put(key, topGroups);
    }

    return result;
  }

  protected ScoreDoc[] transformToNativeShardDoc(List<NamedList<Object>> documents, SortSpec sortSpec, String shard,
                                                 IndexSchema schema) {
    ScoreDoc[] scoreDocs = new ScoreDoc[documents.size()];

    final List<SchemaField> schemaFields = sortSpec.getSchemaFields();
    final SortField[] sortFields = sortSpec.getSort().getSort();

    assert (schemaFields.size() == sortFields.length);


    int j = 0;
    for (NamedList<Object> document : documents) {
      Object docId = document.get(ID);
      if (docId != null) {
        docId = docId.toString();
      } else {
        log.error("doc {} has null 'id'", document);
      }
      Float score = (Float) document.get("score");
      if (score == null) {
        score = Float.NaN;
      }
      Object[] sortValues = null;
      Object sortValuesVal = document.get("sortValues");
      if (sortValuesVal != null) {
        sortValues = ((List) sortValuesVal).toArray();

        assert (schemaFields.size() == sortValues.length);

        for (int k = 0; k < sortValues.length; k++) {
          sortValues[k] = ShardResultTransformerUtils.unmarshalSortValue(sortValues[k], schemaFields.get(k));
        }
      } else {
        log.debug("doc {} has null 'sortValues'", document);
      }
      scoreDocs[j++] = new ShardDoc(score, sortValues, docId, shard);
    }
    return scoreDocs;
  }

  protected NamedList serializeTopGroups(TopGroups<BytesRef> data, SchemaField groupField) throws IOException {
    NamedList<Object> result = new NamedList<>();
    result.add("totalGroupedHitCount", data.totalGroupedHitCount);
    result.add("totalHitCount", data.totalHitCount);
    if (data.totalGroupCount != null) {
      result.add("totalGroupCount", data.totalGroupCount);
    }

    final IndexSchema schema = rb.req.getSearcher().getSchema();
    SchemaField uniqueField = schema.getUniqueKeyField();

    final SortSpec withinGroupSortSpec = rb.getGroupingSpec().getWithinGroupSortSpec();
    final List<SchemaField> withinGroupSchemaFields = withinGroupSortSpec.getSchemaFields();
    final SortField[] withinGroupSortFields = withinGroupSortSpec.getSort().getSort();

    assert (withinGroupSchemaFields.size() == withinGroupSortFields.length);

    for (GroupDocs<BytesRef> searchGroup : data.groups) {
      NamedList<Object> groupResult = new NamedList<>();
      groupResult.add("totalHits", searchGroup.totalHits);
      if (!Float.isNaN(searchGroup.maxScore)) {
        groupResult.add("maxScore", searchGroup.maxScore);
      }

      List<NamedList<Object>> documents = new ArrayList<>();
      for (int i = 0; i < searchGroup.scoreDocs.length; i++) {
        NamedList<Object> document = new NamedList<>();
        documents.add(document);

        Document doc = retrieveDocument(uniqueField, searchGroup.scoreDocs[i].doc);
        document.add(ID, uniqueField.getType().toExternal(doc.getField(uniqueField.getName())));
        if (!Float.isNaN(searchGroup.scoreDocs[i].score))  {
          document.add("score", searchGroup.scoreDocs[i].score);
        }
        if (!(searchGroup.scoreDocs[i] instanceof FieldDoc)) {
          continue; // thus don't add sortValues below
        }

        FieldDoc fieldDoc = (FieldDoc) searchGroup.scoreDocs[i];

        assert (withinGroupSchemaFields.size() == fieldDoc.fields.length);  // JTODO (?)

        Object[] convertedSortValues  = new Object[fieldDoc.fields.length];
        for (int j = 0; j < fieldDoc.fields.length; j++) {
            convertedSortValues[j] = ShardResultTransformerUtils.marshalSortValue(fieldDoc.fields[j], withinGroupSchemaFields.get(j));
        }
        document.add("sortValues", convertedSortValues);
      }
      groupResult.add("documents", documents);
      String groupValue = searchGroup.groupValue != null ?
          groupField.getType().indexedToReadable(searchGroup.groupValue, new CharsRefBuilder()).toString(): null;
      result.add(groupValue, groupResult);
    }

    return result;
  }

  protected NamedList serializeTopDocs(QueryCommandResult result) throws IOException {
    NamedList<Object> queryResult = new NamedList<>();
    queryResult.add("matches", result.getMatches());
    queryResult.add("totalHits", result.getTopDocs().totalHits);
    // debug: assert !Float.isNaN(result.getTopDocs().getMaxScore()) == rb.getGroupingSpec().isNeedScore();
    if (!Float.isNaN(result.getTopDocs().getMaxScore())) {
      queryResult.add("maxScore", result.getTopDocs().getMaxScore());
    }
    List<NamedList> documents = new ArrayList<>();
    queryResult.add("documents", documents);

    final IndexSchema schema = rb.req.getSearcher().getSchema();
    SchemaField uniqueField = schema.getUniqueKeyField();

    final SortSpec groupSortSpec = rb.getGroupingSpec().getGroupSortSpec();
    final List<SchemaField> groupSchemaFields = groupSortSpec.getSchemaFields();
    final SortField[] groupSortFields = groupSortSpec.getSort().getSort();

    assert (groupSchemaFields.size() == groupSortFields.length);

    for (ScoreDoc scoreDoc : result.getTopDocs().scoreDocs) {
      NamedList<Object> document = new NamedList<>();
      documents.add(document);

      Document doc = retrieveDocument(uniqueField, scoreDoc.doc);
      document.add(ID, uniqueField.getType().toExternal(doc.getField(uniqueField.getName())));
      if (!Float.isNaN(scoreDoc.score))  {
        document.add("score", scoreDoc.score);
      }
      if (!FieldDoc.class.isInstance(scoreDoc)) {
        continue; // thus don't add sortValues below
      }

      FieldDoc fieldDoc = (FieldDoc) scoreDoc;
      Object[] convertedSortValues  = new Object[fieldDoc.fields.length];
      for (int j = 0; j < fieldDoc.fields.length; j++) {
        convertedSortValues[j] = ShardResultTransformerUtils.marshalSortValue(fieldDoc.fields[j], groupSchemaFields.get(j));
      }
      document.add("sortValues", convertedSortValues);
    }

    return queryResult;
  }

  private Document retrieveDocument(final SchemaField uniqueField, int doc) throws IOException {
    return rb.req.getSearcher().doc(doc, Collections.singleton(uniqueField.getName()));
  }

}
