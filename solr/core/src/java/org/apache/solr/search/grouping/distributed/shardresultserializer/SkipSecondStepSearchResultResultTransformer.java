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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Extends {@link SearchGroupsResultTransformer} and overrides the <code>serializeOneSearchGroup</code>,
 * <code>deserializeOneSearchGroup</code> and <code>getSortValues</code> methods because additional data
 * (top doc id and top doc score) needs to be transformed for each group.
 */
public class SkipSecondStepSearchResultResultTransformer extends SearchGroupsResultTransformer {

  private static final String TOP_DOC_SOLR_ID_KEY = "topDocSolrId";
  private static final String TOP_DOC_SCORE_KEY = "topDocScore";
  private static final String SORTVALUES_KEY = "sortValues";

  private final SchemaField uniqueField;
  private final Set<String> uniqueFieldNameAsSet;

  public SkipSecondStepSearchResultResultTransformer(SolrIndexSearcher searcher) {
    super(searcher);
    this.uniqueField = searcher.getSchema().getUniqueKeyField();
    this.uniqueFieldNameAsSet = Collections.singleton(this.uniqueField.getName());
  }

  @Override
  protected Object[] getSortValues(Object rawSearchGroupData) {
    NamedList<Object> groupInfo = (NamedList) rawSearchGroupData;
    ArrayList<?> sortValues = (ArrayList<?>) groupInfo.get(SORTVALUES_KEY);
    return sortValues.toArray(new Comparable[sortValues.size()]);
  }

  @Override
  protected SearchGroup<BytesRef> deserializeOneSearchGroup(SchemaField groupField, String groupValue,
                                                            SortField[] groupSortField, Object rawSearchGroupData) {
    SearchGroup<BytesRef> searchGroup = super.deserializeOneSearchGroup(groupField, groupValue, groupSortField, rawSearchGroupData);
    NamedList<Object> groupInfo = (NamedList) rawSearchGroupData;
    searchGroup.topDocLuceneId = DocIdSetIterator.NO_MORE_DOCS;
    searchGroup.topDocScore = (float) groupInfo.get(TOP_DOC_SCORE_KEY);
    searchGroup.topDocSolrId = groupInfo.get(TOP_DOC_SOLR_ID_KEY);
    return searchGroup;
  }

  @Override
  protected Object serializeOneSearchGroup(SortField[] groupSortField, SearchGroup<BytesRef> searchGroup) {
    Document luceneDoc = null;
    /** Use the lucene id to get the unique solr id so that it can be sent to the federator.
     * The lucene id of a document is not unique across all shards i.e. different documents
     * in different shards could have the same lucene id, whereas the solr id is guaranteed
     * to be unique so this is what we need to return to the federator
     **/
    try {
      luceneDoc = searcher.doc(searchGroup.topDocLuceneId, uniqueFieldNameAsSet);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot retrieve document for unique field " + uniqueField + " (" + e.toString() + ")");
    }
    String topDocSolrId = uniqueField.getType().toExternal(luceneDoc.getField(uniqueField.getName()));
    NamedList<Object> groupInfo = new NamedList<>();
    groupInfo.add(TOP_DOC_SCORE_KEY, searchGroup.topDocScore);
    groupInfo.add(TOP_DOC_SOLR_ID_KEY, topDocSolrId);

    Object convertedSortValues = super.serializeOneSearchGroup(groupSortField, searchGroup);
    groupInfo.add(SORTVALUES_KEY, convertedSortValues);
    return groupInfo;
  }
}
