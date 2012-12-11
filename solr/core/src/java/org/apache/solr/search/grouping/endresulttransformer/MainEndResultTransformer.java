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
import org.apache.solr.handler.component.ResponseBuilder;

import java.util.Map;

/**
 * Implementation of {@link EndResultTransformer} that transforms the grouped result into the main result list in the
 * response.
 */
public class MainEndResultTransformer implements EndResultTransformer {

  /**
   * {@inheritDoc}
   */
  @Override
  public void transform(Map<String, ?> result, ResponseBuilder rb, SolrDocumentSource solrDocumentSource) {
    Object value = result.get(rb.getGroupingSpec().getFields()[0]);
    if (TopGroups.class.isInstance(value)) {
      @SuppressWarnings("unchecked")
      TopGroups<BytesRef> topGroups = (TopGroups<BytesRef>) value;
      SolrDocumentList docList = new SolrDocumentList();
      docList.setStart(rb.getGroupingSpec().getOffset());
      docList.setNumFound(rb.totalHitCount);

      Float maxScore = Float.NEGATIVE_INFINITY;
      for (GroupDocs<BytesRef> group : topGroups.groups) {
        for (ScoreDoc scoreDoc : group.scoreDocs) {
          if (maxScore < scoreDoc.score) {
            maxScore = scoreDoc.score;
          }
          docList.add(solrDocumentSource.retrieve(scoreDoc));
        }
      }
      if (maxScore != Float.NEGATIVE_INFINITY) {
        docList.setMaxScore(maxScore);
      }
      rb.rsp.add("response", docList);
    }
  }
}
