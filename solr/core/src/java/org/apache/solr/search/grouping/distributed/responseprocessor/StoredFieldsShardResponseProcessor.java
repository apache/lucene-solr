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

import org.apache.lucene.search.FieldDoc;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;

/**
 * Concrete implementation for processing the stored field values from shard responses.
 */
public class StoredFieldsShardResponseProcessor implements ShardResponseProcessor {

  @Override
  public void process(ResponseBuilder rb, ShardRequest shardRequest) {
    boolean returnScores = (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0;
    ShardResponse srsp = shardRequest.responses.get(0);
    SolrDocumentList docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
    String uniqueIdFieldName = rb.req.getSchema().getUniqueKeyField().getName();

    if (rb.rsp.getReturnFields().getFieldRenames().get(uniqueIdFieldName) != null) {
      // if id was renamed we need to use the new name
      uniqueIdFieldName = rb.rsp.getReturnFields().getFieldRenames().get(uniqueIdFieldName);
    }
    for (SolrDocument doc : docs) {
      Object id = doc.getFieldValue(uniqueIdFieldName).toString();
      ShardDoc shardDoc = rb.resultIds.get(id);
      FieldDoc fieldDoc = (FieldDoc) shardDoc;
      if (shardDoc != null) {
        if (returnScores && !Float.isNaN(fieldDoc.score)) {
            doc.setField("score", fieldDoc.score);
        }
        rb.retrievedDocuments.put(id, doc);
      }
    }
  }
}
