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

package org.apache.solr.update.processor;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;

public class NestedUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next ) {
    boolean storeParent = shouldStoreDocParent(req.getSchema());
    boolean storePath = shouldStoreDocPath(req.getSchema());
    if(!(storeParent || storePath)) {
      return next;
    }
    return new NestedUpdateProcessor(req, rsp, shouldStoreDocParent(req.getSchema()), shouldStoreDocPath(req.getSchema()), next);
  }

  private static boolean shouldStoreDocParent(IndexSchema schema) {
    return schema.getFields().containsKey(IndexSchema.PARENT_FIELD_NAME);
  }

  private static boolean shouldStoreDocPath(IndexSchema schema) {
    return schema.getFields().containsKey(IndexSchema.PATH_FIELD_NAME);
  }
}
