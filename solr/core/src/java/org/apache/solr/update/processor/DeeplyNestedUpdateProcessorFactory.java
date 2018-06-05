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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

public class DeeplyNestedUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private static List<String> allowedFields = new ArrayList<String>(3) {
    {
      add(IndexSchema.LEVEL_FIELD_NAME);add(IndexSchema.PARENT_FIELD_NAME);
      add(IndexSchema.PATH_FIELD_NAME);
    }
  };
  private List<String> fields;

  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next ) {
    return new DeeplyNestedUpdateProcessor(req, rsp, fields, next);
  }

  @Override
  public void init( NamedList args )
  {
    Object tmp = args.remove("fields");
    if (null == tmp) {
      throw new SolrException(SERVER_ERROR,
          "'versionField' must be configured");
    }
    if (! (tmp instanceof String) ) {
      throw new SolrException(SERVER_ERROR,
          "'versionField' must be configured as a <str>");
    }
    fields = StrUtils.splitSmart((String)tmp, ',');
    if(!allowedFields.containsAll(fields)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Deeply Nested Fields may only contain _nestLevel_, _nestParent_, _nestPath_");
    }
  }
}
