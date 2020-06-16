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
package org.apache.solr.cloud;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;

/**
 * A class for making a request to the config handler. Tests can use this
 * e.g. to add custom components, handlers, parsers, etc. to an otherwise
 * generic configset.
 */
@SuppressWarnings({"rawtypes"})
public class ConfigRequest extends SolrRequest {

  protected final String message;

  public ConfigRequest(String message) {
    super(SolrRequest.METHOD.POST, "/config");
    this.message = message;
  }

  @Override
  public SolrParams getParams() {
    return null;
  }

  @Override
  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    return message == null? null: new RequestWriter.StringPayloadContentWriter(message, CommonParams.JSON_MIME);
  }

  @Override
  public SolrResponse createResponse(SolrClient client) {
    return new SolrResponseBase();
  }
}
