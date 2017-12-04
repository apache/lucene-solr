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
package org.apache.solr.client.solrj.request;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter.ContentWriter;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.SolrParams;

public class GenericSolrRequest extends SolrRequest<SimpleSolrResponse> {
  public SolrParams params;
  public SimpleSolrResponse response = new SimpleSolrResponse();
  public ContentWriter contentWriter;

  public GenericSolrRequest(METHOD m, String path, SolrParams params) {
    super(m, path);
    this.params = params;
  }

  public GenericSolrRequest setContentWriter(ContentWriter contentWriter) {
    this.contentWriter = contentWriter;
    return this;
  }

  @Override
  public ContentWriter getContentWriter(String expectedType) {
    return contentWriter;
  }

  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  protected SimpleSolrResponse createResponse(SolrClient client) {
    return response;
  }
}
