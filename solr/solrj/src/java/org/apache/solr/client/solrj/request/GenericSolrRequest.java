package org.apache.solr.client.solrj.request;

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

import java.io.IOException;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

public class GenericSolrRequest extends SolrRequest<SimpleSolrResponse> {
  public SolrParams params;
  public SimpleSolrResponse response = new SimpleSolrResponse();
  private Collection<ContentStream> contentStreams;

  public GenericSolrRequest(METHOD m, String path, SolrParams params) {
    super(m, path);
    this.params = params;
  }

  public void setContentStreams(Collection<ContentStream> streams) {
    contentStreams = streams;
  }


  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return contentStreams;
  }

  @Override
  protected SimpleSolrResponse createResponse(SolrClient client) {
    return response;
  }
}
