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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;

public class V2Request extends SolrRequest {
  static final Pattern COLL_REQ_PATTERN = Pattern.compile("/(c|collections)/[^/]+/(?!shards)");
  private InputStream payload;
  private SolrParams solrParams;

  private V2Request(METHOD m, String resource, InputStream payload) {
    super(m, resource);
    this.payload = payload;
  }

  @Override
  public SolrParams getParams() {
    return solrParams;
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    if (payload != null) {
      return Collections.singleton(new ContentStreamBase() {
        @Override
        public InputStream getStream() throws IOException {
          return payload;
        }

        @Override
        public String getContentType() {
          return "application/json";
        }
      });
    }
    return null;
  }

  public boolean isPerCollectionRequest() {
    return COLL_REQ_PATTERN.matcher(getPath()).find();
  }

  @Override
  protected SolrResponse createResponse(SolrClient client) {
    return null;
  }

  public static class Builder {
    private String resource;
    private METHOD method = METHOD.GET;
    private InputStream payload;
    private SolrParams params;

    /**
     * Create a Builder object based on the provided resource.
     * The default method is GET.
     *
     * @param resource resource of the request for example "/collections" or "/cores/core-name"
     */
    public Builder(String resource) {
      if (!resource.startsWith("/")) resource = "/" + resource;
      this.resource = resource;
    }

    public Builder withMethod(METHOD m) {
      this.method = m;
      return this;
    }

    /**
     * Set payload for request.
     * @param payload as UTF-8 String
     * @return builder object
     */
    public Builder withPayload(String payload) {
      this.payload = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));
      return this;
    }

    public Builder withPayLoad(InputStream payload) {
      this.payload = payload;
      return this;
    }

    public Builder withParams(SolrParams params) {
      this.params = params;
      return this;
    }

    public V2Request build() {
      V2Request v2Request = new V2Request(method, resource, payload);
      v2Request.solrParams = params;
      return v2Request;
    }
  }
}
