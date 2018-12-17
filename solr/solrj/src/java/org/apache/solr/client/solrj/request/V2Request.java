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

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.params.CommonParams.JAVABIN_MIME;
import static org.apache.solr.common.params.CommonParams.JSON_MIME;

public class V2Request extends SolrRequest<V2Response> implements MapWriter {
  //only for debugging purposes
  public static final ThreadLocal<AtomicLong> v2Calls = new ThreadLocal<>();
  static final Pattern COLL_REQ_PATTERN = Pattern.compile("/(c|collections)/([^/])+/(?!shards)");
  private Object payload;
  private SolrParams solrParams;
  public final boolean useBinary;
  private String collection;
  private boolean isPerCollectionRequest = false;

  private V2Request(METHOD m, String resource, boolean useBinary) {
    super(m, resource);
    Matcher matcher = COLL_REQ_PATTERN.matcher(getPath());
    if (matcher.find()) {
      this.collection = matcher.group(2);
      isPerCollectionRequest = true;
    }
    this.useBinary = useBinary;

  }

  @Override
  public SolrParams getParams() {
    return solrParams;
  }

  @Override
  public RequestWriter.ContentWriter getContentWriter(String s) {
    if (v2Calls.get() != null) v2Calls.get().incrementAndGet();
    if (payload == null) return null;
    if (payload instanceof String) {
      return new RequestWriter.StringPayloadContentWriter((String) payload, JSON_MIME);
    }
    return new RequestWriter.ContentWriter() {
      @Override
      public void write(OutputStream os) throws IOException {
        if (useBinary) {
          new JavaBinCodec().marshal(payload, os);
        } else {
          Utils.writeJson(payload, os, false);
        }
      }

      @Override
      public String getContentType() {
        return useBinary ? JAVABIN_MIME : JSON_MIME;
      }
    };
  }

  public boolean isPerCollectionRequest() {
    return isPerCollectionRequest;
  }

  @Override
  public String getCollection() {
    return collection;
  }

  @Override
  protected V2Response createResponse(SolrClient client) {
    return new V2Response();
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("method", getMethod().toString());
    ew.put("path", getPath());
    ew.putIfNotNull("params", solrParams);
    ew.putIfNotNull("command", payload);
  }

  public static class Builder {
    private String resource;
    private METHOD method = METHOD.GET;
    private Object payload;
    private SolrParams params;
    private boolean useBinary = false;

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
      if (payload != null) {
        this.payload = payload;
      }
      return this;
    }

    public Builder withPayload(Object payload) {
      this.payload = payload;
      return this;
    }


    public Builder withParams(SolrParams params) {
      this.params = params;
      return this;
    }

    public Builder useBinary(boolean flag) {
      this.useBinary = flag;
      return this;
    }

    public V2Request build() {
      V2Request v2Request = new V2Request(method, resource, useBinary);
      v2Request.solrParams = params;
      v2Request.payload = payload;
      return v2Request;
    }
  }
}
