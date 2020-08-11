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
package org.apache.solr.response;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Writer;

import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.analytics.AnalyticsRequestManager;
import org.apache.solr.analytics.stream.AnalyticsShardResponseParser;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Writes the reduction data of a analytics shard request to a bit-stream to send to the originating shard.
 * The response must be parsed by the {@link AnalyticsShardResponseParser} initialized with the same analytics request
 * as the shard request was sent.
 */
public class AnalyticsShardResponseWriter implements BinaryQueryResponseWriter {
  public static final String NAME = "analytics_shard_stream";
  public static final String ANALYTICS_MANGER = "analyticsManager";

  @Override
  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response) throws IOException {
    ((AnalyticsResponse)response.getResponse()).write(new DataOutputStream(out));;
  }

  @Override
  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    throw new RuntimeException("This is a binary writer , Cannot write to a characterstream");
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return BinaryResponseParser.BINARY_CONTENT_TYPE;
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {}

  /**
   * Manages the streaming of analytics reduction data if no exception occurred.
   * Otherwise the exception is streamed over.
   */
  public static class AnalyticsResponse {
    private final AnalyticsRequestManager manager;
    private final SolrException exception;

    private final boolean requestSuccessful;

    public AnalyticsResponse(AnalyticsRequestManager manager) {
      this.manager = manager;
      this.exception = null;
      this.requestSuccessful = true;
    }

    public AnalyticsResponse(SolrException exception) {
      this.manager = null;
      this.exception = exception;
      this.requestSuccessful = false;
    }

    @SuppressForbidden(reason = "XXX: security hole")
    public void write(DataOutputStream output) throws IOException {
      output.writeBoolean(requestSuccessful);
      if (requestSuccessful) {
        manager.exportShardData(output);
      } else {
        new ObjectOutputStream(output).writeObject(exception);
      }
    }
  }

}
