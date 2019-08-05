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
package org.apache.solr.analytics.stream;

import org.apache.solr.analytics.AnalyticsRequestManager;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.AnalyticsHandler;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;

/**
 * This parser initiates a merge of an Analytics Shard Response, sent from the {@link AnalyticsHandler}.
 *
 * The input stream is immediately sent to the given {@link AnalyticsRequestManager} to merge.
 */
public class AnalyticsShardResponseParser extends ResponseParser {
  public static final String BINARY_CONTENT_TYPE = "application/octet-stream";
  public static final String STREAM = "application/octet-stream";

  private final AnalyticsRequestManager manager;

  /**
   *
   * @param manager the manager of the current Analytics Request, will manage the merging of shard data
   */
  public AnalyticsShardResponseParser(AnalyticsRequestManager manager) {
    this.manager = manager;
  }

  @Override
  public String getWriterType() {
    return "analytics_shard_stream";
  }

  @Override
  public NamedList<Object> processResponse(InputStream body, String encoding) {
    DataInputStream input = new DataInputStream(body);
    //check to see if the response is an exception
    NamedList<Object> exception = new NamedList<>();
    try {
      if (input.readBoolean()) {
        manager.importShardData(input);
      } else {
        exception.add("Exception", new ObjectInputStream(input).readObject());
      }
    } catch (IOException e) {
      exception.add("Exception", new SolrException(ErrorCode.SERVER_ERROR, "Couldn't process analytics shard response", e));
    } catch (ClassNotFoundException e1) {
      throw new RuntimeException(e1);
    }
    return exception;
  }

  @Override
  public String getContentType() {
    return BINARY_CONTENT_TYPE;
  }

  @Override
  public String getVersion() {
    return "1";
  }

  @Override
  public NamedList<Object> processResponse(Reader reader) {
    throw new RuntimeException("Cannot handle character stream");
  }
}
