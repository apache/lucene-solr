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
package org.apache.solr.handler.loader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;


/**
 * Load a {@link org.apache.solr.common.util.ContentStream} into Solr
 *
 * This should be thread safe and can be called from multiple threads
 */
public abstract class ContentStreamLoader {

  /**
   * This should be called once for each RequestHandler
   */
  public ContentStreamLoader init(SolrParams args) {
    return this;
  }
  
  public String getDefaultWT() {
    return null;
  }

  /**
   * Loaders are responsible for closing the stream
   *
   * @param req The input {@link org.apache.solr.request.SolrQueryRequest}
   * @param rsp The response, in case the Loader wishes to add anything
   * @param stream The {@link org.apache.solr.common.util.ContentStream} to add
   * @param processor The {@link UpdateRequestProcessor} to use
   */
  public abstract void load(SolrQueryRequest req, 
      SolrQueryResponse rsp, 
      ContentStream stream, 
      UpdateRequestProcessor processor) throws Exception;
}
