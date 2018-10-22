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
package org.apache.solr.handler;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;


/**
 * Load a {@link org.apache.solr.common.util.ContentStream} into Solr
 *
 **/
public abstract class ContentStreamLoader {

  protected String errHeader;

  public String getErrHeader() {
    return errHeader;
  }

  public void setErrHeader(String errHeader) {
    this.errHeader = errHeader;
  }

  /**
   * Loaders are responsible for closing the stream
   *
   * @param req The input {@link org.apache.solr.request.SolrQueryRequest}
   * @param rsp The response, in case the Loader wishes to add anything
   * @param stream The {@link org.apache.solr.common.util.ContentStream} to add
   */
  public abstract void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) throws Exception;


}
