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
package org.apache.solr.request;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Implementations of <code>SolrRequestHandler</code> are called to handle query requests.
 *
 * Different <code>SolrRequestHandler</code>s are registered with the <code>SolrCore</code>.
 * One way to register a SolrRequestHandler with the core is through the <code>solrconfig.xml</code> file.
 * <p>
 * Example <code>solrconfig.xml</code> entry to register a <code>SolrRequestHandler</code> implementation to
 * handle all queries with a Request Handler of "/test":
 * <p>
 * <code>
 *    &lt;requestHandler name="/test" class="solr.tst.TestRequestHandler" /&gt;
 * </code>
 * <p>
 * A single instance of any registered SolrRequestHandler is created
 * via the default constructor and is reused for all relevant queries.
 *
 *
 */
public interface SolrRequestHandler extends SolrInfoBean {

  /** <code>init</code> will be called just once, immediately after creation.
   * <p>The args are user-level initialization parameters that
   * may be specified when declaring a request handler in
   * solrconfig.xml
   */
  public void init(@SuppressWarnings({"rawtypes"})NamedList args);


  /**
   * Handles a query request, this method must be thread safe.
   * <p>
   * Information about the request may be obtained from <code>req</code> and
   * response information may be set using <code>rsp</code>.
   * <p>
   * There are no mandatory actions that handleRequest must perform.
   * An empty handleRequest implementation would fulfill
   * all interface obligations.
   */
  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp);

  public static final String TYPE = "requestHandler";
}

