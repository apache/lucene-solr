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
import java.util.Collection;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 * Verify that there is a working Solr core at the URL of a {@link SolrServer}.
 * To use this class, the solrconfig.xml for the relevant core must include the
 * request handler for <code>/admin/ping</code>.
 * 
 * @since solr 1.3
 */
public class SolrPing extends SolrRequest {
  
  /** serialVersionUID. */
  private static final long serialVersionUID = 5828246236669090017L;
  
  /** Request parameters. */
  private ModifiableSolrParams params;
  
  /**
   * Create a new SolrPing object.
   */
  public SolrPing() {
    super(METHOD.GET, CommonParams.PING_HANDLER);
    params = new ModifiableSolrParams();
  }
  
  @Override
  public Collection<ContentStream> getContentStreams() {
    return null;
  }
  
  @Override
  public ModifiableSolrParams getParams() {
    return params;
  }
  
  @Override
  public SolrPingResponse process(SolrServer server)
      throws SolrServerException, IOException {
    long startTime = System.currentTimeMillis();
    SolrPingResponse res = new SolrPingResponse();
    res.setResponse(server.request(this));
    res.setElapsedTime(System.currentTimeMillis() - startTime);
    return res;
  }
  
  /**
   * Remove the action parameter from this request. This will result in the same
   * behavior as {@code SolrPing#setActionPing()}. For Solr server version 4.0
   * and later.
   * 
   * @return this
   */
  public SolrPing removeAction() {
    params.remove(CommonParams.ACTION);
    return this;
  }
  
  /**
   * Set the action parameter on this request to enable. This will delete the
   * health-check file for the Solr core. For Solr server version 4.0 and later.
   * 
   * @return this
   */
  public SolrPing setActionDisable() {
    params.set(CommonParams.ACTION, CommonParams.DISABLE);
    return this;
  }
  
  /**
   * Set the action parameter on this request to enable. This will create the
   * health-check file for the Solr core. For Solr server version 4.0 and later.
   * 
   * @return this
   */
  public SolrPing setActionEnable() {
    params.set(CommonParams.ACTION, CommonParams.ENABLE);
    return this;
  }
  
  /**
   * Set the action parameter on this request to ping. This is the same as not
   * including the action at all. For Solr server version 4.0 and later.
   * 
   * @return this
   */
  public SolrPing setActionPing() {
    params.set(CommonParams.ACTION, CommonParams.PING);
    return this;
  }
}
