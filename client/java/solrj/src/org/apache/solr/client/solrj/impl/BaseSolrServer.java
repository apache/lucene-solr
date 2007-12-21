/**
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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

/**
 * Base class that converts the server request into the more verbose general request framework.
 * 
 * @version $Id$
 * @since solr 1.3
 */
public abstract class BaseSolrServer implements SolrServer 
{
  protected String defaultCore = null;
  
  public UpdateResponse add(Collection<SolrInputDocument> docs, boolean overwrite ) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(docs);
    req.setOverwrite(overwrite);
    return req.process(this);
  }

  public UpdateResponse add(SolrInputDocument doc, boolean overwrite ) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setOverwrite(overwrite);
    return req.process(this);
  }

  public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
    return add(doc, true);
  }

  public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    return add(docs, true);
  }

  /** waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * @throws IOException 
   */
  public UpdateResponse commit( ) throws SolrServerException, IOException {
    return commit(true, true);
  }

  /** waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * @throws IOException 
   */
  public UpdateResponse optimize( ) throws SolrServerException, IOException {
    return optimize(true, true);
  }
  
  public UpdateResponse commit( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException {
    return new UpdateRequest().setAction( UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher ).process( this );
  }

  public UpdateResponse optimize( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException {
    return new UpdateRequest().setAction( UpdateRequest.ACTION.OPTIMIZE, waitFlush, waitSearcher ).process( this );
  }

  public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    return new UpdateRequest().deleteById( id ).process( this );
  }

  public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
    return new UpdateRequest().deleteByQuery( query ).process( this );
  }

  public SolrPingResponse ping() throws SolrServerException, IOException {
    return new SolrPing().process( this );
  }

  public QueryResponse query(SolrParams params) throws SolrServerException {
    return new QueryRequest( params ).process( this );
  }

  public String getDefaultCore() {
    return defaultCore;
  }

  public void setDefaultCore(String defaultCore) {
    this.defaultCore = defaultCore;
  }
}
