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

package org.apache.solr.client.solrj;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 *
 * @since solr 1.3
 */
public abstract class SolrServer implements Serializable
{
  private DocumentObjectBinder binder;

  public UpdateResponse add(Collection<SolrInputDocument> docs ) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(docs);
    return req.process(this);
  }

  public UpdateResponse addBeans(Collection<?> beans ) throws SolrServerException, IOException {
    DocumentObjectBinder binder = this.getBinder();
    ArrayList<SolrInputDocument> docs =  new ArrayList<SolrInputDocument>(beans.size());
    for (Object bean : beans) {
      docs.add(binder.toSolrInputDocument(bean));
    }
    return add(docs);
  }

  public UpdateResponse add(SolrInputDocument doc ) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    return req.process(this);
  }

  public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
    return add(getBinder().toSolrInputDocument(obj));
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
    return optimize(true, true, 1);
  }
  
  public UpdateResponse commit( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException {
    return new UpdateRequest().setAction( UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher ).process( this );
  }

  public UpdateResponse optimize( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException {
    return optimize(waitFlush, waitSearcher, 1);
  }

  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments ) throws SolrServerException, IOException {
    return new UpdateRequest().setAction( UpdateRequest.ACTION.OPTIMIZE, waitFlush, waitSearcher, maxSegments ).process( this );
  }
  
  public UpdateResponse rollback() throws SolrServerException, IOException {
    return new UpdateRequest().rollback().process( this );
  }
  
  public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    return new UpdateRequest().deleteById( id ).process( this );
  }

  public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
    return new UpdateRequest().deleteById( ids ).process( this );
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
  
  public QueryResponse query(SolrParams params, METHOD method) throws SolrServerException {
    return new QueryRequest( params, method ).process( this );
  }

  /**
   * Query solr, and stream the results.  Unlike the standard query, this will 
   * send events for each Document rather then add them to the QueryResponse.
   * 
   * Although this function returns a 'QueryResponse' it should be used with care
   * since it excludes anything that was passed to callback.  Also note that
   * future version may pass even more info to the callback and may not return 
   * the results in the QueryResponse.
   *
   * @since solr 4.0
   */
  public QueryResponse queryAndStreamResponse( SolrParams params, StreamingResponseCallback callback ) throws SolrServerException, IOException
  {
    ResponseParser parser = new StreamingBinaryResponseParser( callback );
    QueryRequest req = new QueryRequest( params );
    req.setStreamingResponseCallback( callback );
    req.setResponseParser( parser );    
    return req.process(this);
  }

  /**
   * SolrServer implementations need to implement how a request is actually processed
   */ 
  public abstract NamedList<Object> request( final SolrRequest request ) throws SolrServerException, IOException;

  public DocumentObjectBinder getBinder() {
    if(binder == null){
      binder = new DocumentObjectBinder();
    }
    return binder;
  }
}
