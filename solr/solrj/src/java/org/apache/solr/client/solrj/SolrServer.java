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
  private static final long serialVersionUID = 1L;
  private DocumentObjectBinder binder;

  /**
   * Adds a collection of documents
   * @param docs  the collection of documents
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    return add(docs, -1);
  }

  /**
   * Adds a collection of documents, specifying max time before they become committed
   * @param docs  the collection of documents
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws SolrServerException
   * @throws IOException
   * @since solr 3.5
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(docs);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Adds a collection of beans
   * @param beans  the collection of beans
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse addBeans(Collection<?> beans ) throws SolrServerException, IOException {
    return addBeans(beans, -1);
  }
  
  /**
   * Adds a collection of beans specifying max time before they become committed
   * @param beans  the collection of beans
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws SolrServerException
   * @throws IOException
   * @since solr 3.5
   */
  public UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    DocumentObjectBinder binder = this.getBinder();
    ArrayList<SolrInputDocument> docs =  new ArrayList<SolrInputDocument>(beans.size());
    for (Object bean : beans) {
      docs.add(binder.toSolrInputDocument(bean));
    }
    return add(docs, commitWithinMs);
  }

  /**
   * Adds a single document
   * @param doc  the input document
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse add(SolrInputDocument doc ) throws SolrServerException, IOException {
    return add(doc, -1);
  }

  /**
   * Adds a single document specifying max time before it becomes committed
   * @param doc  the input document
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws SolrServerException
   * @throws IOException
   * @since solr 3.5
   */
  public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Adds a single bean
   * @param obj  the input bean
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
    return addBean(obj, -1);
  }

  /**
   * Adds a single bean specifying max time before it becomes committed
   * @param obj  the input bean
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws SolrServerException
   * @throws IOException
   * @since solr 3.5
   */
  public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
    return add(getBinder().toSolrInputDocument(obj),commitWithinMs);
  }

  /** 
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * <p>
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * @throws SolrServerException
   * @throws IOException 
   */
  public UpdateResponse commit( ) throws SolrServerException, IOException {
    return commit(true, true);
  }

  /** 
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @throws SolrServerException
   * @throws IOException 
   */
  public UpdateResponse optimize( ) throws SolrServerException, IOException {
    return optimize(true, true, 1);
  }
  
  /** 
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible 
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse commit( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException {
    return new UpdateRequest().setAction( UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher ).process( this );
  }

  /** 
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible 
   * @throws SolrServerException
   * @throws IOException 
   */
  public UpdateResponse optimize( boolean waitFlush, boolean waitSearcher ) throws SolrServerException, IOException {
    return optimize(waitFlush, waitSearcher, 1);
  }

  /** 
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible 
   * @param maxSegments  optimizes down to at most this number of segments
   * @throws SolrServerException
   * @throws IOException 
   */
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments ) throws SolrServerException, IOException {
    return new UpdateRequest().setAction( UpdateRequest.ACTION.OPTIMIZE, waitFlush, waitSearcher, maxSegments ).process( this );
  }
  
  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have been committed due to autoCommit, buffer full, other client performing
   * a commit etc.
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse rollback() throws SolrServerException, IOException {
    return new UpdateRequest().rollback().process( this );
  }
  
  /**
   * Deletes a single document by unique ID
   * @param id  the ID of the document to delete
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    return new UpdateRequest().deleteById( id ).process( this );
  }

  /**
   * Deletes a list of documents by unique ID
   * @param ids  the list of document IDs to delete 
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
    return new UpdateRequest().deleteById( ids ).process( this );
  }

  /**
   * Deletes documents from the index based on a query
   * @param query  the query expressing what documents to delete
   * @throws SolrServerException
   * @throws IOException
   */
  public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
    return new UpdateRequest().deleteByQuery( query ).process( this );
  }

  /**
   * Issues a ping request to check if the server is alive
   * @throws SolrServerException
   * @throws IOException
   */
  public SolrPingResponse ping() throws SolrServerException, IOException {
    return new SolrPing().process( this );
  }

  /**
   * Performs a query to the Solr server
   * @param params  an object holding all key/value parameters to send along the request
   * @throws SolrServerException
   */
  public QueryResponse query(SolrParams params) throws SolrServerException {
    return new QueryRequest( params ).process( this );
  }
  
  /**
   * Performs a query to the Solr server
   * @param params  an object holding all key/value parameters to send along the request
   * @param method  specifies the HTTP method to use for the request, such as GET or POST
   * @throws SolrServerException
   */
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
