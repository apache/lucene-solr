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

package org.apache.solr.client.solrj.request;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.XML;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class UpdateRequest extends SolrRequest
{
  public enum ACTION {
    COMMIT,
    OPTIMIZE
  };
  
  private List<SolrInputDocument> documents = null;
  private List<String> deleteById = null;
  private List<String> deleteQuery = null;

  private ModifiableSolrParams params;
  
  public UpdateRequest()
  {
    super( METHOD.POST, "/update" );
  }

  public UpdateRequest(String url) {
    super(METHOD.POST, url);
  }

  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  
  /**
   * clear the pending documents and delete commands
   */
  public void clear()
  {
    if( documents != null ) {
      documents.clear();
    }
    if( deleteById != null ) {
      deleteById.clear();
    }
    if( deleteQuery != null ) {
      deleteQuery.clear();
    }
  }
  
  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  
  public UpdateRequest add( final SolrInputDocument doc )
  {
    if( documents == null ) {
      documents = new ArrayList<SolrInputDocument>( 2 );
    }
    documents.add( doc );
    return this;
  }
  
  public UpdateRequest add( final Collection<SolrInputDocument> docs )
  {
    if( documents == null ) {
      documents = new ArrayList<SolrInputDocument>( docs.size()+1 );
    }
    documents.addAll( docs );
    return this;
  }
  
  public UpdateRequest deleteById( String id )
  {
    if( deleteById == null ) {
      deleteById = new ArrayList<String>();
    }
    deleteById.add( id );
    return this;
  }
  
  public UpdateRequest deleteByQuery( String q )
  {
    if( deleteQuery == null ) {
      deleteQuery = new ArrayList<String>();
    }
    deleteQuery.add( q );
    return this;
  }

  /** Sets appropriate parameters for the given ACTION */
  public UpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher ) {
    return setAction(action, waitFlush, waitSearcher, 1);
  }

  public UpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher, int maxSegments ) {
    if (params == null)
      params = new ModifiableSolrParams();

    if( action == ACTION.OPTIMIZE ) {
      params.set( UpdateParams.OPTIMIZE, "true" );
      params.set(UpdateParams.MAX_OPTIMIZE_SEGMENTS, maxSegments);
    }
    else if( action == ACTION.COMMIT ) {
      params.set( UpdateParams.COMMIT, "true" );
    }
    params.set( UpdateParams.WAIT_FLUSH, waitFlush+"" );
    params.set( UpdateParams.WAIT_SEARCHER, waitSearcher+"" );
    return this;
  }
  

  public void setParam(String param, String value) {
    if (params == null)
      params = new ModifiableSolrParams();
    params.set(param, value);
  }

  /** Sets the parameters for this update request, overwriting any previous */
  public void setParams(ModifiableSolrParams params) {
    this.params = params;
  }

  //--------------------------------------------------------------------------
  //--------------------------------------------------------------------------

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return ClientUtils.toContentStreams( getXML(), ClientUtils.TEXT_XML );
  }
  
  public String getXML() throws IOException {
    StringWriter writer = new StringWriter();
    if( documents != null && documents.size() > 0 ) {
      writer.write("<add>");
      for (SolrInputDocument doc : documents ) {
        if( doc != null ) {
          ClientUtils.writeXML( doc, writer );
        }
      }
      writer.write("</add>");
    }
    
    // Add the delete commands
    boolean deleteI = deleteById != null && deleteById.size() > 0;
    boolean deleteQ = deleteQuery != null && deleteQuery.size() > 0;
    if( deleteI || deleteQ ) {
      writer.append( "<delete>" );
      if( deleteI ) {
        for( String id : deleteById ) {
          writer.append( "<id>" );
          XML.escapeCharData( id, writer );
          writer.append( "</id>" );
        }
      }
      if( deleteQ ) {
        for( String q : deleteQuery ) {
          writer.append( "<query>" );
          XML.escapeCharData( q, writer );
          writer.append( "</query>" );
        }
      }
      writer.append( "</delete>" );
    }
    
    // If action is COMMIT or OPTIMIZE, it is sent with params
    String xml = writer.toString();
    //System.out.println( "SEND:"+xml );
    return (xml.length() > 0) ? xml : null;
  }


  //--------------------------------------------------------------------------
  //--------------------------------------------------------------------------

  @Override
  public ModifiableSolrParams getParams() {
    return params;
  }
  
  @Override
  public UpdateResponse process( SolrServer server ) throws SolrServerException, IOException
  {
    long startTime = System.currentTimeMillis();
    UpdateResponse res = new UpdateResponse();
    res.setResponse( server.request( this ) );
    res.setElapsedTime( System.currentTimeMillis()-startTime );
    return res;
  }
  
  //--------------------------------------------------------------------------
  // 
  //--------------------------------------------------------------------------

  public boolean isWaitFlush() {
    return params != null && params.getBool(UpdateParams.WAIT_FLUSH, false);
  }

  public boolean isWaitSearcher() {
    return params != null && params.getBool(UpdateParams.WAIT_SEARCHER, false);
  }

  public ACTION getAction() {
    if (params==null) return null;
    if (params.getBool(UpdateParams.COMMIT, false)) return ACTION.COMMIT; 
    if (params.getBool(UpdateParams.OPTIMIZE, false)) return ACTION.OPTIMIZE;
    return null;
  }

  public void setWaitFlush(boolean waitFlush) {
    setParam( UpdateParams.WAIT_FLUSH, waitFlush+"" );
  }

  public void setWaitSearcher(boolean waitSearcher) {
    setParam( UpdateParams.WAIT_SEARCHER, waitSearcher+"" );
  }
}
