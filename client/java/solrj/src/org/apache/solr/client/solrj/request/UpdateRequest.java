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
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.XML;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class UpdateRequest extends RequestBase
{
  public enum ACTION {
    COMMIT,
    OPTIMIZE
  };
  
  private boolean waitFlush = true;
  private boolean waitSearcher = true;
  private boolean allowDups = false;
  private boolean overwriteCommitted = false;
  private boolean overwritePending = false;
  private ACTION action = null;
  
  private List<SolrInputDocument> documents = null;
  private List<String> deleteById = null;
  private List<String> deleteQuery = null;
  
  public UpdateRequest()
  {
    super( METHOD.POST, "/update" );
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

  public UpdateRequest setAction(ACTION action, boolean waitFlush, boolean waitSearcher ) {
    this.action = action;
    this.waitFlush = waitFlush;
    this.waitSearcher = waitSearcher;
    return this;
  }

  //--------------------------------------------------------------------------
  //--------------------------------------------------------------------------

  public Collection<ContentStream> getContentStreams() throws IOException {
    return ClientUtils.toContentStreams( getXML(), ClientUtils.TEXT_XML );
  }
  
  public String getXML() throws IOException {
    StringWriter writer = new StringWriter();
    if( documents != null && documents.size() > 0 ) {
      writer.write("<add ");
      writer.write("allowDups=\"" + allowDups + "\" ");
      writer.write("overwriteCommitted=\"" + overwriteCommitted + "\" ");
      writer.write("overwritePending=\"" + overwritePending + "\">");
      for (SolrInputDocument doc : documents ) {
        if( doc != null ) {
          ClientUtils.writeXML( doc, writer );
        }
      }
      writer.write("</add>");
    }
    
    // Add the delete commands
    if( deleteById != null || deleteQuery != null ) {
      writer.append( "<delete>" );
      if( deleteById != null ) {
        for( String id : deleteById ) {
          writer.append( "<id>" );
          XML.escapeCharData( id, writer );
          writer.append( "</id>" );
        }
      }
      if( deleteQuery != null ) {
        for( String q : deleteQuery ) {
          writer.append( "<query>" );
          XML.escapeCharData( q, writer );
          writer.append( "</query>" );
        }
      }
      writer.append( "</delete>" );
    }
    
    // add the commits
    if (action == ACTION.COMMIT) {
      writer.append("<commit ");
      writer.append("waitFlush=\"" + waitFlush + "\" ");
      writer.append("waitSearcher=\"" + waitSearcher + "\" ");
      writer.append(">");
      writer.append("</commit>");
    }
    
    // add the optimizes
    if (action == ACTION.OPTIMIZE) {
      writer.append("<optimize ");
      writer.append("waitFlush=\"" + waitFlush + "\" ");
      writer.append("waitSearcher=\"" + waitSearcher + "\" ");
      writer.append(">");
      writer.append("</optimize>");
    }
    return writer.toString();
  }

  //--------------------------------------------------------------------------
  //--------------------------------------------------------------------------

  public SolrParams getParams() {
    if( action != null ) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      if( action == ACTION.OPTIMIZE ) {
        params.set( UpdateParams.OPTIMIZE, "true" );
      }
      else if( action == ACTION.COMMIT ) {
        params.set( UpdateParams.COMMIT, "true" );
      }
      params.set( UpdateParams.WAIT_FLUSH, waitFlush+"" );
      params.set( UpdateParams.WAIT_SEARCHER, waitSearcher+"" );
      return params;
    }
    return null; 
  }
  
  public UpdateResponse process( SolrServer server ) throws SolrServerException, IOException
  {
    long startTime = System.currentTimeMillis();
    UpdateResponse res = new UpdateResponse( server.request( this ) );
    res.setElapsedTime( System.currentTimeMillis()-startTime );
    return res;
  }

  //--------------------------------------------------------------------------
  // 
  //--------------------------------------------------------------------------

  public void setOverwrite( boolean v )
  {
    allowDups = !v;
    overwriteCommitted = v;
    overwritePending = v;
  }
  
  //--------------------------------------------------------------------------
  // 
  //--------------------------------------------------------------------------

  public boolean isWaitFlush() {
    return waitFlush;
  }

  public boolean isWaitSearcher() {
    return waitSearcher;
  }

  public ACTION getAction() {
    return action;
  }

  public boolean isAllowDups() {
    return allowDups;
  }

  /**
   * Use setOverwrite()
   */
  @Deprecated
  public void setAllowDups(boolean allowDups) {
    this.allowDups = allowDups;
  }

  public boolean isOverwriteCommitted() {
    return overwriteCommitted;
  }

  /**
   * Use setOverwrite()
   */
  @Deprecated
  public void setOverwriteCommitted(boolean overwriteCommitted) {
    this.overwriteCommitted = overwriteCommitted;
  }

  public boolean isOverwritePending() {
    return overwritePending;
  }

  /**
   * Use setOverwrite()
   */
  @Deprecated
  public void setOverwritePending(boolean overwritePending) {
    this.overwritePending = overwritePending;
  }

  public void setWaitFlush(boolean waitFlush) {
    this.waitFlush = waitFlush;
  }

  public void setWaitSearcher(boolean waitSearcher) {
    this.waitSearcher = waitSearcher;
  }
}
