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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 * 
 *
 * @since solr 1.3
 */
public class LukeRequest extends SolrRequest
{
  private List<String> fields;
  private int numTerms = -1;
  private boolean showSchema = false;
  
  public LukeRequest()
  {
    super( METHOD.GET, "/admin/luke" );
  }

  public LukeRequest( String path )
  {
    super( METHOD.GET, path );
  }

  //---------------------------------------------------------------------------------
  //---------------------------------------------------------------------------------
  
  public void addField( String f )
  {
    if( fields == null ) {
      fields = new ArrayList<String>();
    }
    fields.add( f );
  }

  public void setFields( List<String> f )
  {
    fields = f;
  }
  
  //---------------------------------------------------------------------------------
  //---------------------------------------------------------------------------------
  
  public boolean isShowSchema() {
    return showSchema;
  }

  public void setShowSchema(boolean showSchema) {
    this.showSchema = showSchema;
  }

  public int getNumTerms() {
    return numTerms;
  }

  /**
   * the number of terms to return for a given field.  If the number is 0, it will not traverse the terms.  
   */
  public void setNumTerms(int count) {
    this.numTerms = count;
  }

  //---------------------------------------------------------------------------------
  //---------------------------------------------------------------------------------
  
  @Override
  public Collection<ContentStream> getContentStreams() {
    return null;
  }

  @Override
  public SolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if( fields != null && fields.size() > 0 ) {
      params.add( CommonParams.FL, fields.toArray( new String[fields.size()] ) );
    }
    if( numTerms >= 0 ) {
      params.add( "numTerms", numTerms+"" );
    }
    if (showSchema) {
    	params.add("show", "schema");
    }
    return params;
  }

  @Override
  public LukeResponse process( SolrServer server ) throws SolrServerException, IOException 
  {
    long startTime = System.currentTimeMillis();
    LukeResponse res = new LukeResponse();
    res.setResponse( server.request( this ) );
    res.setElapsedTime( System.currentTimeMillis()-startTime );
    return res;
  }
}

