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
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class LukeRequest extends RequestBase
{
  private List<String> fields;
  private int count = -1;
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
  
  //---------------------------------------------------------------------------------
  //---------------------------------------------------------------------------------
  
  public boolean isShowSchema() {
    return showSchema;
  }

  public void setShowSchema(boolean showSchema) {
    this.showSchema = showSchema;
  }

  public Collection<ContentStream> getContentStreams() {
    return null;
  }

  public SolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if( fields != null && fields.size() > 0 ) {
      params.add( CommonParams.FL, fields.toArray( new String[fields.size()] ) );
    }
    if( count >= 0 ) {
      params.add( "count", count+"" );
    }
    if (showSchema) {
    	params.add("show", "schema");
    }
    return params;
  }

  public LukeResponse process( SolrServer server ) throws SolrServerException, IOException 
  {
    long startTime = System.currentTimeMillis();
    LukeResponse res = new LukeResponse( server.request( this ) );
    res.setElapsedTime( System.currentTimeMillis()-startTime );
    return res;
  }
}

