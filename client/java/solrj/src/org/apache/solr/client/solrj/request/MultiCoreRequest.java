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
import java.util.Collection;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.MultiCoreResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiCoreParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.MultiCoreParams.MultiCoreAction;
import org.apache.solr.common.util.ContentStream;

/**
 * 
 * @version $Id: MultiCoreRequest.java 606335 2007-12-21 22:23:39Z ryan $
 * @since solr 1.3
 */
public class MultiCoreRequest extends SolrRequest
{
  protected String core = null;
  protected MultiCoreParams.MultiCoreAction action = null;
  
  //a create core request
  public static class Create extends MultiCoreRequest {
    protected String instanceDir;
    protected String configName = null;
    protected String schemaName = null;
    
    public Create() {
      action = MultiCoreAction.CREATE;
    }
    
    public void setInstanceDir(String instanceDir) { this.instanceDir = instanceDir; }
    public void setSchemaName(String schema) { this.schemaName = schema; }
    public void setConfigName(String config) { this.configName = config; }
    
    public String getInstanceDir() { return instanceDir; }
    public String getSchemaName()  { return schemaName; }
    public String getConfigName()  { return configName; }
    
    @Override
    public SolrParams getParams() {
      if( action == null ) {
        throw new RuntimeException( "no action specified!" );
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set( MultiCoreParams.ACTION, action.toString() );
      params.set( MultiCoreParams.CORE, core );
      params.set( MultiCoreParams.INSTANCE_DIR, instanceDir);
      if (configName != null) {
        params.set( MultiCoreParams.CONFIG, configName);
      }
      if (schemaName != null) {
        params.set( MultiCoreParams.SCHEMA, schemaName);
      }
      return params;
    }
  }
  
  public MultiCoreRequest()
  {
    super( METHOD.GET, "/admin/multicore" );
  }

  public MultiCoreRequest( String path )
  {
    super( METHOD.GET, path );
  }

  public final void setCoreParam( String v )
  {
    this.core = v;
  }
  
  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  public void setAction( MultiCoreAction action )
  {
    this.action = action;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  @Override
  public SolrParams getParams() 
  {
    if( action == null ) {
      throw new RuntimeException( "no action specified!" );
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set( MultiCoreParams.ACTION, action.toString() );
    params.set( MultiCoreParams.CORE, core );
    return params;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return null;
  }

  @Override
  public MultiCoreResponse process(SolrServer server) throws SolrServerException, IOException 
  {
    long startTime = System.currentTimeMillis();
    MultiCoreResponse res = new MultiCoreResponse( server.request( this ) );
    res.setElapsedTime( System.currentTimeMillis()-startTime );
    return res;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  public static MultiCoreResponse reloadCore( String name, SolrServer server ) throws SolrServerException, IOException
  {
    MultiCoreRequest req = new MultiCoreRequest();
    req.setCoreParam( name );
    req.setAction( MultiCoreAction.RELOAD );
    return req.process( server );
  }

  public static MultiCoreResponse getStatus( String name, SolrServer server ) throws SolrServerException, IOException
  {
    MultiCoreRequest req = new MultiCoreRequest();
    req.setCoreParam( name );
    req.setAction( MultiCoreAction.STATUS );
    return req.process( server );
  }
  
  public static MultiCoreResponse createCore( String name, String instanceDir, SolrServer server ) throws SolrServerException, IOException 
  {
    MultiCoreRequest.Create req = new MultiCoreRequest.Create();
    req.setCoreParam( name );
    req.setInstanceDir(instanceDir);
    return req.process( server );
  }
}

