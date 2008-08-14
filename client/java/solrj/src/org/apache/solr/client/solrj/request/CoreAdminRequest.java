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
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.ContentStream;

/**
 * 
 * @version $Id: CoreAdminRequest.java 606335 2007-12-21 22:23:39Z ryan $
 * @since solr 1.3
 */
public class CoreAdminRequest extends SolrRequest
{
  protected String core = null;
  protected CoreAdminParams.CoreAdminAction action = null;
  
  //a create core request
  public static class Create extends CoreAdminRequest {
    protected String instanceDir;
    protected String configName = null;
    protected String schemaName = null;
    
    public Create() {
      action = CoreAdminAction.CREATE;
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
      params.set( CoreAdminParams.ACTION, action.toString() );
      params.set( CoreAdminParams.CORE, core );
      params.set( CoreAdminParams.INSTANCE_DIR, instanceDir);
      if (configName != null) {
        params.set( CoreAdminParams.CONFIG, configName);
      }
      if (schemaName != null) {
        params.set( CoreAdminParams.SCHEMA, schemaName);
      }
      return params;
    }
  }
  
  public CoreAdminRequest()
  {
    super( METHOD.GET, "/admin/cores" );
  }

  public CoreAdminRequest( String path )
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

  public void setAction( CoreAdminAction action )
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
    params.set( CoreAdminParams.ACTION, action.toString() );
    params.set( CoreAdminParams.CORE, core );
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
  public CoreAdminResponse process(SolrServer server) throws SolrServerException, IOException 
  {
    long startTime = System.currentTimeMillis();
    CoreAdminResponse res = new CoreAdminResponse();
    res.setResponse( server.request( this ) );
    res.setElapsedTime( System.currentTimeMillis()-startTime );
    return res;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  public static CoreAdminResponse reloadCore( String name, SolrServer server ) throws SolrServerException, IOException
  {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreParam( name );
    req.setAction( CoreAdminAction.RELOAD );
    return req.process( server );
  }

  public static CoreAdminResponse getStatus( String name, SolrServer server ) throws SolrServerException, IOException
  {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreParam( name );
    req.setAction( CoreAdminAction.STATUS );
    return req.process( server );
  }
  
  public static CoreAdminResponse createCore( String name, String instanceDir, SolrServer server ) throws SolrServerException, IOException 
  {
    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreParam( name );
    req.setInstanceDir(instanceDir);
    return req.process( server );
  }
}

