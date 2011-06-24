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
import java.util.List;
import java.util.Arrays;

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
 * This class is experimental and subject to change.
 *
 * @since solr 1.3
 */
public class CoreAdminRequest extends SolrRequest
{
  protected String core = null;
  protected String other = null;
  protected CoreAdminParams.CoreAdminAction action = null;
  
  //a create core request
  public static class Create extends CoreAdminRequest {
    protected String instanceDir;
    protected String configName = null;
    protected String schemaName = null;
    protected String dataDir = null;

    public Create() {
      action = CoreAdminAction.CREATE;
    }
    
    public void setInstanceDir(String instanceDir) { this.instanceDir = instanceDir; }
    public void setSchemaName(String schema) { this.schemaName = schema; }
    public void setConfigName(String config) { this.configName = config; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public String getInstanceDir() { return instanceDir; }
    public String getSchemaName()  { return schemaName; }
    public String getConfigName()  { return configName; }
    public String getDataDir() { return dataDir; }

    @Override
    public SolrParams getParams() {
      if( action == null ) {
        throw new RuntimeException( "no action specified!" );
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set( CoreAdminParams.ACTION, action.toString() );
      if( action.equals(CoreAdminAction.CREATE) ) {
        params.set( CoreAdminParams.NAME, core );
      } else {
        params.set( CoreAdminParams.CORE, core );
      }
      params.set( CoreAdminParams.INSTANCE_DIR, instanceDir);
      if (configName != null) {
        params.set( CoreAdminParams.CONFIG, configName);
      }
      if (schemaName != null) {
        params.set( CoreAdminParams.SCHEMA, schemaName);
      }
      if (dataDir != null) {
        params.set( CoreAdminParams.DATA_DIR, dataDir);
      }
      return params;
    }
  }
    //a persist core request
  public static class Persist extends CoreAdminRequest {
    protected String fileName = null;
    
    public Persist() {
      action = CoreAdminAction.PERSIST;
    }
    
    public void setFileName(String name) {
      fileName = name;
    }
    public String getFileName() {
      return fileName;
    }
    @Override
    public SolrParams getParams() {
      if( action == null ) {
        throw new RuntimeException( "no action specified!" );
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set( CoreAdminParams.ACTION, action.toString() );
      if (fileName != null) {
        params.set( CoreAdminParams.FILE, fileName);
      }
      return params;
    }
  }
  
  public static class MergeIndexes extends CoreAdminRequest {
    protected List<String> indexDirs;
    protected List<String> srcCores;

    public MergeIndexes() {
      action = CoreAdminAction.MERGEINDEXES;
    }

    public void setIndexDirs(List<String> indexDirs) {
      this.indexDirs = indexDirs;
    }

    public List<String> getIndexDirs() {
      return indexDirs;
    }

    public List<String> getSrcCores() {
      return srcCores;
    }

    public void setSrcCores(List<String> srcCores) {
      this.srcCores = srcCores;
    }

    @Override
    public SolrParams getParams() {
      if (action == null) {
        throw new RuntimeException("no action specified!");
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, action.toString());
      params.set(CoreAdminParams.CORE, core);
      if (indexDirs != null)  {
        for (String indexDir : indexDirs) {
          params.set(CoreAdminParams.INDEX_DIR, indexDir);
        }
      }
      if (srcCores != null) {
        for (String srcCore : srcCores) {
          params.set(CoreAdminParams.SRC_CORE, srcCore);
        }
      }
      return params;
    }
  }

  public static class Unload extends CoreAdminRequest {
    protected boolean deleteIndex;

    public Unload(boolean deleteIndex) {
      action = CoreAdminAction.UNLOAD;
      this.deleteIndex = deleteIndex;
    }

    public boolean isDeleteIndex() {
      return deleteIndex;
    }

    public void setDeleteIndex(boolean deleteIndex) {
      this.deleteIndex = deleteIndex;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.DELETE_INDEX, deleteIndex);
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

  public final void setCoreName( String coreName )
  {
    this.core = coreName;
  }

  public final void setOtherCoreName( String otherCoreName )
  {
    this.other = otherCoreName;
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
    if (other != null) {
      params.set(CoreAdminParams.OTHER, other);
    }
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
    req.setCoreName( name );
    req.setAction( CoreAdminAction.RELOAD );
    return req.process( server );
  }

  public static CoreAdminResponse unloadCore( String name, SolrServer server ) throws SolrServerException, IOException
  {
    return unloadCore(name, false, server);
  }

  public static CoreAdminResponse unloadCore( String name, boolean deleteIndex, SolrServer server ) throws SolrServerException, IOException
  {
    Unload req = new Unload(deleteIndex);
    req.setCoreName( name );
    return req.process( server );
  }

  public static CoreAdminResponse renameCore(String coreName, String newName, SolrServer server ) throws SolrServerException, IOException
  {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreName(coreName);
    req.setOtherCoreName(newName);
    req.setAction( CoreAdminAction.RENAME );
    return req.process( server );
  }

  public static CoreAdminResponse aliasCore(String coreName, String newName, SolrServer server ) throws SolrServerException, IOException
  {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreName(coreName);
    req.setOtherCoreName(newName);
    req.setAction( CoreAdminAction.ALIAS );
    return req.process( server );
  }

  public static CoreAdminResponse getStatus( String name, SolrServer server ) throws SolrServerException, IOException
  {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreName( name );
    req.setAction( CoreAdminAction.STATUS );
    return req.process( server );
  }
  
  public static CoreAdminResponse createCore( String name, String instanceDir, SolrServer server ) throws SolrServerException, IOException 
  {
    return CoreAdminRequest.createCore(name, instanceDir, server, null, null);
  }
  
  public static CoreAdminResponse createCore( String name, String instanceDir, SolrServer server, String configFile, String schemaFile ) throws SolrServerException, IOException 
  {
    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName( name );
    req.setInstanceDir(instanceDir);
    if(configFile != null){
      req.setConfigName(configFile);
    }
    if(schemaFile != null){
      req.setSchemaName(schemaFile);
    }
    return req.process( server );
  }

  public static CoreAdminResponse persist(String fileName, SolrServer server) throws SolrServerException, IOException 
  {
    CoreAdminRequest.Persist req = new CoreAdminRequest.Persist();
    req.setFileName(fileName);
    return req.process(server);
  }

  public static CoreAdminResponse mergeIndexes(String name,
      String[] indexDirs, String[] srcCores, SolrServer server) throws SolrServerException,
      IOException {
    CoreAdminRequest.MergeIndexes req = new CoreAdminRequest.MergeIndexes();
    req.setCoreName(name);
    req.setIndexDirs(Arrays.asList(indexDirs));
    req.setSrcCores(Arrays.asList(srcCores));
    return req.process(server);
  }
}
