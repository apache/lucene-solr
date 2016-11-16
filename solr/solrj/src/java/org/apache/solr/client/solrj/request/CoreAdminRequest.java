/*
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 1.3
 */
public class CoreAdminRequest extends SolrRequest<CoreAdminResponse> {

  protected String core = null;
  protected String other = null;
  protected boolean isIndexInfoNeeded = true;
  protected CoreAdminParams.CoreAdminAction action = null;
  
  //a create core request
  public static class Create extends CoreAdminRequest {

    protected String instanceDir;
    protected String configName = null;
    protected String schemaName = null;
    protected String dataDir = null;
    protected String ulogDir = null;
    protected String configSet = null;
    protected String collection;
    private Integer numShards;
    private String shardId;
    private String roles;
    private String coreNodeName;
    private Boolean loadOnStartup;
    private Boolean isTransient;
    private String collectionConfigName;

    public Create() {
      action = CoreAdminAction.CREATE;
    }
    
    public void setInstanceDir(String instanceDir) { this.instanceDir = instanceDir; }
    public void setSchemaName(String schema) { this.schemaName = schema; }
    public void setConfigName(String config) { this.configName = config; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public void setUlogDir(String ulogDir) { this.ulogDir = ulogDir; }
    public void setConfigSet(String configSet) {
      this.configSet = configSet;
    }
    public void setCollection(String collection) { this.collection = collection; }
    public void setNumShards(int numShards) {this.numShards = numShards;}
    public void setShardId(String shardId) {this.shardId = shardId;}
    public void setRoles(String roles) {this.roles = roles;}
    public void setCoreNodeName(String coreNodeName) {this.coreNodeName = coreNodeName;}
    public void setIsTransient(Boolean isTransient) { this.isTransient = isTransient; }
    public void setIsLoadOnStartup(Boolean loadOnStartup) { this.loadOnStartup = loadOnStartup;}
    public void setCollectionConfigName(String name) { this.collectionConfigName = name;}

    public String getInstanceDir() { return instanceDir; }
    public String getSchemaName()  { return schemaName; }
    public String getConfigName()  { return configName; }
    public String getDataDir() { return dataDir; }
    public String getUlogDir() { return ulogDir; }
    public String getConfigSet() {
      return configSet;
    }
    public String getCollection() { return collection; }
    public String getShardId() { return shardId; }
    public String getRoles() { return roles; }
    public String getCoreNodeName() { return coreNodeName; }
    public Boolean getIsLoadOnStartup() { return loadOnStartup; }
    public Boolean getIsTransient() { return isTransient; }
    public String getCollectionConfigName() { return collectionConfigName;}
    
    /**
     * Provide the name of the core to be created.
     * 
     * Core names must consist entirely of periods, underscores and alphanumerics.  Other characters are not allowed.
     * 
     * @throws IllegalArgumentException if the core name contains invalid characters.
     */
    @Override
    public void setCoreName(String coreName) {
      this.core = SolrIdentifierValidator.validateCoreName(coreName);
    }
    
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
      if (ulogDir != null) {
        params.set( CoreAdminParams.ULOG_DIR, ulogDir);
      }
      if (configSet != null) {
        params.set( CoreAdminParams.CONFIGSET, configSet);
      }
      if (collection != null) {
        params.set( CoreAdminParams.COLLECTION, collection);
      }
      if (numShards != null) {
        params.set( ZkStateReader.NUM_SHARDS_PROP, numShards);
      }
      if (shardId != null) {
        params.set( CoreAdminParams.SHARD, shardId);
      }
      if (roles != null) {
        params.set( CoreAdminParams.ROLES, roles);
      }
      if (coreNodeName != null) {
        params.set( CoreAdminParams.CORE_NODE_NAME, coreNodeName);
      }

      if (isTransient != null) {
        params.set(CoreAdminParams.TRANSIENT, isTransient);
      }

      if (loadOnStartup != null) {
        params.set(CoreAdminParams.LOAD_ON_STARTUP, loadOnStartup);
      }
      
      if (collectionConfigName != null) {
        params.set("collection.configName", collectionConfigName);
      }
      
      return params;
    }

  }
  
  public static class WaitForState extends CoreAdminRequest {
    protected String nodeName;
    protected String coreNodeName;
    protected Replica.State state;
    protected Boolean checkLive;
    protected Boolean onlyIfLeader;
    protected Boolean onlyIfLeaderActive;

    public WaitForState() {
      action = CoreAdminAction.PREPRECOVERY;
    }
    
    public void setNodeName(String nodeName) {
      this.nodeName = nodeName;
    }
    
    public String getNodeName() {
      return nodeName;
    }
    
    public String getCoreNodeName() {
      return coreNodeName;
    }

    public void setCoreNodeName(String coreNodeName) {
      this.coreNodeName = coreNodeName;
    }

    public Replica.State getState() {
      return state;
    }

    public void setState(Replica.State state) {
      this.state = state;
    }

    public Boolean getCheckLive() {
      return checkLive;
    }

    public void setCheckLive(Boolean checkLive) {
      this.checkLive = checkLive;
    }
    
    public boolean isOnlyIfLeader() {
      return onlyIfLeader;
    }

    public void setOnlyIfLeader(boolean onlyIfLeader) {
      this.onlyIfLeader = onlyIfLeader;
    }
    
    public void setOnlyIfLeaderActive(boolean onlyIfLeaderActive) {
      this.onlyIfLeaderActive = onlyIfLeaderActive;
    }
    
    @Override
    public SolrParams getParams() {
      if( action == null ) {
        throw new RuntimeException( "no action specified!" );
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set( CoreAdminParams.ACTION, action.toString() );
 
      params.set( CoreAdminParams.CORE, core );
      
      if (nodeName != null) {
        params.set( "nodeName", nodeName);
      }
      
      if (coreNodeName != null) {
        params.set( "coreNodeName", coreNodeName);
      }
      
      if (state != null) {
        params.set(ZkStateReader.STATE_PROP, state.toString());
      }
      
      if (checkLive != null) {
        params.set( "checkLive", checkLive);
      }
      
      if (onlyIfLeader != null) {
        params.set( "onlyIfLeader", onlyIfLeader);
      }
      
      if (onlyIfLeaderActive != null) {
        params.set( "onlyIfLeaderActive", onlyIfLeaderActive);
      }

      return params;
    }
    
    public String toString() {
      if (action != null) {
        return "WaitForState: "+getParams();
      } else {
        return super.toString();
      }
    }
  }
  
  public static class RequestRecovery extends CoreAdminRequest {

    public RequestRecovery() {
      action = CoreAdminAction.REQUESTRECOVERY;
    }

    @Override
    public SolrParams getParams() {
      if( action == null ) {
        throw new RuntimeException( "no action specified!" );
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set( CoreAdminParams.ACTION, action.toString() );
 
      params.set( CoreAdminParams.CORE, core );

      return params;
    }
  }
  
  public static class RequestSyncShard extends CoreAdminRequest {
    private String shard;
    private String collection;
    
    public RequestSyncShard() {
      action = CoreAdminAction.REQUESTSYNCSHARD;
    }

    @Override
    public SolrParams getParams() {
      if( action == null ) {
        throw new RuntimeException( "no action specified!" );
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, action.toString());
      params.set("shard", shard);
      params.set("collection", collection);
      params.set(CoreAdminParams.CORE, core);
      return params;
    }

    public String getShard() {
      return shard;
    }

    public void setShard(String shard) {
      this.shard = shard;
    }

    public String getCollection() {
      return collection;
    }

    public void setCollection(String collection) {
      this.collection = collection;
    }
  }
  
  public static class OverrideLastPublished extends CoreAdminRequest {
    protected String state;

    public OverrideLastPublished() {
      action = CoreAdminAction.FORCEPREPAREFORLEADERSHIP;
    }

    @Override
    public SolrParams getParams() {
      if( action == null ) {
        throw new RuntimeException( "no action specified!" );
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, action.toString());
      params.set(CoreAdminParams.CORE, core);
      params.set(ZkStateReader.STATE_PROP, state);
      return params;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
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
          params.add(CoreAdminParams.INDEX_DIR, indexDir);
        }
      }
      if (srcCores != null) {
        for (String srcCore : srcCores) {
          params.add(CoreAdminParams.SRC_CORE, srcCore);
        }
      }
      return params;
    }
  }

  public static class Unload extends CoreAdminRequest {
    protected boolean deleteIndex;
    protected boolean deleteDataDir;
    protected boolean deleteInstanceDir;

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

    public void setDeleteDataDir(boolean deleteDataDir) {
     this.deleteDataDir = deleteDataDir; 
    }

    public void setDeleteInstanceDir(boolean deleteInstanceDir){
        this.deleteInstanceDir = deleteInstanceDir;
    }

    public boolean isDeleteDataDir() {
      return deleteDataDir;
    }

    public boolean isDeleteInstanceDir() {
      return deleteInstanceDir;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.DELETE_INDEX, deleteIndex);
      params.set(CoreAdminParams.DELETE_DATA_DIR, deleteDataDir);
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, deleteInstanceDir);
      return params;
    }

  }

  public static class CreateSnapshot extends CoreAdminRequest {
    private String commitName;

    public CreateSnapshot(String commitName) {
      super();
      this.action = CoreAdminAction.CREATESNAPSHOT;
      if(commitName == null) {
        throw new NullPointerException("Please specify non null value for commitName parameter.");
      }
      this.commitName = commitName;
    }

    public String getCommitName() {
      return commitName;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.COMMIT_NAME, this.commitName);
      return params;
    }
  }

  public static class DeleteSnapshot extends CoreAdminRequest {
    private String commitName;

    public DeleteSnapshot(String commitName) {
      super();
      this.action = CoreAdminAction.DELETESNAPSHOT;

      if(commitName == null) {
        throw new NullPointerException("Please specify non null value for commitName parameter.");
      }
      this.commitName = commitName;
    }

    public String getCommitName() {
      return commitName;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.COMMIT_NAME, this.commitName);
      return params;
    }
  }

  public static class ListSnapshots extends CoreAdminRequest {
    public ListSnapshots() {
      super();
      this.action = CoreAdminAction.LISTSNAPSHOTS;
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

  public void setCoreName( String coreName )
  {
    this.core = coreName;
  }

  public final void setOtherCoreName( String otherCoreName )
  {
    this.other = otherCoreName;
  }

  public final void setIndexInfoNeeded(boolean isIndexInfoNeeded) {
    this.isIndexInfoNeeded = isIndexInfoNeeded;
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
    params.set(CoreAdminParams.INDEX_INFO, (isIndexInfoNeeded ? "true" : "false"));
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
  protected CoreAdminResponse createResponse(SolrClient client) {
    return new CoreAdminResponse();
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  public static CoreAdminResponse reloadCore(String name, SolrClient client) throws SolrServerException, IOException
  {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreName(name);
    req.setAction(CoreAdminAction.RELOAD);
    return req.process(client);
  }

  public static CoreAdminResponse unloadCore(String name, SolrClient client) throws SolrServerException, IOException
  {
    return unloadCore(name, false, client);
  }

  public static CoreAdminResponse unloadCore(String name, boolean deleteIndex, SolrClient client) throws SolrServerException, IOException {
    return unloadCore(name, deleteIndex, false, client);
  }

  public static CoreAdminResponse unloadCore(String name, boolean deleteIndex, boolean deleteInstanceDir, SolrClient client) throws SolrServerException, IOException {
    Unload req = new Unload(deleteIndex);
    req.setCoreName(name);
    req.setDeleteInstanceDir(deleteInstanceDir);
    return req.process(client);
  }

  /**
   * Rename an existing core.
   * 
   * @throws IllegalArgumentException if the new core name contains invalid characters.
   */
  public static CoreAdminResponse renameCore(String coreName, String newName, SolrClient client )
      throws SolrServerException, IOException {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreName(coreName);
    req.setOtherCoreName(SolrIdentifierValidator.validateCoreName(newName));
    req.setAction( CoreAdminAction.RENAME );
    return req.process( client );
  }

  public static CoreStatus getCoreStatus(String coreName, SolrClient client) throws SolrServerException, IOException {
    return getCoreStatus(coreName, true, client);
  }

  public static CoreStatus getCoreStatus(String coreName, boolean getIndexInfo, SolrClient client)
      throws SolrServerException, IOException {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setAction(CoreAdminAction.STATUS);
    req.setIndexInfoNeeded(getIndexInfo);
    return new CoreStatus(req.process(client).getCoreStatus(coreName));
  }

  public static CoreAdminResponse getStatus( String name, SolrClient client ) throws SolrServerException, IOException
  {
    CoreAdminRequest req = new CoreAdminRequest();
    req.setCoreName( name );
    req.setAction( CoreAdminAction.STATUS );
    return req.process( client );
  }
  
  public static CoreAdminResponse createCore( String name, String instanceDir, SolrClient client ) throws SolrServerException, IOException
  {
    return CoreAdminRequest.createCore(name, instanceDir, client, null, null);
  }
  
  public static CoreAdminResponse createCore( String name, String instanceDir, SolrClient client, String configFile, String schemaFile ) throws SolrServerException, IOException {
    return createCore(name, instanceDir, client, configFile, schemaFile, null, null);
  }
  
  public static CoreAdminResponse createCore( String name, String instanceDir, SolrClient client, String configFile, String schemaFile, String dataDir, String tlogDir ) throws SolrServerException, IOException
  {
    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName( name );
    req.setInstanceDir(instanceDir);
    if (dataDir != null) {
      req.setDataDir(dataDir);
    }
    if (tlogDir != null) {
      req.setUlogDir(tlogDir);
    }
    if(configFile != null){
      req.setConfigName(configFile);
    }
    if(schemaFile != null){
      req.setSchemaName(schemaFile);
    }
    return req.process( client );
  }

  public static CoreAdminResponse mergeIndexes(String name,
      String[] indexDirs, String[] srcCores, SolrClient client) throws SolrServerException,
      IOException {
    CoreAdminRequest.MergeIndexes req = new CoreAdminRequest.MergeIndexes();
    req.setCoreName(name);
    req.setIndexDirs(Arrays.asList(indexDirs));
    req.setSrcCores(Arrays.asList(srcCores));
    return req.process(client);
  }
}
