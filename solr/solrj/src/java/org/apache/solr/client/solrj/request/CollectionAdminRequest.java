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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 4.5
 */
public class CollectionAdminRequest extends SolrRequest<CollectionAdminResponse> {

  protected CollectionAction action = null;

  private static String PROPERTY_PREFIX = "property.";

  protected void setAction(CollectionAction action) {
    this.action = action;
  }

  public CollectionAdminRequest() {
    super(METHOD.GET, "/admin/collections");
  }

  public CollectionAdminRequest(String path) {
    super(METHOD.GET, path);
  }

  @Override
  public SolrParams getParams() {
    if (action == null) {
      throw new RuntimeException( "no action specified!" );
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, action.toString());
    return params;
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return null;
  }

  @Override
  protected CollectionAdminResponse createResponse(SolrClient client) {
    return new CollectionAdminResponse();
  }
  
  protected void addProperties(ModifiableSolrParams params, Properties props) {
    Iterator<Map.Entry<Object, Object>> iter = props.entrySet().iterator();
    while(iter.hasNext()) {
      Map.Entry<Object, Object> prop = iter.next();
      String key = (String) prop.getKey();
      String value = (String) prop.getValue();
      params.set(PROPERTY_PREFIX + key, value);
    }
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  protected static class CollectionSpecificAdminRequest extends CollectionAdminRequest {
    protected String collection = null;

    public final void setCollectionName(String collectionName) {
      this.collection = collectionName;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set( CoreAdminParams.NAME, collection );
      return params;
    }


  }
  
  protected static class CollectionShardAdminRequest extends CollectionAdminRequest {
    protected String shardName = null;
    protected String collection = null;

    public void setCollectionName(String collectionName) {
      this.collection = collectionName;
    }
    
    public String getCollectionName() {
      return collection;
    }
    
    public void setShardName(String shard) {
      this.shardName = shard;
    }
    
    public String getShardName() {
      return this.shardName;
    }

    public ModifiableSolrParams getCommonParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.SHARD, shardName);
      return params;
    }

    @Override
    public SolrParams getParams() {
      return getCommonParams();
    }
  }
  
  protected static class CollectionAdminRoleRequest extends CollectionAdminRequest {
    private String node;
    private String role;

    public void setNode(String node) {
      this.node = node;
    }

    public String getNode() {
      return this.node;
    }

    public void setRole(String role) {
      this.role = role;
    }

    public String getRole() {
      return this.role;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set("role", this.role);
      params.set("node", this.node);
      return params;
    }

  }

  /** Specific Collection API call implementations **/ 
  
  // CREATE request
  public static class Create extends CollectionSpecificAdminRequest {
    protected String configName = null;
    protected String createNodeSet = null;
    protected String routerName;
    protected String shards;
    protected String routerField;
    protected Integer numShards;
    protected Integer maxShardsPerNode;
    protected Integer replicationFactor;

    private Properties properties;
    protected Boolean autoAddReplicas;
    protected Integer stateFormat;
    protected String asyncId;

    public Create() {
      action = CollectionAction.CREATE;
    }

    public void setConfigName(String config) { this.configName = config; }
    public void setCreateNodeSet(String nodeSet) { this.createNodeSet = nodeSet; }
    public void setRouterName(String routerName) { this.routerName = routerName; }
    public void setShards(String shards) { this.shards = shards; }
    public void setRouterField(String routerField) { this.routerField = routerField; }
    public void setNumShards(Integer numShards) {this.numShards = numShards;}
    public void setMaxShardsPerNode(Integer numShards) { this.maxShardsPerNode = numShards; }
    public void setAutoAddReplicas(boolean autoAddReplicas) { this.autoAddReplicas = autoAddReplicas; }
    public void setReplicationFactor(Integer repl) { this.replicationFactor = repl; }
    public void setStateFormat(Integer stateFormat) { this.stateFormat = stateFormat; }
    public void setAsyncId(String asyncId) {
      this.asyncId = asyncId;
    }

    public String getConfigName()  { return configName; }
    public String getCreateNodeSet() { return createNodeSet; }
    public String getRouterName() { return  routerName; }
    public String getShards() { return  shards; }
    public Integer getNumShards() { return numShards; }
    public Integer getMaxShardsPerNode() { return maxShardsPerNode; }
    public Integer getReplicationFactor() { return replicationFactor; }
    public Boolean getAutoAddReplicas() { return autoAddReplicas; }
    public Integer getStateFormat() { return stateFormat; }
    public String getAsyncId() {
      return asyncId;
    }

    public Properties getProperties() {
      return properties;
    }

    public void setProperties(Properties properties) {
      this.properties = properties;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();

      params.set( "collection.configName", configName);
      params.set( "createNodeSet", createNodeSet);
      if (numShards != null) {
        params.set( ZkStateReader.NUM_SHARDS_PROP, numShards);
      }
      if (maxShardsPerNode != null) {
        params.set( "maxShardsPerNode", maxShardsPerNode);
      }
      params.set( "router.name", routerName);
      params.set("shards", shards);
      if (routerField != null) {
        params.set("router.field", routerField);
      }
      if (replicationFactor != null) {
        params.set( "replicationFactor", replicationFactor);
      }
      params.set("async", asyncId);
      if (autoAddReplicas != null) {
        params.set(ZkStateReader.AUTO_ADD_REPLICAS, autoAddReplicas);
      }
      if(properties != null) {
        addProperties(params, properties);
      }
      if (stateFormat != null) {
        params.set(DocCollection.STATE_FORMAT, stateFormat);
      }
      return params;
    }
    
  }

  // RELOAD request
  public static class Reload extends CollectionSpecificAdminRequest {
    public Reload() {
      action = CollectionAction.RELOAD;
    }
  }

  // DELETE request
  public static class Delete extends CollectionSpecificAdminRequest {
    public Delete() {
      action = CollectionAction.DELETE;
    }
  }

  // CREATESHARD request
  public static class CreateShard extends CollectionShardAdminRequest {
    protected String nodeSet;
    private Properties properties;

    public void setNodeSet(String nodeSet) {
      this.nodeSet = nodeSet;
    }

    public String getNodeSet() {
      return nodeSet;
    }

    public Properties getProperties() {
      return properties;
    }

    public void setProperties(Properties properties) {
      this.properties = properties;
    }

    public CreateShard() {
      action = CollectionAction.CREATESHARD;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = getCommonParams();
      if (nodeSet != null) {
        params.set("createNodeSet", nodeSet);
      }
      if (properties != null) {
        addProperties(params, properties);
      }
      return params;
    }
  }

  // SPLITSHARD request
  public static class SplitShard extends CollectionShardAdminRequest {
    protected String ranges;
    protected String splitKey;
    protected String asyncId;
    
    private Properties properties;

    public SplitShard() {
      action = CollectionAction.SPLITSHARD;
    }

    public void setRanges(String ranges) { this.ranges = ranges; }
    public String getRanges() { return ranges; }

    public void setSplitKey(String splitKey) {
      this.splitKey = splitKey;
    }
    
    public String getSplitKey() {
      return this.splitKey;
    }
    
    public Properties getProperties() {
      return properties;
    }

    public void setProperties(Properties properties) {
      this.properties = properties;
    }

    public void setAsyncId(String asyncId) {
      this.asyncId = asyncId;
    }

    public String getAsyncId() {
      return asyncId;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = getCommonParams();
      params.set( "ranges", ranges);

      if(splitKey != null)
        params.set("split.key", this.splitKey);
      
      if(properties != null) {
        addProperties(params, properties);
      }
      
      params.set("async", asyncId);
      return params;
    }
  }

  // DELETESHARD request
  public static class DeleteShard extends CollectionShardAdminRequest {
    public DeleteShard() {
      action = CollectionAction.DELETESHARD;
    }
  }

  // REQUESTSTATUS request
  public static class RequestStatus extends CollectionAdminRequest {
    protected  String requestId = null;

    public RequestStatus() {
      action = CollectionAction.REQUESTSTATUS;
    }

    public void setRequestId(String requestId) {
      this.requestId = requestId;
    }
    
    public String getRequestId() {
      return this.requestId;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.REQUESTID, requestId);
      return params;
    }
  }
  
  // CREATEALIAS request
  public static class CreateAlias extends CollectionAdminRequest {
    protected String aliasName;
    protected String aliasedCollections;

    public CreateAlias() {
      action = CollectionAction.CREATEALIAS;
    }

    public void setAliasName(String aliasName) {
      this.aliasName = aliasName;
    }

    public String getAliasName() {
      return aliasName;
    }
    
    public void setAliasedCollections(String alias) {
      this.aliasedCollections = alias;
    }
    
    public String getAliasedCollections() {
      return this.aliasedCollections;
    }
    
    @Deprecated
    public void setCollectionName(String aliasName) {
      this.aliasName = aliasName;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.NAME, aliasName);
      params.set("collections", aliasedCollections);
      return params;
    }
  }

  // DELETEALIAS request
  public static class DeleteAlias extends CollectionAdminRequest {
    protected String aliasName;
    
    public DeleteAlias() {
      action = CollectionAction.DELETEALIAS;
    }
    
    public void setAliasName(String aliasName) {
      this.aliasName = aliasName;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.NAME, aliasName);
      return params;
    }
  }

  // ADDREPLICA request
  public static class AddReplica extends CollectionShardAdminRequest {
    private String node;
    private String routeKey;
    private String instanceDir;
    private String dataDir;
    private Properties properties;
    private String asyncId;

    public AddReplica() {
      action = CollectionAction.ADDREPLICA;
    }

    public Properties getProperties() {
      return properties;
    }

    public void setProperties(Properties properties) {
      this.properties = properties;
    }

    public String getNode() {
      return node;
    }

    public void setNode(String node) {
      this.node = node;
    }

    public String getRouteKey() {
      return routeKey;
    }

    public void setRouteKey(String routeKey) {
      this.routeKey = routeKey;
    }

    public String getInstanceDir() {
      return instanceDir;
    }

    public void setInstanceDir(String instanceDir) {
      this.instanceDir = instanceDir;
    }

    public String getDataDir() {
      return dataDir;
    }

    public void setDataDir(String dataDir) {
      this.dataDir = dataDir;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (shardName == null || shardName.isEmpty()) {
        params.remove(CoreAdminParams.SHARD);
        if (routeKey == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Either shard or routeKey must be provided");
        }
        params.add(ShardParams._ROUTE_, routeKey);
      }
      if (asyncId != null) {
        params.set("async", asyncId);
      }
      if (node != null) {
        params.add("node", node);
      }
      if (instanceDir != null)  {
        params.add("instanceDir", instanceDir);
      }
      if (dataDir != null)  {
        params.add("dataDir", dataDir);
      }
      if (properties != null) {
        addProperties(params, properties);
      }
      return params;
    }

    public void setAsyncId(String asyncId) {
      this.asyncId = asyncId;
    }
    
    public String getAsyncId() {
      return asyncId;
    }
  }

  // DELETEREPLICA request
  public static class DeleteReplica extends CollectionShardAdminRequest {
    private String replica;
    private Boolean onlyIfDown;
    
    public DeleteReplica() {
      action = CollectionAction.DELETEREPLICA;
    }

    public void setReplica(String replica) {
      this.replica = replica;
    }

    public String getReplica() {
      return this.replica;
    }
    
    public void setOnlyIfDown(boolean onlyIfDown) {
      this.onlyIfDown = onlyIfDown;
    }
    
    public Boolean getOnlyIfDown() {
      return this.onlyIfDown;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(ZkStateReader.REPLICA_PROP, this.replica);
      
      if(onlyIfDown != null) {
        params.set("onlyIfDown", this.onlyIfDown);
      }
      return params;
    }
  }
  
  // CLUSTERPROP request
  public static class ClusterProp extends CollectionAdminRequest {
    private String propertyName;
    private String propertyValue;
    
    public ClusterProp() {
      this.action = CollectionAction.CLUSTERPROP;
    }
    
    public void setPropertyName(String propertyName) {
      this.propertyName = propertyName;
    }

    public String getPropertyName() {
      return this.propertyName;
    }

    public void setPropertyValue(String propertyValue) {
      this.propertyValue = propertyValue;
    }
    
    public String getPropertyValue() {
      return this.propertyValue;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.add(CoreAdminParams.NAME, propertyName);
      params.add("val", propertyValue);
      
      return params;
    }
    
  }
  
  // MIGRATE request
  public static class Migrate extends CollectionAdminRequest {
    private String collection;
    private String targetCollection;
    private String splitKey;
    private Integer forwardTimeout;
    private Properties properties;
    private String asyncId;
    
    public Migrate() {
      action = CollectionAction.MIGRATE;
    }
    
    public void setCollectionName(String collection) {
      this.collection = collection;
    }
    
    public String getCollectionName() {
      return collection;
    }
    
    public void setTargetCollection(String targetCollection) {
      this.targetCollection = targetCollection;
    }
    
    public String getTargetCollection() {
      return this.targetCollection;
    }
    
    public void setSplitKey(String splitKey) {
      this.splitKey = splitKey;
    }
    
    public String getSplitKey() {
      return this.splitKey;
    }
    
    public void setForwardTimeout(int forwardTimeout) {
      this.forwardTimeout = forwardTimeout;
    }
    
    public Integer getForwardTimeout() {
      return this.forwardTimeout;
    }
    
    public void setProperties(Properties properties) {
      this.properties = properties;
    }
    
    public Properties getProperties() {
      return this.properties;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set("target.collection", targetCollection);
      params.set("split.key", splitKey);
      if (forwardTimeout != null) {
        params.set("forward.timeout", forwardTimeout);
      }
      params.set("async", asyncId);
      
      if (properties != null) {
        addProperties(params, properties);
      }
      
      return params;
    }

    public void setAsyncId(String asyncId) {
      this.asyncId = asyncId;
    }
    
    public String getAsyncId() {
      return asyncId;
    }
  }
  
  // ADDROLE request
  public static class AddRole extends CollectionAdminRoleRequest {
    public AddRole() {
      action = CollectionAction.ADDROLE;
    }
  }

  // REMOVEROLE request
  public static class RemoveRole extends CollectionAdminRoleRequest {
    public RemoveRole() {
      action = CollectionAction.REMOVEROLE;
    }
  }
  
  // OVERSEERSTATUS request
  public static class OverseerStatus extends CollectionAdminRequest {
    public OverseerStatus () {
      action = CollectionAction.OVERSEERSTATUS;
    }
  }

  // CLUSTERSTATUS request
  public static class ClusterStatus extends CollectionAdminRequest {
    
    protected String shardName = null;
    protected String collection = null;
    
    public ClusterStatus () {
      action = CollectionAction.CLUSTERSTATUS;
    }
    
    public void setCollectionName(String collectionName) {
      this.collection = collectionName;
    }
    
    public String getCollectionName() {
      return collection;
    }
    
    public void setShardName(String shard) {
      this.shardName = shard;
    }
    
    public String getShardName() {
      return this.shardName;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      if (collection != null) {
        params.set(CoreAdminParams.COLLECTION, collection);
      }
      if (shardName != null) {
        params.set(CoreAdminParams.SHARD, shardName);
      }
      return params;
    }
    
  }

  // LIST request
  public static class List extends CollectionAdminRequest {
    public List () {
      action = CollectionAction.LIST;
    }
  }
  
  // ADDREPLICAPROP request
  public static class AddReplicaProp extends CollectionShardAdminRequest {
    private String replica;
    private String propertyName;
    private String propertyValue;
    private Boolean shardUnique;
    
    public AddReplicaProp() {
      action = CollectionAction.ADDREPLICAPROP;
    }

    public String getReplica() {
      return replica;
    }

    public void setReplica(String replica) {
      this.replica = replica;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public void setPropertyName(String propertyName) {
      this.propertyName = propertyName;
    }

    public String getPropertyValue() {
      return propertyValue;
    }

    public void setPropertyValue(String propertyValue) {
      this.propertyValue = propertyValue;
    }

    public Boolean getShardUnique() {
      return shardUnique;
    }

    public void setShardUnique(Boolean shardUnique) {
      this.shardUnique = shardUnique;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.REPLICA, replica);
      params.set("property", propertyName);
      params.set("property.value", propertyValue);
      
      if (shardUnique != null) {
        params.set("shardUnique", shardUnique);
      }
      
      return params;
    }
    
  }
  
  // DELETEREPLICAPROP request
  public static class DeleteReplicaProp extends CollectionShardAdminRequest {
    private String replica;
    private String propertyName;

    public DeleteReplicaProp() {
      this.action = CollectionAction.DELETEREPLICAPROP;
    }
    
    public String getReplica() {
      return replica;
    }

    public void setReplica(String replica) {
      this.replica = replica;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public void setPropertyName(String propertyName) {
      this.propertyName = propertyName;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set("replica", replica);
      params.set("property", propertyName);
      return params;
    }
  }
  
  // BALANCESHARDUNIQUE request
  public static class BalanceShardUnique extends CollectionAdminRequest {
    private String collection;
    private String propertyName;
    private Boolean onlyActiveNodes;
    private Boolean shardUnique;
    
    public BalanceShardUnique() {
      this.action = CollectionAction.BALANCESHARDUNIQUE;
    }
    
    public String getPropertyName() {
      return propertyName;
    }

    public void setPropertyName(String propertyName) {
      this.propertyName = propertyName;
    }

    public Boolean getOnlyActiveNodes() {
      return onlyActiveNodes;
    }

    public void setOnlyActiveNodes(Boolean onlyActiveNodes) {
      this.onlyActiveNodes = onlyActiveNodes;
    }

    public Boolean getShardUnique() {
      return shardUnique;
    }

    public void setShardUnique(Boolean shardUnique) {
      this.shardUnique = shardUnique;
    }

    public void setCollection(String collection) {
      this.collection = collection;
    }
    
    public String getCollection() {
      return collection;
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set("property", propertyName);
      if(onlyActiveNodes != null)
        params.set("onlyactivenodes", onlyActiveNodes);
      if(shardUnique != null)
        params.set("shardUnique", shardUnique);
      return params;
    }

  }
}
