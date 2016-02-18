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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 4.5
 */
public abstract class CollectionAdminRequest <Q extends CollectionAdminRequest<Q>> extends SolrRequest<CollectionAdminResponse> {

  protected CollectionAction action = null;

  private static String PROPERTY_PREFIX = "property.";

  protected CollectionAdminRequest setAction(CollectionAction action) {
    this.action = action;
    return this;
  }

  public CollectionAdminRequest() {
    super(METHOD.GET, "/admin/collections");
  }

  public CollectionAdminRequest(String path) {
    super(METHOD.GET, path);
  }

  protected abstract Q getThis();

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

  protected abstract static class AsyncCollectionAdminRequest <T extends CollectionAdminRequest<T>> extends CollectionAdminRequest<T> {
    protected String asyncId = null;

    public final T setAsyncId(String asyncId) {
      this.asyncId = asyncId;
      return getThis();
    }

    public String getAsyncId() {
      return asyncId;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (asyncId != null) {
        params.set(CommonAdminParams.ASYNC, asyncId);
      }
      return params;
    }
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  protected abstract static class CollectionSpecificAdminRequest <T extends CollectionAdminRequest<T>> extends CollectionAdminRequest<T> {
    protected String collection = null;

    public T setCollectionName(String collectionName) {
      this.collection = collectionName;
      return getThis();
    }

    public final String getCollectionName() {
      return collection;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.NAME, collection);
      return params;
    }
  }

  protected abstract static class CollectionSpecificAsyncAdminRequest<T extends CollectionAdminRequest<T>> extends CollectionSpecificAdminRequest<T> {
    protected String asyncId = null;

    public final T setAsyncId(String asyncId) {
      this.asyncId = asyncId;
      return getThis();
    }

    public String getAsyncId() {
      return asyncId;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (asyncId != null) {
        params.set(CommonAdminParams.ASYNC, asyncId);
      }
      return params;
    }
  }

  protected abstract static class CollectionShardAdminRequest <T extends CollectionAdminRequest<T>> extends CollectionAdminRequest<T> {
    protected String shardName = null;
    protected String collection = null;

    public T setCollectionName(String collectionName) {
      this.collection = collectionName;
      return getThis();
    }

    public String getCollectionName() {
      return collection;
    }

    public T setShardName(String shard) {
      this.shardName = shard;
      return getThis();
    }

    public String getShardName() {
      return this.shardName;
    }

    @Deprecated
    public ModifiableSolrParams getCommonParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.SHARD, shardName);
      return params;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.SHARD, shardName);
      return params;
    }
  }

  protected abstract static class CollectionShardAsyncAdminRequest<T extends CollectionAdminRequest<T>> extends CollectionShardAdminRequest<T> {
    protected String asyncId = null;

    public final T setAsyncId(String asyncId) {
      this.asyncId = asyncId;
      return getThis();
    }

    public String getAsyncId() {
      return asyncId;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (asyncId != null) {
        params.set(CommonAdminParams.ASYNC, asyncId);
      }
      return params;
    }
  }

  protected abstract static class CollectionAdminRoleRequest <T extends CollectionAdminRequest<T>> extends AsyncCollectionAdminRequest<T> {
    protected String node;
    protected String role;
    public T setNode(String node) {
      this.node = node;
      return getThis();
    }

    public String getNode() {
      return this.node;
    }

    public T setRole(String role) {
      this.role = role;
      return getThis();
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
  public static class Create extends CollectionSpecificAsyncAdminRequest<Create> {
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
    private String[] rule , snitch;
    public Create() {
      action = CollectionAction.CREATE;
    }

    public Create setConfigName(String config) { this.configName = config; return this; }
    public Create setCreateNodeSet(String nodeSet) { this.createNodeSet = nodeSet; return this; }
    public Create setRouterName(String routerName) { this.routerName = routerName; return this; }
    public Create setRouterField(String routerField) { this.routerField = routerField; return this; }
    public Create setNumShards(Integer numShards) {this.numShards = numShards; return this; }
    public Create setMaxShardsPerNode(Integer numShards) { this.maxShardsPerNode = numShards; return this; }
    public Create setAutoAddReplicas(boolean autoAddReplicas) { this.autoAddReplicas = autoAddReplicas; return this; }
    public Create setReplicationFactor(Integer repl) { this.replicationFactor = repl; return this; }
    public Create setStateFormat(Integer stateFormat) { this.stateFormat = stateFormat; return this; }
    public Create setRule(String... s){ this.rule = s; return this; }
    public Create setSnitch(String... s){ this.snitch = s; return this; }

    public String getConfigName()  { return configName; }
    public String getCreateNodeSet() { return createNodeSet; }
    public String getRouterName() { return  routerName; }
    public String getShards() { return  shards; }
    public Integer getNumShards() { return numShards; }
    public Integer getMaxShardsPerNode() { return maxShardsPerNode; }
    public Integer getReplicationFactor() { return replicationFactor; }
    public Boolean getAutoAddReplicas() { return autoAddReplicas; }
    public Integer getStateFormat() { return stateFormat; }
    
    /**
     * Provide the name of the shards to be created, separated by commas
     * 
     * Shard names must consist entirely of periods, underscores and alphanumerics.  Other characters are not allowed.
     * 
     * @throws IllegalArgumentException if any of the shard names contain invalid characters.
     */
    public Create setShards(String shards) {
      for (String shard : shards.split(",")) {
        if (!SolrIdentifierValidator.validateShardName(shard)) {
          throw new IllegalArgumentException("Invalid shard: " + shard
              + ". Shard names must consist entirely of periods, underscores and alphanumerics");
        }
      }
      this.shards = shards;
      return this;
    }
    
    /**
     * Provide the name of the collection to be created.
     * 
     * Collection names must consist entirely of periods, underscores and alphanumerics.  Other characters are not allowed.
     * 
     * @throws IllegalArgumentException if the collection name contains invalid characters.
     */
    @Override
    public Create setCollectionName(String collectionName) throws SolrException {
      if (!SolrIdentifierValidator.validateCollectionName(collectionName)) {
        throw new IllegalArgumentException("Invalid collection: " + collectionName
            + ". Collection names must consist entirely of periods, underscores, and alphanumerics");
      }
      this.collection = collectionName;
      return this;
    }

    public Properties getProperties() {
      return properties;
    }

    public Create setProperties(Properties properties) {
      this.properties = properties;
      return this;
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
      if (autoAddReplicas != null) {
        params.set(ZkStateReader.AUTO_ADD_REPLICAS, autoAddReplicas);
      }
      if(properties != null) {
        addProperties(params, properties);
      }
      if (stateFormat != null) {
        params.set(DocCollection.STATE_FORMAT, stateFormat);
      }
      if(rule != null) params.set("rule", rule);
      if(snitch != null) params.set("snitch", snitch);
      return params;
    }

    @Override
    protected Create getThis() {
      return this;
    }
  }

  // RELOAD request
  public static class Reload extends CollectionSpecificAsyncAdminRequest<Reload> {
    public Reload() {
      action = CollectionAction.RELOAD;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      return params;
    }

    @Override
    protected Reload getThis() {
      return this;
    }
  }

  // DELETE request
  public static class Delete extends CollectionSpecificAsyncAdminRequest<Delete> {

    public Delete() {
      action = CollectionAction.DELETE;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      return params;
    }

    @Override
    protected Delete getThis() {
      return this;
    }
  }

  // CREATESHARD request
  public static class CreateShard extends CollectionShardAsyncAdminRequest<CreateShard> {
    protected String nodeSet;
    protected Properties properties;

    public CreateShard setNodeSet(String nodeSet) {
      this.nodeSet = nodeSet;
      return this;
    }

    public String getNodeSet() {
      return nodeSet;
    }

    public Properties getProperties() {
      return properties;
    }

    public CreateShard setProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public CreateShard() {
      action = CollectionAction.CREATESHARD;
    }
    
    /**
     * Provide the name of the shard to be created.
     * 
     * Shard names must consist entirely of periods, underscores and alphanumerics.  Other characters are not allowed.
     * 
     * @throws IllegalArgumentException if the shard name contains invalid characters.
     */
    @Override
    public CreateShard setShardName(String shardName) {
      if (!SolrIdentifierValidator.validateShardName(shardName)) {
        throw new IllegalArgumentException("Invalid shard: " + shardName
            + ". Shard names must consist entirely of periods, underscores and alphanumerics");
      }
      this.shardName = shardName;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      if (nodeSet != null) {
        params.set("createNodeSet", nodeSet);
      }
      if (properties != null) {
        addProperties(params, properties);
      }
      return params;
    }

    @Override
    protected CreateShard getThis() {
      return this;
    }
  }

  // SPLITSHARD request
  public static class SplitShard extends CollectionShardAsyncAdminRequest<SplitShard> {
    protected String ranges;
    protected String splitKey;

    private Properties properties;

    public SplitShard() {
      action = CollectionAction.SPLITSHARD;
    }

    public SplitShard setRanges(String ranges) { this.ranges = ranges; return this; }
    public String getRanges() { return ranges; }

    public SplitShard setSplitKey(String splitKey) {
      this.splitKey = splitKey;
      return this;
    }

    public String getSplitKey() {
      return this.splitKey;
    }

    public Properties getProperties() {
      return properties;
    }

    public SplitShard setProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set( "ranges", ranges);

      if(splitKey != null)
        params.set("split.key", this.splitKey);

      if(properties != null) {
        addProperties(params, properties);
      }
      return params;
    }

    @Override
    protected SplitShard getThis() {
      return this;
    }
  }

  // DELETESHARD request
  public static class DeleteShard extends CollectionShardAsyncAdminRequest<DeleteShard> {
    public DeleteShard() {
      action = CollectionAction.DELETESHARD;
    }

    @Override
    protected DeleteShard getThis() {
      return this;
    }
  }

  // FORCELEADER request
  public static class ForceLeader extends CollectionShardAdminRequest<ForceLeader> {

    public ForceLeader() {
      action = CollectionAction.FORCELEADER;
    }

    @Override
    protected ForceLeader getThis() {
      return this;
    }
  }

  // REQUESTSTATUS request
  public static class RequestStatus extends CollectionAdminRequest<RequestStatus> {
    protected  String requestId = null;

    public RequestStatus() {
      action = CollectionAction.REQUESTSTATUS;
    }

    public RequestStatus setRequestId(String requestId) {
      this.requestId = requestId;
      return this;
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

    @Override
    protected RequestStatus getThis() {
      return this;
    }
  }

  // DELETESTATUS request
  public static class DeleteStatus extends CollectionAdminRequest<DeleteStatus> {
    protected String requestId = null;
    protected Boolean flush = null;

    public DeleteStatus() {
      action = CollectionAction.DELETESTATUS;
    }

    public DeleteStatus setRequestId(String requestId) {
      this.requestId = requestId;
      return this;
    }

    public DeleteStatus setFlush(Boolean flush) {
      this.flush = flush;
      return this;
    }

    public String getRequestId() {
      return this.requestId;
    }

    public Boolean getFlush() {
      return this.flush;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      if (requestId != null)
        params.set(CoreAdminParams.REQUESTID, requestId);

      if (flush != null)
        params.set(CollectionAdminParams.FLUSH, flush);
      return params;
    }

    @Override
    protected DeleteStatus getThis() {
      return this;
    }
  }

  // CREATEALIAS request
  public static class CreateAlias extends AsyncCollectionAdminRequest<CreateAlias> {
    protected String aliasName;
    protected String aliasedCollections;

    public CreateAlias() {
      action = CollectionAction.CREATEALIAS;
    }

    /**
     * Provide the name of the alias to be created.
     * 
     * Alias names must consist entirely of periods, underscores and alphanumerics.  Other characters are not allowed.
     * 
     * @throws IllegalArgumentException if the alias name contains invalid characters.
     */
    public CreateAlias setAliasName(String aliasName) {
      if (!SolrIdentifierValidator.validateCollectionName(aliasName)) {
        throw new IllegalArgumentException("Invalid alias: " + aliasName
            + ". Aliases must consist entirely of periods, underscores, and alphanumerics");
      }
      this.aliasName = aliasName;
      return this;
    }

    public String getAliasName() {
      return aliasName;
    }

    public CreateAlias setAliasedCollections(String alias) {
      this.aliasedCollections = alias;
      return this;
    }

    public String getAliasedCollections() {
      return this.aliasedCollections;
    }

    /**
     * @param aliasName the alias name
     * @deprecated use {@link #setAliasName(String)} instead
     */
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

    @Override
    protected CreateAlias getThis() {
      return this;
    }
  }

  // DELETEALIAS request
  public static class DeleteAlias extends AsyncCollectionAdminRequest<DeleteAlias> {
    protected String aliasName;

    public DeleteAlias() {
      action = CollectionAction.DELETEALIAS;
    }

    public DeleteAlias setAliasName(String aliasName) {
      this.aliasName = aliasName;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.NAME, aliasName);
      return params;
    }

    @Override
    protected DeleteAlias getThis() {
      return this;
    }
  }

  // ADDREPLICA request
  public static class AddReplica extends CollectionShardAsyncAdminRequest<AddReplica> {
    protected String node;
    protected String routeKey;
    protected String instanceDir;
    protected String dataDir;
    protected Properties properties;

    public AddReplica() {
      action = CollectionAction.ADDREPLICA;
    }

    public Properties getProperties() {
      return properties;
    }

    public AddReplica setProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public String getNode() {
      return node;
    }

    public AddReplica setNode(String node) {
      this.node = node;
      return this;
    }

    public String getRouteKey() {
      return routeKey;
    }

    public AddReplica setRouteKey(String routeKey) {
      this.routeKey = routeKey;
      return this;
    }

    public String getInstanceDir() {
      return instanceDir;
    }

    public AddReplica setInstanceDir(String instanceDir) {
      this.instanceDir = instanceDir;
      return this;
    }

    public String getDataDir() {
      return dataDir;
    }

    public AddReplica setDataDir(String dataDir) {
      this.dataDir = dataDir;
      return this;
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

    @Override
    protected AddReplica getThis() {
      return this;
    }
  }

  // DELETEREPLICA request
  public static class DeleteReplica extends CollectionShardAsyncAdminRequest<DeleteReplica> {
    protected String replica;
    protected Boolean onlyIfDown;

    public DeleteReplica() {
      action = CollectionAction.DELETEREPLICA;
    }

    public DeleteReplica setReplica(String replica) {
      this.replica = replica;
      return this;
    }

    public String getReplica() {
      return this.replica;
    }

    public DeleteReplica setOnlyIfDown(boolean onlyIfDown) {
      this.onlyIfDown = onlyIfDown;
      return this;
    }

    public Boolean getOnlyIfDown() {
      return this.onlyIfDown;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(ZkStateReader.REPLICA_PROP, this.replica);

      if (onlyIfDown != null) {
        params.set("onlyIfDown", onlyIfDown);
      }
      return params;
    }

    @Override
    protected DeleteReplica getThis() {
      return this;
    }
  }

  // CLUSTERPROP request
  public static class ClusterProp extends CollectionAdminRequest<ClusterProp> {
    private String propertyName;
    private String propertyValue;

    public ClusterProp() {
      this.action = CollectionAction.CLUSTERPROP;
    }

    public ClusterProp setPropertyName(String propertyName) {
      this.propertyName = propertyName;
      return this;
    }

    public String getPropertyName() {
      return this.propertyName;
    }

    public ClusterProp setPropertyValue(String propertyValue) {
      this.propertyValue = propertyValue;
      return this;
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

    @Override
    protected ClusterProp getThis() {
      return this;
    }
  }

  // MIGRATE request
  public static class Migrate extends AsyncCollectionAdminRequest<Migrate> {
    private String collection;
    private String targetCollection;
    private String splitKey;
    private Integer forwardTimeout;
    private Properties properties;

    public Migrate() {
      action = CollectionAction.MIGRATE;
    }

    public Migrate setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    public String getCollectionName() {
      return collection;
    }

    public Migrate setTargetCollection(String targetCollection) {
      this.targetCollection = targetCollection;
      return this;
    }

    public String getTargetCollection() {
      return this.targetCollection;
    }

    public Migrate setSplitKey(String splitKey) {
      this.splitKey = splitKey;
      return this;
    }

    public String getSplitKey() {
      return this.splitKey;
    }

    public Migrate setForwardTimeout(int forwardTimeout) {
      this.forwardTimeout = forwardTimeout;
      return this;
    }

    public Integer getForwardTimeout() {
      return this.forwardTimeout;
    }

    public Migrate setProperties(Properties properties) {
      this.properties = properties;
      return this;
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
      if (properties != null) {
        addProperties(params, properties);
      }

      return params;
    }

    @Override
    protected Migrate getThis() {
      return this;
    }
  }

  // ADDROLE request
  public static class AddRole extends CollectionAdminRoleRequest<AddRole> {
    public AddRole() {
      action = CollectionAction.ADDROLE;
    }

    @Override
    protected AddRole getThis() {
      return this;
    }
  }

  // REMOVEROLE request
  public static class RemoveRole extends CollectionAdminRoleRequest<RemoveRole> {
    public RemoveRole() {
      action = CollectionAction.REMOVEROLE;
    }

    @Override
    protected RemoveRole getThis() {
      return this;
    }
  }

  // OVERSEERSTATUS request
  public static class OverseerStatus extends AsyncCollectionAdminRequest<OverseerStatus> {

    public OverseerStatus () {
      action = CollectionAction.OVERSEERSTATUS;
    }

    @Override
    protected OverseerStatus getThis() {
      return this;
    }
  }

  // CLUSTERSTATUS request
  public static class ClusterStatus extends CollectionAdminRequest<ClusterStatus> {

    protected String shardName = null;
    protected String collection = null;
    protected String routeKey = null;

    public ClusterStatus () {
      action = CollectionAction.CLUSTERSTATUS;
    }

    public ClusterStatus setCollectionName(String collectionName) {
      this.collection = collectionName;
      return this;
    }

    public String getCollectionName() {
      return collection;
    }

    public ClusterStatus setShardName(String shard) {
      this.shardName = shard;
      return this;
    }

    public String getShardName() {
      return this.shardName;
    }

    public String getRouteKey() {
      return routeKey;
    }

    public ClusterStatus setRouteKey(String routeKey) {
      this.routeKey = routeKey;
      return this;
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
      if (routeKey != null) {
        params.set(ShardParams._ROUTE_, routeKey);
      }
      return params;
    }

    @Override
    protected ClusterStatus getThis() {
      return this;
    }
  }

  // LIST request
  public static class List extends CollectionAdminRequest<List> {
    public List () {
      action = CollectionAction.LIST;
    }

    @Override
    protected List getThis() {
      return this;
    }
  }

  // ADDREPLICAPROP request
  public static class AddReplicaProp extends CollectionShardAsyncAdminRequest<AddReplicaProp> {
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

    public AddReplicaProp setReplica(String replica) {
      this.replica = replica;
      return this;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public AddReplicaProp setPropertyName(String propertyName) {
      this.propertyName = propertyName;
      return this;
    }

    public String getPropertyValue() {
      return propertyValue;
    }

    public AddReplicaProp setPropertyValue(String propertyValue) {
      this.propertyValue = propertyValue;
      return this;
    }

    public Boolean getShardUnique() {
      return shardUnique;
    }

    public AddReplicaProp setShardUnique(Boolean shardUnique) {
      this.shardUnique = shardUnique;
      return this;
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

    @Override
    protected AddReplicaProp getThis() {
      return this;
    }
  }

  // DELETEREPLICAPROP request
  public static class DeleteReplicaProp extends CollectionShardAsyncAdminRequest<DeleteReplicaProp> {
    private String replica;
    private String propertyName;

    public DeleteReplicaProp() {
      this.action = CollectionAction.DELETEREPLICAPROP;
    }

    public String getReplica() {
      return replica;
    }

    public DeleteReplicaProp setReplica(String replica) {
      this.replica = replica;
      return this;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public DeleteReplicaProp setPropertyName(String propertyName) {
      this.propertyName = propertyName;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set("replica", replica);
      params.set("property", propertyName);
      return params;
    }

    @Override
    protected DeleteReplicaProp getThis() {
      return this;
    }
  }

  // MIGRATECLUSTERSTATE request
  public static class MigrateClusterState extends CollectionShardAsyncAdminRequest<MigrateClusterState> {

    public MigrateClusterState() {
      this.action = CollectionAction.MIGRATESTATEFORMAT;
    }

    @Override
    public MigrateClusterState setShardName(String shard) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getShardName() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected MigrateClusterState getThis() {
      return this;
    }
  }

  // BALANCESHARDUNIQUE request
  public static class BalanceShardUnique extends AsyncCollectionAdminRequest<BalanceShardUnique> {
    protected String collection;
    protected String propertyName;
    protected Boolean onlyActiveNodes;
    protected Boolean shardUnique;

    public BalanceShardUnique() {
      this.action = CollectionAction.BALANCESHARDUNIQUE;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public BalanceShardUnique setPropertyName(String propertyName) {
      this.propertyName = propertyName;
      return this;
    }

    public Boolean getOnlyActiveNodes() {
      return onlyActiveNodes;
    }

    public BalanceShardUnique setOnlyActiveNodes(Boolean onlyActiveNodes) {
      this.onlyActiveNodes = onlyActiveNodes;
      return this;
    }

    public Boolean getShardUnique() {
      return shardUnique;
    }

    public BalanceShardUnique setShardUnique(Boolean shardUnique) {
      this.shardUnique = shardUnique;
      return this;
    }

    public BalanceShardUnique setCollection(String collection) {
      this.collection = collection;
      return this;
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

    @Override
    protected BalanceShardUnique getThis() {
      return this;
    }
  }
}
