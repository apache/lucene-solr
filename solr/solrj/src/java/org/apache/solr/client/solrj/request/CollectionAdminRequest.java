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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
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
import org.apache.solr.common.util.NamedList;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 4.5
 */
public abstract class CollectionAdminRequest<T extends CollectionAdminResponse> extends SolrRequest<T> {

  protected final CollectionAction action;

  private static String PROPERTY_PREFIX = "property.";

  public CollectionAdminRequest(CollectionAction action) {
    this("/admin/collections", action);
  }

  public CollectionAdminRequest(String path, CollectionAction action) {
    super(METHOD.GET, path);
    this.action = action;
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

  protected void addProperties(ModifiableSolrParams params, Properties props) {
    Iterator<Map.Entry<Object, Object>> iter = props.entrySet().iterator();
    while(iter.hasNext()) {
      Map.Entry<Object, Object> prop = iter.next();
      String key = (String) prop.getKey();
      String value = (String) prop.getValue();
      params.set(PROPERTY_PREFIX + key, value);
    }
  }

  protected abstract static class AsyncCollectionAdminRequest extends CollectionAdminRequest<CollectionAdminResponse> {

    public AsyncCollectionAdminRequest(CollectionAction action) {
      super(action);
    }

    @Override
    protected CollectionAdminResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }

    private static String generateAsyncId() {
      return UUID.randomUUID().toString();
    }

    protected String asyncId = null;

    public String getAsyncId() {
      return asyncId;
    }

    /**
     * @deprecated Use {@link #processAsync(String, SolrClient)} or {@link #processAsync(SolrClient)}
     */
    @Deprecated
    public abstract AsyncCollectionAdminRequest setAsyncId(String id);

    /**
     * Process this request asynchronously, generating and returning a request id
     * @param client a Solr client
     * @return the request id
     * @see CollectionAdminRequest.RequestStatus
     */
    public String processAsync(SolrClient client) throws IOException, SolrServerException {
      return processAsync(generateAsyncId(), client);
    }

    /**
     * Process this request asynchronously, using a specified request id
     * @param asyncId the request id
     * @param client a Solr client
     * @return the request id
     */
    public String processAsync(String asyncId, SolrClient client) throws IOException, SolrServerException {
      this.asyncId = asyncId;
      NamedList<Object> resp = client.request(this);
      if (resp.get("error") != null) {
        throw new SolrServerException((String)resp.get("error"));
      }
      return (String) resp.get("requestid");
    }

    /**
     * Send this request to a Solr server, and wait (up to a timeout) for the request to
     * complete or fail
     * @param client a Solr client
     * @param timeoutSeconds the maximum time to wait
     * @return the status of the request on completion or timeout
     */
    public RequestStatusState processAndWait(SolrClient client, long timeoutSeconds)
        throws SolrServerException, InterruptedException, IOException {
      return processAndWait(generateAsyncId(), client, timeoutSeconds);
    }

    /**
     * Send this request to a Solr server, and wait (up to a timeout) for the request to
     * complete or fail
     * @param asyncId an id for the request
     * @param client a Solr client
     * @param timeoutSeconds the maximum time to wait
     * @return the status of the request on completion or timeout
     */
    public RequestStatusState processAndWait(String asyncId, SolrClient client, long timeoutSeconds)
        throws IOException, SolrServerException, InterruptedException {
      processAsync(asyncId, client);
      return new RequestStatus().setRequestId(asyncId).waitFor(client, timeoutSeconds);
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

  protected abstract static class AsyncCollectionSpecificAdminRequest extends AsyncCollectionAdminRequest {

    protected String collection;

    public AsyncCollectionSpecificAdminRequest(CollectionAction action) {
      super(action);
    }

    public abstract AsyncCollectionSpecificAdminRequest setCollectionName(String collection);

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (collection == null)
        throw new IllegalArgumentException("You must call setCollectionName() on this request");
      params.set(CoreAdminParams.NAME, collection);
      return params;
    }
  }

  protected abstract static class AsyncShardSpecificAdminRequest extends AsyncCollectionAdminRequest {

    protected String collection;
    protected String shard;

    public AsyncShardSpecificAdminRequest(CollectionAction action) {
      super(action);
    }

    public abstract AsyncShardSpecificAdminRequest setCollectionName(String collection);

    public abstract AsyncShardSpecificAdminRequest setShardName(String shard);

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (collection == null)
        throw new IllegalArgumentException("You must call setCollectionName() on this request");
      if (shard == null)
        throw new IllegalArgumentException("You must call setShardName() on this request");
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.SHARD, shard);
      return params;
    }
  }

  protected abstract static class ShardSpecificAdminRequest extends CollectionAdminRequest {

    protected String collection;
    protected String shard;

    public ShardSpecificAdminRequest(CollectionAction action) {
      super(action);
    }

    public abstract ShardSpecificAdminRequest setCollectionName(String collection);

    public abstract ShardSpecificAdminRequest setShardName(String shard);

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (collection == null)
        throw new IllegalArgumentException("You must call setCollectionName() on this request");
      if (shard == null)
        throw new IllegalArgumentException("You must call setShardName() on this request");
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.SHARD, shard);
      return params;
    }

    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------


  protected abstract static class CollectionAdminRoleRequest extends AsyncCollectionAdminRequest {

    protected String node;
    protected String role;

    public CollectionAdminRoleRequest(CollectionAction action) {
      super(action);
    }

    @Override
    public CollectionAdminRoleRequest setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    public abstract CollectionAdminRoleRequest setNode(String node);

    public String getNode() {
      return this.node;
    }

    public abstract CollectionAdminRoleRequest setRole(String role);

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
  public static class Create extends AsyncCollectionSpecificAdminRequest {

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
      super(CollectionAction.CREATE);
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
     * Shard names must consist entirely of periods, underscores, hyphens, and alphanumerics.  Other characters are not allowed.
     * 
     * @throws IllegalArgumentException if any of the shard names contain invalid characters.
     */
    public Create setShards(String shards) {
      for (String shard : shards.split(",")) {
        if (!SolrIdentifierValidator.validateShardName(shard)) {
          throw new IllegalArgumentException(SolrIdentifierValidator.getIdentifierMessage(SolrIdentifierValidator.IdentifierType.SHARD,
              shard));
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
    public Create setCollectionName(String collectionName) throws SolrException {
      if (!SolrIdentifierValidator.validateCollectionName(collectionName)) {
        throw new IllegalArgumentException(SolrIdentifierValidator.getIdentifierMessage(SolrIdentifierValidator.IdentifierType.COLLECTION,
            collectionName));
      }
      this.collection = collectionName;
      return this;
    }

    @Override
    public Create setAsyncId(String id) {
      this.asyncId = id;
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

      params.set("collection.configName", configName);
      params.set("createNodeSet", createNodeSet);
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

  }

  // RELOAD request
  public static class Reload extends AsyncCollectionSpecificAdminRequest {

    public Reload() {
      super(CollectionAction.RELOAD);
    }

    @Override
    public Reload setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public Reload setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }
  }

  // DELETE request
  public static class Delete extends AsyncCollectionSpecificAdminRequest {

    public Delete() {
      super(CollectionAction.DELETE);
    }

    @Override
    public Delete setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public Delete setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }
  }

  // CREATESHARD request
  public static class CreateShard extends AsyncShardSpecificAdminRequest {

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
      super(CollectionAction.CREATESHARD);
    }

    @Override
    public CreateShard setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    /**
     * Provide the name of the shard to be created.
     * 
     * Shard names must consist entirely of periods, underscores, hyphens, and alphanumerics.  Other characters are not allowed.
     * 
     * @throws IllegalArgumentException if the shard name contains invalid characters.
     */
    @Override
    public CreateShard setShardName(String shardName) {
      if (!SolrIdentifierValidator.validateShardName(shardName)) {
        throw new IllegalArgumentException(SolrIdentifierValidator.getIdentifierMessage(SolrIdentifierValidator.IdentifierType.SHARD,
            shardName));
      }
      this.shard = shardName;
      return this;
    }

    @Override
    public CreateShard setAsyncId(String id) {
      this.asyncId = id;
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


  }

  // SPLITSHARD request
  public static class SplitShard extends AsyncShardSpecificAdminRequest {
    protected String ranges;
    protected String splitKey;

    private Properties properties;

    public SplitShard() {
      super(CollectionAction.SPLITSHARD);
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
    public SplitShard setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public SplitShard setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    public SplitShard setAsyncId(String id) {
      this.asyncId = id;
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

  }

  // DELETESHARD request
  public static class DeleteShard extends AsyncShardSpecificAdminRequest {

    private Boolean deleteInstanceDir;
    private Boolean deleteDataDir;

    public DeleteShard() {
      super(CollectionAction.DELETESHARD);
    }

    public Boolean getDeleteInstanceDir() {
      return deleteInstanceDir;
    }

    public DeleteShard setDeleteInstanceDir(Boolean deleteInstanceDir) {
      this.deleteInstanceDir = deleteInstanceDir;
      return this;
    }

    public Boolean getDeleteDataDir() {
      return deleteDataDir;
    }

    public DeleteShard setDeleteDataDir(Boolean deleteDataDir) {
      this.deleteDataDir = deleteDataDir;
      return this;
    }

    @Override
    public DeleteShard setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public DeleteShard setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    public DeleteShard setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (deleteInstanceDir != null) {
        params.set(CoreAdminParams.DELETE_INSTANCE_DIR, deleteInstanceDir);
      }
      if (deleteDataDir != null) {
        params.set(CoreAdminParams.DELETE_DATA_DIR, deleteDataDir);
      }
      return params;
    }
  }

  // FORCELEADER request
  public static class ForceLeader extends ShardSpecificAdminRequest {

    public ForceLeader() {
      super(CollectionAction.FORCELEADER);
    }


    @Override
    public ForceLeader setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public ForceLeader setShardName(String shard) {
      this.shard = shard;
      return this;
    }

  }

  public static class RequestStatusResponse extends CollectionAdminResponse {

    public RequestStatusState getRequestStatus() {
      NamedList innerResponse = (NamedList) getResponse().get("status");
      return RequestStatusState.fromKey((String) innerResponse.get("state"));
    }

  }

  // REQUESTSTATUS request
  public static class RequestStatus extends CollectionAdminRequest<RequestStatusResponse> {

    protected String requestId = null;

    public RequestStatus() {
      super(CollectionAction.REQUESTSTATUS);
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
      if (requestId == null)
        throw new IllegalArgumentException("You must call setRequestId() on this request");
      params.set(CoreAdminParams.REQUESTID, requestId);
      return params;
    }

    @Override
    protected RequestStatusResponse createResponse(SolrClient client) {
      return new RequestStatusResponse();
    }

    public RequestStatusState waitFor(SolrClient client, long timeoutSeconds)
        throws IOException, SolrServerException, InterruptedException {
      long finishTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
      RequestStatusState state = RequestStatusState.NOT_FOUND;
      while (System.nanoTime() < finishTime) {
        state = this.process(client).getRequestStatus();
        if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
          new DeleteStatus().setRequestId(requestId).process(client);
          return state;
        }
        TimeUnit.SECONDS.sleep(1);
      }
      return state;
    }
  }

  // DELETESTATUS request
  public static class DeleteStatus extends CollectionAdminRequest<CollectionAdminResponse> {

    protected String requestId = null;
    protected Boolean flush = null;

    public DeleteStatus() {
      super(CollectionAction.DELETESTATUS);
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
    protected CollectionAdminResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }

  }

  // CREATEALIAS request
  public static class CreateAlias extends AsyncCollectionAdminRequest {

    protected String aliasName;
    protected String aliasedCollections;

    public CreateAlias() {
      super(CollectionAction.CREATEALIAS);
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
        throw new IllegalArgumentException(SolrIdentifierValidator.getIdentifierMessage(SolrIdentifierValidator.IdentifierType.ALIAS,
            aliasName));
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

    @Override
    public CreateAlias setAsyncId(String id) {
      this.asyncId = id;
      return this;
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
  public static class DeleteAlias extends AsyncCollectionAdminRequest {

    protected String aliasName;

    public DeleteAlias() {
      super(CollectionAction.DELETEALIAS);
    }

    public DeleteAlias setAliasName(String aliasName) {
      this.aliasName = aliasName;
      return this;
    }

    @Override
    public DeleteAlias setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.NAME, aliasName);
      return params;
    }


  }

  // ADDREPLICA request
  public static class AddReplica extends AsyncCollectionAdminRequest {

    protected String collection;
    protected String shard;
    protected String node;
    protected String routeKey;
    protected String instanceDir;
    protected String dataDir;
    protected Properties properties;

    public AddReplica() {
      super(CollectionAction.ADDREPLICA);
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

    public AddReplica setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    public AddReplica setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    public AddReplica setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (collection == null)
        throw new IllegalArgumentException("You must call setCollection() on this request");
      params.add(CoreAdminParams.COLLECTION, collection);
      if (shard == null || shard.isEmpty()) {
        if (routeKey == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Either shard or routeKey must be provided");
        }
        params.add(ShardParams._ROUTE_, routeKey);
      }
      else {
        params.add(CoreAdminParams.SHARD, shard);
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


  }

  // DELETEREPLICA request
  public static class DeleteReplica extends AsyncShardSpecificAdminRequest {

    protected String replica;
    protected Boolean onlyIfDown;
    private Boolean deleteDataDir;
    private Boolean deleteInstanceDir;
    private Boolean deleteIndexDir;

    public DeleteReplica() {
      super(CollectionAction.DELETEREPLICA);
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
    public DeleteReplica setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public DeleteReplica setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    public DeleteReplica setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(ZkStateReader.REPLICA_PROP, this.replica);

      if (onlyIfDown != null) {
        params.set("onlyIfDown", onlyIfDown);
      }
      if (deleteDataDir != null) {
        params.set(CoreAdminParams.DELETE_DATA_DIR, deleteDataDir);
      }
      if (deleteInstanceDir != null) {
        params.set(CoreAdminParams.DELETE_INSTANCE_DIR, deleteInstanceDir);
      }
      if (deleteIndexDir != null) {
        params.set(CoreAdminParams.DELETE_INDEX, deleteIndexDir);
      }
      return params;
    }

    public Boolean getDeleteDataDir() {
      return deleteDataDir;
    }

    public DeleteReplica setDeleteDataDir(Boolean deleteDataDir) {
      this.deleteDataDir = deleteDataDir;
      return this;
    }

    public Boolean getDeleteInstanceDir() {
      return deleteInstanceDir;
    }

    public DeleteReplica setDeleteInstanceDir(Boolean deleteInstanceDir) {
      this.deleteInstanceDir = deleteInstanceDir;
      return this;
    }
  }

  // CLUSTERPROP request
  public static class ClusterProp extends CollectionAdminRequest<CollectionAdminResponse> {

    private String propertyName;
    private String propertyValue;

    public ClusterProp() {
      super(CollectionAction.CLUSTERPROP);
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
    protected CollectionAdminResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }


  }

  // MIGRATE request
  public static class Migrate extends AsyncCollectionAdminRequest {

    private String collection;
    private String targetCollection;
    private String splitKey;
    private Integer forwardTimeout;
    private Properties properties;

    public Migrate() {
      super(CollectionAction.MIGRATE);
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
    public Migrate setAsyncId(String id) {
      this.asyncId = id;
      return this;
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


  }

  // ADDROLE request
  public static class AddRole extends CollectionAdminRoleRequest {

    public AddRole() {
      super(CollectionAction.ADDROLE);
    }

    @Override
    public AddRole setNode(String node) {
      this.node = node;
      return this;
    }

    @Override
    public AddRole setRole(String role) {
      this.role = role;
      return this;
    }
  }

  // REMOVEROLE request
  public static class RemoveRole extends CollectionAdminRoleRequest {

    public RemoveRole() {
      super(CollectionAction.REMOVEROLE);
    }

    @Override
    public RemoveRole setNode(String node) {
      this.node = node;
      return this;
    }

    @Override
    public RemoveRole setRole(String role) {
      this.role = role;
      return this;
    }
  }

  // OVERSEERSTATUS request
  public static class OverseerStatus extends AsyncCollectionAdminRequest {

    public OverseerStatus () {
      super(CollectionAction.OVERSEERSTATUS);
    }

    @Override
    public OverseerStatus setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }
  }

  // CLUSTERSTATUS request
  public static class ClusterStatus extends CollectionAdminRequest<CollectionAdminResponse> {

    protected String shardName = null;
    protected String collection = null;
    protected String routeKey = null;

    public ClusterStatus () {
      super(CollectionAction.CLUSTERSTATUS);
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
    protected CollectionAdminResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }


  }

  // LIST request
  public static class List extends CollectionAdminRequest<CollectionAdminResponse> {
    public List () {
      super(CollectionAction.LIST);
    }

    @Override
    protected CollectionAdminResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }
  }

  // ADDREPLICAPROP request
  public static class AddReplicaProp extends AsyncShardSpecificAdminRequest {

    private String replica;
    private String propertyName;
    private String propertyValue;
    private Boolean shardUnique;

    public AddReplicaProp() {
      super(CollectionAction.ADDREPLICAPROP);
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
    public AddReplicaProp setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public AddReplicaProp setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    public AddReplicaProp setAsyncId(String id) {
      this.asyncId = id;
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

  }

  // DELETEREPLICAPROP request
  public static class DeleteReplicaProp extends AsyncShardSpecificAdminRequest {

    private String replica;
    private String propertyName;

    public DeleteReplicaProp() {
      super(CollectionAction.DELETEREPLICAPROP);
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
    public DeleteReplicaProp setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public DeleteReplicaProp setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    public DeleteReplicaProp setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set("replica", replica);
      params.set("property", propertyName);
      return params;
    }


  }

  // MIGRATECLUSTERSTATE request
  public static class MigrateClusterState extends AsyncCollectionAdminRequest {

    protected String collection;

    public MigrateClusterState() {
      super(CollectionAction.MIGRATESTATEFORMAT);
    }

    public MigrateClusterState setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    public MigrateClusterState setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (collection == null)
        throw new IllegalArgumentException("You must call setCollection() on this request");
      params.set(CoreAdminParams.COLLECTION, collection);
      return params;
    }
  }

  // BALANCESHARDUNIQUE request
  public static class BalanceShardUnique extends AsyncCollectionAdminRequest {

    protected String collection;
    protected String propertyName;
    protected Boolean onlyActiveNodes;
    protected Boolean shardUnique;

    public BalanceShardUnique() {
      super(CollectionAction.BALANCESHARDUNIQUE);
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
    public BalanceShardUnique setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set("property", propertyName);
      if (onlyActiveNodes != null)
        params.set("onlyactivenodes", onlyActiveNodes);
      if (shardUnique != null)
        params.set("shardUnique", shardUnique);
      return params;
    }

  }

}
