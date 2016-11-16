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
import java.util.Map;
import java.util.Optional;
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
import org.apache.solr.common.cloud.ImplicitDocRouter;
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

import static org.apache.solr.common.params.CollectionAdminParams.COUNT_PROP;

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
    for (String propertyName : props.stringPropertyNames()) {
      params.set(PROPERTY_PREFIX + propertyName, props.getProperty(propertyName));
    }
  }

  /**
   * Base class for asynchronous collection admin requests
   */
  public abstract static class AsyncCollectionAdminRequest extends CollectionAdminRequest<CollectionAdminResponse> {

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
    public  AsyncCollectionAdminRequest setAsyncId(String id){return this;};

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
      return requestStatus(asyncId).waitFor(client, timeoutSeconds);
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

    public AsyncCollectionSpecificAdminRequest(CollectionAction action, String collection) {
      super(action);
      this.collection = collection;
    }

    @Deprecated
    public abstract AsyncCollectionSpecificAdminRequest setCollectionName(String collection);

    public String getCollectionName() {
      return collection;
    }

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

    public AsyncShardSpecificAdminRequest(CollectionAction action, String collection, String shard) {
      super(action);
      this.collection = collection;
      this.shard = shard;
    }

    @Deprecated
    public abstract AsyncShardSpecificAdminRequest setCollectionName(String collection);

    @Deprecated
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

    public ShardSpecificAdminRequest(CollectionAction action, String collection, String shard) {
      super(action);
    }

    @Deprecated
    public abstract ShardSpecificAdminRequest setCollectionName(String collection);

    @Deprecated
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

    public CollectionAdminRoleRequest(CollectionAction action, String node, String role) {
      super(action);
    }

    @Override
    public CollectionAdminRoleRequest setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Deprecated
    public abstract CollectionAdminRoleRequest setNode(String node);

    public String getNode() {
      return this.node;
    }

    @Deprecated
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

  /**
   * Returns a SolrRequest for creating a collection
   * @param collection the collection name
   * @param config     the collection config
   * @param numShards  the number of shards in the collection
   * @param numReplicas the replication factor of the collection
   */
  public static Create createCollection(String collection, String config, int numShards, int numReplicas) {
    return new Create(collection, config, numShards, numReplicas);
  }

  /**
   * Returns a SolrRequest for creating a collection using a default configSet
   *
   * This requires that there is either a single configset configured in the cluster, or
   * that there is a configset with the same name as the collection
   *
   * @param collection  the collection name
   * @param numShards   the number of shards in the collection
   * @param numReplicas the replication factor of the collection
   */
  public static Create createCollection(String collection, int numShards, int numReplicas) {
    return new Create(collection, numShards, numReplicas);
  }

  /**
   * Returns a SolrRequest for creating a collection with the implicit router
   * @param collection  the collection name
   * @param config      the collection config
   * @param shards      a shard definition string
   * @param numReplicas the replication factor of the collection
   */
  public static Create createCollectionWithImplicitRouter(String collection, String config, String shards, int numReplicas) {
    return new Create(collection, config, shards, numReplicas);
  }

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

    /**
     * @deprecated Use {@link #createCollection(String, String, int, int)}
     */
    @Deprecated
    public Create() {
      super(CollectionAction.CREATE, null);
    }

    private Create(String collection, String config, int numShards, int numReplicas) {
      super(CollectionAction.CREATE, SolrIdentifierValidator.validateCollectionName(collection));
      this.configName = config;
      this.numShards = numShards;
      this.replicationFactor = numReplicas;
    }

    private Create(String collection, int numShards, int numReplicas) {
      super(CollectionAction.CREATE, SolrIdentifierValidator.validateCollectionName(collection));
      this.numShards = numShards;
      this.replicationFactor = numReplicas;
    }

    private Create(String collection, String config, String shards, int numReplicas) {
      super(CollectionAction.CREATE, SolrIdentifierValidator.validateCollectionName(collection));
      this.configName = config;
      this.replicationFactor = numReplicas;
      this.shards = shards;
      this.routerName = ImplicitDocRouter.NAME;
    }

    @Deprecated
    public Create setConfigName(String config) { this.configName = config; return this; }
    public Create setCreateNodeSet(String nodeSet) { this.createNodeSet = nodeSet; return this; }
    public Create setRouterName(String routerName) { this.routerName = routerName; return this; }
    public Create setRouterField(String routerField) { this.routerField = routerField; return this; }
    @Deprecated
    public Create setNumShards(Integer numShards) {this.numShards = numShards; return this; }
    public Create setMaxShardsPerNode(Integer numShards) { this.maxShardsPerNode = numShards; return this; }
    public Create setAutoAddReplicas(boolean autoAddReplicas) { this.autoAddReplicas = autoAddReplicas; return this; }
    @Deprecated
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
        SolrIdentifierValidator.validateShardName(shard);
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
    @Deprecated
    public Create setCollectionName(String collectionName) throws SolrException {
      this.collection = SolrIdentifierValidator.validateCollectionName(collectionName);
      return this;
    }

    @Override
    @Deprecated
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

    public Create setProperties(Map<String, String> properties) {
      this.properties = new Properties();
      this.properties.putAll(properties);
      return this;
    }

    public Create withProperty(String key, String value) {
      if (this.properties == null)
        this.properties = new Properties();
      this.properties.setProperty(key, value);
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();

      if (configName != null)
        params.set("collection.configName", configName);
      if (createNodeSet != null)
        params.set("createNodeSet", createNodeSet);
      if (numShards != null) {
        params.set( ZkStateReader.NUM_SHARDS_PROP, numShards);
      }
      if (maxShardsPerNode != null) {
        params.set( "maxShardsPerNode", maxShardsPerNode);
      }
      if (routerName != null)
        params.set( "router.name", routerName);
      if (shards != null)
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

  /**
   * Returns a SolrRequest to reload a collection
   */
  public static Reload reloadCollection(String collection) {
    return new Reload(collection);
  }

  // RELOAD request
  public static class Reload extends AsyncCollectionSpecificAdminRequest {

    /**
     * @deprecated use {@link #reloadCollection(String)}
     */
    @Deprecated
    public Reload() {
      super(CollectionAction.RELOAD, null);
    }

    private Reload(String collection) {
      super(CollectionAction.RELOAD, collection);
    }

    @Override
    @Deprecated
    public Reload setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    @Deprecated
    public Reload setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }
  }

  public static class DeleteNode extends AsyncCollectionAdminRequest {
    String node;

    /**
     * @param node The node to be deleted
     */
    public DeleteNode(String node) {
      super(CollectionAction.DELETENODE);
      this.node = node;
    }
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set("node", node);
      return params;
    }


  }

  public static class ReplaceNode extends AsyncCollectionAdminRequest {
    String source, target;
    Boolean parallel;

    /**
     * @param source node to be cleaned up
     * @param target node where the new replicas are to be created
     */
    public ReplaceNode(String source, String target) {
      super(CollectionAction.REPLACENODE);
      this.source = source;
      this.target = target;
    }

    public ReplaceNode setParallel(Boolean flag) {
      this.parallel = flag;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set("source", source);
      params.set("target", target);
      if (parallel != null) params.set("parallel", parallel.toString());
      return params;
    }

  }

  /*
   * Returns a RebalanceLeaders object to rebalance leaders for a collection
   */
  public static RebalanceLeaders rebalanceLeaders(String collection) {
    return new RebalanceLeaders(collection);
  }

  public static class RebalanceLeaders extends AsyncCollectionAdminRequest {

    protected Integer maxAtOnce;
    protected Integer maxWaitSeconds;
    protected String collection;

    public RebalanceLeaders setMaxAtOnce(Integer maxAtOnce) {
      this.maxAtOnce = maxAtOnce;
      return this;
    }

    public RebalanceLeaders setMaxWaitSeconds(Integer maxWaitSeconds) {
      this.maxWaitSeconds = maxWaitSeconds;
      return this;
    }

    public Integer getMaxAtOnce() {
      return maxAtOnce;
    }

    public Integer getMaxWaitSeconds() {
      return maxWaitSeconds;
    }

    public RebalanceLeaders(String collection) {
      super(CollectionAction.REBALANCELEADERS);
      this.collection = collection;
    }

    @Override
    public RebalanceLeaders setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();

      params.set(CoreAdminParams.COLLECTION, collection);

      if(this.maxWaitSeconds != null) {
        params.set("maxWaitSeconds", this.maxWaitSeconds);
      }

      if(this.maxAtOnce != null) {
        params.set("maxAtOnce", this.maxAtOnce);
      }

      return params;
    }

  }

  /**
   * Returns a SolrRequest to delete a collection
   */
  public static Delete deleteCollection(String collection) {
    return new Delete(collection);
  }

  // DELETE request
  public static class Delete extends AsyncCollectionSpecificAdminRequest {

    /**
     * @deprecated Use {@link #deleteCollection(String)}
     */
    @Deprecated
    public Delete() {
      super(CollectionAction.DELETE, null);
    }

    private Delete(String collection) {
      super(CollectionAction.DELETE, collection);
    }

    @Override
    @Deprecated
    public Delete setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    @Deprecated
    public Delete setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }
  }

  public static Backup backupCollection(String collection, String backupName) {
    return new Backup(collection, backupName);
  }

  // BACKUP request
  public static class Backup extends AsyncCollectionSpecificAdminRequest {
    protected final String name;
    protected Optional<String> repositoryName = Optional.empty();
    protected String location;
    protected Optional<String> commitName = Optional.empty();
    protected Optional<String> indexBackupStrategy = Optional.empty();

    public Backup(String collection, String name) {
      super(CollectionAction.BACKUP, collection);
      this.name = name;
      this.repositoryName = Optional.empty();
    }

    @Override
    @Deprecated
    public Backup setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    @Deprecated
    public Backup setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    public String getLocation() {
      return location;
    }

    public Backup setLocation(String location) {
      this.location = location;
      return this;
    }

    public Optional<String> getRepositoryName() {
      return repositoryName;
    }

    public Backup setRepositoryName(String repositoryName) {
      this.repositoryName = Optional.ofNullable(repositoryName);
      return this;
    }

    public Optional<String> getCommitName() {
      return commitName;
    }

    public Backup setCommitName(String commitName) {
      this.commitName = Optional.ofNullable(commitName);
      return this;
    }

    public Optional<String> getIndexBackupStrategy() {
      return indexBackupStrategy;
    }

    public Backup setIndexBackupStrategy(String indexBackupStrategy) {
      this.indexBackupStrategy = Optional.ofNullable(indexBackupStrategy);
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.NAME, name);
      params.set(CoreAdminParams.BACKUP_LOCATION, location); //note: optional
      if (repositoryName.isPresent()) {
        params.set(CoreAdminParams.BACKUP_REPOSITORY, repositoryName.get());
      }
      if (commitName.isPresent()) {
        params.set(CoreAdminParams.COMMIT_NAME, commitName.get());
      }
      if (indexBackupStrategy.isPresent()) {
        params.set(CollectionAdminParams.INDEX_BACKUP_STRATEGY, indexBackupStrategy.get());
      }
      return params;
    }

  }

  public static Restore restoreCollection(String collection, String backupName) {
    return new Restore(collection, backupName);
  }

  // RESTORE request
  public static class Restore extends AsyncCollectionSpecificAdminRequest {
    protected final String backupName;
    protected Optional<String> repositoryName = Optional.empty();
    protected String location;

    // in common with collection creation:
    protected String configName;
    protected Integer maxShardsPerNode;
    protected Integer replicationFactor;
    protected Boolean autoAddReplicas;
    protected Properties properties;

    public Restore(String collection, String backupName) {
      super(CollectionAction.RESTORE, collection);
      this.backupName = backupName;
    }

    @Override
    public Restore setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public Restore setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    public String getLocation() {
      return location;
    }

    public Restore setLocation(String location) {
      this.location = location;
      return this;
    }

    public Optional<String> getRepositoryName() {
      return repositoryName;
    }

    public Restore setRepositoryName(String repositoryName) {
      this.repositoryName = Optional.ofNullable(repositoryName);
      return this;
    }

    // Collection creation params in common:
    public Restore setConfigName(String config) { this.configName = config; return this; }
    public String getConfigName()  { return configName; }

    public Integer getMaxShardsPerNode() { return maxShardsPerNode; }
    public Restore setMaxShardsPerNode(int maxShardsPerNode) { this.maxShardsPerNode = maxShardsPerNode; return this; }

    public Integer getReplicationFactor() { return replicationFactor; }
    public Restore setReplicationFactor(Integer repl) { this.replicationFactor = repl; return this; }

    public Boolean getAutoAddReplicas() { return autoAddReplicas; }
    public Restore setAutoAddReplicas(boolean autoAddReplicas) { this.autoAddReplicas = autoAddReplicas; return this; }

    public Properties getProperties() {
      return properties;
    }
    public Restore setProperties(Properties properties) { this.properties = properties; return this;}

    // TODO support createNodeSet, rule, snitch

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.NAME, backupName);
      params.set(CoreAdminParams.BACKUP_LOCATION, location); //note: optional
      params.set("collection.configName", configName); //note: optional
      if (maxShardsPerNode != null) {
        params.set( "maxShardsPerNode", maxShardsPerNode);
      }
      if (replicationFactor != null) {
        params.set("replicationFactor", replicationFactor);
      }
      if (autoAddReplicas != null) {
        params.set(ZkStateReader.AUTO_ADD_REPLICAS, autoAddReplicas);
      }
      if (properties != null) {
        addProperties(params, properties);
      }
      if (repositoryName.isPresent()) {
        params.set(CoreAdminParams.BACKUP_REPOSITORY, repositoryName.get());
      }

      return params;
    }

  }

  //Note : This method is added since solrj module does not use Google
  // guava library. Also changes committed for SOLR-8765 result in wrong
  // error message when "collection" parameter is specified as Null.
  // This is because the setCollectionName method is deprecated.
  static <T> T checkNotNull(String param, T value) {
    if (value == null) {
      throw new NullPointerException("Please specify a value for parameter " + param);
    }
    return value;
  }

  @SuppressWarnings("serial")
  public static class CreateSnapshot extends AsyncCollectionSpecificAdminRequest {
    protected final String commitName;

    public CreateSnapshot(String collection, String commitName) {
      super(CollectionAction.CREATESNAPSHOT, checkNotNull(CoreAdminParams.COLLECTION ,collection));
      this.commitName = checkNotNull(CoreAdminParams.COMMIT_NAME, commitName);
    }

    public String getCollectionName() {
      return collection;
    }

    public String getCommitName() {
      return commitName;
    }

    @Override
    public AsyncCollectionSpecificAdminRequest setCollectionName (String collection) {
      this.collection = checkNotNull(CoreAdminParams.COLLECTION ,collection);
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.COMMIT_NAME, commitName);
      return params;
    }
  }

  @SuppressWarnings("serial")
  public static class DeleteSnapshot extends AsyncCollectionSpecificAdminRequest {
    protected final String commitName;

    public DeleteSnapshot (String collection, String commitName) {
      super(CollectionAction.DELETESNAPSHOT, checkNotNull(CoreAdminParams.COLLECTION ,collection));
      this.commitName = checkNotNull(CoreAdminParams.COMMIT_NAME, commitName);
    }

    public String getCollectionName() {
      return collection;
    }

    public String getCommitName() {
      return commitName;
    }

    @Override
    public AsyncCollectionSpecificAdminRequest setCollectionName (String collection) {
      this.collection = checkNotNull(CoreAdminParams.COLLECTION ,collection);
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.COMMIT_NAME, commitName);
      return params;
    }
  }

  @SuppressWarnings("serial")
  public static class ListSnapshots extends AsyncCollectionSpecificAdminRequest {
    public ListSnapshots (String collection) {
      super(CollectionAction.LISTSNAPSHOTS, checkNotNull(CoreAdminParams.COLLECTION ,collection));
    }

    public String getCollectionName() {
      return collection;
    }

    @Override
    public AsyncCollectionSpecificAdminRequest setCollectionName (String collection) {
      this.collection = checkNotNull(CoreAdminParams.COLLECTION ,collection);
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      return params;
    }
  }

  /**
   * Returns a SolrRequest to create a new shard in a collection
   */
  public static CreateShard createShard(String collection, String shard) {
    return new CreateShard(collection, shard);
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

    /**
     * @deprecated use {@link #createShard(String, String)}
     */
    @Deprecated
    public CreateShard() {
      super(CollectionAction.CREATESHARD, null, null);
    }

    private CreateShard(String collection, String shard) {
      super(CollectionAction.CREATESHARD, collection, SolrIdentifierValidator.validateShardName(shard));
    }

    @Override
    @Deprecated
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
    @Deprecated
    public CreateShard setShardName(String shardName) {
      this.shard = SolrIdentifierValidator.validateShardName(shardName);
      return this;
    }

    @Override
    @Deprecated
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

  /**
   * Returns a SolrRequest to split a shard in a collection
   */
  public static SplitShard splitShard(String collection) {
    return new SplitShard(collection);
  }

  // SPLITSHARD request
  public static class SplitShard extends AsyncCollectionAdminRequest {
    protected String collection;
    protected String ranges;
    protected String splitKey;
    protected String shard;

    private Properties properties;

    private SplitShard(String collection) {
      super(CollectionAction.SPLITSHARD);
      this.collection = collection;
    }

    /**
     * @deprecated Use {@link #splitShard(String)}
     */
    @Deprecated
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

    @Deprecated
    public SplitShard setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    public SplitShard setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    @Deprecated
    public SplitShard setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();

      if(this.collection == null) {
        throw new IllegalArgumentException("You must set collection name for this request.");
      }

      params.set(CollectionAdminParams.COLLECTION, collection);

      if (this.shard == null && this.splitKey == null) {
        throw new IllegalArgumentException("You must set shardname OR splitkey for this request.");
      }

      params.set("shard", shard);
      params.set("split.key", this.splitKey);
      params.set( "ranges", ranges);

      if(properties != null) {
        addProperties(params, properties);
      }
      return params;
    }

  }

  /**
   * Returns a SolrRequest to delete a shard from a collection
   */
  public static DeleteShard deleteShard(String collection, String shard) {
    return new DeleteShard(collection, shard);
  }

  // DELETESHARD request
  public static class DeleteShard extends AsyncShardSpecificAdminRequest {

    private Boolean deleteInstanceDir;
    private Boolean deleteDataDir;

    /**
     * @deprecated Use {@link #deleteShard(String, String)}
     */
    @Deprecated
    public DeleteShard() {
      super(CollectionAction.DELETESHARD, null, null);
    }

    private DeleteShard(String collection, String shard) {
      super(CollectionAction.DELETESHARD, collection, shard);
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
    @Deprecated
    public DeleteShard setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    @Deprecated
    public DeleteShard setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    @Deprecated
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

  /**
   * Returns a SolrRequest to force a leader election for a shard in a collection
   *
   * WARNING: This may cause data loss if the new leader does not contain updates
   * acknowledged by the old leader.  Use only if leadership elections are entirely
   * broken.
   */
  public static ForceLeader forceLeaderElection(String collection, String shard) {
    return new ForceLeader(collection, shard);
  }

  // FORCELEADER request
  public static class ForceLeader extends ShardSpecificAdminRequest {

    /**
     * @deprecated Use {@link #forceLeaderElection(String, String)}
     */
    @Deprecated
    public ForceLeader() {
      super(CollectionAction.FORCELEADER, null, null);
    }

    private ForceLeader(String collection, String shard) {
      super(CollectionAction.FORCELEADER, collection, shard);
    }

    @Override
    @Deprecated
    public ForceLeader setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    @Deprecated
    public ForceLeader setShardName(String shard) {
      this.shard = shard;
      return this;
    }

  }

  /**
   * A response object for {@link RequestStatus} requests
   */
  public static class RequestStatusResponse extends CollectionAdminResponse {

    public RequestStatusState getRequestStatus() {
      NamedList innerResponse = (NamedList) getResponse().get("status");
      return RequestStatusState.fromKey((String) innerResponse.get("state"));
    }

  }

  /**
   * Returns a SolrRequest for checking the status of an asynchronous request
   *
   * @see CollectionAdminRequest.AsyncCollectionAdminRequest
   */
  public static RequestStatus requestStatus(String requestId) {
    return new RequestStatus(requestId);
  }

  public static void waitForAsyncRequest(String requestId, SolrClient client, long timeout) throws SolrServerException, InterruptedException, IOException {
    requestStatus(requestId).waitFor(client, timeout);
  }

  // REQUESTSTATUS request
  public static class RequestStatus extends CollectionAdminRequest<RequestStatusResponse> {

    protected String requestId = null;

    private RequestStatus(String requestId) {
      super(CollectionAction.REQUESTSTATUS);
      this.requestId = requestId;
    }

    /**
     * @deprecated Use {@link #requestStatus(String)}
     */
    @Deprecated
    public RequestStatus() {
      super(CollectionAction.REQUESTSTATUS);
    }

    @Deprecated
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

    /**
     * Wait until the asynchronous request is either completed or failed, up to a timeout
     * @param client a SolrClient
     * @param timeoutSeconds the maximum time to wait in seconds
     * @return the last seen state of the request
     */
    public RequestStatusState waitFor(SolrClient client, long timeoutSeconds)
        throws IOException, SolrServerException, InterruptedException {
      long finishTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
      RequestStatusState state = RequestStatusState.NOT_FOUND;
      while (System.nanoTime() < finishTime) {
        state = this.process(client).getRequestStatus();
        if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
          deleteAsyncId(requestId).process(client);
          return state;
        }
        TimeUnit.SECONDS.sleep(1);
      }
      return state;
    }
  }

  /**
   * Returns a SolrRequest to delete an asynchronous request status
   */
  public static DeleteStatus deleteAsyncId(String requestId) {
    return new DeleteStatus(requestId);
  }

  public static DeleteStatus deleteAllAsyncIds() {
    return new DeleteStatus().setFlush(true);
  }

  // DELETESTATUS request
  public static class DeleteStatus extends CollectionAdminRequest<CollectionAdminResponse> {

    protected String requestId = null;
    protected Boolean flush = null;

    private DeleteStatus(String requestId) {
      super(CollectionAction.DELETESTATUS);
      this.requestId = requestId;
    }

    /**
     * @deprecated Use {@link #deleteAsyncId(String)} or {@link #deleteAllAsyncIds()}
     */
    @Deprecated
    public DeleteStatus() {
      super(CollectionAction.DELETESTATUS);
    }

    @Deprecated
    public DeleteStatus setRequestId(String requestId) {
      this.requestId = requestId;
      return this;
    }

    @Deprecated
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
      if (requestId == null && flush == null)
        throw new IllegalArgumentException("Either requestid or flush parameter must be specified.");
      if (requestId != null && flush != null)
        throw new IllegalArgumentException("Both requestid and flush parameters can not be specified together.");
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

  /**
   * Returns a SolrRequest to create a new alias
   * @param aliasName           the alias name
   * @param aliasedCollections  the collections to alias
   */
  public static CreateAlias createAlias(String aliasName, String aliasedCollections) {
    return new CreateAlias(aliasName, aliasedCollections);
  }

  // CREATEALIAS request
  public static class CreateAlias extends AsyncCollectionAdminRequest {

    protected String aliasName;
    protected String aliasedCollections;

    private CreateAlias(String aliasName, String aliasedCollections) {
      super(CollectionAction.CREATEALIAS);
      this.aliasName = SolrIdentifierValidator.validateAliasName(aliasName);
      this.aliasedCollections = aliasedCollections;
    }

    /**
     * @deprecated Use {@link #createAlias(String, String)}
     */
    @Deprecated
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
    @Deprecated
    public CreateAlias setAliasName(String aliasName) {
      this.aliasName = SolrIdentifierValidator.validateAliasName(aliasName);
      return this;
    }

    public String getAliasName() {
      return aliasName;
    }

    @Deprecated
    public CreateAlias setAliasedCollections(String alias) {
      this.aliasedCollections = alias;
      return this;
    }

    public String getAliasedCollections() {
      return this.aliasedCollections;
    }

    @Override
    @Deprecated
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

  /**
   * Returns a SolrRequest to delete an alias
   */
  public static DeleteAlias deleteAlias(String aliasName) {
    return new DeleteAlias(aliasName);
  }

  // DELETEALIAS request
  public static class DeleteAlias extends AsyncCollectionAdminRequest {

    protected String aliasName;

    private DeleteAlias(String aliasName) {
      super(CollectionAction.DELETEALIAS);
      this.aliasName = aliasName;
    }

    /**
     * @deprecated Use {@link #deleteAlias(String)}
     */
    @Deprecated
    public DeleteAlias() {
      super(CollectionAction.DELETEALIAS);
    }

    @Deprecated
    public DeleteAlias setAliasName(String aliasName) {
      this.aliasName = aliasName;
      return this;
    }

    @Override
    @Deprecated
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

  /**
   * Returns a SolrRequest to add a replica to a shard in a collection
   */
  public static AddReplica addReplicaToShard(String collection, String shard) {
    return new AddReplica(collection, shard, null);
  }

  /**
   * Returns a SolrRequest to add a replica to a collection using a route key
   */
  public static AddReplica addReplicaByRouteKey(String collection, String routeKey) {
    return new AddReplica(collection, null, routeKey);
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

    /**
     * @deprecated Use {@link #addReplicaByRouteKey(String, String)} or {@link #addReplicaToShard(String, String)}
     */
    @Deprecated
    public AddReplica() {
      super(CollectionAction.ADDREPLICA);
    }

    private AddReplica(String collection, String shard, String routeKey) {
      super(CollectionAction.ADDREPLICA);
      this.collection = collection;
      this.shard = shard;
      this.routeKey = routeKey;
    }

    public Properties getProperties() {
      return properties;
    }

    public AddReplica setProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public AddReplica withProperty(String key, String value) {
      if (this.properties == null)
        this.properties = new Properties();
      this.properties.setProperty(key, value);
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

    @Deprecated
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

    @Deprecated
    public AddReplica setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Deprecated
    public AddReplica setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    @Deprecated
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
          throw new IllegalArgumentException("Either shard or routeKey must be provided");
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

  /**
   * Returns a SolrRequest to delete a replica from a shard in a collection
   */
  public static DeleteReplica deleteReplica(String collection, String shard, String replica) {
    return new DeleteReplica(collection, shard, replica);
  }

  /**
   * Returns a SolrRequest to remove a number of replicas from a specific shard
   */
  public static DeleteReplica deleteReplicasFromShard(String collection, String shard, int count) {
    return new DeleteReplica(collection, shard, count);
  }

  public static DeleteReplica deleteReplicasFromAllShards(String collection, int count) {
    return new DeleteReplica(collection, count);
  }

  // DELETEREPLICA request
  public static class DeleteReplica extends AsyncCollectionSpecificAdminRequest {

    protected String shard;
    protected String replica;
    protected Boolean onlyIfDown;
    private Boolean deleteDataDir;
    private Boolean deleteInstanceDir;
    private Boolean deleteIndexDir;
    private Integer count;

    /**
     * @deprecated Use {@link #deleteReplica(String, String, String)}
     */
    @Deprecated
    public DeleteReplica() {
      super(CollectionAction.DELETEREPLICA, null);
    }

    private DeleteReplica(String collection, String shard, String replica) {
      super(CollectionAction.DELETEREPLICA, collection);
      this.shard = shard;
      this.replica = replica;
    }

    private DeleteReplica(String collection, String shard, int count) {
      super(CollectionAction.DELETEREPLICA, collection);
      this.shard = shard;
      this.count = count;
    }

    private DeleteReplica(String collection, int count) {
      super(CollectionAction.DELETEREPLICA, collection);
      this.count = count;
    }

    @Deprecated
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
    @Deprecated
    public DeleteReplica setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Deprecated
    public DeleteReplica setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Deprecated
    public DeleteReplica setCount(Integer count) {
      this.count = count;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());

      // AsyncCollectionSpecificAdminRequest uses 'name' rather than 'collection'
      // TODO - deal with this inconsistency
      params.remove(CoreAdminParams.NAME);
      if (this.collection == null)
        throw new IllegalArgumentException("You must set a collection name for this request");
      params.set(ZkStateReader.COLLECTION_PROP, this.collection);

      if (this.replica != null)
        params.set(ZkStateReader.REPLICA_PROP, this.replica);
      if (this.shard != null)
        params.set(ZkStateReader.SHARD_ID_PROP, this.shard);

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
      if (count != null) {
        params.set(COUNT_PROP, count);
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

    public Boolean getDeleteIndexDir() {
      return deleteIndexDir;
    }

    public DeleteReplica setDeleteIndexDir(Boolean deleteIndexDir) {
      this.deleteIndexDir = deleteIndexDir;
      return this;
    }
  }

  /**
   * Returns a SolrRequest to set a cluster property
   */
  public static ClusterProp setClusterProperty(String propertyName, String propertyValue) {
    return new ClusterProp(propertyName, propertyValue);
  }

  // CLUSTERPROP request
  public static class ClusterProp extends CollectionAdminRequest<CollectionAdminResponse> {

    private String propertyName;
    private String propertyValue;

    /**
     * @deprecated Use {@link #setClusterProperty(String, String)}
     */
    @Deprecated
    public ClusterProp() {
      super(CollectionAction.CLUSTERPROP);
    }

    private ClusterProp(String propertyName, String propertyValue) {
      super(CollectionAction.CLUSTERPROP);
      this.propertyName = propertyName;
      this.propertyValue = propertyValue;
    }

    @Deprecated
    public ClusterProp setPropertyName(String propertyName) {
      this.propertyName = propertyName;
      return this;
    }

    public String getPropertyName() {
      return this.propertyName;
    }

    @Deprecated
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

  /**
   * Returns a SolrRequest to migrate data matching a split key to another collection
   */
  public static Migrate migrateData(String collection, String targetCollection, String splitKey) {
    return new Migrate(collection, targetCollection, splitKey);
  }

  // MIGRATE request
  public static class Migrate extends AsyncCollectionAdminRequest {

    private String collection;
    private String targetCollection;
    private String splitKey;
    private Integer forwardTimeout;
    private Properties properties;

    /**
     * @deprecated Use {@link #migrateData(String, String, String)}
     */
    @Deprecated
    public Migrate() {
      super(CollectionAction.MIGRATE);
    }

    private Migrate(String collection, String targetCollection, String splitKey) {
      super(CollectionAction.MIGRATE);
      this.collection = collection;
      this.targetCollection = targetCollection;
      this.splitKey = splitKey;
    }

    @Deprecated
    public Migrate setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    public String getCollectionName() {
      return collection;
    }

    @Deprecated
    public Migrate setTargetCollection(String targetCollection) {
      this.targetCollection = targetCollection;
      return this;
    }

    public String getTargetCollection() {
      return this.targetCollection;
    }

    @Deprecated
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
    @Deprecated
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

  /**
   * Returns a SolrRequest to add a role to a node
   */
  public static AddRole addRole(String node, String role) {
    return new AddRole(node, role);
  }

  // ADDROLE request
  public static class AddRole extends CollectionAdminRoleRequest {

    /**
     * @deprecated Use {@link #addRole(String, String)}
     */
    @Deprecated
    public AddRole() {
      super(CollectionAction.ADDROLE, null, null);
    }

    private AddRole(String node, String role) {
      super(CollectionAction.ADDROLE, node, role);
    }

    @Override
    @Deprecated
    public AddRole setNode(String node) {
      this.node = node;
      return this;
    }

    @Override
    @Deprecated
    public AddRole setRole(String role) {
      this.role = role;
      return this;
    }
  }

  /**
   * Returns a SolrRequest to remove a role from a node
   */
  public static RemoveRole removeRole(String node, String role) {
    return new RemoveRole(node, role);
  }

  // REMOVEROLE request
  public static class RemoveRole extends CollectionAdminRoleRequest {

    /**
     * @deprecated Use {@link #removeRole(String, String)}
     */
    @Deprecated
    public RemoveRole() {
      super(CollectionAction.REMOVEROLE, null, null);
    }

    private RemoveRole(String node, String role) {
      super(CollectionAction.REMOVEROLE, node, role);
    }

    @Override
    @Deprecated
    public RemoveRole setNode(String node) {
      this.node = node;
      return this;
    }

    @Override
    @Deprecated
    public RemoveRole setRole(String role) {
      this.role = role;
      return this;
    }
  }

  /**
   * Return a SolrRequest to get the Overseer status
   */
  public static OverseerStatus getOverseerStatus() {
    return new OverseerStatus();
  }

  // OVERSEERSTATUS request
  public static class OverseerStatus extends AsyncCollectionAdminRequest {

    public OverseerStatus () {
      super(CollectionAction.OVERSEERSTATUS);
    }

    @Override
    @Deprecated
    public OverseerStatus setAsyncId(String id) {
      this.asyncId = id;
      return this;
    }
  }

  /**
   * Return a SolrRequest to get the Cluster status
   */
  public static ClusterStatus getClusterStatus() {
    return new ClusterStatus();
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

  /**
   * Returns a SolrRequest to get a list of collections in the cluster
   */
  public static java.util.List<String> listCollections(SolrClient client) throws IOException, SolrServerException {
    CollectionAdminResponse resp = new List().process(client);
    return (java.util.List<String>) resp.getResponse().get("collections");
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

  /**
   * Returns a SolrRequest to add a property to a specific replica
   */
  public static AddReplicaProp addReplicaProperty(String collection, String shard, String replica,
                                                  String propertyName, String propertyValue) {
    return new AddReplicaProp(collection, shard, replica, propertyName, propertyValue);
  }

  // ADDREPLICAPROP request
  public static class AddReplicaProp extends AsyncShardSpecificAdminRequest {

    private String replica;
    private String propertyName;
    private String propertyValue;
    private Boolean shardUnique;

    /**
     * @deprecated Use {@link #addReplicaProperty(String, String, String, String, String)}
     */
    @Deprecated
    public AddReplicaProp() {
      super(CollectionAction.ADDREPLICAPROP, null, null);
    }

    private AddReplicaProp(String collection, String shard, String replica, String propertyName, String propertyValue) {
      super(CollectionAction.ADDREPLICAPROP, collection, shard);
      this.replica = replica;
      this.propertyName = propertyName;
      this.propertyValue = propertyValue;
    }

    public String getReplica() {
      return replica;
    }

    @Deprecated
    public AddReplicaProp setReplica(String replica) {
      this.replica = replica;
      return this;
    }

    public String getPropertyName() {
      return propertyName;
    }

    @Deprecated
    public AddReplicaProp setPropertyName(String propertyName) {
      this.propertyName = propertyName;
      return this;
    }

    public String getPropertyValue() {
      return propertyValue;
    }

    @Deprecated
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
    @Deprecated
    public AddReplicaProp setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    @Deprecated
    public AddReplicaProp setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    @Deprecated
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

  /**
   * Returns a SolrRequest to delete a property from a specific replica
   */
  public static DeleteReplicaProp deleteReplicaProperty(String collection, String shard,
                                                        String replica, String propertyName) {
    return new DeleteReplicaProp(collection, shard, replica, propertyName);
  }

  // DELETEREPLICAPROP request
  public static class DeleteReplicaProp extends AsyncShardSpecificAdminRequest {

    private String replica;
    private String propertyName;

    /**
     * @deprecated Use {@link #deleteReplicaProperty(String, String, String, String)}
     */
    @Deprecated
    public DeleteReplicaProp() {
      super(CollectionAction.DELETEREPLICAPROP, null, null);
    }

    private DeleteReplicaProp(String collection, String shard, String replica, String propertyName) {
      super(CollectionAction.DELETEREPLICAPROP, collection, shard);
      this.replica = replica;
      this.propertyName = propertyName;
    }

    public String getReplica() {
      return replica;
    }

    @Deprecated
    public DeleteReplicaProp setReplica(String replica) {
      this.replica = replica;
      return this;
    }

    public String getPropertyName() {
      return propertyName;
    }

    @Deprecated
    public DeleteReplicaProp setPropertyName(String propertyName) {
      this.propertyName = propertyName;
      return this;
    }

    @Override
    @Deprecated
    public DeleteReplicaProp setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    @Deprecated
    public DeleteReplicaProp setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    @Override
    @Deprecated
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

  /**
   * Returns a SolrRequest to migrate a collection state format
   *
   * This is an expert-level request, and should not generally be necessary.
   */
  public static MigrateClusterState migrateCollectionFormat(String collection) {
    return new MigrateClusterState(collection);
  }

  // MIGRATECLUSTERSTATE request
  public static class MigrateClusterState extends AsyncCollectionAdminRequest {

    protected String collection;

    private MigrateClusterState(String collection) {
      super(CollectionAction.MIGRATESTATEFORMAT);
      this.collection = collection;
    }

    /**
     * @deprecated Use {@link #migrateCollectionFormat(String)}
     */
    @Deprecated
    public MigrateClusterState() {
      super(CollectionAction.MIGRATESTATEFORMAT);
    }

    @Deprecated
    public MigrateClusterState setCollectionName(String collection) {
      this.collection = collection;
      return this;
    }

    @Override
    @Deprecated
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

  /**
   * Returns a SolrRequest to balance a replica property across the shards of a collection
   */
  public static BalanceShardUnique balanceReplicaProperty(String collection, String propertyName) {
    return new BalanceShardUnique(collection, propertyName);
  }

  // BALANCESHARDUNIQUE request
  public static class BalanceShardUnique extends AsyncCollectionAdminRequest {

    protected String collection;
    protected String propertyName;
    protected Boolean onlyActiveNodes;
    protected Boolean shardUnique;

    private BalanceShardUnique(String collection, String propertyName) {
      super(CollectionAction.BALANCESHARDUNIQUE);
      this.collection = collection;
      this.propertyName = propertyName;
    }

    /**
     * @deprecated Use {@link #balanceReplicaProperty(String, String)}
     */
    @Deprecated
    public BalanceShardUnique() {
      super(CollectionAction.BALANCESHARDUNIQUE);
    }

    public String getPropertyName() {
      return propertyName;
    }

    @Deprecated
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

    @Deprecated
    public BalanceShardUnique setCollection(String collection) {
      this.collection = collection;
      return this;
    }

    public String getCollection() {
      return collection;
    }

    @Override
    @Deprecated
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
