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

import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.common.cloud.DocCollection.PER_REPLICA_STATE;
import static org.apache.solr.common.cloud.ZkStateReader.*;
import static org.apache.solr.common.params.CollectionAdminParams.*;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 4.5
 */
public abstract class CollectionAdminRequest<T extends CollectionAdminResponse> extends SolrRequest<T> implements MapWriter {

  /**
   * The set of modifiable collection properties
   */
  public static final java.util.List<String> MODIFIABLE_COLLECTION_PROPERTIES = Arrays.asList(
      REPLICATION_FACTOR,
      COLL_CONF,
      PER_REPLICA_STATE,
      READ_ONLY);

  protected final CollectionAction action;

  public static String PROPERTY_PREFIX = "property.";

  public CollectionAdminRequest(CollectionAction action) {
    this("/admin/collections", action);
  }

  public CollectionAdminRequest(String path, CollectionAction action) {
    super(METHOD.GET, path);
    this.action = checkNotNull(CoreAdminParams.ACTION, action);
  }

  @Override
  public SolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, action.toString());
    return params;
  }

  protected void addProperties(ModifiableSolrParams params, Properties props) {
    for (String propertyName : props.stringPropertyNames()) {
      params.set(PROPERTY_PREFIX + propertyName, props.getProperty(propertyName));
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("class", this.getClass().getName());
    ew.put("method", getMethod().toString());
    SolrParams params = getParams();
    if (params != null) {
      for (Iterator<String> it = params.getParameterNamesIterator(); it.hasNext(); ) {
        final String name = it.next();
        final String [] values = params.getParams(name);
        for (String value : values) {
          ew.put("params." + name, value);
        }
      }
    }
  }

  @Override
  public String toString() {
    return jsonStr();
  }

  @Override
  public String getRequestType() {
    return SolrRequestType.ADMIN.toString();
  }

  /**
   * Base class for asynchronous collection admin requests
   */
  public abstract static class AsyncCollectionAdminRequest extends CollectionAdminRequest<CollectionAdminResponse> {

    protected String asyncId = null;
    protected boolean waitForFinalState = false;

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

    public String getAsyncId() {
      return asyncId;
    }

    public void setWaitForFinalState(boolean waitForFinalState) {
      this.waitForFinalState = waitForFinalState;
    }

    public void setAsyncId(String asyncId) {
      this.asyncId = asyncId;
    }

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
      if (waitForFinalState) {
        params.set(CommonAdminParams.WAIT_FOR_FINAL_STATE, waitForFinalState);
      }
      return params;
    }
  }

  protected abstract static class AsyncCollectionSpecificAdminRequest extends AsyncCollectionAdminRequest {

    protected String collection;
    protected Boolean followAliases;

    public AsyncCollectionSpecificAdminRequest(CollectionAction action, String collection) {
      super(action);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
    }

    public String getCollectionName() {
      return collection;
    }

    public void setFollowAliases(Boolean followAliases) {
      this.followAliases = followAliases;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.NAME, collection);
      params.setNonNull(CollectionAdminParams.FOLLOW_ALIASES, followAliases);
      return params;
    }
  }

  protected abstract static class AsyncShardSpecificAdminRequest extends AsyncCollectionAdminRequest {

    protected String collection;
    protected String shard;

    public AsyncShardSpecificAdminRequest(CollectionAction action, String collection, String shard) {
      super(action);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
      this.shard = checkNotNull(CoreAdminParams.SHARD, shard);
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.SHARD, shard);
      return params;
    }
  }

  @SuppressWarnings({"rawtypes"})
  protected abstract static class ShardSpecificAdminRequest extends CollectionAdminRequest {

    protected String collection;
    protected String shard;

    public ShardSpecificAdminRequest(CollectionAction action, String collection, String shard) {
      super(action);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
      this.shard = checkNotNull(CoreAdminParams.SHARD, shard);
    }


    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
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
      this.role = checkNotNull(CollectionAdminParams.ROLE, role);
      this.node = checkNotNull(CoreAdminParams.NODE, node);
    }

    public String getNode() {
      return this.node;
    }

    public String getRole() {
      return this.role;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CollectionAdminParams.ROLE, this.role);
      params.set(CoreAdminParams.NODE, this.node);
      return params;
    }

  }

  /** Specific Collection API call implementations **/

  /**
   * Returns a SolrRequest for creating a collection
   * @param collection the collection name
   * @param config     the collection config
   * @param numShards  the number of shards in the collection
   * @param numNrtReplicas the number of {@link org.apache.solr.common.cloud.Replica.Type#NRT} replicas
   * @param numTlogReplicas the number of {@link org.apache.solr.common.cloud.Replica.Type#TLOG} replicas
   * @param numPullReplicas the number of {@link org.apache.solr.common.cloud.Replica.Type#PULL} replicas
   */
  public static Create createCollection(String collection, String config, Integer numShards, Integer numNrtReplicas, Integer numTlogReplicas, Integer numPullReplicas) {
    return new Create(collection, config, numShards, numNrtReplicas, numTlogReplicas, numPullReplicas);
  }

  /**
   * Returns a SolrRequest for creating a collection
   * @param collection the collection name
   * @param config     the collection config
   * @param numShards  the number of shards in the collection
   * @param numReplicas the replication factor of the collection (same as numNrtReplicas)
   */
  public static Create createCollection(String collection, String config, int numShards, int numReplicas) {
    return new Create(collection, config, numShards, numReplicas, null, null);
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
    return new Create(collection, null, numShards, numReplicas, 0, 0);
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

  /**
   * Returns a SolrRequest for creating a collection with the implicit router and specific types of replicas
   * @param collection  the collection name
   * @param config      the collection config
   * @param shards      a shard definition string
   * @param numNrtReplicas the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#NRT}
   * @param numTlogReplicas the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#TLOG}
   * @param numPullReplicas the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#PULL}
   */
  public static Create createCollectionWithImplicitRouter(String collection, String config, String shards, int numNrtReplicas, int numTlogReplicas, int numPullReplicas) {
    return new Create(collection, config, ImplicitDocRouter.NAME, null, checkNotNull("shards",shards), numNrtReplicas, numTlogReplicas, numPullReplicas);
  }

  /**
   * Returns a SolrRequest for modifying a collection with the given properties
   * @param collection  the collection name
   * @param properties a map of key and values with which the collection is to be modified
   */
  public static Modify modifyCollection(String collection, Map<String, Object> properties) {
    return new Modify(collection, properties);
  }

  // CREATE request
  public static class Create extends AsyncCollectionSpecificAdminRequest {

    protected String configName = null;
    protected String createNodeSet = null;
    protected String routerName;
    protected String policy;
    protected String shards;
    protected String routerField;
    protected Integer numShards;
    protected Integer nrtReplicas;
    protected Integer pullReplicas;
    protected Integer tlogReplicas;
    protected Boolean perReplicaState;

    protected Properties properties;
    protected String alias;
    protected String[] rule , snitch;

    /** Constructor intended for typical use cases */
    protected Create(String collection, String config, Integer numShards, Integer numNrtReplicas, Integer numTlogReplicas, Integer numPullReplicas) { // TODO: maybe add other constructors
      this(collection, config, null, numShards, null, numNrtReplicas, numTlogReplicas, numPullReplicas);
    }

    /** Constructor that assumes {@link ImplicitDocRouter#NAME} and an explicit list of <code>shards</code> */
    protected Create(String collection, String config, String shards, int numNrtReplicas) {
      this(collection, config, ImplicitDocRouter.NAME, null, checkNotNull("shards",shards), numNrtReplicas, null, null);
    }

    private Create(String collection, String config, String routerName, Integer numShards, String shards, Integer numNrtReplicas, Integer  numTlogReplicas, Integer numPullReplicas) {
      super(CollectionAction.CREATE, SolrIdentifierValidator.validateCollectionName(collection));
      // NOTE: there's very little we can assert about the args because nothing but "collection" is required by the server
      if ((null != shards) && (null != numShards)) {
        throw new IllegalArgumentException("Can not specify both a numShards and a list of shards");
      }
      this.configName = config;
      this.routerName = routerName;
      this.numShards = numShards;
      this.setShards(shards);
      this.nrtReplicas = numNrtReplicas;
      this.tlogReplicas = numTlogReplicas;
      this.pullReplicas = numPullReplicas;
    }

    public Create setCreateNodeSet(String nodeSet) { this.createNodeSet = nodeSet; return this; }
    public Create setRouterName(String routerName) { this.routerName = routerName; return this; }
    public Create setRouterField(String routerField) { this.routerField = routerField; return this; }
    public Create setNrtReplicas(Integer nrtReplicas) { this.nrtReplicas = nrtReplicas; return this;}
    public Create setTlogReplicas(Integer tlogReplicas) { this.tlogReplicas = tlogReplicas; return this;}
    public Create setPullReplicas(Integer pullReplicas) { this.pullReplicas = pullReplicas; return this;}

    public Create setReplicationFactor(Integer repl) { this.nrtReplicas = repl; return this; }
    public Create setRule(String... s){ this.rule = s; return this; }
    public Create setSnitch(String... s){ this.snitch = s; return this; }
    public Create setPerReplicaState(Boolean b) {this.perReplicaState =  b; return this; }

    public Create setAlias(String alias) {
      this.alias = alias;
      return this;
    }

    public String getConfigName()  { return configName; }
    public String getCreateNodeSet() { return createNodeSet; }
    public String getRouterName() { return  routerName; }
    public String getShards() { return  shards; }
    public Integer getNumShards() { return numShards; }

    public Integer getReplicationFactor() { return getNumNrtReplicas(); }
    public Integer getNumNrtReplicas() { return nrtReplicas; }
    public Integer getNumTlogReplicas() {return tlogReplicas;}
    public Integer getNumPullReplicas() {return pullReplicas;}
    public Boolean getPerReplicaState() {return perReplicaState;}

    /**
     * Provide the name of the shards to be created, separated by commas
     *
     * Shard names must consist entirely of periods, underscores, hyphens, and alphanumerics.  Other characters are not allowed.
     *
     * @throws IllegalArgumentException if any of the shard names contain invalid characters.
     */
    public Create setShards(String shards) {
      if (null != shards) {
        for (String shard : shards.split(",")) {
          SolrIdentifierValidator.validateShardName(shard);
        }
      }
      this.shards = shards;
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
        params.set(CREATE_NODE_SET_PARAM, createNodeSet);
      if (numShards != null) {
        params.set( ZkStateReader.NUM_SHARDS_PROP, numShards);
      }
      if (routerName != null)
        params.set( "router.name", routerName);
      if (shards != null)
        params.set("shards", shards);
      if (routerField != null) {
        params.set("router.field", routerField);
      }
      if (nrtReplicas != null) {
        params.set( ZkStateReader.NRT_REPLICAS, nrtReplicas);
      }
      if (properties != null) {
        addProperties(params, properties);
      }
      if (pullReplicas != null) {
        params.set(ZkStateReader.PULL_REPLICAS, pullReplicas);
      }
      if (tlogReplicas != null) {
        params.set(ZkStateReader.TLOG_REPLICAS, tlogReplicas);
      }
      if(Boolean.TRUE.equals(perReplicaState)) {
        params.set(PER_REPLICA_STATE, perReplicaState);
      }
      params.setNonNull(ALIAS, alias);
      return params;
    }

    public Create setPolicy(String policy) {
      this.policy = policy;
      return this;
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

    private Reload(String collection) {
      super(CollectionAction.RELOAD, collection);
    }
  }

  public static Rename renameCollection(String collection, String target) {
    return new Rename(collection, target);
  }

  public static class Rename extends AsyncCollectionSpecificAdminRequest {
    String target;

    public Rename(String collection, String target) {
      super(CollectionAction.RENAME, collection);
      this.target = target;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CollectionAdminParams.TARGET, target);
      return params;
    }
  }

  /**
   * Returns a SolrRequest to delete a node.
   */
  public static DeleteNode deleteNode(String node) {
    return new DeleteNode(node);
  }

  public static class DeleteNode extends AsyncCollectionAdminRequest {
    String node;

    /**
     * @param node The node to be deleted
     */
    public DeleteNode(String node) {
      super(CollectionAction.DELETENODE);
      this.node = checkNotNull("node",node);
    }
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.NODE, node);
      return params;
    }


  }

  public static class ReplaceNode extends AsyncCollectionAdminRequest {
    String sourceNode, targetNode;
    Boolean parallel;

    /**
     * @param source node to be cleaned up
     * @param target node where the new replicas are to be created
     */
    public ReplaceNode(String source, String target) {
      super(CollectionAction.REPLACENODE);
      this.sourceNode = checkNotNull(CollectionParams.SOURCE_NODE, source);
      this.targetNode = target;
    }

    public ReplaceNode setParallel(Boolean flag) {
      this.parallel = flag;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CollectionParams.SOURCE_NODE, sourceNode);
      params.set(CollectionParams.TARGET_NODE, targetNode);
      if (parallel != null) params.set("parallel", parallel.toString());
      return params;
    }

  }

  public static MoveReplica moveReplica(String collection, String replica, String targetNode) {
    return new MoveReplica(collection, replica, targetNode);
  }

  public static class MoveReplica extends AsyncCollectionAdminRequest {
    protected String collection, replica, targetNode;
    protected String shard, sourceNode;
    protected boolean randomlyMoveReplica;
    protected boolean inPlaceMove = true;
    protected int timeout = -1;

    public MoveReplica(String collection, String replica, String targetNode) {
      super(CollectionAction.MOVEREPLICA);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
      this.replica = checkNotNull(CoreAdminParams.REPLICA, replica);
      this.targetNode = checkNotNull(CollectionParams.TARGET_NODE, targetNode);
      this.randomlyMoveReplica = false;
    }

    public MoveReplica(String collection, String shard, String sourceNode, String targetNode) {
      super(CollectionAction.MOVEREPLICA);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
      this.shard = checkNotNull(CoreAdminParams.SHARD, shard);
      this.sourceNode = checkNotNull(CollectionParams.SOURCE_NODE, sourceNode);
      this.targetNode = checkNotNull(CollectionParams.TARGET_NODE, targetNode);
      this.randomlyMoveReplica = true;
    }

    public void setInPlaceMove(boolean inPlaceMove) {
      this.inPlaceMove = inPlaceMove;
    }

    public void setTimeout(int timeout) {
      this.timeout = timeout;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CollectionParams.TARGET_NODE, targetNode);
      params.set(CommonAdminParams.IN_PLACE_MOVE, inPlaceMove);
      if (timeout != -1) {
        params.set(CommonAdminParams.TIMEOUT, timeout);
      }
      if (randomlyMoveReplica) {
        params.set(CoreAdminParams.SHARD, shard);
        params.set(CollectionParams.SOURCE_NODE, sourceNode);
      } else {
        params.set(CoreAdminParams.REPLICA, replica);
      }
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
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
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
   * Returns a SolrRequest to reindex a collection
   */
  public static ReindexCollection reindexCollection(String collection) {
    return new ReindexCollection(collection);
  }

  public static class ReindexCollection extends AsyncCollectionSpecificAdminRequest {
    String target;
    String query;
    String fields;
    String configName;
    Boolean removeSource;
    String cmd;
    Integer batchSize;
    Map<String, Object> collectionParams = new HashMap<>();

    private ReindexCollection(String collection) {
      super(CollectionAction.REINDEXCOLLECTION, collection);
    }

    /** Target collection name (null if the same). */
    public ReindexCollection setTarget(String target) {
      this.target = target;
      return this;
    }

    /** Set optional command (eg. abort, status). */
    public ReindexCollection setCommand(String command) {
      this.cmd = command;
      return this;
    }

    /** Query matching the documents to reindex (default is '*:*'). */
    public ReindexCollection setQuery(String query) {
      this.query = query;
      return this;
    }

    /** Fields to reindex (the same syntax as {@link CommonParams#FL}), default is '*'. */
    public ReindexCollection setFields(String fields) {
      this.fields = fields;
      return this;
    }

    /** Remove source collection after success. Default is false. */
    public ReindexCollection setRemoveSource(boolean removeSource) {
      this.removeSource = removeSource;
      return this;
    }

    /** Copy documents in batches of this size. Default is 100. */
    public ReindexCollection setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /** Config name for the target collection. Default is the same as source. */
    public ReindexCollection setConfigName(String configName) {
      this.configName = configName;
      return this;
    }

    /** Set other supported collection CREATE parameters. */
    public ReindexCollection setCollectionParam(String key, Object value) {
      this.collectionParams.put(key, value);
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.setNonNull("target", target);
      params.setNonNull("cmd", cmd);
      params.setNonNull(ZkStateReader.CONFIGNAME_PROP, configName);
      params.setNonNull(CommonParams.Q, query);
      params.setNonNull(CommonParams.FL, fields);
      params.setNonNull("removeSource", removeSource);
      params.setNonNull(CommonParams.ROWS, batchSize);
      collectionParams.forEach((k, v) -> params.setNonNull(k, v));
      return params;
    }
  }

  /**
   * Return a SolrRequest for low-level detailed status of the specified collection. 
   * @param collection the collection to get the status of.
   */
  public static ColStatus collectionStatus(String collection) {
    checkNotNull(CoreAdminParams.COLLECTION, collection);
    return new ColStatus(collection);
  }
  
  /**
   * Return a SolrRequest for low-level detailed status of all collections on the cluster.
   */
  public static ColStatus collectionStatuses() {
    return new ColStatus();
  }

  public static class ColStatus extends AsyncCollectionAdminRequest {
    protected String collection = null;
    protected Boolean withSegments = null;
    protected Boolean withFieldInfo = null;
    protected Boolean withCoreInfo = null;
    protected Boolean withSizeInfo = null;
    protected Boolean withRawSizeInfo = null;
    protected Boolean withRawSizeSummary = null;
    protected Boolean withRawSizeDetails = null;
    protected Float rawSizeSamplingPercent = null;

    private ColStatus(String collection) {
      super(CollectionAction.COLSTATUS);
      this.collection = collection;
    }
    
    private ColStatus() {
      super(CollectionAction.COLSTATUS);
    }

    public ColStatus setWithSegments(boolean withSegments) {
      this.withSegments = withSegments;
      return this;
    }

    public ColStatus setWithFieldInfo(boolean withFieldInfo) {
      this.withFieldInfo = withFieldInfo;
      return this;
    }

    public ColStatus setWithCoreInfo(boolean withCoreInfo) {
      this.withCoreInfo = withCoreInfo;
      return this;
    }

    public ColStatus setWithSizeInfo(boolean withSizeInfo) {
      this.withSizeInfo = withSizeInfo;
      return this;
    }

    public ColStatus setWithRawSizeInfo(boolean withRawSizeInfo) {
      this.withRawSizeInfo = withRawSizeInfo;
      return this;
    }

    public ColStatus setWithRawSizeSummary(boolean withRawSizeSummary) {
      this.withRawSizeSummary = withRawSizeSummary;
      return this;
    }

    public ColStatus setWithRawSizeDetails(boolean withRawSizeDetails) {
      this.withRawSizeDetails = withRawSizeDetails;
      return this;
    }

    public ColStatus setRawSizeSamplingPercent(float rawSizeSamplingPercent) {
      this.rawSizeSamplingPercent = rawSizeSamplingPercent;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams)super.getParams();
      params.setNonNull(CoreAdminParams.COLLECTION, collection);
      params.setNonNull("segments", withSegments);
      params.setNonNull("fieldInfo", withFieldInfo);
      params.setNonNull("coreInfo", withCoreInfo);
      params.setNonNull("sizeInfo", withSizeInfo);
      params.setNonNull("rawSizeInfo", withRawSizeInfo);
      params.setNonNull("rawSizeSummary", withRawSizeSummary);
      params.setNonNull("rawSizeDetails", withRawSizeDetails);
      params.setNonNull("rawSizeSamplingPercent", rawSizeSamplingPercent);
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

    private Delete(String collection) {
      super(CollectionAction.DELETE, collection);
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
    protected boolean incremental = true;
    protected Optional<Integer> maxNumBackupPoints = Optional.empty();

    public Backup(String collection, String name) {
      super(CollectionAction.BACKUP, collection);
      this.name = name;
      this.repositoryName = Optional.empty();
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

    /**
     * Specifies the backup method to use: the deprecated 'full-snapshot' format, or the current 'incremental' format.
     *
     * Defaults to 'true' if unspecified.
     *
     * Incremental backups are almost always preferable to the deprecated 'full-snapshot' format, as incremental backups
     * can take advantage of previously backed-up files and will only upload those that aren't already stored in the
     * repository - saving lots of time and network bandwidth.  The older 'full-snapshot' format should only be used by
     * experts with a particular reason to do so.
     *
     * @param incremental true to use incremental backups, false otherwise.
     */
    @Deprecated
    public Backup setIncremental(boolean incremental) {
      this.incremental = incremental;
      return this;
    }

    /**
     * Specifies the maximum number of backup points to keep at the backup location.
     *
     * If the current backup causes the number of stored backup points to exceed this value, the oldest backup points
     * are cleaned up so that only {@code #maxNumBackupPoints} are retained.
     *
     * This parameter is ignored if the request uses a non-incremental backup.
     * @param maxNumBackupPoints the number of backup points to retain after the current backup
     */
    public Backup setMaxNumberBackupPoints(int maxNumBackupPoints) {
      this.maxNumBackupPoints = Optional.of(maxNumBackupPoints);
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
      if (maxNumBackupPoints.isPresent()) {
        params.set(CoreAdminParams.MAX_NUM_BACKUP_POINTS, maxNumBackupPoints.get());
      }
      params.set(CoreAdminParams.BACKUP_INCREMENTAL, incremental);
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
    protected Integer replicationFactor;
    protected Integer nrtReplicas;
    protected Integer tlogReplicas;
    protected Integer pullReplicas;
    protected Optional<String> createNodeSet = Optional.empty();
    protected Optional<Boolean> createNodeSetShuffle = Optional.empty();
    protected Properties properties;
    protected Integer backupId;

    public Restore(String collection, String backupName) {
      super(CollectionAction.RESTORE, collection);
      this.backupName = backupName;
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

    public void setCreateNodeSet(String createNodeSet) {
      this.createNodeSet = Optional.of(createNodeSet);
    }

    public Optional<String> getCreateNodeSet() {
      return createNodeSet;
    }

    public Optional<Boolean> getCreateNodeSetShuffle() {
      return createNodeSetShuffle;
    }

    public void setCreateNodeSetShuffle(boolean createNodeSetShuffle) {
      this.createNodeSetShuffle = Optional.of(createNodeSetShuffle);
    }

    // Collection creation params in common:
    public Restore setConfigName(String config) { this.configName = config; return this; }
    public String getConfigName()  { return configName; }

    public Integer getReplicationFactor() { return replicationFactor; }
    public Restore setReplicationFactor(Integer replicationFactor) { this.replicationFactor = replicationFactor; return this; }

    public Integer getNrtReplicas() { return nrtReplicas; }
    public Restore setNrtReplicas(Integer nrtReplicas) { this.nrtReplicas= nrtReplicas; return this; };

    public Integer getTlogReplicas() { return tlogReplicas; }
    public Restore setTlogReplicas(Integer tlogReplicas) { this.tlogReplicas = tlogReplicas; return this; }

    public Integer getPullReplicas() { return pullReplicas; }
    public Restore setPullReplicas(Integer pullReplicas) { this.pullReplicas = pullReplicas; return this; }

    public Properties getProperties() {
      return properties;
    }
    public Restore setProperties(Properties properties) { this.properties = properties; return this;}

    /**
     * Specify the ID of the backup-point to restore from.
     *
     * '-1'q is used by default to have Solr restore from the most recent backup-point.
     *
     * Solr can store multiple backup points for a given collection - each identified by a unique backup ID.  Users who
     * want to restore a particular backup-point can specify it using this method.
     *
     * @param backupId the ID of the backup-point to restore from
     */
    public Restore setBackupId(int backupId) {
      this.backupId = backupId;
      return this;
    }

    // TODO support rule, snitch

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.COLLECTION, collection);
      params.set(CoreAdminParams.NAME, backupName);
      params.set(CoreAdminParams.BACKUP_LOCATION, location); //note: optional
      params.set("collection.configName", configName); //note: optional
      if (replicationFactor != null && nrtReplicas != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Cannot set both replicationFactor and nrtReplicas as they mean the same thing");
      }
      if (replicationFactor != null) {
        params.set(ZkStateReader.REPLICATION_FACTOR, replicationFactor);
      }
      if (nrtReplicas != null) {
        params.set(ZkStateReader.NRT_REPLICAS, nrtReplicas);
      }
      if (pullReplicas != null) {
        params.set(ZkStateReader.PULL_REPLICAS, pullReplicas);
      }
      if (tlogReplicas != null) {
        params.set(ZkStateReader.TLOG_REPLICAS, tlogReplicas);
      }
      if (properties != null) {
        addProperties(params, properties);
      }
      if (repositoryName.isPresent()) {
        params.set(CoreAdminParams.BACKUP_REPOSITORY, repositoryName.get());
      }
      if (createNodeSet.isPresent()) {
        params.set(CREATE_NODE_SET_PARAM, createNodeSet.get());
      }
      if (createNodeSetShuffle.isPresent()) {
        params.set(CREATE_NODE_SET_SHUFFLE_PARAM, createNodeSetShuffle.get());
      }
      if (backupId != null) {
        params.set(CoreAdminParams.BACKUP_ID, backupId);
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
      throw new NullPointerException("Please specify a non-null value for parameter " + param);
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

    private CreateShard(String collection, String shard) {
      super(CollectionAction.CREATESHARD, collection, SolrIdentifierValidator.validateShardName(shard));
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      if (nodeSet != null) {
        params.set(CREATE_NODE_SET_PARAM, nodeSet);
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
    protected String splitMethod;
    protected Boolean splitByPrefix;
    protected Integer numSubShards;
    protected Float splitFuzz;

    private Properties properties;

    private SplitShard(String collection) {
      super(CollectionAction.SPLITSHARD);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
    }

    public SplitShard setRanges(String ranges) { this.ranges = ranges; return this; }
    public String getRanges() { return ranges; }

    public Integer getNumSubShards() {
      return numSubShards;
    }

    public SplitShard setNumSubShards(Integer numSubShards) {
      this.numSubShards = numSubShards;
      return this;
    }

    public SplitShard setSplitMethod(String splitMethod) {
      this.splitMethod = splitMethod;
      return this;
    }

    public String getSplitMethod() {
      return splitMethod;
    }

    public SplitShard setSplitFuzz(float splitFuzz) {
      this.splitFuzz = splitFuzz;
      return this;
    }

    public Float getSplitFuzz() {
      return splitFuzz;
    }

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

    public SplitShard setShardName(String shard) {
      this.shard = shard;
      return this;
    }

    public Boolean getSplitByPrefix() {
      return splitByPrefix;
    }

    public SplitShard setSplitByPrefix(Boolean splitByPrefix) {
      this.splitByPrefix = splitByPrefix;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();

      params.set(CollectionAdminParams.COLLECTION, collection);

      if (this.shard == null && this.splitKey == null) {
        throw new IllegalArgumentException("You must set shardname OR splitkey for this request.");
      }

      params.set(CoreAdminParams.SHARD, shard);
      params.set("split.key", this.splitKey);
      params.set(CoreAdminParams.RANGES, ranges);
      params.set(CommonAdminParams.SPLIT_METHOD, splitMethod);
      if(numSubShards != null) {
        params.set("numSubShards", numSubShards);
      }
      if (splitFuzz != null) {
        params.set(CommonAdminParams.SPLIT_FUZZ, String.valueOf(splitFuzz));
      }

      if (splitByPrefix != null) {
        params.set(CommonAdminParams.SPLIT_BY_PREFIX, splitByPrefix);
      }

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
    private ForceLeader(String collection, String shard) {
      super(CollectionAction.FORCELEADER, collection, shard);
    }
  }

  /**
   * A response object for {@link RequestStatus} requests
   */
  public static class RequestStatusResponse extends CollectionAdminResponse {

    public RequestStatusState getRequestStatus() {
      @SuppressWarnings({"rawtypes"})
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
      this.requestId = checkNotNull("requestId", requestId);
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
    return new DeleteStatus(checkNotNull("requestId", requestId), null);
  }

  /**
   * Returns a SolrRequest to delete a all asynchronous request statuses
   */
  public static DeleteStatus deleteAllAsyncIds() {
    return new DeleteStatus(null, true);
  }

  // DELETESTATUS request
  public static class DeleteStatus extends CollectionAdminRequest<CollectionAdminResponse> {

    protected String requestId = null;
    protected Boolean flush = null;

    private DeleteStatus(String requestId, Boolean flush) {
      super(CollectionAction.DELETESTATUS);
      if (requestId == null && flush == null)
        throw new IllegalArgumentException("Either requestid or flush parameter must be specified.");
      if (requestId != null && flush != null)
        throw new IllegalArgumentException("Both requestid and flush parameters can not be specified together.");

      this.requestId = requestId;
      this.flush = flush;
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

  // ALIASPROP request

  /**
   * Returns a SolrRequest to add or remove properties from an alias
   * @param aliasName         the alias to modify
   */

  public static SetAliasProperty setAliasProperty(String aliasName) {
    return new SetAliasProperty(aliasName);
  }

  public static class SetAliasProperty extends AsyncCollectionAdminRequest {

    private final String aliasName;
    private Map<String,String> properties = new HashMap<>();

    public SetAliasProperty(String aliasName) {
      super(CollectionAction.ALIASPROP);
      this.aliasName = SolrIdentifierValidator.validateAliasName(aliasName);
    }

    public SetAliasProperty addProperty(String key, String value) {
      properties.put(key,value);
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set(CoreAdminParams.NAME, aliasName);
      properties.forEach((key, value) ->  params.set("property." + key, value));
      return params;
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
      this.aliasedCollections = checkNotNull("aliasedCollections",aliasedCollections);
    }

    public String getAliasName() {
      return aliasName;
    }

    public String getAliasedCollections() {
      return this.aliasedCollections;
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
   * Returns a SolrRequest to create a time routed alias. For time based routing, the start
   * should be a standard Solr timestamp string (possibly with "date math").
   *
   * @param aliasName the name of the alias to create.
   * @param start the start of the routing.  A standard Solr date: ISO-8601 or NOW with date math.
   * @param interval date math representing the time duration of each collection (e.g. {@code +1DAY})
   * @param routerField the document field to contain the timestamp to route on
   * @param createCollTemplate Holds options to create a collection.  The "name" is ignored.
   */
  public static CreateTimeRoutedAlias createTimeRoutedAlias(String aliasName, String start,
                                                            String interval,
                                                            String routerField,
                                                            Create createCollTemplate) {

    return new CreateTimeRoutedAlias(aliasName, routerField, start, interval, createCollTemplate);
  }

  public static class CreateTimeRoutedAlias extends AsyncCollectionAdminRequest implements RoutedAliasAdminRequest {
    // TODO: This and other commands in this file seem to need to share some sort of constants class with core
    // to allow this stuff not to be duplicated. (this is pasted from CreateAliasCmd.java), however I think
    // a comprehensive cleanup of this for all the requests in this class should be done as a separate ticket.

    public static final String ROUTER_START = "router.start";
    public static final String ROUTER_INTERVAL = "router.interval";
    public static final String ROUTER_MAX_FUTURE = "router.maxFutureMs";
    public static final String ROUTER_PREEMPTIVE_CREATE_WINDOW = "router.preemptiveCreateMath";
    public static final String ROUTER_AUTO_DELETE_AGE = "router.autoDeleteAge";

    private final String aliasName;
    private final String routerField;
    private final String start;
    private final String interval;
    //Optional:
    private TimeZone tz;
    private Integer maxFutureMs;
    private String preemptiveCreateMath;
    private String autoDeleteAge;

    private final Create createCollTemplate;

    public CreateTimeRoutedAlias(String aliasName, String routerField, String start, String interval, Create createCollTemplate) {
      super(CollectionAction.CREATEALIAS);
      this.aliasName = aliasName;
      this.start = start;
      this.interval = interval;
      this.routerField = routerField;
      this.createCollTemplate = createCollTemplate;
    }

    /** Sets the timezone for interpreting any Solr "date math. */
    public CreateTimeRoutedAlias setTimeZone(TimeZone tz) {
      this.tz = tz;
      return this;
    }

    /** Sets how long into the future (millis) that we will allow a document to pass. */
    public CreateTimeRoutedAlias setMaxFutureMs(Integer maxFutureMs) {
      this.maxFutureMs = maxFutureMs;
      return this;
    }

    public CreateTimeRoutedAlias setPreemptiveCreateWindow(String preemptiveCreateMath) {
      this.preemptiveCreateMath = preemptiveCreateMath;
      return this;
    }

    public CreateTimeRoutedAlias setAutoDeleteAge(String autoDeleteAge) {
      this.autoDeleteAge = autoDeleteAge;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.add(CommonParams.NAME, aliasName);
      params.add(ROUTER_TYPE_NAME, "time");
      params.add(ROUTER_FIELD, routerField);
      params.add(ROUTER_START, start);
      params.add(ROUTER_INTERVAL, interval);
      if (tz != null) {
        params.add(CommonParams.TZ, tz.getID());
      }
      if (maxFutureMs != null) {
        params.add(ROUTER_MAX_FUTURE, ""+maxFutureMs);
      }
      if (preemptiveCreateMath != null) {
        params.add(ROUTER_PREEMPTIVE_CREATE_WINDOW, preemptiveCreateMath);
      }
      if (autoDeleteAge != null) {
        params.add(ROUTER_AUTO_DELETE_AGE, autoDeleteAge);
      }

      // merge the above with collectionParams.  Above takes precedence.
      ModifiableSolrParams createCollParams = mergeCollParams(createCollTemplate);
      return SolrParams.wrapDefaults(params, createCollParams);
    }

    @Override
    public RoutedAliasTypes getType() {
      return RoutedAliasTypes.TIME;
    }

    @Override
    public String getRouterField() {
      return routerField;
    }

    @Override
    public java.util.List<String> getParamNames() {
      return java.util.List.of(ROUTER_TYPE_NAME, ROUTER_FIELD, ROUTER_START, ROUTER_INTERVAL,ROUTER_MAX_FUTURE, ROUTER_PREEMPTIVE_CREATE_WINDOW, ROUTER_AUTO_DELETE_AGE, CommonParams.TZ);
    }

    @Override
    public java.util.List<String> getRequiredParamNames() {
      return java.util.List.of(ROUTER_TYPE_NAME, ROUTER_FIELD,ROUTER_START, ROUTER_INTERVAL);
    }
  }
  /**
   * Returns a SolrRequest to create a category routed alias.
   *
   * @param aliasName the name of the alias to create.
   * @param routerField the document field to contain the timestamp to route on
   * @param maxCardinality the maximum number of collections under this CRA
   * @param createCollTemplate Holds options to create a collection.  The "name" is ignored.
   */
  public static CreateCategoryRoutedAlias createCategoryRoutedAlias(String aliasName,
                                                            String routerField,
                                                            int maxCardinality,
                                                            Create createCollTemplate) {

    return new CreateCategoryRoutedAlias(aliasName, routerField, maxCardinality, createCollTemplate);
  }

  public static class CreateCategoryRoutedAlias extends AsyncCollectionAdminRequest implements RoutedAliasAdminRequest {

    public static final String ROUTER_MAX_CARDINALITY = "router.maxCardinality";
    public static final String ROUTER_MUST_MATCH = "router.mustMatch";

    private final String aliasName;
    private final String routerField;
    private Integer maxCardinality;
    private String mustMatch;

    private final Create createCollTemplate;

    public CreateCategoryRoutedAlias(String aliasName, String routerField, int maxCardinality, Create createCollTemplate) {
      super(CollectionAction.CREATEALIAS);
      this.aliasName = aliasName;
      this.routerField = routerField;
      this.maxCardinality = maxCardinality;
      this.createCollTemplate = createCollTemplate;
    }

    public CreateCategoryRoutedAlias setMustMatch(String regex) {
      this.mustMatch = regex;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.add(CommonParams.NAME, aliasName);
      params.add(ROUTER_TYPE_NAME, RoutedAliasTypes.CATEGORY.name());
      params.add(ROUTER_FIELD, routerField);
      params.add(ROUTER_MAX_CARDINALITY, maxCardinality.toString());

      if (mustMatch != null) {
        params.add(ROUTER_MUST_MATCH, mustMatch);
      }

      // merge the above with collectionParams.  Above takes precedence.
      ModifiableSolrParams createCollParams = mergeCollParams(createCollTemplate);
      return SolrParams.wrapDefaults(params, createCollParams);
    }

    @Override
    public RoutedAliasTypes getType() {
      return RoutedAliasTypes.CATEGORY;
    }

    @Override
    public String getRouterField() {
      return routerField;
    }

    @Override
    public java.util.List<String> getParamNames() {
      return java.util.List.of(ROUTER_TYPE_NAME, ROUTER_FIELD,ROUTER_MAX_CARDINALITY, ROUTER_MUST_MATCH);
    }

    @Override
    public java.util.List<String> getRequiredParamNames() {
      return java.util.List.of(ROUTER_TYPE_NAME, ROUTER_FIELD,ROUTER_MAX_CARDINALITY);
    }
  }

  public interface RoutedAliasAdminRequest {
    String ROUTER_TYPE_NAME = "router.name";
    String ROUTER_FIELD = "router.field";

    RoutedAliasTypes getType();
    String getRouterField();
    java.util.List<String> getParamNames();
    java.util.List<String> getRequiredParamNames();
    SolrParams getParams();
    default ModifiableSolrParams mergeCollParams(Create createCollTemplate) {
      ModifiableSolrParams createCollParams = new ModifiableSolrParams(); // output target
      if (createCollTemplate == null) {
        return createCollParams;
      }
      final SolrParams collParams = createCollTemplate.getParams();
      final Iterator<String> pIter = collParams.getParameterNamesIterator();
      while (pIter.hasNext()) {
        String key = pIter.next();
        if (key.equals(CollectionParams.ACTION) || key.equals("name")) {
          continue;
        }
        createCollParams.set("create-collection." + key, collParams.getParams(key));
      }
      return createCollParams;
    }
  }

  /**
   * Create a Dimensional Routed alias from two or more routed alias types.
   *
   * @param aliasName The name of the alias
   * @param createCollTemplate a create command that will be used for all collections created
   * @param dims Routed Alias requests. Note that the aliasName and collection templates inside dimensions
   *             will be ignored and may be safely set to null
   * @return An object representing a basic DimensionalRoutedAlias creation request.
   */
  public static DimensionalRoutedAlias createDimensionalRoutedAlias(String aliasName, Create createCollTemplate, RoutedAliasAdminRequest... dims) {
    return new DimensionalRoutedAlias(aliasName, createCollTemplate, dims);
  }

  public static class DimensionalRoutedAlias extends AsyncCollectionAdminRequest implements RoutedAliasAdminRequest {

    private String aliasName;
    private final Create createCollTemplate;
    private final RoutedAliasAdminRequest[] dims;

    public DimensionalRoutedAlias(String aliasName, Create createCollTemplate, RoutedAliasAdminRequest... dims) {
      super(CollectionAction.CREATEALIAS);
      this.aliasName = aliasName;
      this.createCollTemplate = createCollTemplate;
      this.dims = dims;
    }

    public static void addDimensionIndexIfRequired(Set<String> params, int i, String param) {
      params.add(withDimensionIndexIfRequired(param, i));
    }

    private static String withDimensionIndexIfRequired(String param, int index) {
      if (param.startsWith(ROUTER_PREFIX)) {
        return ROUTER_PREFIX + index + "." + param.split("\\.")[1];
      } else {
        return param;
      }
    }


    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      java.util.List<String> types = new ArrayList<>();
      java.util.List<String> fields = new ArrayList<>();
      for (int i = 0; i < dims.length; i++) {
        RoutedAliasAdminRequest dim = dims[i];
        types.add(dim.getType().name());
        fields.add(dim.getRouterField());
        for (String param : dim.getParamNames()) {
          String value = dim.getParams().get(param);
          if (value != null) {
            params.add(withDimensionIndexIfRequired(param, i), value);
          } else {
            if (dim.getRequiredParamNames().contains(param)) {
              throw new IllegalArgumentException("Dimension of type " + dim.getType() + " requires a value for " + param);
            }
          }
        }
      }
      params.add(CommonParams.NAME, aliasName);
      params.add(ROUTER_TYPE_NAME, "Dimensional[" + String.join(",", types) + "]");
      params.add(ROUTER_FIELD, String.join(",", fields));

      // merge the above with collectionParams.  Above takes precedence.
      ModifiableSolrParams createCollParams = mergeCollParams(createCollTemplate);
      return SolrParams.wrapDefaults(params, createCollParams);
    }



    @Override
    public RoutedAliasTypes getType() {
      throw new UnsupportedOperationException("Dimensions of dimensions are not allowed, the multiverse might collapse!");
    }

    @Override
    public String getRouterField() {
      throw new UnsupportedOperationException("Dimensions of dimensions are not allowed, the multiverse might collapse!");
    }

    @Override
    public java.util.List<String> getParamNames() {
      throw new UnsupportedOperationException("Dimensions of dimensions are not allowed, the multiverse might collapse!");
    }

    @Override
    public java.util.List<String> getRequiredParamNames() {
      throw new UnsupportedOperationException("Dimensions of dimensions are not allowed, the multiverse might collapse!");
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
      this.aliasName = checkNotNull("aliasName",aliasName);
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.NAME, aliasName);
      return params;
    }
  }

  /**
   * Returns a SolrRequest to add a replica of type {@link org.apache.solr.common.cloud.Replica.Type#NRT} to a shard in a collection
   *
   */
  public static AddReplica addReplicaToShard(String collection, String shard) {
    return addReplicaToShard(collection, shard, Replica.Type.NRT);
  }

  /**
   * Returns a SolrRequest to add a replica of the specified type to a shard in a collection.
   * If the replica type is null, the server default will be used.
   *
   */
  public static AddReplica addReplicaToShard(String collection, String shard, Replica.Type replicaType) {
    return new AddReplica(collection, checkNotNull(CoreAdminParams.SHARD, shard), null, replicaType);
  }

  /**
   * Returns a SolrRequest to add a replica to a collection using a route key
   */
  public static AddReplica addReplicaByRouteKey(String collection, String routeKey) {
    return new AddReplica(collection, null, checkNotNull("routeKey",routeKey), null);
  }

  // ADDREPLICA request
  public static class AddReplica extends AsyncCollectionAdminRequest {

    protected String collection;
    protected String shard;
    protected String node;
    protected String coreName;
    protected String routeKey;
    protected String instanceDir;
    protected String dataDir;
    protected String ulogDir;
    protected Properties properties;
    protected Replica.Type type;
    protected Integer nrtReplicas, tlogReplicas, pullReplicas;
    protected Boolean skipNodeAssignment;
    protected String createNodeSet;

    private AddReplica(String collection, String shard, String routeKey, Replica.Type type) {
      super(CollectionAction.ADDREPLICA);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
      this.shard = shard;
      this.routeKey = routeKey;
      this.type = type;
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

    public AddReplica setSkipNodeAssignment(Boolean skipNodeAssignment) {
      this.skipNodeAssignment = skipNodeAssignment;
      return this;
    }

    public String getRouteKey() {
      return routeKey;
    }

    public String getInstanceDir() {
      return instanceDir;
    }

    public String getUlogDir() {
      return ulogDir;
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

    public AddReplica setType(Replica.Type type) {
      this.type = type;
      return this;
    }

    public AddReplica setCoreName(String coreName) {
      this.coreName = coreName;
      return this;
    }

    public AddReplica setUlogDir(String ulogDir) {
      this.ulogDir = ulogDir;
      return this;
    }

    public String getShard() {
      return shard;
    }

    public Integer getNrtReplicas() {
      return nrtReplicas;
    }

    public AddReplica setNrtReplicas(Integer nrtReplicas) {
      this.nrtReplicas = nrtReplicas;
      return this;
    }

    public Integer getTlogReplicas() {
      return tlogReplicas;
    }

    public AddReplica setTlogReplicas(Integer tlogReplicas) {
      this.tlogReplicas = tlogReplicas;
      return this;
    }

    public Integer getPullReplicas() {
      return pullReplicas;
    }

    public AddReplica setPullReplicas(Integer pullReplicas) {
      this.pullReplicas = pullReplicas;
      return this;
    }

    public String getCreateNodeSet() {
      return createNodeSet;
    }

    public AddReplica setCreateNodeSet(String createNodeSet) {
      this.createNodeSet = createNodeSet;
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.add(CoreAdminParams.COLLECTION, collection);
      assert ((null == routeKey) ^ (null == shard));
      if (null != shard) {
        params.add(CoreAdminParams.SHARD, shard);
      }
      if (null != routeKey) {
        params.add(ShardParams._ROUTE_, routeKey);
      }
      if (node != null) {
        params.add(CoreAdminParams.NODE, node);
      }
      if (skipNodeAssignment != null) {
        params.add(SKIP_NODE_ASSIGNMENT, String.valueOf(skipNodeAssignment));
      }
      if (instanceDir != null)  {
        params.add(CoreAdminParams.INSTANCE_DIR, instanceDir);
      }
      if (dataDir != null)  {
        params.add(CoreAdminParams.DATA_DIR, dataDir);
      }
      if (ulogDir != null) {
        params.add(CoreAdminParams.ULOG_DIR, ulogDir);
      }
      if (coreName != null) {
        params.add(CoreAdminParams.NAME, coreName);
      }
      if (type != null) {
        params.add(ZkStateReader.REPLICA_TYPE, type.name());
      }
      if (properties != null) {
        addProperties(params, properties);
      }
      if (nrtReplicas != null)  {
        params.add(NRT_REPLICAS, String.valueOf(nrtReplicas));
      }
      if (tlogReplicas != null)  {
        params.add(TLOG_REPLICAS, String.valueOf(tlogReplicas));
      }
      if (pullReplicas != null)  {
        params.add(PULL_REPLICAS, String.valueOf(pullReplicas));
      }
      if (createNodeSet != null)  {
        params.add(CREATE_NODE_SET_PARAM, createNodeSet);
      }
      return params;
    }

  }

  /**
   * Returns a SolrRequest to delete a replica from a shard in a collection
   */
  public static DeleteReplica deleteReplica(String collection, String shard, String replica) {
    return new DeleteReplica(collection, checkNotNull(CoreAdminParams.SHARD, shard),
        checkNotNull(CoreAdminParams.REPLICA, replica));
  }

  public static DeleteReplica deleteReplica(String collection, String shard, int count) {
    return new DeleteReplica(collection, checkNotNull(CoreAdminParams.SHARD, shard), count);
  }

  /**
   * Returns a SolrRequest to remove a number of replicas from a specific shard
   */
  public static DeleteReplica deleteReplicasFromShard(String collection, String shard, int count) {
    return new DeleteReplica(collection, checkNotNull(CoreAdminParams.SHARD, shard), count);
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

      // AsyncCollectionSpecificAdminRequest uses 'name' rather than 'collection'
      // TODO - deal with this inconsistency
      params.remove(CoreAdminParams.NAME);
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
   * Returns a SolrRequest to set (or unset) a cluster property
   */
  public static ClusterProp setClusterProperty(String propertyName, String propertyValue) {
    return new ClusterProp(propertyName, propertyValue);
  }

  // CLUSTERPROP request
  public static class ClusterProp extends CollectionAdminRequest<CollectionAdminResponse> {

    private String propertyName;
    private String propertyValue;

    private ClusterProp(String propertyName, String propertyValue) {
      super(CollectionAction.CLUSTERPROP);
      this.propertyName = checkNotNull("propertyName",propertyName);
      this.propertyValue = propertyValue;
    }

    public String getPropertyName() {
      return this.propertyName;
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

  public static CollectionProp setCollectionProperty(String collection, String propertyName, String propertyValue) {
    return new CollectionProp(collection, propertyName, propertyValue);
  }

  // COLLECTIONPROP request
  public static class CollectionProp extends AsyncCollectionSpecificAdminRequest {

    private String propertyName;
    private String propertyValue;

    private CollectionProp(String collection, String propertyName, String propertyValue) {
      super(CollectionAction.COLLECTIONPROP, collection);
      this.propertyName = checkNotNull("propertyName", propertyName);
      this.propertyValue = propertyValue;
    }

    public String getPropertyName() {
      return this.propertyName;
    }

    public String getPropertyValue() {
      return this.propertyValue;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.add(CollectionAdminParams.PROPERTY_NAME, propertyName);
      params.add(CollectionAdminParams.PROPERTY_VALUE, propertyValue);

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

    private Migrate(String collection, String targetCollection, String splitKey) {
      super(CollectionAction.MIGRATE);
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
      this.targetCollection = checkNotNull("targetCollection", targetCollection);
      this.splitKey = checkNotNull("split.key", splitKey);
    }

    public String getCollectionName() {
      return collection;
    }

    public String getTargetCollection() {
      return this.targetCollection;
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


  }

  /**
   * Returns a SolrRequest to add a role to a node
   */
  public static AddRole addRole(String node, String role) {
    return new AddRole(node, role);
  }

  // ADDROLE request
  public static class AddRole extends CollectionAdminRoleRequest {
    private AddRole(String node, String role) {
      super(CollectionAction.ADDROLE, node, role);
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
    private RemoveRole(String node, String role) {
      super(CollectionAction.REMOVEROLE, node, role);
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

  // LISTALIASES request
  public static class ListAliases extends CollectionAdminRequest<CollectionAdminResponse> {

    public ListAliases() {
      super(CollectionAction.LISTALIASES);
    }

    @Override
    protected CollectionAdminResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }

  }

  /**
   * Returns a SolrRequest to get a list of collections in the cluster
   */
  @SuppressWarnings({"unchecked"})
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

    private AddReplicaProp(String collection, String shard, String replica, String propertyName, String propertyValue) {
      super(CollectionAction.ADDREPLICAPROP, collection, shard);
      this.replica = checkNotNull(CoreAdminParams.REPLICA, replica);
      this.propertyName = checkNotNull("propertyName",propertyName);
      this.propertyValue = checkNotNull("propertyValue",propertyValue);
    }

    public String getReplica() {
      return replica;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public String getPropertyValue() {
      return propertyValue;
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

    private DeleteReplicaProp(String collection, String shard, String replica, String propertyName) {
      super(CollectionAction.DELETEREPLICAPROP, collection, shard);
      this.replica = checkNotNull(CoreAdminParams.REPLICA, replica);
      this.propertyName = checkNotNull("propertyName",propertyName);
    }

    public String getReplica() {
      return replica;
    }

    public String getPropertyName() {
      return propertyName;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.REPLICA, replica);
      params.set("property", propertyName);
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
      this.collection = checkNotNull(CoreAdminParams.COLLECTION, collection);
      this.propertyName = checkNotNull("propertyName",propertyName);
    }

    public String getPropertyName() {
      return propertyName;
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

    public String getCollection() {
      return collection;
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

  /**
   * A Modify Collection request
   */
  public static class Modify extends AsyncCollectionSpecificAdminRequest {
    protected Map<String, Object> attributes;

    private Modify(String collection, Map<String, Object> attributes) {
      super(CollectionAction.MODIFYCOLLECTION, collection);
      this.attributes = attributes;
    }

    /**
     * Sets the attributes to be modified using the Modify Collection API.
     * <b>Note: this method will overwrite any previously set attributes</b>
     *
     * @param attributes a map of attribute key vs value
     */
    public void setAttributes(Map<String, Object> attributes) {
      this.attributes = attributes;
    }

    /**
     * Sets the collection attribute to the given value
     *
     * @param key   a string attribute key, must be one of the entries documented
     *              in the <a href="https://lucene.apache.org/solr/guide/collections-api.html#modifycollection">Modify Collection API documentation</a>
     * @param value the attribute value for the given key
     */
    public Modify setAttribute(String key, Object value) {
      if (key == null) {
        throw new IllegalArgumentException("Attribute key cannot be null for the modify collection API");
      }
      if (!MODIFIABLE_COLLECTION_PROPERTIES.contains(key)) {
        throw new IllegalArgumentException("Unknown attribute key: "
            + key + ". Must be one of: " + MODIFIABLE_COLLECTION_PROPERTIES);
      }
      if (value == null) {
        throw new IllegalArgumentException("Value cannot be null for key: " + key);
      }
      if (attributes == null) {
        attributes = new HashMap<>();
      }
      attributes.put(key, value);
      return this;
    }

    /**
     * Removes the given key from the collection
     *
     * @param key the string attribute key, must be one of the entries documented
     *            in the <a href="https://lucene.apache.org/solr/guide/collections-api.html#modifycollection">Modify Collection API documentation</a>
     */
    public Modify unsetAttribute(String key) {
      if (key == null) {
        throw new IllegalArgumentException("Attribute key cannot be null for the modify collection API");
      }
      if (!MODIFIABLE_COLLECTION_PROPERTIES.contains(key)) {
        throw new IllegalArgumentException("Unknown attribute key: "
            + key + ". Must be one of: " + MODIFIABLE_COLLECTION_PROPERTIES);
      }
      if (attributes == null) {
        attributes = new HashMap<>();
      }
      attributes.put(key, "");
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      params.set(CoreAdminParams.COLLECTION, collection);
      for (Map.Entry<String, Object> entry : attributes.entrySet()) {
        params.set(entry.getKey(), String.valueOf(entry.getValue()));
      }
      return params;
    }
  }

}
