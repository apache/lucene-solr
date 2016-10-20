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
package org.apache.solr.cloud;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.CdcrParams;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.SHARDS_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.handler.admin.CoreAdminHandler.COMPLETED;
import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE_STATUS;

/**
 * <p>
 * Abstract class for CDCR unit testing. This class emulates two clusters, a source and target, by using different
 * collections in the same SolrCloud cluster. Therefore, the two clusters will share the same Zookeeper cluster. In
 * real scenario, the two collections/clusters will likely have their own zookeeper cluster.
 * </p>
 * <p>
 * This class will automatically create two collections, the source and the target. Each collection will have
 * {@link #shardCount} shards, and {@link #replicationFactor} replicas per shard. One jetty instance will
 * be created per core.
 * </p>
 * <p>
 * The source and target collection can be reinitialised at will by calling {@link #clearSourceCollection()} and
 * {@link #clearTargetCollection()}. After reinitialisation, a collection will have a new fresh index and update log.
 * </p>
 * <p>
 * Servers can be restarted at will by calling
 * {@link #restartServer(BaseCdcrDistributedZkTest.CloudJettyRunner)} or
 * {@link #restartServers(java.util.List)}.
 * </p>
 * <p>
 * The creation of the target collection can be disabled with the flag {@link #createTargetCollection};
 * </p>
 * <p>
 * NB: We cannot use multiple cores per jetty instance, as jetty will load only one core when restarting. It seems
 * that this is a limitation of the {@link org.apache.solr.client.solrj.embedded.JettySolrRunner}. This class
 * tries to ensure that there always is one single core per jetty instance.
 * </p>
 */
public class BaseCdcrDistributedZkTest extends AbstractDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected int shardCount = 2;
  protected int replicationFactor = 2;
  protected boolean createTargetCollection = true;

  private static final String CDCR_PATH = "/cdcr";

  protected static final String SOURCE_COLLECTION = "source_collection";
  protected static final String TARGET_COLLECTION = "target_collection";

  public static final String SHARD1 = "shard1";
  public static final String SHARD2 = "shard2";

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-cdcr.xml";
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();

    if (shardCount > 0) {
      System.setProperty("numShards", Integer.toString(shardCount));
    } else {
      System.clearProperty("numShards");
    }

    if (isSSLMode()) {
      System.clearProperty("urlScheme");
      ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(),
          AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT);
      try {
        zkStateReader.getZkClient().create(ZkStateReader.CLUSTER_PROPS,
            Utils.toJSON(Collections.singletonMap("urlScheme", "https")),
            CreateMode.PERSISTENT, true);
      } finally {
        zkStateReader.close();
      }
    }
  }

  @Override
  protected void createServers(int numServers) throws Exception {
  }

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.update.delay", "0");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solrcloud.update.delay");
  }

  @Before
  public void baseBefore() throws Exception {
    this.createSourceCollection();
    if (this.createTargetCollection) this.createTargetCollection();
    RandVal.uniqueValues = new HashSet(); //reset random values
  }

  @After
  public void baseAfter() throws Exception {
    for (List<CloudJettyRunner> runners : cloudJettys.values()) {
      for (CloudJettyRunner runner : runners) {
        runner.client.close();
      }
    }
    destroyServers();
  }

  protected CloudSolrClient createCloudClient(String defaultCollection) {
    CloudSolrClient server = getCloudSolrClient(zkServer.getZkAddress(), random().nextBoolean());
    server.setParallelUpdates(random().nextBoolean());
    if (defaultCollection != null) server.setDefaultCollection(defaultCollection);
    return server;
  }

  protected void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }

  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }

  protected void index(String collection, SolrInputDocument doc) throws IOException, SolrServerException {
    CloudSolrClient client = createCloudClient(collection);
    try {
      client.add(doc);
      client.commit(true, true);
    } finally {
      client.close();
    }
  }

  protected void index(String collection, List<SolrInputDocument> docs) throws IOException, SolrServerException {
    CloudSolrClient client = createCloudClient(collection);
    try {
      client.add(docs);
      client.commit(true, true);
    } finally {
      client.close();
    }
  }

  protected void deleteById(String collection, List<String> ids) throws IOException, SolrServerException {
    CloudSolrClient client = createCloudClient(collection);
    try {
      client.deleteById(ids);
      client.commit(true, true);
    } finally {
      client.close();
    }
  }

  protected void deleteByQuery(String collection, String q) throws IOException, SolrServerException {
    CloudSolrClient client = createCloudClient(collection);
    try {
      client.deleteByQuery(q);
      client.commit(true, true);
    } finally {
      client.close();
    }
  }

  /**
   * Invokes a commit on the given collection.
   */
  protected void commit(String collection) throws IOException, SolrServerException {
    CloudSolrClient client = createCloudClient(collection);
    try {
      client.commit(true, true);
    } finally {
      client.close();
    }
  }

  /**
   * Assert the number of documents in a given collection
   */
  protected void assertNumDocs(int expectedNumDocs, String collection)
  throws SolrServerException, IOException, InterruptedException {
    CloudSolrClient client = createCloudClient(collection);
    try {
      int cnt = 30; // timeout after 15 seconds
      AssertionError lastAssertionError = null;
      while (cnt > 0) {
        try {
          assertEquals(expectedNumDocs, client.query(new SolrQuery("*:*")).getResults().getNumFound());
          return;
        }
        catch (AssertionError e) {
          lastAssertionError = e;
          cnt--;
          Thread.sleep(500);
        }
      }
      throw new AssertionError("Timeout while trying to assert number of documents @ " + collection, lastAssertionError);
    } finally {
      client.close();
    }
  }

  /**
   * Invokes a CDCR action on a given node.
   */
  protected NamedList invokeCdcrAction(CloudJettyRunner jetty, CdcrParams.CdcrAction action) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.ACTION, action.toString());

    SolrRequest request = new QueryRequest(params);
    request.setPath(CDCR_PATH);

    return jetty.client.request(request);
  }

  protected void waitForCdcrStateReplication(String collection) throws Exception {
    log.info("Wait for CDCR state to replicate - collection: " + collection);

    int cnt = 30;
    while (cnt > 0) {
      NamedList status = null;
      boolean allEquals = true;
      for (CloudJettyRunner jetty : cloudJettys.get(collection)) { // check all replicas
        NamedList rsp = invokeCdcrAction(jetty, CdcrParams.CdcrAction.STATUS);
        if (status == null) {
          status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
          continue;
        }
        allEquals &= status.equals(rsp.get(CdcrParams.CdcrAction.STATUS.toLower()));
      }

      if (allEquals) {
        break;
      }
      else {
        if (cnt == 0) {
          throw new RuntimeException("Timeout waiting for CDCR state to replicate: collection="+collection);
        }
        cnt--;
        Thread.sleep(500);
      }
    }

    log.info("CDCR state is identical across nodes - collection: " + collection);
  }

  /**
   * Assert the state of CDCR on each nodes of the given collection.
   */
  protected void assertState(String collection, CdcrParams.ProcessState processState, CdcrParams.BufferState bufferState)
  throws Exception {
    this.waitForCdcrStateReplication(collection); // ensure that cdcr state is replicated and stable
    for (CloudJettyRunner jetty : cloudJettys.get(collection)) { // check all replicas
      NamedList rsp = invokeCdcrAction(jetty, CdcrParams.CdcrAction.STATUS);
      NamedList status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
      assertEquals(processState.toLower(), status.get(CdcrParams.ProcessState.getParam()));
      assertEquals(bufferState.toLower(), status.get(CdcrParams.BufferState.getParam()));
    }
  }

  /**
   * A mapping between collection and node names. This is used when creating the collection in
   * {@link #createCollection(String)}.
   */
  private Map<String, List<String>> collectionToNodeNames = new HashMap<>();

  /**
   * Starts the servers, saves and associates the node names to the source collection,
   * and finally creates the source collection.
   */
  private void createSourceCollection() throws Exception {
    List<String> nodeNames = this.startServers(shardCount * replicationFactor);
    this.collectionToNodeNames.put(SOURCE_COLLECTION, nodeNames);
    this.createCollection(SOURCE_COLLECTION);
    this.waitForRecoveriesToFinish(SOURCE_COLLECTION, true);
    this.updateMappingsFromZk(SOURCE_COLLECTION);
  }

  /**
   * Clear the source collection. It will delete then create the collection through the collection API.
   * The collection will have a new fresh index, i.e., including a new update log.
   */
  protected void clearSourceCollection() throws Exception {
    this.deleteCollection(SOURCE_COLLECTION);
    this.waitForCollectionToDisappear(SOURCE_COLLECTION);
    this.createCollection(SOURCE_COLLECTION);
    this.waitForRecoveriesToFinish(SOURCE_COLLECTION, true);
    this.updateMappingsFromZk(SOURCE_COLLECTION);
  }

  /**
   * Starts the servers, saves and associates the node names to the target collection,
   * and finally creates the target collection.
   */
  private void createTargetCollection() throws Exception {
    List<String> nodeNames = this.startServers(shardCount * replicationFactor);
    this.collectionToNodeNames.put(TARGET_COLLECTION, nodeNames);
    this.createCollection(TARGET_COLLECTION);
    this.waitForRecoveriesToFinish(TARGET_COLLECTION, true);
    this.updateMappingsFromZk(TARGET_COLLECTION);
  }

  /**
   * Clear the source collection. It will delete then create the collection through the collection API.
   * The collection will have a new fresh index, i.e., including a new update log.
   */
  protected void clearTargetCollection() throws Exception {
    this.deleteCollection(TARGET_COLLECTION);
    this.waitForCollectionToDisappear(TARGET_COLLECTION);
    this.createCollection(TARGET_COLLECTION);
    this.waitForRecoveriesToFinish(TARGET_COLLECTION, true);
    this.updateMappingsFromZk(TARGET_COLLECTION);
  }

  /**
   * Create a new collection through the Collection API. It enforces the use of one max shard per node.
   * It will define the nodes to spread the new collection across by using the mapping {@link #collectionToNodeNames},
   * to ensure that a node will not host more than one core (which will create problem when trying to restart servers).
   */
  private void createCollection(String name) throws Exception {
    CloudSolrClient client = createCloudClient(null);
    try {
      // Create the target collection
      Map<String, List<Integer>> collectionInfos = new HashMap<>();
      int maxShardsPerNode = 1;

      StringBuilder sb = new StringBuilder();
      for (String nodeName : collectionToNodeNames.get(name)) {
        sb.append(nodeName);
        sb.append(',');
      }
      sb.deleteCharAt(sb.length() - 1);

      createCollection(collectionInfos, name, shardCount, replicationFactor, maxShardsPerNode, client, sb.toString());
    } finally {
      client.close();
    }
  }

  private CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos,
                                                   String collectionName, int numShards, int replicationFactor,
                                                   int maxShardsPerNode, SolrClient client, String createNodeSetStr)
      throws SolrServerException, IOException {
    return createCollection(collectionInfos, collectionName,
        Utils.makeMap(
            NUM_SLICES, numShards,
            REPLICATION_FACTOR, replicationFactor,
            CREATE_NODE_SET, createNodeSetStr,
            MAX_SHARDS_PER_NODE, maxShardsPerNode),
        client, null);
  }

  private CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos, String collectionName,
                                                   Map<String, Object> collectionProps, SolrClient client,
                                                   String confSetName)
      throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.CREATE.toString());
    for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
      if (entry.getValue() != null) params.set(entry.getKey(), String.valueOf(entry.getValue()));
    }
    Integer numShards = (Integer) collectionProps.get(NUM_SLICES);
    if (numShards == null) {
      String shardNames = (String) collectionProps.get(SHARDS_PROP);
      numShards = StrUtils.splitSmart(shardNames, ',').size();
    }
    Integer replicationFactor = (Integer) collectionProps.get(REPLICATION_FACTOR);
    if (replicationFactor == null) {
      replicationFactor = (Integer) OverseerCollectionMessageHandler.COLL_PROPS.get(REPLICATION_FACTOR);
    }

    if (confSetName != null) {
      params.set("collection.configName", confSetName);
    }

    List<Integer> list = new ArrayList<>();
    list.add(numShards);
    list.add(replicationFactor);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    res.setResponse(client.request(request));
    return res;
  }

  /**
   * Delete a collection through the Collection API.
   */
  protected CollectionAdminResponse deleteCollection(String collectionName) throws Exception {
    SolrClient client = createCloudClient(null);
    CollectionAdminResponse res;

    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.DELETE.toString());
      params.set("name", collectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      res = new CollectionAdminResponse();
      res.setResponse(client.request(request));
    } catch (Exception e) {
      log.warn("Error while deleting the collection " + collectionName, e);
      return new CollectionAdminResponse();
    } finally {
      client.close();
    }

    return res;
  }

  private void waitForCollectionToDisappear(String collection) throws Exception {
    CloudSolrClient client = this.createCloudClient(null);
    try {
      client.connect();
      ZkStateReader zkStateReader = client.getZkStateReader();
      AbstractDistribZkTestBase.waitForCollectionToDisappear(collection, zkStateReader, false, true, 15);
    } finally {
      client.close();
    }
  }

  private void waitForRecoveriesToFinish(String collection, boolean verbose) throws Exception {
    CloudSolrClient client = this.createCloudClient(null);
    try {
      client.connect();
      ZkStateReader zkStateReader = client.getZkStateReader();
      super.waitForRecoveriesToFinish(collection, zkStateReader, verbose);
    } finally {
      client.close();
    }
  }

  /**
   * Asserts that the collection has the correct number of shards and replicas
   */
  protected void assertCollectionExpectations(String collectionName) throws Exception {
    CloudSolrClient client = this.createCloudClient(null);
    try {
      client.connect();
      ClusterState clusterState = client.getZkStateReader().getClusterState();

      assertTrue("Could not find new collection " + collectionName, clusterState.hasCollection(collectionName));
      Map<String, Slice> shards = clusterState.getCollection(collectionName).getSlicesMap();
      // did we find expectedSlices shards/shards?
      assertEquals("Found new collection " + collectionName + ", but mismatch on number of shards.", shardCount, shards.size());
      int totalShards = 0;
      for (String shardName : shards.keySet()) {
        totalShards += shards.get(shardName).getReplicas().size();
      }
      int expectedTotalShards = shardCount * replicationFactor;
      assertEquals("Found new collection " + collectionName + " with correct number of shards, but mismatch on number " +
          "of shards.", expectedTotalShards, totalShards);
    } finally {
      client.close();
    }
  }

  /**
   * Restart a server.
   */
  protected void restartServer(CloudJettyRunner server) throws Exception {
    // it seems we need to set the collection property to have the jetty properly restarted
    System.setProperty("collection", server.collection);
    JettySolrRunner jetty = server.jetty;
    ChaosMonkey.stop(jetty);
    ChaosMonkey.start(jetty);
    System.clearProperty("collection");
    waitForRecoveriesToFinish(server.collection, true);
    updateMappingsFromZk(server.collection); // must update the mapping as the core node name might have changed
  }

  /**
   * Restarts a list of servers.
   */
  protected void restartServers(List<CloudJettyRunner> servers) throws Exception {
    for (CloudJettyRunner server : servers) {
      this.restartServer(server);
    }
  }

  private List<JettySolrRunner> jettys = new ArrayList<>();

  /**
   * Creates and starts a given number of servers.
   */
  protected List<String> startServers(int nServer) throws Exception {
    String temporaryCollection = "tmp_collection";
    System.setProperty("collection", temporaryCollection);
    for (int i = 1; i <= nServer; i++) {
      // give everyone there own solrhome
      File jettyDir = createTempDir("jetty").toFile();
      jettyDir.mkdirs();
      setupJettySolrHome(jettyDir);
      JettySolrRunner jetty = createJetty(jettyDir, null, "shard" + i);
      jettys.add(jetty);
    }

    ZkStateReader zkStateReader = jettys.get(0).getCoreContainer().getZkController().getZkStateReader();

    // now wait till we see the leader for each shard
    for (int i = 1; i <= shardCount; i++) {
      this.printLayout();
      zkStateReader.getLeaderRetry(temporaryCollection, "shard" + i, 15000);
    }

    // store the node names
    List<String> nodeNames = new ArrayList<>();
    for (Slice shard : zkStateReader.getClusterState().getCollection(temporaryCollection).getSlices()) {
      for (Replica replica : shard.getReplicas()) {
        nodeNames.add(replica.getNodeName());
      }
    }

    this.waitForRecoveriesToFinish(temporaryCollection,zkStateReader, true);
    // delete the temporary collection - we will create our own collections later
    this.deleteCollection(temporaryCollection);
    this.waitForCollectionToDisappear(temporaryCollection);
    System.clearProperty("collection");

    return nodeNames;
  }

  @Override
  protected void destroyServers() throws Exception {
    for (JettySolrRunner runner : jettys) {
      try {
        ChaosMonkey.stop(runner);
      } catch (Exception e) {
        log.error("", e);
      }
    }

    jettys.clear();
  }

  /**
   * Mapping from collection to jettys
   */
  protected Map<String, List<CloudJettyRunner>> cloudJettys = new HashMap<>();

  /**
   * Mapping from collection/shard to jettys
   */
  protected Map<String, Map<String, List<CloudJettyRunner>>> shardToJetty = new HashMap<>();

  /**
   * Mapping from collection/shard leader to jettys
   */
  protected Map<String, Map<String, CloudJettyRunner>> shardToLeaderJetty = new HashMap<>();

  /**
   * Updates the mappings between the jetty's instances and the zookeeper cluster state.
   */
  protected void updateMappingsFromZk(String collection) throws Exception {
    List<CloudJettyRunner> cloudJettys = new ArrayList<>();
    Map<String, List<CloudJettyRunner>> shardToJetty = new HashMap<>();
    Map<String, CloudJettyRunner> shardToLeaderJetty = new HashMap<>();

    CloudSolrClient cloudClient = this.createCloudClient(null);
    try {
      cloudClient.connect();
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollection(collection);

      for (JettySolrRunner jetty : jettys) {
        int port = jetty.getLocalPort();
        if (port == -1) {
          throw new RuntimeException("Cannot find the port for jetty");
        }

        nextJetty:
        for (Slice shard : coll.getSlices()) {
          Set<Map.Entry<String, Replica>> entries = shard.getReplicasMap().entrySet();
          for (Map.Entry<String, Replica> entry : entries) {
            Replica replica = entry.getValue();
            if (replica.getStr(ZkStateReader.BASE_URL_PROP).contains(":" + port)) {
              if (!shardToJetty.containsKey(shard.getName())) {
                shardToJetty.put(shard.getName(), new ArrayList<CloudJettyRunner>());
              }
              boolean isLeader = shard.getLeader() == replica;
              CloudJettyRunner cjr = new CloudJettyRunner(jetty, replica, collection, shard.getName(), entry.getKey());
              shardToJetty.get(shard.getName()).add(cjr);
              if (isLeader) {
                shardToLeaderJetty.put(shard.getName(), cjr);
              }
              cloudJettys.add(cjr);
              break nextJetty;
            }
          }
        }
      }

      List<CloudJettyRunner> oldRunners = this.cloudJettys.putIfAbsent(collection, cloudJettys);
      if (oldRunners != null)  {
        // must close resources for the old entries
        for (CloudJettyRunner oldRunner : oldRunners) {
          IOUtils.closeQuietly(oldRunner.client);
        }
      }

      this.cloudJettys.put(collection, cloudJettys);
      this.shardToJetty.put(collection, shardToJetty);
      this.shardToLeaderJetty.put(collection, shardToLeaderJetty);
    } finally {
      cloudClient.close();
    }
  }

  /**
   * Wrapper around a {@link org.apache.solr.client.solrj.embedded.JettySolrRunner} to map the jetty
   * instance to various information of the cloud cluster, such as the collection and shard
   * that is served by the jetty instance, the node name, core node name, url, etc.
   */
  public static class CloudJettyRunner {

    public JettySolrRunner jetty;
    public String nodeName;
    public String coreNodeName;
    public String url;
    public SolrClient client;
    public Replica info;
    public String shard;
    public String collection;

    public CloudJettyRunner(JettySolrRunner jetty, Replica replica,
                            String collection, String shard, String coreNodeName) {
      this.jetty = jetty;
      this.info = replica;
      this.collection = collection;
      
      Properties nodeProperties = jetty.getNodeProperties();

      // we need to update the jetty's shard so that it registers itself to the right shard when restarted
      this.shard = shard;
      nodeProperties.setProperty(CoreDescriptor.CORE_SHARD, this.shard);

      // we need to update the jetty's shard so that it registers itself under the right core name when restarted
      this.coreNodeName = coreNodeName;
      nodeProperties.setProperty(CoreDescriptor.CORE_NODE_NAME, this.coreNodeName);

      this.nodeName = replica.getNodeName();

      ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(info);
      this.url = coreNodeProps.getCoreUrl();

      // strip the trailing slash as this can cause issues when executing requests
      this.client = createNewSolrServer(this.url.substring(0, this.url.length() - 1));
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((url == null) ? 0 : url.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      CloudJettyRunner other = (CloudJettyRunner) obj;
      if (url == null) {
        if (other.url != null) return false;
      } else if (!url.equals(other.url)) return false;
      return true;
    }

    @Override
    public String toString() {
      return "CloudJettyRunner [url=" + url + "]";
    }

  }

  protected static SolrClient createNewSolrServer(String baseUrl) {
    try {
      // setup the server...
      HttpSolrClient s = getHttpSolrClient(baseUrl);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      return s;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected void waitForBootstrapToComplete(String collectionName, String shardId) throws Exception {
    NamedList rsp;// we need to wait until bootstrap is complete otherwise the replicator thread will never start
    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS);
    while (!timeOut.hasTimedOut())  {
      rsp = invokeCdcrAction(shardToLeaderJetty.get(collectionName).get(shardId), CdcrParams.CdcrAction.BOOTSTRAP_STATUS);
      if (rsp.get(RESPONSE_STATUS).toString().equals(COMPLETED))  {
        break;
      }
      Thread.sleep(1000);
    }
  }

  protected void waitForReplicationToComplete(String collectionName, String shardId) throws Exception {
    int cnt = 15;
    while (cnt > 0) {
      log.info("Checking queue size @ {}:{}", collectionName, shardId);
      long size = this.getQueueSize(collectionName, shardId);
      if (size == 0) { // if we received -1, it means that the log reader is not yet initialised, we should wait
        return;
      }
      log.info("Waiting for replication to complete. Queue size: {} @ {}:{}", size, collectionName, shardId);
      cnt--;
      Thread.sleep(1000); // wait a bit for the replication to complete
    }
    throw new RuntimeException("Timeout waiting for CDCR replication to complete @" + collectionName + ":"  + shardId);
  }

  protected long getQueueSize(String collectionName, String shardId) throws Exception {
    NamedList rsp = this.invokeCdcrAction(shardToLeaderJetty.get(collectionName).get(shardId), CdcrParams.CdcrAction.QUEUES);
    NamedList host = (NamedList) ((NamedList) rsp.get(CdcrParams.QUEUES)).getVal(0);
    NamedList status = (NamedList) host.get(TARGET_COLLECTION);
    return (Long) status.get(CdcrParams.QUEUE_SIZE);
  }

  protected CollectionInfo collectInfo(String collection) throws Exception {
    CollectionInfo info = new CollectionInfo(collection);
    for (String shard : shardToJetty.get(collection).keySet()) {
      List<CloudJettyRunner> jettyRunners = shardToJetty.get(collection).get(shard);
      for (CloudJettyRunner jettyRunner : jettyRunners) {
        for (SolrCore core : jettyRunner.jetty.getCoreContainer().getCores()) {
          info.addCore(core, shard, shardToLeaderJetty.get(collection).containsValue(jettyRunner));
        }
      }
    }

    return info;
  }

  protected class CollectionInfo {

    List<CoreInfo> coreInfos = new ArrayList<>();

    String collection;

    CollectionInfo(String collection) {
      this.collection = collection;
    }

    /**
     * @return Returns a map shard -> list of cores
     */
    Map<String, List<CoreInfo>> getShardToCoresMap() {
      Map<String, List<CoreInfo>> map = new HashMap<>();
      for (CoreInfo info : coreInfos) {
        List<CoreInfo> list = map.get(info.shard);
        if (list == null) {
          list = new ArrayList<>();
          map.put(info.shard, list);
        }
        list.add(info);
      }
      return map;
    }

    CoreInfo getLeader(String shard) {
      List<CoreInfo> coreInfos = getShardToCoresMap().get(shard);
      for (CoreInfo info : coreInfos) {
        if (info.isLeader) {
          return info;
        }
      }
      assertTrue(String.format(Locale.ENGLISH, "There is no leader for collection %s shard %s", collection, shard), false);
      return null;
    }

    List<CoreInfo> getReplicas(String shard) {
      List<CoreInfo> coreInfos = getShardToCoresMap().get(shard);
      coreInfos.remove(getLeader(shard));
      return coreInfos;
    }

    void addCore(SolrCore core, String shard, boolean isLeader) throws Exception {
      CoreInfo info = new CoreInfo();
      info.collectionName = core.getName();
      info.shard = shard;
      info.isLeader = isLeader;
      info.ulogDir = core.getUpdateHandler().getUpdateLog().getLogDir();

      this.coreInfos.add(info);
    }

    public class CoreInfo {
      String collectionName;
      String shard;
      boolean isLeader;
      String ulogDir;
    }

  }

}

