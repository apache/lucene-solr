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

import static org.apache.solr.common.cloud.ZkStateReader.HTTPS;
import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;
import static org.apache.solr.common.util.Utils.makeMap;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.ZkController.NotInClusterStateException;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: we should still test this works as a custom update chain as well as
 * what we test now - the default update chain
 */
@Slow
public abstract class AbstractFullDistribZkTestBase extends AbstractDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeFullSolrCloudTest() {

  }
  
  @Before
  public void beforeTest() {
    cloudInit = false;
  }

  public static final String SHARD1 = "shard1";
  public static final String SHARD2 = "shard2";

  protected boolean printLayoutOnTearDown = false;

  String t1 = "a_t";
  String i1 = "a_i1";
  String tlong = "other_tl1";

  String oddField = "oddField_s";
  String missingField = "ignore_exception__missing_but_valid_field_t";
  protected int sliceCount;

  protected volatile CloudSolrClient controlClientCloud;  // cloud version of the control client
  protected volatile CloudSolrClient cloudClient;
  protected final List<SolrClient> coreClients = Collections.synchronizedList(new ArrayList<>());
  
  protected final List<CloudJettyRunner> cloudJettys = Collections.synchronizedList(new ArrayList<>());
  protected final Map<String,List<CloudJettyRunner>> shardToJetty = new ConcurrentHashMap<>();
  private AtomicInteger jettyIntCntr = new AtomicInteger(0);
  protected volatile ChaosMonkey chaosMonkey;

  protected Map<String,CloudJettyRunner> shardToLeaderJetty = new ConcurrentHashMap<>();
  private static volatile boolean cloudInit;
  protected volatile boolean useJettyDataDir = true;

  private final List<RestTestHarness> restTestHarnesses = Collections.synchronizedList(new ArrayList<>());

  public static class CloudJettyRunner {
    public JettySolrRunner jetty;
    public String nodeName;
    public String coreNodeName;
    public String url;
    public CloudSolrServerClient client;
    public ZkNodeProps info;
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

  public static class CloudSolrServerClient {
    SolrClient solrClient;
    String shardName;
    int port;
    public ZkNodeProps info;

    public CloudSolrServerClient() {}
    
    public CloudSolrServerClient(SolrClient client) {
      this.solrClient = client;
    }

    public SolrClient getSolrClient() {
      return solrClient;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((solrClient == null) ? 0 : solrClient.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      CloudSolrServerClient other = (CloudSolrServerClient) obj;
      if (solrClient == null) {
        if (other.solrClient != null) return false;
      } else if (!solrClient.equals(other.solrClient)) return false;
      return true;
    }

  }

  protected static void setErrorHook() {
    SolrCmdDistributor.testing_errorHook = new Diagnostics.Callable() {
      @Override
      public void call(Object... data) {
        Exception e = (Exception) data[0];
        if (e == null) return;
        String msg = e.getMessage();
        if (msg != null && msg.contains("Timeout")) {
          Diagnostics.logThreadDumps("REQUESTING THREAD DUMP DUE TO TIMEOUT: " + e.getMessage());
        }
      }
    };
  }

  protected static void clearErrorHook() {
    SolrCmdDistributor.testing_errorHook = null;
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    // ignoreException(".*");
    
    cloudInit = false;
    
    if (sliceCount > 0) {
      System.setProperty("numShards", Integer.toString(sliceCount));
    } else {
      System.clearProperty("numShards");
    }

    if (isSSLMode()) {
      System.clearProperty(URL_SCHEME);
      try (ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(),
          AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT)) {
        try {
          zkStateReader.getZkClient().create(ZkStateReader.CLUSTER_PROPS,
              Utils.toJSON(Collections.singletonMap(URL_SCHEME, HTTPS)),
              CreateMode.PERSISTENT, true);
        } catch (KeeperException.NodeExistsException e) {
          ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient().getData(ZkStateReader.CLUSTER_PROPS,
              null, null, true));
          zkStateReader.getZkClient().setData(ZkStateReader.CLUSTER_PROPS, Utils.toJSON(props.plus(URL_SCHEME, HTTPS)), true);
        }
      }
    }
    if (useTlogReplicas()) {
      log.info("Will use {} replicas unless explicitly asked otherwise", Replica.Type.TLOG);
    } else {
      log.info("Will use {} replicas unless explicitly asked otherwise", Replica.Type.NRT);
    }
  }

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.update.delay", "0");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solrcloud.update.delay");
    System.clearProperty("genericCoreNodeNames");
  }

  public AbstractFullDistribZkTestBase() {
    sliceCount = 2;
    fixShardCount(4);

    // TODO: for now, turn off stress because it uses regular clients, and we
    // need the cloud client because we kill servers
    stress = 0;

    useExplicitNodeNames = random().nextBoolean();
  }

  protected String getDataDir(String dataDir) throws IOException {
    return dataDir;
  }

  protected void initCloud() throws Exception {
    assert(cloudInit == false);
    cloudInit = true;
    cloudClient = createCloudClient(DEFAULT_COLLECTION);
    cloudClient.connect();

    ZkStateReader zkStateReader = cloudClient.getZkStateReader();

    chaosMonkey = new ChaosMonkey(zkServer, zkStateReader, DEFAULT_COLLECTION,
        shardToJetty, shardToLeaderJetty);
  }

  protected boolean useTlogReplicas() {
    return false;
  }
  
  protected CloudSolrClient createCloudClient(String defaultCollection) {
    CloudSolrClient client = getCloudSolrClient(zkServer.getZkAddress(), random().nextBoolean(), 30000, 120000);
    if (defaultCollection != null) client.setDefaultCollection(defaultCollection);
    return client;
  }

  @Override
  protected void createServers(int numServers) throws Exception {
    File controlJettyDir = createTempDir("control").toFile();
    setupJettySolrHome(controlJettyDir);
    controlJetty = createJetty(controlJettyDir, useJettyDataDir ? getDataDir(testDir
        + "/control/data") : null);
    controlJetty.start();
    try (CloudSolrClient client = createCloudClient("control_collection")) {
      assertEquals(0, CollectionAdminRequest
          .createCollection("control_collection", "conf1", 1, 1)
          .setCreateNodeSet(controlJetty.getNodeName())
          .process(client).getStatus());
      waitForActiveReplicaCount(client, "control_collection", 1);
    }

    
    controlClient = new HttpSolrClient.Builder(controlJetty.getBaseUrl() + "/control_collection").build();
    if (sliceCount <= 0) {
      // for now, just create the cloud client for the control if we don't
      // create the normal cloud client.
      // this can change if more tests need it.
      controlClientCloud = createCloudClient("control_collection");
      controlClientCloud.connect();
      // NOTE: we are skipping creation of the chaos monkey by returning here
      cloudClient = controlClientCloud; // temporary - some code needs/uses
      // cloudClient
      return;
    }

    initCloud();

    createJettys(numServers);
    
  }

  public static void waitForCollection(ZkStateReader reader, String collection, int slices) throws Exception {
    log.info("waitForCollection ({}): slices={}", collection, slices);
    // wait until shards have started registering...
    int cnt = 30;
    while (!reader.getClusterState().hasCollection(collection)) {
      if (cnt == 0) {
        throw new RuntimeException("timeout waiting for collection in cluster state: collection=" + collection);
      }
      cnt--;
      Thread.sleep(500);
    }
    cnt = 30;
    
    while (reader.getClusterState().getCollection(collection).getSlices().size() < slices) {
      if (cnt == 0) {
        throw new RuntimeException("timeout waiting for collection shards to come up: collection="+collection
            + ", slices.expected="+slices+ " slices.actual= " + reader.getClusterState().getCollection(collection).getSlices().size()
            + " slices : "+ reader.getClusterState().getCollection(collection).getSlices() );
      }
      cnt--;
      Thread.sleep(500);
    }
  }

  protected String defaultStateFormat = String.valueOf( 1 + random().nextInt(2));

  protected String getStateFormat()  {
    String stateFormat = System.getProperty("tests.solr.stateFormat", null);
    if (stateFormat != null)  {
      defaultStateFormat = stateFormat;
    }
    return defaultStateFormat; // random
  }

  protected List<JettySolrRunner> createJettys(int numJettys) throws Exception {
    List<JettySolrRunner> jettys = Collections.synchronizedList(new ArrayList<>());
    List<SolrClient> clients = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings({"rawtypes"})
    List<CollectionAdminRequest> createReplicaRequests = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings({"rawtypes"})
    List<CollectionAdminRequest> createPullReplicaRequests = Collections.synchronizedList(new ArrayList<>());
    StringBuilder sb = new StringBuilder();

    // HACK: Don't be fooled by the replication factor of '1'...
    //
    // This CREATE command asks for a repFactor of 1, but uses an empty nodeSet.
    // This allows this method to create a collection with numShards == sliceCount,
    // but no actual cores ... yet.  The actual replicas are added later (once the actual
    // jetty instances are started)
    assertEquals(0, CollectionAdminRequest
                 .createCollection(DEFAULT_COLLECTION, "conf1", sliceCount, 1) // not real rep factor!
                 .setStateFormat(Integer.parseInt(getStateFormat()))
                 .setCreateNodeSet("") // empty node set prevents creation of cores
                 .process(cloudClient).getStatus());
    
    cloudClient.waitForState(DEFAULT_COLLECTION, 30, TimeUnit.SECONDS,
                             // expect sliceCount active shards, but no active replicas
                             SolrCloudTestCase.activeClusterShape(sliceCount, 0));
    
    ExecutorService customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("closeThreadPool"));

    int numOtherReplicas = numJettys - getPullReplicaCount() * sliceCount;

    if (log.isInfoEnabled()) {
      log.info("Creating jetty instances pullReplicaCount={} numOtherReplicas={}"
          , getPullReplicaCount(), numOtherReplicas);
    }
    
    int addedReplicas = 0;
    for (int i = 1; i <= numJettys; i++) {
      if (sb.length() > 0) sb.append(',');
      int cnt = this.jettyIntCntr.incrementAndGet();

      File jettyDir = createTempDir("shard-" + i).toFile();

      jettyDir.mkdirs();
      setupJettySolrHome(jettyDir);
      int currentI = i;
      if (numOtherReplicas > 0) {
        numOtherReplicas--;
        if (useTlogReplicas()) {
          if (log.isInfoEnabled()) {
            log.info("create jetty {} in directory {} of type {} in shard {}"
                , i, jettyDir, Replica.Type.TLOG, ((currentI % sliceCount) + 1)); // nowarn
          }
          customThreadPool.submit(() -> {
            try {
              JettySolrRunner j = createJetty(jettyDir, useJettyDataDir ? getDataDir(testDir + "/jetty"
                  + cnt) : null, null, "solrconfig.xml", null, Replica.Type.TLOG);
              j.start();
              jettys.add(j);
              waitForLiveNode(j);

              createReplicaRequests.add(CollectionAdminRequest
                  .addReplicaToShard(DEFAULT_COLLECTION, "shard" + ((currentI % sliceCount) + 1))
                  .setNode(j.getNodeName())
                  .setType(Replica.Type.TLOG));

              coreClients.add(createNewSolrClient(coreName, j.getLocalPort()));
              SolrClient client = createNewSolrClient(j.getLocalPort());
              clients.add(client);

            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            } catch (Exception e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });

          addedReplicas++;
        } else {
          if (log.isInfoEnabled()) {
            log.info("create jetty {} in directory {} of type {} for shard{}"
                , i, jettyDir, Replica.Type.NRT, ((currentI % sliceCount) + 1)); // nowarn
          }
          
          customThreadPool.submit(() -> {
            try {
              JettySolrRunner j = createJetty(jettyDir, useJettyDataDir ? getDataDir(testDir + "/jetty"
                  + cnt) : null, null, "solrconfig.xml", null, null);
              j.start();
              jettys.add(j);
              waitForLiveNode(j);
              createReplicaRequests.add(CollectionAdminRequest
                  .addReplicaToShard(DEFAULT_COLLECTION, "shard"+((currentI%sliceCount)+1))
                  .setNode(j.getNodeName())
                  .setType(Replica.Type.NRT));
              coreClients.add(createNewSolrClient(coreName, j.getLocalPort()));
              SolrClient client = createNewSolrClient(j.getLocalPort());
              clients.add(client);
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            } catch (Exception e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });
          
          addedReplicas++;
        }
      } else {
        log.info("create jetty {} in directory {} of type {} for shard{}", i, jettyDir, Replica.Type.PULL, ((currentI % sliceCount) + 1)); // nowarn
        customThreadPool.submit(() -> {
          try {
            JettySolrRunner j = createJetty(jettyDir, useJettyDataDir ? getDataDir(testDir + "/jetty"
                + cnt) : null, null, "solrconfig.xml", null, Replica.Type.PULL);
            j.start();
            jettys.add(j);
            waitForLiveNode(j);
            createPullReplicaRequests.add(CollectionAdminRequest
                .addReplicaToShard(DEFAULT_COLLECTION, "shard"+((currentI%sliceCount)+1))
                .setNode(j.getNodeName())
                .setType(Replica.Type.PULL));
            coreClients.add(createNewSolrClient(coreName, j.getLocalPort()));
            SolrClient client = createNewSolrClient(j.getLocalPort());
            clients.add(client);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        });
        addedReplicas++;
      }

    }
    
    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
    
    customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("createReplicaRequests"));
    
    for (@SuppressWarnings({"rawtypes"})CollectionAdminRequest r : createReplicaRequests) {
      customThreadPool.submit(() -> {
        CollectionAdminResponse response;
        try {
          response = (CollectionAdminResponse) r.process(cloudClient);
        } catch (SolrServerException | IOException e) {
          throw new RuntimeException(e);
        }

        assertTrue(response.isSuccess());
        String coreName = response.getCollectionCoresStatus().keySet().iterator().next();
      });
    }
   
    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
    
    customThreadPool = ExecutorUtil
        .newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("createPullReplicaRequests"));
    for (@SuppressWarnings({"rawtypes"})CollectionAdminRequest r : createPullReplicaRequests) {
      customThreadPool.submit(() -> {
        CollectionAdminResponse response;
        try {
          response = (CollectionAdminResponse) r.process(cloudClient);
        } catch (SolrServerException | IOException e) {
          throw new RuntimeException(e);
        }

        assertTrue(response.isSuccess());
        String coreName = response.getCollectionCoresStatus().keySet().iterator().next();
      });
    }
    
    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
    
    waitForActiveReplicaCount(cloudClient, DEFAULT_COLLECTION, addedReplicas);

    this.jettys.addAll(jettys);
    this.clients.addAll(clients);

    
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    // make sure we have a leader for each shard
    for (int i = 1; i <= sliceCount; i++) {
      zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + i, 10000);
    }

    if (sliceCount > 0) {
      updateMappingsFromZk(this.jettys, this.clients);
    }

    // build the shard string
    for (int i = 1; i <= numJettys / 2; i++) {
      JettySolrRunner j = this.jettys.get(i);
      JettySolrRunner j2 = this.jettys.get(i + (numJettys / 2 - 1));
      if (sb.length() > 0) sb.append(',');
      sb.append(buildUrl(j.getLocalPort()));
      sb.append("|").append(buildUrl(j2.getLocalPort()));
    }
    shards = sb.toString();

    return jettys;
  }

  protected void waitForLiveNode(JettySolrRunner j) throws InterruptedException, TimeoutException {
    if (log.isInfoEnabled()) {
      log.info("waitForLiveNode: {}", j.getNodeName());
    }
    cloudClient.getZkStateReader().waitForLiveNodes(30, TimeUnit.SECONDS, SolrCloudTestCase.containsLiveNode(j.getNodeName()));
  }

  protected void waitForActiveReplicaCount(CloudSolrClient client, String collection, int expectedNumReplicas) throws TimeoutException, NotInClusterStateException {
    log.info("Waiting to see {} active replicas in collection: {}", expectedNumReplicas, collection);
    AtomicInteger nReplicas = new AtomicInteger();
    try {
      client.getZkStateReader().waitForState(collection, 30, TimeUnit.SECONDS, (liveNodes, collectionState) -> {
          if (collectionState == null) {
            return false;
          }
          int activeReplicas = 0;
          for (Slice slice : collectionState) {
            for (Replica replica : slice) {
              if (replica.isActive(liveNodes)) {
                activeReplicas++;
              }
            }
          }
          nReplicas.set(activeReplicas);
          return (activeReplicas == expectedNumReplicas);
        });
    } catch (TimeoutException | InterruptedException e) {
      try {
        printLayout();
      } catch (Exception e1) {
        throw new RuntimeException(e1);
      }
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR,
          "Number of replicas in the state does not match what we set:" + nReplicas + " vs " + expectedNumReplicas);
    }
  }


  protected int getPullReplicaCount() {
    return 0;
  }

  /** 
   * Total number of replicas for all shards as indicated by the cluster state, regardless of status.
   *
   * @deprecated This method is virtually useless as it does not consider the status of either
   *             the shard or replica, nor wether the node hosting each replica is alive.
   */
  @Deprecated
  protected int getTotalReplicas(DocCollection c, String collection) {
    if (c == null) return 0;  // support for when collection hasn't been created yet
    int cnt = 0;
    for (Slice slices : c.getSlices()) {
      cnt += slices.getReplicas().size();
    }
    return cnt;
  }

  public JettySolrRunner createJetty(String dataDir, String ulogDir, String shardList,
      String solrConfigOverride) throws Exception {

    JettyConfig jettyconfig = JettyConfig.builder()
        .setContext(context)
        .stopAtShutdown(false)
        .withServlets(getExtraServlets())
        .withFilters(getExtraRequestFilters())
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();

    Properties props = new Properties();
    props.setProperty("solr.data.dir", getDataDir(dataDir));
    props.setProperty("shards", shardList);
    props.setProperty("solr.ulog.dir", ulogDir);
    props.setProperty("solrconfig", solrConfigOverride);
    
    JettySolrRunner jetty = new JettySolrRunner(getSolrHome(), props, jettyconfig);

    jetty.start();

    return jetty;
  }

  public final JettySolrRunner createJetty(File solrHome, String dataDir, String shardList, String solrConfigOverride, String schemaOverride) throws Exception {
    return createJetty(solrHome, dataDir, shardList, solrConfigOverride, schemaOverride, null);
  }

  public JettySolrRunner createJetty(File solrHome, String dataDir, String shardList, String solrConfigOverride, String schemaOverride, Replica.Type replicaType) throws Exception {
    // randomly test a relative solr.home path
    if (random().nextBoolean()) {
      solrHome = getRelativeSolrHomePath(solrHome);
    }

    JettyConfig jettyconfig = JettyConfig.builder()
        .setContext(context)
        .stopAtShutdown(false)
        .withServlets(getExtraServlets())
        .withFilters(getExtraRequestFilters())
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();

    Properties props = new Properties();
    if (solrConfigOverride != null)
      props.setProperty("solrconfig", solrConfigOverride);
    if (schemaOverride != null)
      props.setProperty("schema", schemaOverride);
    if (shardList != null)
      props.setProperty("shards", shardList);
    if (dataDir != null)
      props.setProperty("solr.data.dir", getDataDir(dataDir));
    if (replicaType != null) {
      props.setProperty("replicaType", replicaType.toString());
    } else if (random().nextBoolean()) {
      props.setProperty("replicaType", Replica.Type.NRT.toString());
    }
    props.setProperty("coreRootDirectory", solrHome.toPath().resolve("cores").toAbsolutePath().toString());
    
    JettySolrRunner jetty = new JettySolrRunner(solrHome.getPath(), props, jettyconfig);

    return jetty;
  }

  /**
   * Creates a JettySolrRunner with a socket proxy sitting infront of the Jetty server,
   * which gives us the ability to simulate network partitions without having to fuss
   * with IPTables.
   */
  public JettySolrRunner createProxiedJetty(File solrHome, String dataDir,
                                     String shardList, String solrConfigOverride, String schemaOverride, Replica.Type replicaType)
      throws Exception {

    JettyConfig jettyconfig = JettyConfig.builder()
        .setContext(context)
        .stopAtShutdown(false)
        .withServlets(getExtraServlets())
        .withFilters(getExtraRequestFilters())
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();

    Properties props = new Properties();
    if (solrConfigOverride != null)
      props.setProperty("solrconfig", solrConfigOverride);
    if (schemaOverride != null)
      props.setProperty("schema", schemaOverride);
    if (shardList != null)
      props.setProperty("shards", shardList);
    if (dataDir != null)
      props.setProperty("solr.data.dir", getDataDir(dataDir));
    if (replicaType != null) {
      props.setProperty("replicaType", replicaType.toString());
    } else if (random().nextBoolean()) {
      props.setProperty("replicaType", Replica.Type.NRT.toString());
    }
    props.setProperty("coreRootDirectory", solrHome.toPath().resolve("cores").toAbsolutePath().toString());

    JettySolrRunner jetty = new JettySolrRunner(solrHome.getPath(), props, jettyconfig, true);

    return jetty;
  }

  protected int getReplicaPort(Replica replica) {
    String replicaNode = replica.getNodeName();
    String tmp = replicaNode.substring(replicaNode.indexOf(':')+1);
    if (tmp.indexOf('_') != -1)
      tmp = tmp.substring(0,tmp.indexOf('_'));
    return Integer.parseInt(tmp);
  }

  protected JettySolrRunner getJettyOnPort(int port) {
    JettySolrRunner theJetty = null;
    for (JettySolrRunner jetty : jettys) {
      if (port == jetty.getLocalPort()) {
        theJetty = jetty;
        break;
      }
    }

    if (theJetty == null) {
      if (controlJetty.getLocalPort() == port) {
        theJetty = controlJetty;
      }
    }

    if (theJetty == null)
      fail("Not able to find JettySolrRunner for port: "+port);

    return theJetty;
  }

  protected SocketProxy getProxyForReplica(Replica replica) throws Exception {
    String replicaBaseUrl = replica.getBaseUrl();
    assertNotNull(replicaBaseUrl);

    List<JettySolrRunner> runners = new ArrayList<>(jettys);
    runners.add(controlJetty);
    
    for (JettySolrRunner j : runners) {
      if (replicaBaseUrl.replaceAll("/$", "").equals(j.getProxyBaseUrl().toExternalForm().replaceAll("/$", ""))) {
        return j.getProxy();
      }
    }
    
    printLayout();

    fail("No proxy found for " + replicaBaseUrl + "!");
    return null;
  }

  private File getRelativeSolrHomePath(File solrHome) {
    final Path solrHomePath = solrHome.toPath();
    final Path curDirPath = new File("").getAbsoluteFile().toPath();
    
    if (!solrHomePath.getRoot().equals(curDirPath.getRoot())) {
      // root of current directory and solrHome are not the same, therefore cannot relativize
      return solrHome;
    }
    
    final Path root = solrHomePath.getRoot();
    
    // relativize current directory to root: /tmp/foo -> /tmp/foo/../..
    final File relativizedCurDir = new File(curDirPath.toFile(), curDirPath.relativize(root).toString());
    
    // exclude the root from solrHome: /tmp/foo/solrHome -> tmp/foo/solrHome
    final Path solrHomeRelativeToRoot = root.relativize(solrHomePath);
    
    // create the relative solrHome: /tmp/foo/../../tmp/foo/solrHome
    return new File(relativizedCurDir, solrHomeRelativeToRoot.toString()).getAbsoluteFile();
  }
  
  protected void updateMappingsFromZk(List<JettySolrRunner> jettys, List<SolrClient> clients) throws Exception {
    updateMappingsFromZk(jettys, clients, false);
  }
  
  protected void updateMappingsFromZk(List<JettySolrRunner> jettys, List<SolrClient> clients, boolean allowOverSharding) throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.forceUpdateCollection(DEFAULT_COLLECTION);
    cloudJettys.clear();
    shardToJetty.clear();

    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection coll = clusterState.getCollection(DEFAULT_COLLECTION);

    List<CloudSolrServerClient> theClients = new ArrayList<>();
    for (SolrClient client : clients) {
      // find info for this client in zk 
      nextClient:
      // we find out state by simply matching ports...
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          int port = new URI(((HttpSolrClient) client).getBaseURL())
              .getPort();

          if (replica.getBaseUrl().contains(":" + port)) {
            CloudSolrServerClient csc = new CloudSolrServerClient();
            csc.solrClient = client;
            csc.port = port;
            csc.shardName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            csc.info = replica;

            theClients .add(csc);

            break nextClient;
          }
        }
      }
    }

    for (JettySolrRunner jetty : jettys) {
      int port = jetty.getLocalPort();
      if (port == -1) {
        throw new RuntimeException("Cannot find the port for jetty");
      }

      nextJetty:
      for (Slice slice : coll.getSlices()) {
        Set<Entry<String,Replica>> entries = slice.getReplicasMap().entrySet();
        for (Entry<String,Replica> entry : entries) {
          Replica replica = entry.getValue();
          if (replica.getBaseUrl().contains(":" + port)) {
            List<CloudJettyRunner> list = shardToJetty.get(slice.getName());
            if (list == null) {
              list = new ArrayList<>();
              shardToJetty.put(slice.getName(), list);
            }
            boolean isLeader = slice.getLeader() == replica;
            CloudJettyRunner cjr = new CloudJettyRunner();
            cjr.jetty = jetty;
            cjr.info = replica;
            cjr.nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            cjr.coreNodeName = entry.getKey();
            // TODO: no trailing slash on end desired, so replica.getCoreUrl is not applicable here
            cjr.url = replica.getBaseUrl() + "/" + replica.getStr(ZkStateReader.CORE_NAME_PROP);
            cjr.client = findClientByPort(port, theClients);
            list.add(cjr);
            if (isLeader) {
              shardToLeaderJetty.put(slice.getName(), cjr);
            }
            cloudJettys.add(cjr);
            break nextJetty;
          }
        }
      }
    }

    // # of jetties may not match replicas in shard here, because we don't map
    // jetties that are not running - every shard should have at least one
    // running jetty though
    for (Slice slice : coll.getSlices()) {
      // check that things look right
      List<CloudJettyRunner> jetties = shardToJetty.get(slice.getName());
      if (!allowOverSharding) {
        assertNotNull("Test setup problem: We found no jetties for shard: "
            + slice.getName() + " just:" + shardToJetty.keySet(), jetties);

        assertEquals("slice:" + slice.getName(), slice.getReplicas().size(),
            jetties.size());
      }
    }
  }

  private CloudSolrServerClient findClientByPort(int port, List<CloudSolrServerClient> theClients) {
    for (CloudSolrServerClient client : theClients) {
      if (client.port == port) {
        return client;
      }
    }
    throw new IllegalArgumentException("Client with the given port does not exist:" + port);
  }

  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {

    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < sliceCount; i++) {
        if (i > 0) sb.append(',');
        sb.append("shard").append(i + 1);
      }
      params.set("shards", sb.toString());
    }
  }

  protected int sendDocsWithRetry(List<SolrInputDocument> batch, int minRf, int maxRetries, int waitBeforeRetry) throws Exception {
    return sendDocsWithRetry(cloudClient, cloudClient.getDefaultCollection(), batch, minRf, maxRetries, waitBeforeRetry);
  }

  @SuppressWarnings("rawtypes")
  protected static int sendDocsWithRetry(CloudSolrClient cloudClient, String collection, List<SolrInputDocument> batch, int minRf, int maxRetries, int waitBeforeRetry) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.add(batch);
    NamedList resp = null;
    int numRetries = 0;
    while(true) {
      try {
        resp = cloudClient.request(up, collection);
        return cloudClient.getMinAchievedReplicationFactor(cloudClient.getDefaultCollection(), resp);
      } catch (Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        if (++numRetries <= maxRetries) {
          log.warn("ERROR: {} ... Sleeping for {} seconds before re-try ...", rootCause, waitBeforeRetry);
          Thread.sleep(waitBeforeRetry * 1000L);
        } else {
          log.error("No more retries available! Add batch failed due to: {}", rootCause);
          throw exc;
        }
      }
    }
  }

  @Override
  protected void indexDoc(SolrInputDocument doc) throws IOException,
      SolrServerException {

    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setParam("CONTROL", "TRUE");
    req.process(controlClient);

    // if we wanted to randomly pick a client - but sometimes they may be
    // down...

    // boolean pick = random.nextBoolean();
    //
    // int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) %
    // sliceCount;
    //
    // if (pick && sliceCount > 1) {
    // which = which + ((shardCount / sliceCount) *
    // random.nextInt(sliceCount-1));
    // }
    //
    // HttpSolrServer client = (HttpSolrServer)
    // clients.get(which);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    ureq.process(cloudClient);
  }

  @Override
  protected void index_specific(int serverNumber, Object... fields)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);
    
    HttpSolrClient client = (HttpSolrClient) clients
        .get(serverNumber);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
  }
  
  protected void index_specific(SolrClient client, Object... fields)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);

    // add to control second in case adding to shards fails
    controlClient.add(doc);
  }
  
  protected ZkCoreNodeProps getLeaderUrlFromZk(String collection, String slice) {
    ClusterState clusterState = getCommonCloudSolrClient().getZkStateReader().getClusterState();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection != null && docCollection.getLeader(slice) != null) {
      return new ZkCoreNodeProps(docCollection.getLeader(slice));
    }
    throw new RuntimeException("Could not find leader:" + collection + " " + slice);
  }

  @Override
  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    cloudClient.deleteByQuery(q);

    /***
    for (SolrServer client : clients) {
      UpdateRequest ureq = new UpdateRequest();
      // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
      ureq.deleteByQuery(q).process(client);
    }
     ***/
  }// serial commit...

  protected void waitForRecoveriesToFinish(boolean verbose)
      throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    super.waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, verbose);
  }

  protected void waitForRecoveriesToFinish(String collection, boolean verbose)
      throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    super.waitForRecoveriesToFinish(collection, zkStateReader, verbose);
  }

  protected void waitForRecoveriesToFinish(boolean verbose, int timeoutSeconds)
      throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, verbose, true, timeoutSeconds);
  }

  protected void checkQueries() throws Exception {

    handle.put("_version_", SKIPVAL);

    query("q", "*:*", "sort", "n_tl1 desc");

    handle.put("response", UNORDERED);  // get?ids=a,b,c requests are unordered
    String ids = "987654";
    for (int i=0; i<20; i++) {
      query("qt","/get", "id",Integer.toString(i));
      query("qt","/get", "ids",Integer.toString(i));
      ids = ids + ',' + Integer.toString(i);
      query("qt","/get", "ids",ids);
    }
    handle.remove("response");



    // random value sort
    for (String f : fieldNames) {
      query("q", "*:*", "sort", f + " desc");
      query("q", "*:*", "sort", f + " asc");
    }

    // these queries should be exactly ordered and scores should exactly match
    query("q", "*:*", "sort", i1 + " desc");
    query("q", "*:*", "sort", i1 + " asc");
    query("q", "*:*", "sort", i1 + " desc", "fl", "*,score");
    query("q", "*:*", "sort", "n_tl1 asc", "fl", "score"); // test legacy
                                                           // behavior -
                                                           // "score"=="*,score"
    query("q", "*:*", "sort", "n_tl1 desc");
    handle.put("maxScore", SKIPVAL);
    query("q", "{!func}" + i1);// does not expect maxScore. So if it comes
                               // ,ignore it.
                               // JavaBinCodec.writeSolrDocumentList()
    // is agnostic of request params.
    handle.remove("maxScore");
    query("q", "{!func}" + i1, "fl", "*,score"); // even scores should match
                                                 // exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query("q", "quick");
    query("q", "all", "fl", "id", "start", "0");
    query("q", "all", "fl", "foofoofoo", "start", "0"); // no fields in returned
                                                        // docs
    query("q", "all", "fl", "id", "start", "100");

    handle.put("score", SKIPVAL);
    query("q", "quick", "fl", "*,score");
    query("q", "all", "fl", "*,score", "start", "1");
    query("q", "all", "fl", "*,score", "start", "100");

    query("q", "now their fox sat had put", "fl", "*,score", "hl", "true",
        "hl.fl", t1);

    query("q", "now their fox sat had put", "fl", "foofoofoo", "hl", "true",
        "hl.fl", t1);

    query("q", "matchesnothing", "fl", "*,score");

    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "count");
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "count", "facet.mincount", 2);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "index");
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "index", "facet.mincount", 2);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", 1);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.query", "quick",
        "facet.query", "all", "facet.query", "*:*");
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.offset", 1);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.mincount", 2);

    // test faceting multiple things at once
    query("q", "*:*", "rows", 100, "facet", "true", "facet.query", "quick",
        "facet.query", "all", "facet.query", "*:*", "facet.field", t1);

    // test filter tagging, facet exclusion, and naming (multi-select facet
    // support)
    query("q", "*:*", "rows", 100, "facet", "true", "facet.query",
        "{!key=myquick}quick", "facet.query", "{!key=myall ex=a}all",
        "facet.query", "*:*", "facet.field", "{!key=mykey ex=a}" + t1,
        "facet.field", "{!key=other ex=b}" + t1, "facet.field",
        "{!key=again ex=a,b}" + t1, "facet.field", t1, "fq",
        "{!tag=a}id:[1 TO 7]", "fq", "{!tag=b}id:[3 TO 9]");
    query("q", "*:*", "facet", "true", "facet.field",
        "{!ex=t1}SubjectTerms_mfacet", "fq",
        "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", "10",
        "facet.mincount", "1");

    // test field that is valid in schema but missing in all shards
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field",
        missingField, "facet.mincount", 2);
    // test field that is valid in schema and missing in some shards
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", oddField,
        "facet.mincount", 2);

    query("q", "*:*", "sort", i1 + " desc", "stats", "true", "stats.field", i1);

    // Try to get better coverage for refinement queries by turning off over
    // requesting.
    // This makes it much more likely that we may not get the top facet values
    // and hence
    // we turn of that checking.
    handle.put("facet_fields", SKIPVAL);
    query("q", "*:*", "rows", 0, "facet", "true", "facet.field", t1,
        "facet.limit", 5, "facet.shard.limit", 5);
    // check a complex key name
    query("q", "*:*", "rows", 0, "facet", "true", "facet.field",
        "{!key='a b/c \\' \\} foo'}" + t1, "facet.limit", 5,
        "facet.shard.limit", 5);
    handle.remove("facet_fields");

    query("q", "*:*", "sort", "n_tl1 desc");

    // index the same document to two shards and make sure things
    // don't blow up.
    // assumes first n clients are first n shards
    if (clients.size() >= 2) {
      index(id, 100, i1, 107, t1, "oh no, a duplicate!");
      for (int i = 0; i < getShardCount(); i++) {
        index_specific(i, id, 100, i1, 107, t1, "oh no, a duplicate!");
      }
      commit();
      query("q", "duplicate", "hl", "true", "hl.fl", t1);
      query("q", "fox duplicate horses", "hl", "true", "hl.fl", t1);
      query("q", "*:*", "rows", 100);
    }
  }

  protected void indexAbunchOfDocs() throws Exception {
    indexr(id, 2, i1, 50, t1, "to come to the aid of their country.");
    indexr(id, 3, i1,  2, t1, "how now brown cow");
    indexr(id, 4, i1, -100, t1,
        "the quick fox jumped over the lazy dog");
    indexr(id, 5, i1, 500, t1,
        "the quick fox jumped way over the lazy dog");
    indexr(id, 6, i1, -600, t1, "humpty dumpy sat on a wall");
    indexr(id, 7, i1, 123, t1, "humpty dumpy had a great fall");
    indexr(id, 8, i1, 876, t1,
        "all the kings horses and all the kings men");
    indexr(id, 9, i1, 7, t1, "couldn't put humpty together again");
    indexr(id, 10, i1, 4321, t1, "this too shall pass");
    indexr(id, 11, i1, -987, t1,
        "An eye for eye only ends up making the whole world blind.");
    indexr(id, 12, i1, 379, t1,
        "Great works are performed, not by strength, but by perseverance.");
    indexr(id, 13, i1, 232, t1, "no eggs on wall, lesson learned",
        oddField, "odd man out");

    indexr(id, 14, "SubjectTerms_mfacet", new String[] {"mathematical models",
        "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[] {"test 1", "test 2",
        "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[] {"test 1", "test 2",
        "test3"});
    String[] vals = new String[100];
    for (int i = 0; i < 100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);

    for (int i = 100; i < 150; i++) {
      indexr(id, i);
    }
  }

  /**
   * Executes a query against each live and active replica of the specified shard
   * and aserts that the results are identical.
   *
   * @see #queryAndCompare
   */
  public QueryResponse queryAndCompareReplicas(SolrParams params, String shard)
    throws Exception {

    ArrayList<SolrClient> shardClients = new ArrayList<>(7);

    updateMappingsFromZk(jettys, clients);
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
    assertNotNull("no jetties found for shard: " + shard, solrJetties);


    for (CloudJettyRunner cjetty : solrJetties) {
      ZkNodeProps props = cjetty.info;
      String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      boolean active = Replica.State.getState(props.getStr(ZkStateReader.STATE_PROP)) == Replica.State.ACTIVE;
      boolean live = zkStateReader.getClusterState().liveNodesContain(nodeName);
      if (active && live) {
        shardClients.add(cjetty.client.solrClient);
      }
    }
    return queryAndCompare(params, shardClients);
  }

  /**
   * For each Shard, executes a query against each live and active replica of that shard
   * and asserts that the results are identical for each replica of the same shard.
   * Because results are not compared between replicas of different shards, this method
   * should be safe for comparing the results of any query, even if it contains
   * "distrib=false", because the replicas should all be identical.
   *
   * @see AbstractFullDistribZkTestBase#queryAndCompareReplicas(SolrParams, String)
   */
  public void queryAndCompareShards(SolrParams params) throws Exception {

    updateMappingsFromZk(jettys, clients);
    List<String> shards = new ArrayList<>(shardToJetty.keySet());
    for (String shard : shards) {
      queryAndCompareReplicas(params, shard);
    }
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a
   * consistent number of documents.
   */
  protected void checkShardConsistency(String shard) throws Exception {
    checkShardConsistency(shard, false, false);
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a
   * consistent number of documents.
   * If expectFailure==false, the exact differences found will be logged since
   * this would be an unexpected failure.
   * verbose causes extra debugging into to be displayed, even if everything is
   * consistent.
   */
  protected String checkShardConsistency(String shard, boolean expectFailure, boolean verbose)
      throws Exception {

    List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
    if (solrJetties == null) {
      throw new RuntimeException("shard not found:" + shard + " keys:"
          + shardToJetty.keySet());
    }
    long num = -1;
    long lastNum = -1;
    String failMessage = null;
    if (verbose) System.err.println("check const of " + shard);
    int cnt = 0;
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    assertEquals(
        "The client count does not match up with the shard count for slice:"
            + shard,
        zkStateReader.getClusterState().getCollection(DEFAULT_COLLECTION).getSlice(shard)
            .getReplicasMap().size(), solrJetties.size());

    CloudJettyRunner lastJetty = null;
    for (CloudJettyRunner cjetty : solrJetties) {
      ZkNodeProps props = cjetty.info;
      if (verbose) System.err.println("client" + cnt++);
      if (verbose) System.err.println("PROPS:" + props);

      try {
        SolrParams query = params("q","*:*", "rows","0", "distrib","false", "tests","checkShardConsistency"); // "tests" is just a tag that won't do anything except be echoed in logs
        num = cjetty.client.solrClient.query(query).getResults().getNumFound();
      } catch (SolrServerException e) {
        if (verbose) System.err.println("error contacting client: "
            + e.getMessage() + "\n");
        continue;
      } catch (SolrException e) {
        if (verbose) System.err.println("error contacting client: "
            + e.getMessage() + "\n");
        continue;
      }

      boolean live = false;
      String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      if (zkStateReader.getClusterState().liveNodesContain(nodeName)) {
        live = true;
      }
      if (verbose) System.err.println(" live:" + live);
      if (verbose) System.err.println(" num:" + num + "\n");

      boolean active = Replica.State.getState(props.getStr(ZkStateReader.STATE_PROP)) == Replica.State.ACTIVE;
      if (active && live) {
        if (lastNum > -1 && lastNum != num && failMessage == null) {
          failMessage = shard + " is not consistent.  Got " + lastNum + " from " + lastJetty.url + " (previous client)"
              + " and got " + num + " from " + cjetty.url;

          if (!expectFailure || verbose) {
            System.err.println("######" + failMessage);
            SolrQuery query = new SolrQuery("*:*");
            query.set("distrib", false);
            query.set("fl","id,_version_");
            query.set("rows","100000");
            query.set("sort","id asc");
            query.set("tests","checkShardConsistency/showDiff");

            SolrDocumentList lst1 = lastJetty.client.solrClient.query(query).getResults();
            SolrDocumentList lst2 = cjetty.client.solrClient.query(query).getResults();

            CloudInspectUtil.showDiff(lst1, lst2, lastJetty.url, cjetty.url);
          }

        }
        lastNum = num;
        lastJetty = cjetty;
      }
    }
    return failMessage;

  }

  public void showCounts() {
    Set<String> theShards = shardToJetty.keySet();

    for (String shard : theShards) {
      List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);

      for (CloudJettyRunner cjetty : solrJetties) {
        ZkNodeProps props = cjetty.info;
        System.err.println("PROPS:" + props);

        try {
          SolrParams query = params("q", "*:*", "rows", "0", "distrib",
              "false", "tests", "checkShardConsistency"); // "tests" is just a
                                                          // tag that won't do
                                                          // anything except be
                                                          // echoed in logs
          long num = cjetty.client.solrClient.query(query).getResults()
              .getNumFound();
          System.err.println("DOCS:" + num);
        } catch (SolrServerException | SolrException | IOException e) {
          System.err.println("error contacting client: " + e.getMessage()
              + "\n");
          continue;
        }
        boolean live = false;
        String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
        ZkStateReader zkStateReader = cloudClient.getZkStateReader();
        if (zkStateReader.getClusterState().liveNodesContain(nodeName)) {
          live = true;
        }
        System.err.println(" live:" + live);

      }
    }
  }

  protected void randomlyEnableAutoSoftCommit() {
    if (r.nextBoolean()) {
      enableAutoSoftCommit(1000);
    } else {
      log.info("Not turning on auto soft commit");
    }
  }

  protected void enableAutoSoftCommit(int time) {
    log.info("Turning on auto soft commit: {}", time);
    for (List<CloudJettyRunner> jettyList : shardToJetty.values()) {
      for (CloudJettyRunner jetty : jettyList) {
        CoreContainer cores = jetty.jetty.getCoreContainer();
        for (SolrCore core : cores.getCores()) {
          ((DirectUpdateHandler2) core.getUpdateHandler())
              .getSoftCommitTracker().setTimeUpperBound(time);
        }
      }
    }
  }

  /* Checks both shard replcia consistency and against the control shard.
  * The test will be failed if differences are found.
  */
  protected void checkShardConsistency() throws Exception {
    checkShardConsistency(true, false);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(boolean checkVsControl, boolean verbose)
      throws Exception {
    checkShardConsistency(checkVsControl, verbose, null, null);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(boolean checkVsControl, boolean verbose, Set<String> addFails, Set<String> deleteFails)
      throws Exception {

    updateMappingsFromZk(jettys, coreClients, true);

    Set<String> theShards = shardToJetty.keySet();
    String failMessage = null;
    for (String shard : theShards) {
      String shardFailMessage = checkShardConsistency(shard, false, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }

    if (failMessage != null) {
      fail(failMessage);
    }

    if (!checkVsControl) return;

    SolrParams q = params("q","*:*","rows","0", "tests","checkShardConsistency(vsControl)");    // add a tag to aid in debugging via logs

    SolrDocumentList controlDocList = controlClient.query(q).getResults();
    long controlDocs = controlDocList.getNumFound();

    SolrDocumentList cloudDocList = cloudClient.query(q).getResults();
    long cloudClientDocs = cloudDocList.getNumFound();


    // now check that the right # are on each shard
    theShards = shardToJetty.keySet();
    int cnt = 0;
    for (String s : theShards) {
      int times = shardToJetty.get(s).size();
      for (int i = 0; i < times; i++) {
        try {
          CloudJettyRunner cjetty = shardToJetty.get(s).get(i);
          ZkNodeProps props = cjetty.info;
          SolrClient client = cjetty.client.solrClient;
          boolean active = Replica.State.getState(props.getStr(ZkStateReader.STATE_PROP)) == Replica.State.ACTIVE;
          if (active) {
            SolrQuery query = new SolrQuery("*:*");
            query.set("distrib", false);
            long results = client.query(query).getResults().getNumFound();
            if (verbose) System.err.println(new ZkCoreNodeProps(props)
                .getCoreUrl() + " : " + results);
            if (verbose) System.err.println("shard:"
                + props.getStr(ZkStateReader.SHARD_ID_PROP));
            cnt += results;
            break;
          }
        } catch (Exception e) {
          // if we have a problem, try the next one
          if (i == times - 1) {
            throw e;
          }
        }
      }
    }


    if (controlDocs != cnt || cloudClientDocs != controlDocs) {
      String msg = "document count mismatch.  control=" + controlDocs + " sum(shards)="+ cnt + " cloudClient="+cloudClientDocs;
      log.error(msg);

      boolean shouldFail = CloudInspectUtil.compareResults(controlClient, cloudClient, addFails, deleteFails);
      if (shouldFail) {
        fail(msg);
      }
    }
  }
  
  protected SolrClient getClient(String nodeName) {
    for (CloudJettyRunner cjetty : cloudJettys) {
      CloudSolrServerClient client = cjetty.client;
      if (client.shardName.equals(nodeName)) {
        return client.solrClient;
      }
    }
    return null;
  }

  protected void assertDocCounts(boolean verbose) throws Exception {
    // TODO: as we create the clients, we should build a map from shard to
    // node/client
    // and node/client to shard?
    if (verbose) System.err.println("control docs:"
        + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound()
        + "\n\n");
    long controlCount = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();

    // do some really inefficient mapping...
    Map<String,Slice> slices = null;
    ClusterState clusterState;
    try (ZkStateReader zk = new ZkStateReader(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT,
        AbstractZkTestCase.TIMEOUT)) {
      zk.createClusterStateWatchersAndUpdate();
      clusterState = zk.getClusterState();
      final DocCollection docCollection = clusterState.getCollectionOrNull(DEFAULT_COLLECTION);
      slices = (docCollection != null) ? docCollection.getSlicesMap() : null;
    }

    if (slices == null) {
      throw new RuntimeException("Could not find collection "
          + DEFAULT_COLLECTION + " in " + clusterState.getCollectionsMap().keySet());
    }

    for (CloudJettyRunner cjetty : cloudJettys) {
      CloudSolrServerClient client = cjetty.client;
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,Replica> theShards = slice.getValue().getReplicasMap();
        for (Map.Entry<String,Replica> shard : theShards.entrySet()) {
          String shardName = new URI(
              ((HttpSolrClient) client.solrClient).getBaseURL()).getPort()
              + "_solr_";
          if (verbose && shard.getKey().endsWith(shardName)) {
            System.err.println("shard:" + slice.getKey());
            System.err.println(shard.getValue());
          }
        }
      }
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      long count = 0;
      final Replica.State currentState = Replica.State.getState(cjetty.info.getStr(ZkStateReader.STATE_PROP));
      if (currentState == Replica.State.ACTIVE
          && zkStateReader.getClusterState().liveNodesContain(cjetty.info.getStr(ZkStateReader.NODE_NAME_PROP))) {
        SolrQuery query = new SolrQuery("*:*");
        query.set("distrib", false);
        count = client.solrClient.query(query).getResults().getNumFound();
      }

      if (verbose) System.err.println("client docs:" + count + "\n\n");
    }
    if (verbose) System.err.println("control docs:"
        + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound()
        + "\n\n");
    SolrQuery query = new SolrQuery("*:*");
    assertEquals("Doc Counts do not add up", controlCount,
        cloudClient.query(query).getResults().getNumFound());
  }

  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params)
      throws SolrServerException, IOException {

    if (r.nextBoolean()) params.set("collection", DEFAULT_COLLECTION);

    return cloudClient.query(params);
  }

  static abstract class StoppableThread extends Thread {
    public StoppableThread(String name) {
      super(name);
    }
    public abstract void safeStop();
  }

  public void waitForThingsToLevelOut(int waitForRecTimeSeconds) throws Exception {
    log.info("Wait for recoveries to finish - wait {} for each attempt", waitForRecTimeSeconds);
    int cnt = 0;
    boolean retry = false;
    do {
      waitForRecoveriesToFinish(VERBOSE, waitForRecTimeSeconds);

      try {
        commit();
      } catch (Throwable t) {
        t.printStackTrace();
        // we don't care if this commit fails on some nodes
      }

      updateMappingsFromZk(jettys, clients);

      Set<String> theShards = shardToJetty.keySet();
      String failMessage = null;
      for (String shard : theShards) {
        failMessage = checkShardConsistency(shard, true, false);
      }

      if (failMessage != null) {
        log.info("shard inconsistency - waiting ...");
        retry = true;
      } else {
        retry = false;
      }
      cnt++;
      if (cnt > 30) break;
      Thread.sleep(2000);
    } while (retry);
  }


  public void waitForNoShardInconsistency() throws Exception {
    log.info("Wait for no shard inconsistency");
    int cnt = 0;
    boolean retry = false;
    do {
      try {
        commit();
      } catch (Throwable t) {
        t.printStackTrace();
        // we don't care if this commit fails on some nodes
      }

      updateMappingsFromZk(jettys, clients);

      Set<String> theShards = shardToJetty.keySet();
      String failMessage = null;
      for (String shard : theShards) {
        try {
          failMessage = checkShardConsistency(shard, true, true);
        } catch (Exception e) {
          // we might hit a node we just stopped
          failMessage="hit exception:" + e.getMessage();
        }
      }

      if (failMessage != null) {
        log.info("shard inconsistency - waiting ...");
        retry = true;
      } else {
        retry = false;
      }
      cnt++;
      if (cnt > 40) break;
      Thread.sleep(2000);
    } while (retry);
  }

  void doQuery(String expectedDocs, String... queryParams) throws Exception {
    Set<String> expectedIds = new HashSet<>( StrUtils.splitSmart(expectedDocs, ",", true) );

    QueryResponse rsp = cloudClient.query(params(queryParams));
    Set<String> obtainedIds = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.add((String) doc.get("id"));
    }

    assertEquals(expectedIds, obtainedIds);
  }

  @Override
  public void distribTearDown() throws Exception {
    try {
      if (VERBOSE || printLayoutOnTearDown) {
        super.printLayout();
      }
      
      closeRestTestHarnesses(); // TODO: close here or later?
      

    } finally {
      super.distribTearDown();

      System.clearProperty("zkHost");
      System.clearProperty("numShards");
    }
  }
  
  @Override
  protected void destroyServers() throws Exception {
    ExecutorService customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("closeThreadPool"));
    
    customThreadPool.submit(() -> IOUtils.closeQuietly(commonCloudSolrClient));
    
    customThreadPool.submit(() -> IOUtils.closeQuietly(controlClient));
    
    customThreadPool.submit(() -> coreClients.parallelStream().forEach(c -> {
      IOUtils.closeQuietly(c);
    }));

    customThreadPool.submit(() -> IOUtils.closeQuietly(controlClientCloud));

    customThreadPool.submit(() -> IOUtils.closeQuietly(cloudClient));

    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
    
    coreClients.clear();
    
    super.destroyServers();
  }

  @Override
  protected void commit() throws Exception {
    controlClient.commit();
    cloudClient.commit();
  }

  protected CollectionAdminResponse createCollection(String collectionName, String configSetName, int numShards, int replicationFactor, int maxShardsPerNode) throws SolrServerException, IOException, InterruptedException, TimeoutException {
    return createCollection(null, collectionName, configSetName, numShards, replicationFactor, maxShardsPerNode, null, null);
  }

  protected CollectionAdminResponse createCollection(Map<String,List<Integer>> collectionInfos, String collectionName, Map<String,Object> collectionProps, SolrClient client)  throws SolrServerException, IOException, InterruptedException, TimeoutException{
    return createCollection(collectionInfos, collectionName, collectionProps, client, "conf1");
  }

  // TODO: Use CollectionAdminRequest#createCollection() instead of a raw request
  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos, String collectionName, Map<String, Object> collectionProps, SolrClient client, String confSetName)  throws SolrServerException, IOException, InterruptedException, TimeoutException{
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
      if(entry.getValue() !=null) params.set(entry.getKey(), String.valueOf(entry.getValue()));
    }
    Integer numShards = (Integer) collectionProps.get(OverseerCollectionMessageHandler.NUM_SLICES);
    if(numShards==null){
      String shardNames = (String) collectionProps.get(OverseerCollectionMessageHandler.SHARDS_PROP);
      numShards = StrUtils.splitSmart(shardNames,',').size();
    }
    Integer numNrtReplicas = (Integer) collectionProps.get(ZkStateReader.NRT_REPLICAS);
    if (numNrtReplicas == null) {
      numNrtReplicas = (Integer) collectionProps.get(ZkStateReader.REPLICATION_FACTOR);
    }
    if(numNrtReplicas == null){
      numNrtReplicas = (Integer) OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.get(ZkStateReader.REPLICATION_FACTOR);
    }
    if (numNrtReplicas == null) {
      numNrtReplicas = Integer.valueOf(0);
    }
    Integer numTlogReplicas = (Integer) collectionProps.get(ZkStateReader.TLOG_REPLICAS);
    if (numTlogReplicas == null) {
      numTlogReplicas = Integer.valueOf(0);
    }
    Integer numPullReplicas = (Integer) collectionProps.get(ZkStateReader.PULL_REPLICAS);
    if (numPullReplicas == null) {
      numPullReplicas = Integer.valueOf(0);
    }
    if (confSetName != null) {
      params.set("collection.configName", confSetName);
    } else {
      params.set("collection.configName", "conf1");
    }

    int clientIndex = random().nextInt(2);
    List<Integer> list = new ArrayList<>();
    list.add(numShards);
    list.add(numNrtReplicas + numTlogReplicas + numPullReplicas);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    if ("1".equals(getStateFormat()) ) {
      log.info("Creating collection with stateFormat=1: {}", collectionName);
      params.set(DocCollection.STATE_FORMAT, "1");
    }
    @SuppressWarnings({"rawtypes"})
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    if (client == null) {
      final String baseUrl = getBaseUrl((HttpSolrClient) clients.get(clientIndex));
      try (SolrClient adminClient = createNewSolrClient("", baseUrl)) {
        res.setResponse(adminClient.request(request));
      }
    } else {
      res.setResponse(client.request(request));
    }
    
    try {
      cloudClient.waitForState(collectionName, 30, TimeUnit.SECONDS, SolrCloudTestCase.activeClusterShape(numShards,
          numShards * (numNrtReplicas + numTlogReplicas + numPullReplicas)));
    } catch (TimeoutException e) {
      new RuntimeException("Timeout waiting for " + numShards + " shards and " + (numNrtReplicas + numTlogReplicas + numPullReplicas) + " replicas.", e);
    }
    return res;
  }


  protected CollectionAdminResponse createCollection(Map<String,List<Integer>> collectionInfos,
      String collectionName, String configSetName, int numShards, int replicationFactor, int maxShardsPerNode, SolrClient client, String createNodeSetStr) throws SolrServerException, IOException, InterruptedException, TimeoutException {

    int numNrtReplicas = useTlogReplicas()?0:replicationFactor;
    int numTlogReplicas = useTlogReplicas()?replicationFactor:0;
    return createCollection(collectionInfos, collectionName,
        Utils.makeMap(
            OverseerCollectionMessageHandler.NUM_SLICES, numShards,
            ZkStateReader.NRT_REPLICAS, numNrtReplicas,
            ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
            ZkStateReader.PULL_REPLICAS, getPullReplicaCount(),
            OverseerCollectionMessageHandler.CREATE_NODE_SET, createNodeSetStr,
            ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode),
        client, configSetName);
  }

  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos,
                                                     String collectionName, int numShards, int replicationFactor, int maxShardsPerNode, SolrClient client, String createNodeSetStr, String configName) throws SolrServerException, IOException, InterruptedException, TimeoutException {

    int numNrtReplicas = useTlogReplicas()?0:replicationFactor;
    int numTlogReplicas = useTlogReplicas()?replicationFactor:0;
    return createCollection(collectionInfos, collectionName,
        Utils.makeMap(
            OverseerCollectionMessageHandler.NUM_SLICES, numShards,
            ZkStateReader.NRT_REPLICAS, numNrtReplicas,
            ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
            ZkStateReader.PULL_REPLICAS, getPullReplicaCount(),
            OverseerCollectionMessageHandler.CREATE_NODE_SET, createNodeSetStr,
            ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode),
        client, configName);
  }

  @Override
  protected SolrClient createNewSolrClient(int port) {
    return createNewSolrClient(DEFAULT_COLLECTION, port);
  }
  
  protected SolrClient createNewSolrClient(int port, int connectionTimeoutMillis, int socketTimeoutMillis) {
    return createNewSolrClient(DEFAULT_COLLECTION, port, connectionTimeoutMillis, socketTimeoutMillis);
  }

  protected SolrClient createNewSolrClient(String coreName, int port) {
    try {
      // setup the server...
      String baseUrl = buildUrl(port);
      String url = baseUrl + (baseUrl.endsWith("/") ? "" : "/") + coreName;
      HttpSolrClient client = getHttpSolrClient(url, DEFAULT_CONNECTION_TIMEOUT, 60000);
      return client;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected SolrClient createNewSolrClient(String coreName, int port, int connectionTimeoutMillis, int socketTimeoutMillis) {
    try {
      // setup the server...
      String baseUrl = buildUrl(port);
      String url = baseUrl + (baseUrl.endsWith("/") ? "" : "/") + coreName;
      HttpSolrClient client = getHttpSolrClient(url, connectionTimeoutMillis, socketTimeoutMillis);
      return client;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected SolrClient createNewSolrClient(String collection, String baseUrl) {
    try {
      // setup the server...
      HttpSolrClient client = getHttpSolrClient(baseUrl + "/" + collection, DEFAULT_CONNECTION_TIMEOUT, 60000);
      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected String getBaseUrl(HttpSolrClient client) {
    return client .getBaseURL().substring(
        0, client.getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
  }

  public static SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }

  private String checkCollectionExpectations(String collectionName, List<Integer> numShardsNumReplicaList, List<String> nodesAllowedToRunShards) {
    ClusterState clusterState = getCommonCloudSolrClient().getZkStateReader().getClusterState();
    
    int expectedSlices = numShardsNumReplicaList.get(0);
    // The Math.min thing is here, because we expect replication-factor to be reduced to if there are not enough live nodes to spread all shards of a collection over different nodes
    int expectedShardsPerSlice = numShardsNumReplicaList.get(1);
    int expectedTotalShards = expectedSlices * expectedShardsPerSlice;

//      Map<String,DocCollection> collections = clusterState
//          .getCollectionStates();
      if (clusterState.hasCollection(collectionName)) {
        Map<String,Slice> slices = clusterState.getCollection(collectionName).getSlicesMap();
        // did we find expectedSlices slices/shards?
      if (slices.size() != expectedSlices) {
        return "Found new collection " + collectionName + ", but mismatch on number of slices. Expected: " + expectedSlices + ", actual: " + slices.size();
      }
      int totalShards = 0;
      for (String sliceName : slices.keySet()) {
        for (Replica replica : slices.get(sliceName).getReplicas()) {
          if (nodesAllowedToRunShards != null && !nodesAllowedToRunShards.contains(replica.getStr(ZkStateReader.NODE_NAME_PROP))) {
            return "Shard " + replica.getName() + " created on node " + replica.getNodeName() + " not allowed to run shards for the created collection " + collectionName;
          }
        }
        totalShards += slices.get(sliceName).getReplicas().size();
      }
      if (totalShards != expectedTotalShards) {
        return "Found new collection " + collectionName + " with correct number of slices, but mismatch on number of shards. Expected: " + expectedTotalShards + ", actual: " + totalShards;
        }
      return null;
    } else {
      return "Could not find new collection " + collectionName;
    }
  }

  protected void checkForCollection(String collectionName,
      List<Integer> numShardsNumReplicaList,
      List<String> nodesAllowedToRunShards) throws Exception {
    // check for an expectedSlices new collection - we poll the state
    final TimeOut timeout = new TimeOut(120, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    boolean success = false;
    String checkResult = "Didnt get to perform a single check";
    while (! timeout.hasTimedOut()) {
      checkResult = checkCollectionExpectations(collectionName,
          numShardsNumReplicaList, nodesAllowedToRunShards);
      if (checkResult == null) {
        success = true;
        break;
      }
      Thread.sleep(500);
    }
    if (!success) {
      super.printLayout();
      fail(checkResult);
    }
  }
  
  private CloudSolrClient commonCloudSolrClient;
  
  protected CloudSolrClient getCommonCloudSolrClient() {
    synchronized (this) {
      if (commonCloudSolrClient == null) {
        commonCloudSolrClient = getCloudSolrClient(zkServer.getZkAddress(),
            random().nextBoolean(), 5000, 120000);
        commonCloudSolrClient.setDefaultCollection(DEFAULT_COLLECTION);
        commonCloudSolrClient.connect();
        if (log.isInfoEnabled()) {
          log.info("Created commonCloudSolrClient with updatesToLeaders={} and parallelUpdates={}",
              commonCloudSolrClient.isUpdatesToLeaders(), commonCloudSolrClient.isParallelUpdates());
        }
      }
    }
    return commonCloudSolrClient;
  }

  public static String getUrlFromZk(ClusterState clusterState, String collection) {
    Map<String,Slice> slices = clusterState.getCollection(collection).getSlicesMap();

    if (slices == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not find collection:" + collection);
    }

    for (Map.Entry<String,Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      Map<String,Replica> shards = slice.getReplicasMap();
      Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
      for (Map.Entry<String,Replica> shardEntry : shardEntries) {
        final ZkNodeProps node = shardEntry.getValue();
        final String nodeName = node.getStr(ZkStateReader.NODE_NAME_PROP);
        if (clusterState.liveNodesContain(nodeName)) {
          return ZkCoreNodeProps.getCoreUrl(node.getStr(ZkStateReader.BASE_URL_PROP), collection);
        }
      }
    }

    throw new RuntimeException("Could not find a live node for collection:" + collection);
  }

 public static void waitForNon403or404or503(HttpSolrClient collectionClient)
      throws Exception {
    SolrException exp = null;
    final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (! timeout.hasTimedOut()) {
      boolean missing = false;

      try {
        collectionClient.query(new SolrQuery("*:*"));
      } catch (SolrException e) {
        if (!(e.code() == 403 || e.code() == 503 || e.code() == 404)) {
          throw e;
        }
        exp = e;
        missing = true;
      }
      if (!missing) {
        return;
      }
      Thread.sleep(50);
    }

    fail("Could not find the new collection - " + exp.code() + " : " + collectionClient.getBaseURL());
  }

  protected void assertCollectionNotExists(String collectionName, int timeoutSeconds) throws Exception {
    waitForCollectionToDisappear(collectionName, getCommonCloudSolrClient().getZkStateReader(), false, true, timeoutSeconds);
    assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
  }


  protected void createCollection(String collName,
                                  CloudSolrClient client,
                                  int replicationFactor ,
                                  int numShards ) throws Exception {
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;
    int numNrtReplicas = useTlogReplicas()?0:replicationFactor;
    int numTlogReplicas = useTlogReplicas()?replicationFactor:0;
    Map<String, Object> props = makeMap(
        ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode,
        ZkStateReader.NRT_REPLICAS, numNrtReplicas,
        ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
        ZkStateReader.PULL_REPLICAS, getPullReplicaCount(),
        OverseerCollectionMessageHandler.NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    createCollection(collectionInfos, collName, props, client);
  }

  protected void createCollectionRetry(String testCollectionName, String configSetName, int numShards, int replicationFactor, int maxShardsPerNode)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {
    CollectionAdminResponse resp = createCollection(testCollectionName, configSetName, numShards, replicationFactor, maxShardsPerNode);
    if (resp.getResponse().get("failure") != null) {
      CollectionAdminRequest.Delete req = CollectionAdminRequest.deleteCollection(testCollectionName);
      req.process(cloudClient);

      resp = createCollection(testCollectionName, configSetName, numShards, replicationFactor, maxShardsPerNode);

      if (resp.getResponse().get("failure") != null) {
        fail("Could not create " + testCollectionName);
      }
    }
  }

  protected Replica getShardLeader(String testCollectionName, String shardId, int timeoutSecs) throws Exception {
    Replica leader = null;
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutSecs, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      Replica tmp = null;
      try {
        tmp = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, shardId);
      } catch (Exception exc) {}
      if (tmp != null && "active".equals(tmp.getStr(ZkStateReader.STATE_PROP))) {
        leader = tmp;
        break;
      }
      Thread.sleep(1000);
    }
    assertNotNull("Could not find active leader for " + shardId + " of " +
        testCollectionName + " after "+timeoutSecs+" secs; clusterState: " +
        printClusterStateInfo(testCollectionName), leader);

    return leader;
  }

  protected List<Replica> ensureAllReplicasAreActive(String testCollectionName, String shardId, int shards, int rf, int maxWaitSecs) throws Exception {
    final RTimer timer = new RTimer();

    Map<String,Replica> notLeaders = new HashMap<>();

    ZkStateReader zkr = cloudClient.getZkStateReader();
    zkr.forceUpdateCollection(testCollectionName); // force the state to be fresh

    ClusterState cs = zkr.getClusterState();
    Collection<Slice> slices = cs.getCollection(testCollectionName).getActiveSlices();
    assertTrue(slices.size() == shards);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    Replica leader = null;
    ZkShardTerms zkShardTerms = new ZkShardTerms(testCollectionName, shardId, cloudClient.getZkStateReader().getZkClient());
    while (waitMs < maxWaitMs && !allReplicasUp) {
      cs = cloudClient.getZkStateReader().getClusterState();
      assertNotNull(cs);
      final DocCollection docCollection = cs.getCollectionOrNull(testCollectionName);
      assertNotNull("No collection found for " + testCollectionName, docCollection);
      Slice shard = docCollection.getSlice(shardId);
      assertNotNull("No Slice for "+shardId, shard);
      allReplicasUp = true; // assume true
      Collection<Replica> replicas = shard.getReplicas();
      assertTrue("Did not find correct number of replicas. Expected:" + rf + " Found:" + replicas.size(), replicas.size() == rf);
      
      leader = shard.getLeader();
      assertNotNull(leader);
      if (log.isInfoEnabled()) {
        log.info("Found {}  replicas and leader on {} for {} in {}"
            , replicas.size(), leader.getNodeName(), shardId, testCollectionName);
      }

      // ensure all replicas are "active" and identify the non-leader replica
      for (Replica replica : replicas) {
        if (!zkShardTerms.canBecomeLeader(replica.getName()) ||
            replica.getState() != Replica.State.ACTIVE) {
          if (log.isInfoEnabled()) {
            log.info("Replica {} is currently {}", replica.getName(), replica.getState());
          }
          allReplicasUp = false;
        }

        if (!leader.equals(replica))
          notLeaders.put(replica.getName(), replica);
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(500L);
        } catch (Exception ignoreMe) {}
        waitMs += 500L;
      }
    } // end while

    zkShardTerms.close();
    if (!allReplicasUp)
      fail("Didn't see all replicas for shard "+shardId+" in "+testCollectionName+
          " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo());

    if (notLeaders.isEmpty())
      fail("Didn't isolate any replicas that are not the leader! ClusterState: " + printClusterStateInfo());

    if (log.isInfoEnabled()) {
      log.info("Took {} ms to see all replicas become active.", timer.getTime());
    }

    List<Replica> replicas = new ArrayList<>(notLeaders.values());
    return replicas;
  }

  protected String printClusterStateInfo() throws Exception {
    return printClusterStateInfo(null);
  }

  protected String printClusterStateInfo(String collection) throws Exception {
    cloudClient.getZkStateReader().forceUpdateCollection(collection);
    String cs = null;
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    if (collection != null) {
      cs = clusterState.getCollection(collection).toString();
    } else {
      Map<String,DocCollection> map = clusterState.getCollectionsMap();
      CharArr out = new CharArr();
      new JSONWriter(out, 2).write(map);
      cs = out.toString();
    }
    return cs;
  }

  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {
    ZkCoreNodeProps coreProps = new ZkCoreNodeProps(replica);
    String coreName = coreProps.getCoreName();
    boolean reloadedOk = false;
    try (HttpSolrClient client = getHttpSolrClient(coreProps.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      long leaderCoreStartTime = statusResp.getStartTime(coreName).getTime();

      Thread.sleep(1000);

      // send reload command for the collection
      log.info("Sending RELOAD command for {}", testCollectionName);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.RELOAD.toString());
      params.set("name", testCollectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);
      Thread.sleep(2000); // reload can take a short while

      // verify reload is done, waiting up to 30 seconds for slow test environments
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      while (System.nanoTime() < timeout) {
        statusResp = CoreAdminRequest.getStatus(coreName, client);
        long startTimeAfterReload = statusResp.getStartTime(coreName).getTime();
        if (startTimeAfterReload > leaderCoreStartTime) {
          reloadedOk = true;
          break;
        }
        // else ... still waiting to see the reloaded core report a later start time
        Thread.sleep(1000);
      }
    }
    return reloadedOk;
  }
  

  protected void logReplicaTypesReplicationInfo(String collectionName, ZkStateReader zkStateReader) throws KeeperException, InterruptedException, IOException {
    log.info("## Collecting extra Replica.Type information of the cluster");
    zkStateReader.updateLiveNodes();
    StringBuilder builder = new StringBuilder();
    zkStateReader.forceUpdateCollection(collectionName);
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    for(Slice s:collection.getSlices()) {
      Replica leader = s.getLeader();
      for (Replica r:s.getReplicas()) {
        if (!r.isActive(zkStateReader.getClusterState().getLiveNodes())) {
          builder.append(String.format(Locale.ROOT, "Replica %s not in liveNodes or is not active%s", r.getName(), System.lineSeparator()));
          continue;
        }
        if (r.equals(leader)) {
          builder.append(String.format(Locale.ROOT, "Replica %s is leader%s", r.getName(), System.lineSeparator()));
        }
        logReplicationDetails(r, builder);
      }
    }
    log.info("Summary of the cluster: {}", builder);
  }

  protected void waitForReplicationFromReplicas(String collectionName, ZkStateReader zkStateReader, TimeOut timeout) throws KeeperException, InterruptedException, IOException {
    log.info("waitForReplicationFromReplicas: {}", collectionName);
    zkStateReader.forceUpdateCollection(collectionName);
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    Map<String, CoreContainer> containers = new HashMap<>();
    for (JettySolrRunner runner:jettys) {
      if (!runner.isRunning()) {
        continue;
      }
      containers.put(runner.getNodeName(), runner.getCoreContainer());
    }
    for(Slice s:collection.getSlices()) {
      Replica leader = zkStateReader.getLeaderRetry(collectionName, s.getName(), (int)timeout.timeLeft(TimeUnit.MILLISECONDS));
      long leaderIndexVersion = -1;
      while (!timeout.hasTimedOut()) {
        leaderIndexVersion = getIndexVersion(leader);
        if (leaderIndexVersion >= 0) {
          break;
        }
        Thread.sleep(1000);
      }
      if (timeout.hasTimedOut()) {
        fail("Unable to get leader indexVersion");
      }
      for (Replica pullReplica:s.getReplicas(EnumSet.of(Replica.Type.PULL,Replica.Type.TLOG))) {
        if (!zkStateReader.getClusterState().liveNodesContain(pullReplica.getNodeName())) {
          continue;
        }
        while (true) {
          long replicaIndexVersion = getIndexVersion(pullReplica); 
          if (leaderIndexVersion == replicaIndexVersion) {
            if (log.isInfoEnabled()) {
              log.info("Leader replica's version ({}) in sync with replica({}): {} == {}"
                  , leader.getName(), pullReplica.getName(), leaderIndexVersion, replicaIndexVersion);
            }
            
            // Make sure the host is serving the correct version
            try (SolrCore core = containers.get(pullReplica.getNodeName()).getCore(pullReplica.getCoreName())) {
              RefCounted<SolrIndexSearcher> ref = core.getRegisteredSearcher();
              try {
                SolrIndexSearcher searcher = ref.get();
                String servingVersion = searcher.getIndexReader().getIndexCommit().getUserData().get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
                if (Long.parseLong(servingVersion) == replicaIndexVersion) {
                  break;
                } else {
                  if (log.isInfoEnabled()) {
                    log.info("Replica {} has the correct version replicated, but the searcher is not ready yet. Replicated version: {}, Serving version: {}"
                        , pullReplica.getName(), replicaIndexVersion, servingVersion);
                  }
                }
              } finally {
                if (ref != null) ref.decref();
              }
            }
          } else {
            if (timeout.hasTimedOut()) {
              logReplicaTypesReplicationInfo(collectionName, zkStateReader);
              fail(String.format(Locale.ROOT, "Timed out waiting for replica %s (%d) to replicate from leader %s (%d)", pullReplica.getName(), replicaIndexVersion, leader.getName(), leaderIndexVersion));
            }
            if (leaderIndexVersion > replicaIndexVersion) {
              if (log.isInfoEnabled()) {
                log.info("{} version is {} and leader's is {}, will wait for replication"
                    , pullReplica.getName(), replicaIndexVersion, leaderIndexVersion);
              }
            } else {
              if (log.isInfoEnabled()) {
                log.info("Leader replica's version ({}) is lower than pull replica({}): {} < {}"
                    , leader.getName(), pullReplica.getName(), leaderIndexVersion, replicaIndexVersion);
              }
            }
          }
          Thread.sleep(1000);
        }
      }
    }
  }
  
  protected void waitForAllWarmingSearchers() throws InterruptedException {
    log.info("waitForAllWarmingSearchers");
    for (JettySolrRunner jetty:jettys) {
      if (!jetty.isRunning()) {
        continue;
      }
      for (SolrCore core:jetty.getCoreContainer().getCores()) {
        waitForWarming(core);
      }
    }
  }

  protected long getIndexVersion(Replica replica) throws IOException {
    try (HttpSolrClient client = new HttpSolrClient.Builder(replica.getCoreUrl()).build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_SHOW_COMMITS);
      try {
        QueryResponse response = client.query(params);
        @SuppressWarnings("unchecked")
        List<NamedList<Object>> commits = (List<NamedList<Object>>)response.getResponse().get(ReplicationHandler.CMD_SHOW_COMMITS);
        Collections.max(commits, (a,b)->((Long)a.get("indexVersion")).compareTo((Long)b.get("indexVersion")));
        return (long) Collections.max(commits, (a,b)->((Long)a.get("indexVersion")).compareTo((Long)b.get("indexVersion"))).get("indexVersion");
      } catch (SolrServerException e) {
        log.warn("Exception getting version from {}, will return an invalid version to retry.", replica.getName(), e);
        return -1;
      }
    }
  }
  
  /** 
   * Logs a WARN if collection can't be deleted, but does not fail or throw an exception
   * @return true if success, else false
   */
  protected static boolean attemptCollectionDelete(CloudSolrClient client, String collectionName) {
    // try to clean up
    try {
      CollectionAdminRequest.deleteCollection(collectionName).process(client);
      return true;
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} - ignoring", collectionName);
    }
    return false;
  }

  protected void logReplicationDetails(Replica replica, StringBuilder builder) throws IOException {
    try (HttpSolrClient client = new HttpSolrClient.Builder(replica.getCoreUrl()).build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_DETAILS);
      try {
        QueryResponse response = client.query(params);
        builder.append(String.format(Locale.ROOT, "%s: %s%s", replica.getName(), response.getResponse(), System.lineSeparator()));
      } catch (SolrServerException e) {
        log.warn("Unable to ger replication details for replica {}", replica.getName(), e);
      }
    }
  }

  public static RequestStatusState getRequestStateAfterCompletion(String requestId, int waitForSeconds, SolrClient client)
      throws IOException, SolrServerException {
    RequestStatusState state = null;
    final TimeOut timeout = new TimeOut(waitForSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (!timeout.hasTimedOut())  {
      state = getRequestState(requestId, client);
      if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
        return state;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted whie waiting for request completion. Last state seen: " + state, e);
      }
    }

    return state;
  }

  static RequestStatusState getRequestState(int requestId, SolrClient client) throws IOException, SolrServerException {
    return getRequestState(String.valueOf(requestId), client);
  }

  static RequestStatusState getRequestState(String requestId, SolrClient client) throws IOException, SolrServerException {
    CollectionAdminResponse response = getStatusResponse(requestId, client);

    @SuppressWarnings({"rawtypes"})
    NamedList innerResponse = (NamedList) response.getResponse().get("status");
    return RequestStatusState.fromKey((String) innerResponse.get("state"));
  }

  static CollectionAdminResponse getStatusResponse(String requestId, SolrClient client) throws SolrServerException, IOException {
    return CollectionAdminRequest.requestStatus(requestId).process(client);
  }

  protected void setupRestTestHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(() -> ((HttpSolrClient) client).getBaseURL());
      restTestHarnesses.add(harness);
    }
  }

  protected void closeRestTestHarnesses() throws IOException {
    for (RestTestHarness h : restTestHarnesses) {
      h.close();
    }
  }

  protected RestTestHarness randomRestTestHarness() {
    return restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
  }

  protected RestTestHarness randomRestTestHarness(Random random) {
    return restTestHarnesses.get(random.nextInt(restTestHarnesses.size()));
  }

  protected void forAllRestTestHarnesses(UnaryOperator<RestTestHarness> op) {
    for (RestTestHarness h : restTestHarnesses) {
      op.apply(h);
    }
  }

}
