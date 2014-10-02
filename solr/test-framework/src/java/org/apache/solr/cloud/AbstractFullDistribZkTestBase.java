package org.apache.solr.cloud;

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

import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARDS_PROP;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FilenameUtils;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
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
  static Logger log = LoggerFactory.getLogger(AbstractFullDistribZkTestBase.class);
  
  @BeforeClass
  public static void beforeFullSolrCloudTest() {
    // shorten the log output more for this test type
    if (formatter != null) formatter.setShorterFormat();
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

  protected CloudSolrServer controlClientCloud;  // cloud version of the control client
  protected volatile CloudSolrServer cloudClient;
  
  protected List<CloudJettyRunner> cloudJettys = new ArrayList<>();
  protected Map<String,List<CloudJettyRunner>> shardToJetty = new HashMap<>();
  private AtomicInteger jettyIntCntr = new AtomicInteger(0);
  protected ChaosMonkey chaosMonkey;
  
  protected Map<String,CloudJettyRunner> shardToLeaderJetty = new HashMap<>();
  private boolean cloudInit;
  protected boolean checkCreatedVsState;
  protected boolean useJettyDataDir = true;

  protected Map<URI,SocketProxy> proxies = new HashMap<URI,SocketProxy>();
  
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
  
  static class CloudSolrServerClient {
    SolrServer solrClient;
    String shardName;
    int port;
    public ZkNodeProps info;
    
    public CloudSolrServerClient() {}
    
    public CloudSolrServerClient(SolrServer client) {
      this.solrClient = client;
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
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // ignoreException(".*");
    if (sliceCount > 0) {
      System.setProperty("numShards", Integer.toString(sliceCount));
    } else {
      System.clearProperty("numShards");
    }
    
    if (isSSLMode()) {
      System.clearProperty("urlScheme");
      ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(),
          AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT);
      try {
        zkStateReader.getZkClient().create(ZkStateReader.CLUSTER_PROPS,
          ZkStateReader.toJSON(Collections.singletonMap("urlScheme","https")), 
          CreateMode.PERSISTENT, true);
      } finally {
        zkStateReader.close();
      }
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
    fixShardCount = true;
    
    shardCount = 4;
    sliceCount = 2;
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
  
  protected CloudSolrServer createCloudClient(String defaultCollection) {
    CloudSolrServer server = new CloudSolrServer(zkServer.getZkAddress(), random().nextBoolean());
    server.setParallelUpdates(random().nextBoolean());
    if (defaultCollection != null) server.setDefaultCollection(defaultCollection);
    server.getLbServer().getHttpClient().getParams()
        .setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 30000);
    return server;
  }
  
  @Override
  protected void createServers(int numServers) throws Exception {
    
    System.setProperty("collection", "control_collection");

    // we want hashes by default for the control, so set to 1 shard as opposed to leaving unset
    String oldNumShards = System.getProperty(ZkStateReader.NUM_SHARDS_PROP);
    System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "1");
    
    try {
      
      File controlJettyDir = createTempDir().toFile();
      setupJettySolrHome(controlJettyDir);
      
      controlJetty = createJetty(controlJettyDir, useJettyDataDir ? getDataDir(testDir
          + "/control/data") : null); // don't pass shard name... let it default to
                               // "shard1"


      controlClient = createNewSolrServer(controlJetty.getLocalPort());
      
      if (sliceCount <= 0) {
        // for now, just create the cloud client for the control if we don't
        // create the normal cloud client.
        // this can change if more tests need it.
        controlClientCloud = createCloudClient("control_collection");
        controlClientCloud.connect();
        waitForCollection(controlClientCloud.getZkStateReader(),
            "control_collection", 0);
        // NOTE: we are skipping creation of the chaos monkey by returning here
        cloudClient = controlClientCloud; // temporary - some code needs/uses
                                          // cloudClient
        return;
      }
      
    } finally {
      System.clearProperty("collection");
      if (oldNumShards != null) {
        System.setProperty(ZkStateReader.NUM_SHARDS_PROP, oldNumShards);
      } else {
        System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
      }
    }


    initCloud();
    
    createJettys(numServers, checkCreatedVsState).size();
    
    int cnt = getTotalReplicas(DEFAULT_COLLECTION);
    if (cnt > 0) {
      waitForCollection(cloudClient.getZkStateReader(), DEFAULT_COLLECTION, sliceCount);
    }

  }

  protected void waitForCollection(ZkStateReader reader, String collection, int slices) throws Exception {
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
    while (reader.getClusterState().getSlices(collection).size() < slices) {
      if (cnt == 0) {
        throw new RuntimeException("timeout waiting for collection shards to come up: collection="+collection
            + ", slices.expected="+slices+ " slices.actual= " + reader.getClusterState().getSlices(collection).size()
            + " slices : "+ reader.getClusterState().getSlices(collection) );
      }
      cnt--;
      Thread.sleep(500);
    }
  }
  
  protected List<JettySolrRunner> createJettys(int numJettys) throws Exception {
    return createJettys(numJettys, false);
  }

  protected int defaultStateFormat = 1 + random().nextInt(2);

  protected int getStateFormat()  {
    String stateFormat = System.getProperty("tests.solr.stateFormat", null);
    if (stateFormat != null)  {
      if ("2".equals(stateFormat)) {
        return defaultStateFormat = 2;
      } else if ("1".equals(stateFormat))  {
        return defaultStateFormat = 1;
      }
    }
    return defaultStateFormat; // random
  }

  /**
   * @param checkCreatedVsState
   *          if true, make sure the number created (numJettys) matches the
   *          number in the cluster state - if you add more jetties this may not
   *          be the case
   */
  protected List<JettySolrRunner> createJettys(int numJettys, boolean checkCreatedVsState) throws Exception {
    List<JettySolrRunner> jettys = new ArrayList<>();
    List<SolrServer> clients = new ArrayList<>();
    StringBuilder sb = new StringBuilder();

    if (getStateFormat() == 2) {
      log.info("Creating collection1 with stateFormat=2");
      SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
          AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT);
      Overseer.getInQueue(zkClient).offer(
          ZkStateReader.toJSON(ZkNodeProps.makeMap(Overseer.QUEUE_OPERATION,
              CollectionParams.CollectionAction.CREATE.toLower(), "name",
              DEFAULT_COLLECTION, "numShards", String.valueOf(sliceCount),
              DocCollection.STATE_FORMAT, getStateFormat())));
      zkClient.close();
    }

    for (int i = 1; i <= numJettys; i++) {
      if (sb.length() > 0) sb.append(',');
      int cnt = this.jettyIntCntr.incrementAndGet();

      File jettyDir = createTempDir().toFile();

      jettyDir.mkdirs();
      setupJettySolrHome(jettyDir);
      log.info("create jetty " + i);
      JettySolrRunner j = createJetty(jettyDir, useJettyDataDir ? getDataDir(testDir + "/jetty"
          + cnt) : null, null, "solrconfig.xml", null);
      jettys.add(j);
      SolrServer client = createNewSolrServer(j.getLocalPort());
      clients.add(client);
    }
  
    this.jettys.addAll(jettys);
    this.clients.addAll(clients);
    
    int numShards = getTotalReplicas(DEFAULT_COLLECTION);
    if (checkCreatedVsState) {
      // now wait until we see that the number of shards in the cluster state
      // matches what we expect
      int retries = 0;
      while (numShards != shardCount) {
        numShards = getTotalReplicas(DEFAULT_COLLECTION);
        if (numShards == shardCount) break;
        if (retries++ == 60) {
          printLayoutOnTearDown = true;
          fail("Shards in the state does not match what we set:" + numShards
              + " vs " + shardCount);
        }
        Thread.sleep(500);
      }

      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      // also make sure we have a leader for each shard
      for (int i = 1; i <= sliceCount; i++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + i, 10000);
      }
    }

    if (numShards > 0) {
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


  protected SolrServer startCloudJetty(String collection, String shard) throws Exception {
    // TODO: use the collection string!!!!
    collection = DEFAULT_COLLECTION;

    int totalReplicas = getTotalReplicas(collection);


    int cnt = this.jettyIntCntr.incrementAndGet();

      File jettyDir = createTempDir("jetty").toFile();
      jettyDir.mkdirs();
      org.apache.commons.io.FileUtils.copyDirectory(new File(getSolrHome()), jettyDir);
      JettySolrRunner j = createJetty(jettyDir, testDir + "/jetty" + cnt, shard, "solrconfig.xml", null);
      jettys.add(j);
      SolrServer client = createNewSolrServer(j.getLocalPort());
      clients.add(client);

    int retries = 60;
    while (--retries >= 0) {
      // total replicas changed.. assume it was us
      if (getTotalReplicas(collection) != totalReplicas) {
       break;
      }
      Thread.sleep(500);
    }

    if (retries <= 0) {
      fail("Timeout waiting for " + j + " to appear in clusterstate");
      printLayout();
    }

    updateMappingsFromZk(this.jettys, this.clients);
    return client;
  }


  /* Total number of replicas (number of cores serving an index to the collection) shown by the cluster state */
  protected int getTotalReplicas(String collection) {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    DocCollection coll = zkStateReader.getClusterState().getCollectionOrNull(collection);
    if (coll == null) return 0;  // support for when collection hasn't been created yet
    int cnt = 0;
    for (Slice slices : coll.getSlices()) {
      cnt += slices.getReplicas().size();
    }
    return cnt;
  }
  
  public JettySolrRunner createJetty(String dataDir, String ulogDir, String shardList,
      String solrConfigOverride) throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(getSolrHome(), context, 0,
        solrConfigOverride, null, false, getExtraServlets(), sslConfig, getExtraRequestFilters());
    jetty.setShards(shardList);
    jetty.setDataDir(getDataDir(dataDir));
    jetty.start();
    
    return jetty;
  }
  
  public JettySolrRunner createJetty(File solrHome, String dataDir, String shardList, String solrConfigOverride, String schemaOverride) throws Exception {
    // randomly test a relative solr.home path
    if (random().nextBoolean()) {
      solrHome = getRelativeSolrHomePath(solrHome);
    }
    
    JettySolrRunner jetty = new JettySolrRunner(solrHome.getPath(), context, 0, solrConfigOverride, schemaOverride, false, getExtraServlets(), sslConfig, getExtraRequestFilters());
    jetty.setShards(shardList);
    jetty.setDataDir(getDataDir(dataDir));
    jetty.start();
    
    return jetty;
  }

  /**
   * Creates a JettySolrRunner with a socket proxy sitting infront of the Jetty server,
   * which gives us the ability to simulate network partitions without having to fuss
   * with IPTables.
   */
  public JettySolrRunner createProxiedJetty(File solrHome, String dataDir,
                                     String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {

    JettySolrRunner jetty = new JettySolrRunner(solrHome.getPath(), context,
        0, solrConfigOverride, schemaOverride, false,
        getExtraServlets(), sslConfig, getExtraRequestFilters());
    jetty.setShards(shardList);
    jetty.setDataDir(getDataDir(dataDir));

    // setup to proxy Http requests to this server unless it is the control
    // server
    int proxyPort = getNextAvailablePort();
    jetty.setProxyPort(proxyPort);
    jetty.start();

    // create a socket proxy for the jetty server ...
    SocketProxy proxy = new SocketProxy(proxyPort, jetty.getBaseUrl().toURI());
    proxies.put(proxy.getUrl(), proxy);

    return jetty;
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
    String replicaBaseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
    assertNotNull(replicaBaseUrl);
    URL baseUrl = new URL(replicaBaseUrl);

    SocketProxy proxy = proxies.get(baseUrl.toURI());
    if (proxy == null && !baseUrl.toExternalForm().endsWith("/")) {
      baseUrl = new URL(baseUrl.toExternalForm() + "/");
      proxy = proxies.get(baseUrl.toURI());
    }
    assertNotNull("No proxy found for " + baseUrl + "!", proxy);
    return proxy;
  }

  protected int getNextAvailablePort() throws Exception {
    int port = -1;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    return port;
  }

  private File getRelativeSolrHomePath(File solrHome) {
    String path = SolrResourceLoader.normalizeDir(new File(".").getAbsolutePath());
    String base = new File(solrHome.getPath()).getAbsolutePath();
    
    if (base.startsWith(".")) {
      base = base.replaceFirst("\\.", new File(".").getName());
    }

    if (path.endsWith(File.separator + ".")) {
      path = path.substring(0, path.length() - 2);
    }
    
    int splits = path.split("\\" + File.separator).length;
    
    StringBuilder p = new StringBuilder();
    for (int i = 0; i < splits - 2; i++) {
      p.append("..").append(File.separator);
    }   
    
    String prefix = FilenameUtils.getPrefix(path);
    if (base.startsWith(prefix)) {
      base = base.substring(prefix.length());
    }

    solrHome = new File(p.toString() + base);
    return solrHome;
  }
  
  protected void updateMappingsFromZk(List<JettySolrRunner> jettys, List<SolrServer> clients) throws Exception {
    updateMappingsFromZk(jettys, clients, false);
  }
  
  protected void updateMappingsFromZk(List<JettySolrRunner> jettys, List<SolrServer> clients, boolean allowOverSharding) throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.updateClusterState(true);
    cloudJettys.clear();
    shardToJetty.clear();
    
    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection coll = clusterState.getCollection(DEFAULT_COLLECTION);

    List<CloudSolrServerClient> theClients = new ArrayList<>();
    for (SolrServer client : clients) {
      // find info for this client in zk 
      nextClient:
      // we find out state by simply matching ports...
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          int port = new URI(((HttpSolrServer) client).getBaseURL())
              .getPort();
          
          if (replica.getStr(ZkStateReader.BASE_URL_PROP).contains(":" + port)) {
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
          if (replica.getStr(ZkStateReader.BASE_URL_PROP).contains(":" + port)) {
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
            cjr.url = replica.getStr(ZkStateReader.BASE_URL_PROP) + "/" + replica.getStr(ZkStateReader.CORE_NAME_PROP);
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
        sb.append("shard" + (i + 1));
      }
      params.set("shards", sb.toString());
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
    
    HttpSolrServer client = (HttpSolrServer) clients
        .get(serverNumber);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
  }
  
  protected void index_specific(SolrServer client, Object... fields)
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
    super.waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, verbose, true, timeoutSeconds);
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
      for (int i = 0; i < shardCount; i++) {
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

    ArrayList<SolrServer> shardClients = new ArrayList<>(7);

    updateMappingsFromZk(jettys, clients);
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
    assertNotNull("no jetties found for shard: " + shard, solrJetties);


    for (CloudJettyRunner cjetty : solrJetties) {
      ZkNodeProps props = cjetty.info;
      String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      boolean active = props.getStr(ZkStateReader.STATE_PROP).equals(ZkStateReader.ACTIVE);
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
        zkStateReader.getClusterState().getSlice(DEFAULT_COLLECTION, shard)
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

      boolean active = props.getStr(ZkStateReader.STATE_PROP).equals(
          ZkStateReader.ACTIVE);
      if (active && live) {
        if (lastNum > -1 && lastNum != num && failMessage == null) {
          failMessage = shard + " is not consistent.  Got " + lastNum + " from " + lastJetty.url + "lastClient"
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
        } catch (SolrServerException e) {
          System.err.println("error contacting client: " + e.getMessage()
              + "\n");
          continue;
        } catch (SolrException e) {
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
    log.info("Turning on auto soft commit: " + time);
    for (List<CloudJettyRunner> jettyList : shardToJetty.values()) {
      for (CloudJettyRunner jetty : jettyList) {
        CoreContainer cores = ((SolrDispatchFilter) jetty.jetty
            .getDispatchFilter().getFilter()).getCores();
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

    updateMappingsFromZk(jettys, clients, true);
    
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
          SolrServer client = cjetty.client.solrClient;
          boolean active = props.getStr(ZkStateReader.STATE_PROP).equals(
              ZkStateReader.ACTIVE);
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
  
  protected SolrServer getClient(String nodeName) {
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
    ZkStateReader zk = new ZkStateReader(zkServer.getZkAddress(), 10000,
        AbstractZkTestCase.TIMEOUT);
    Map<String,Slice> slices = null;
    ClusterState clusterState;
    try {
      zk.createClusterStateWatchersAndUpdate();
      clusterState = zk.getClusterState();
      slices = clusterState.getSlicesMap(DEFAULT_COLLECTION);
    } finally {
      zk.close();
    }
    
    if (slices == null) {
      throw new RuntimeException("Could not find collection "
          + DEFAULT_COLLECTION + " in " + clusterState.getCollections());
    }
    
    for (CloudJettyRunner cjetty : cloudJettys) {
      CloudSolrServerClient client = cjetty.client;
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,Replica> theShards = slice.getValue().getReplicasMap();
        for (Map.Entry<String,Replica> shard : theShards.entrySet()) {
          String shardName = new URI(
              ((HttpSolrServer) client.solrClient).getBaseURL()).getPort()
              + "_solr_";
          if (verbose && shard.getKey().endsWith(shardName)) {
            System.err.println("shard:" + slice.getKey());
            System.err.println(shard.getValue());
          }
        }
      }
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      long count = 0;
      String currentState = cjetty.info.getStr(ZkStateReader.STATE_PROP);
      if (currentState != null
          && currentState.equals(ZkStateReader.ACTIVE)
          && zkStateReader.getClusterState().liveNodesContain(
              cjetty.info.getStr(ZkStateReader.NODE_NAME_PROP))) {
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
      throws SolrServerException {
    
    if (r.nextBoolean()) params.set("collection", DEFAULT_COLLECTION);
    
    QueryResponse rsp = cloudClient.query(params);
    return rsp;
  }
  
  static abstract class StopableThread extends Thread {
    public StopableThread(String name) {
      super(name);
    }
    public abstract void safeStop();
  }
  
  class StopableSearchThread extends StopableThread {
    private volatile boolean stop = false;
    protected final AtomicInteger queryFails = new AtomicInteger();
    private String[] QUERIES = new String[] {"to come","their country","aid","co*"};
    
    public StopableSearchThread() {
      super("StopableSearchThread");
      setDaemon(true);
    }
    
    @Override
    public void run() {
      Random random = random();
      int numSearches = 0;
      
      while (true && !stop) {
        numSearches++;
        try {
          //to come to the aid of their country.
          cloudClient.query(new SolrQuery(QUERIES[random.nextInt(QUERIES.length)]));
        } catch (Exception e) {
          System.err.println("QUERY REQUEST FAILED:");
          e.printStackTrace();
          if (e instanceof SolrServerException) {
            System.err.println("ROOT CAUSE:");
            ((SolrServerException) e).getRootCause().printStackTrace();
          }
          queryFails.incrementAndGet();
        }
        try {
          Thread.sleep(random.nextInt(4000) + 300);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      
      System.err.println("num searches done:" + numSearches + " with " + queryFails + " fails");
    }
    
    @Override
    public void safeStop() {
      stop = true;
    }
    
    public int getFails() {
      return queryFails.get();
    }
    
  };
  
  public void waitForThingsToLevelOut(int waitForRecTimeSeconds) throws Exception {
    log.info("Wait for recoveries to finish - wait " + waitForRecTimeSeconds + " for each attempt");
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
      if (cnt > 20) break;
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
        failMessage = checkShardConsistency(shard, true, false);
      }
      
      if (failMessage != null) {
        log.info("shard inconsistency - waiting ...");
        retry = true;
      } else {
        retry = false;
      }
      cnt++;
      if (cnt > 20) break;
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
  @After
  public void tearDown() throws Exception {
    if (VERBOSE || printLayoutOnTearDown) {
      super.printLayout();
    }
    if (commondCloudSolrServer != null) {
      commondCloudSolrServer.shutdown();
    }
    if (controlClient != null) {
      ((HttpSolrServer) controlClient).shutdown();
    }
    if (cloudClient != null) {
      cloudClient.shutdown();
    }
    if (controlClientCloud != null) {
      controlClientCloud.shutdown();
    }
    super.tearDown();
    
    System.clearProperty("zkHost");
    System.clearProperty("numShards");

    // close socket proxies after super.tearDown
    if (!proxies.isEmpty()) {
      for (SocketProxy proxy : proxies.values()) {
        proxy.close();
      }
    }
  }
  
  @Override
  protected void commit() throws Exception {
    controlClient.commit();
    cloudClient.commit();
  }
  
  @Override
  protected void destroyServers() throws Exception {
    if (controlJetty != null) {
      ChaosMonkey.stop(controlJetty);
    }
    for (JettySolrRunner jetty : jettys) {
      try {
        ChaosMonkey.stop(jetty);
      } catch (Exception e) {
        log.error("", e);
      }
    }
    clients.clear();
    jettys.clear();
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int replicationFactor, int maxShardsPerNode) throws SolrServerException, IOException {
    return createCollection(null, collectionName, numShards, replicationFactor, maxShardsPerNode, null, null);
  }

  protected CollectionAdminResponse createCollection(Map<String,List<Integer>> collectionInfos, String collectionName, Map<String,Object> collectionProps, SolrServer client)  throws SolrServerException, IOException{
    return createCollection(collectionInfos, collectionName, collectionProps, client, null);
  }

  // TODO: Use CollectionAdminRequest#createCollection() instead of a raw request
  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos, String collectionName, Map<String, Object> collectionProps, SolrServer client, String confSetName)  throws SolrServerException, IOException{
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
      if(entry.getValue() !=null) params.set(entry.getKey(), String.valueOf(entry.getValue()));
    }
    Integer numShards = (Integer) collectionProps.get(NUM_SLICES);
    if(numShards==null){
      String shardNames = (String) collectionProps.get(SHARDS_PROP);
      numShards = StrUtils.splitSmart(shardNames,',').size();
    }
    Integer replicationFactor = (Integer) collectionProps.get(REPLICATION_FACTOR);
    if(replicationFactor==null){
      replicationFactor = (Integer) OverseerCollectionProcessor.COLL_PROPS.get(REPLICATION_FACTOR);
    }

    if (confSetName != null) {
      params.set("collection.configName", confSetName);
    }

    int clientIndex = random().nextInt(2);
    List<Integer> list = new ArrayList<>();
    list.add(numShards);
    list.add(replicationFactor);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    if (getStateFormat() == 2) {
      log.info("Creating collection with stateFormat=2: " + collectionName);
      params.set(DocCollection.STATE_FORMAT, "2");
    }
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    if (client == null) {
      final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(clientIndex));
      SolrServer server = createNewSolrServer("", baseUrl);
      try {
        res.setResponse(server.request(request));
      } finally {
        if (server != null) server.shutdown();
      }
    } else {
      res.setResponse(client.request(request));
    }
    return res;
  }


  protected CollectionAdminResponse createCollection(Map<String,List<Integer>> collectionInfos,
      String collectionName, int numShards, int replicationFactor, int maxShardsPerNode, SolrServer client, String createNodeSetStr) throws SolrServerException, IOException {

    return createCollection(collectionInfos, collectionName,
        ZkNodeProps.makeMap(
        NUM_SLICES, numShards,
        REPLICATION_FACTOR, replicationFactor,
        CREATE_NODE_SET, createNodeSetStr,
        MAX_SHARDS_PER_NODE, maxShardsPerNode),
        client);
  }
  
  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos,
                                                     String collectionName, int numShards, int replicationFactor, int maxShardsPerNode, SolrServer client, String createNodeSetStr, String configName) throws SolrServerException, IOException {

    return createCollection(collectionInfos, collectionName,
        ZkNodeProps.makeMap(
        NUM_SLICES, numShards,
        REPLICATION_FACTOR, replicationFactor,
        CREATE_NODE_SET, createNodeSetStr,
        MAX_SHARDS_PER_NODE, maxShardsPerNode),
        client, configName);
  }

  @Override
  protected SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      String baseUrl = buildUrl(port);
      String url = baseUrl + (baseUrl.endsWith("/") ? "" : "/") + DEFAULT_COLLECTION;
      HttpSolrServer s = new HttpSolrServer(url);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      s.setSoTimeout(60000);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected SolrServer createNewSolrServer(String collection, String baseUrl) {
    try {
      // setup the server...
      HttpSolrServer s = new HttpSolrServer(baseUrl + "/" + collection);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected String getBaseUrl(HttpSolrServer client) {
    return client .getBaseURL().substring(
        0, client.getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
  }
  
  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }

  private String checkCollectionExpectations(String collectionName, List<Integer> numShardsNumReplicaList, List<String> nodesAllowedToRunShards) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    
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
    long timeoutAt = System.currentTimeMillis() + 120000;
    boolean success = false;
    String checkResult = "Didnt get to perform a single check";
    while (System.currentTimeMillis() < timeoutAt) {
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
  
  private CloudSolrServer commondCloudSolrServer;
  
  protected CloudSolrServer getCommonCloudSolrServer() {
    synchronized (this) {
      if (commondCloudSolrServer == null) {
        commondCloudSolrServer = new CloudSolrServer(zkServer.getZkAddress(),
            random().nextBoolean());
        commondCloudSolrServer.getLbServer().setConnectionTimeout(30000);
        commondCloudSolrServer.setParallelUpdates(random().nextBoolean());
        commondCloudSolrServer.setDefaultCollection(DEFAULT_COLLECTION);
        commondCloudSolrServer.connect();
      }
    }
    return commondCloudSolrServer;
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
        if (clusterState.liveNodesContain(node.getStr(ZkStateReader.NODE_NAME_PROP))) {
          return ZkCoreNodeProps.getCoreUrl(node.getStr(ZkStateReader.BASE_URL_PROP), collection); //new ZkCoreNodeProps(node).getCoreUrl();
        }
      }
    }

    throw new RuntimeException("Could not find a live node for collection:" + collection);
  }

 public  static void waitForNon403or404or503(HttpSolrServer collectionClient)
      throws Exception {
    SolrException exp = null;
    long timeoutAt = System.currentTimeMillis() + 30000;

    while (System.currentTimeMillis() < timeoutAt) {
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

  protected void checkForMissingCollection(String collectionName)
      throws Exception {
    // check for a  collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 45000;
    boolean found = true;
    while (System.currentTimeMillis() < timeoutAt) {
      getCommonCloudSolrServer().getZkStateReader().updateClusterState(true);
      ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
      if (!clusterState.hasCollection(collectionName)) {
        found = false;
        break;
      }
      Thread.sleep(100);
    }
    if (found) {
      fail("Found collection that should be gone " + collectionName);
    }
  }

  protected NamedList<Object> invokeCollectionApi(String... args) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrRequest request = new QueryRequest(params);
    for (int i = 0; i < args.length - 1; i+=2) {
      params.add(args[i], args[i+1]);
    }
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    baseServer.setSoTimeout(60000 * 5);
    NamedList r = baseServer.request(request);
    baseServer.shutdown();
    return r;
  }

  protected void createCollection(String collName,
                                  CloudSolrServer client,
                                  int replicationFactor ,
                                  int numShards ) throws Exception {
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrServer()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    Map<String, Object> props = makeMap(
        REPLICATION_FACTOR, replicationFactor,
        MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<String,List<Integer>>();
    createCollection(collectionInfos, collName, props, client);
  }

  protected List<Replica> ensureAllReplicasAreActive(String testCollectionName, String shardId, int shards, int rf, int maxWaitSecs) throws Exception {
    long startMs = System.currentTimeMillis();
    
    Map<String,Replica> notLeaders = new HashMap<String,Replica>();
    
    ZkStateReader zkr = cloudClient.getZkStateReader();
    zkr.updateClusterState(true); // force the state to be fresh

    ClusterState cs = zkr.getClusterState();
    Collection<Slice> slices = cs.getActiveSlices(testCollectionName);
    assertTrue(slices.size() == shards);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    Replica leader = null;
    while (waitMs < maxWaitMs && !allReplicasUp) {
      // refresh state every 2 secs
      if (waitMs % 2000 == 0)
        cloudClient.getZkStateReader().updateClusterState(true);
      
      cs = cloudClient.getZkStateReader().getClusterState();
      assertNotNull(cs);
      Slice shard = cs.getSlice(testCollectionName, shardId);
      assertNotNull("No Slice for "+shardId, shard);
      allReplicasUp = true; // assume true
      Collection<Replica> replicas = shard.getReplicas();
      assertTrue(replicas.size() == rf);
      leader = shard.getLeader();
      assertNotNull(leader);
      log.info("Found "+replicas.size()+" replicas and leader on "+
        leader.getNodeName()+" for "+shardId+" in "+testCollectionName);
      
      // ensure all replicas are "active" and identify the non-leader replica
      for (Replica replica : replicas) {
        String replicaState = replica.getStr(ZkStateReader.STATE_PROP);
        if (!ZkStateReader.ACTIVE.equals(replicaState)) {
          log.info("Replica " + replica.getName() + " is currently " + replicaState);
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
    
    if (!allReplicasUp) 
      fail("Didn't see all replicas for shard "+shardId+" in "+testCollectionName+
          " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo());
    
    if (notLeaders.isEmpty()) 
      fail("Didn't isolate any replicas that are not the leader! ClusterState: " + printClusterStateInfo());
    
    long diffMs = (System.currentTimeMillis() - startMs);
    log.info("Took " + diffMs + " ms to see all replicas become active.");
    
    List<Replica> replicas = new ArrayList<Replica>();
    replicas.addAll(notLeaders.values());
    return replicas;
  }  
  
  protected String printClusterStateInfo() throws Exception {
    return printClusterStateInfo(null);
  }

  protected String printClusterStateInfo(String collection) throws Exception {
    cloudClient.getZkStateReader().updateClusterState(true);
    String cs = null;
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    if (collection != null) {
      cs = clusterState.getCollection(collection).toString();
    } else {
      Map<String,DocCollection> map = new HashMap<String,DocCollection>();
      for (String coll : clusterState.getCollections())
        map.put(coll, clusterState.getCollection(coll));
      CharArr out = new CharArr();
      new JSONWriter(out, 2).write(map);
      cs = out.toString();
    }
    return cs;
  }
}
