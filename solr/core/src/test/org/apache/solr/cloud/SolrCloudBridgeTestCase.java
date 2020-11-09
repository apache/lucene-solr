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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.RestTestHarness;
import org.apache.zookeeper.CreateMode;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SolrCloudBridgeTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static String COLLECTION = "collection1";
  protected static String  DEFAULT_COLLECTION = COLLECTION;

  protected static CloudHttp2SolrClient cloudClient;
  
  protected static final String SHARD1 = "s1";
  
  protected String id = "id";

  private static final List<SolrClient> newClients = Collections.synchronizedList(new ArrayList<>());
  
  protected Map<String, Integer> handle = new ConcurrentHashMap<>();
  
  private static final List<RestTestHarness> restTestHarnesses = Collections.synchronizedList(new ArrayList<>());
  
  public final static int ORDERED = 1;
  public final static int SKIP = 2;
  public final static int SKIPVAL = 4;
  public final static int UNORDERED = 8;

  String t1="a_t";
  String i1="a_i1";
  String tlong = "other_tl1";
  String tsort="t_sortable";

  String oddField="oddField_s";
  String missingField="ignore_exception__missing_but_valid_field_t";

  public static RandVal rdate = new RandDate();
  
  protected static String[] fieldNames = new String[]{"n_ti1", "n_f1", "n_tf1", "n_d1", "n_td1", "n_l1", "n_tl1", "n_dt1", "n_tdt1"};
  
  protected static int numJettys = 3;
  
  protected static int sliceCount = 2;
  
  protected static int replicationFactor = 1;

  protected static boolean enableProxy = false;
  
  protected final List<SolrClient> clients = Collections.synchronizedList(new ArrayList<>());
  protected volatile static boolean createCollection1 = true;
  protected volatile static boolean createControl;
  protected volatile static CloudHttp2SolrClient controlClient;
  protected volatile static MiniSolrCloudCluster controlCluster;
  protected volatile static String schemaString;
  protected volatile static String solrconfigString;
  protected volatile static String solrxmlString = "solr.xml";
  protected volatile static boolean uploadSelectCollection1Config = false;
  protected volatile static boolean formatZk = true;

  protected volatile static SortedMap<ServletHolder, String> extraServlets = Collections.emptySortedMap();

  Pattern filenameExclusions = Pattern.compile(".*solrconfig(?:-|_).*?\\.xml|.*schema(?:-|_).*?\\.xml");
  
  public static Path TEST_PATH() { return SolrTestCaseJ4.getFile("solr/collection1").getParentFile().toPath(); }
  
  @Before
  public void beforeSolrCloudBridgeTestCase() throws Exception {

    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    
    cluster = configureCluster(numJettys).formatZk(formatZk).withSolrXml(TEST_PATH().resolve(solrxmlString)).withJettyConfig(jettyCfg -> jettyCfg.withServlets(extraServlets).enableProxy(enableProxy)).build();
    
    SolrZkClient zkClient = cluster.getZkClient();

    if (!zkClient.exists("/configs/_default")) {
      zkClient.uploadToZK(Paths.get(TEST_HOME()).resolve("collection1").resolve("conf"), "/configs" + "/" + "_default", filenameExclusions);
    }
    
    if (schemaString != null) {
      if (zkClient.exists("/configs/_default/schema.xml")) {
        zkClient.setData("/configs/_default/schema.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile(), true);
      } else {
        byte[] data = FileUtils.readFileToByteArray(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile());
        zkClient.create("/configs/_default/schema.xml", data, CreateMode.PERSISTENT, true);
      }

      if (zkClient.exists("/configs/_default/managed-schema")) {
        byte[] data = FileUtils.readFileToByteArray(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile());
        zkClient.setData("/configs/_default/managed-schema", data, true);
      }
    }
    if (solrconfigString != null) {
      //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString), "/configs/_default, null);
      zkClient.setData("/configs/_default/solrconfig.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString).toFile(), true);
    }

    if (uploadSelectCollection1Config) {
      zkClient.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve("solrconfig.snippet.randomindexconfig.xml"),
          "/configs/_default/solrconfig.snippet.randomindexconfig.xml", null);
      zkClient.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve("enumsConfig.xml"),
          "/configs/_default/enumsConfig.xml", null);
      zkClient.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve("currency.xml"),
          "/configs/_default/currency.xml", null);
      zkClient.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve("old_synonyms.txt"),
          "/configs/_default/old_synonyms.txt", null);
      zkClient.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve("open-exchange-rates.json"),
          "/configs/_default/open-exchange-rates.json", null);
      zkClient.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve("mapping-ISOLatin1Accent.txt"),
          "/configs/_default/mapping-ISOLatin1Accent.txt", null);

    }

    if (createCollection1) {
      CollectionAdminRequest.createCollection(COLLECTION, "_default", sliceCount, replicationFactor).setMaxShardsPerNode(10).process(cluster.getSolrClient());
    }

    cloudClient = cluster.getSolrClient();

    if (createCollection1) {
      cloudClient.setDefaultCollection(COLLECTION);
    }
    
    
    for (int i =0;i < cluster.getJettySolrRunners().size(); i++) {
      clients.add(getClient(i));
    }
    
    if (createControl) {
      controlCluster = configureCluster(1).withSolrXml(TEST_PATH().resolve(solrxmlString)).formatZk(formatZk).build();
      
      SolrZkClient zkClientControl = controlCluster.getZkClient();
      
      zkClientControl.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf"), "/configs" + "/" + "_default", filenameExclusions);

      if (schemaString != null) {
        //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString), "/configs/_default", null);
        
        zkClientControl.setData("/configs/_default/schema.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile(), true);
        byte[] data = FileUtils.readFileToByteArray(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile());
      }
      if (solrconfigString != null) {
        //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString), "/configs/_default", null);
        zkClientControl.setData("/configs/_default/solrconfig.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString).toFile(), true);
      }
      CollectionAdminRequest.createCollection(COLLECTION, "_default", 1, 1)
          .setMaxShardsPerNode(10)
          .process(controlCluster.getSolrClient());

      controlClient = controlCluster.getSolrClient();
      controlClient.setDefaultCollection(COLLECTION);
    }
  }
  
  @After
  public void cleanup() throws Exception {
    if (cluster != null) cluster.shutdown();
    if (controlCluster != null) controlCluster.shutdown();
    cluster = null;
    controlCluster = null;
    synchronized (clients) {
      for (SolrClient client : clients) {
        client.close();
      }
    }
    clients.clear();
  }
  
  
  @AfterClass
  public static void afterSolrCloudBridgeTestCase() throws Exception {
    closeRestTestHarnesses();
    synchronized (newClients) {
      for (SolrClient client : newClients) {
        client.close();
      }
    }
    newClients.clear();
    createCollection1 = true;
    createControl = false;
    solrconfigString = null;
    schemaString = null;
    solrxmlString = "solr.xml";
    numJettys = 3;
    formatZk = true;
    uploadSelectCollection1Config = false;
    sliceCount = 2;
    replicationFactor = 1;
    enableProxy = false;
  }
  
  protected String getBaseUrl(HttpSolrClient client) {
    return client .getBaseURL().substring(
        0, client.getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
  }

  protected String getBaseUrl(Http2SolrClient client) {
    return client .getBaseURL().substring(
        0, client.getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
  }

  protected String getShardsString() {
    StringBuilder sb = new StringBuilder();
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      if (sb.length() > 0) sb.append(',');
      sb.append(runner.getBaseUrl() + "/" + DEFAULT_COLLECTION);
    }

    return sb.toString();
  }
  
  public HttpSolrClient getClient(int i) {
    return getClient(DEFAULT_COLLECTION, i);
  }
  
  public HttpSolrClient getClient(String collection, int i) {
    String baseUrl = cluster.getJettySolrRunner(i).getBaseUrl().toString() + "/" + collection;
    HttpSolrClient client = new HttpSolrClient.Builder(baseUrl)
        .withConnectionTimeout(15000)
        .withSocketTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    newClients.add(client);
    return client;
  }

  public HttpSolrClient getClientByNode(String collection, String node) {
    ClusterState cs = cluster.getSolrClient().getZkStateReader().getClusterState();
    DocCollection coll = cs.getCollection(collection);
    List<Replica> replicas = coll.getReplicas();
    for (Replica replica : replicas) {
      if (replica.getNodeName().equals(node)) {
        String baseUrl = replica.getBaseUrl() + "/" + collection;
        HttpSolrClient client = new HttpSolrClient.Builder(baseUrl)
            .withConnectionTimeout(15000)
            .withSocketTimeout(Integer.getInteger("socketTimeout", 30000))
            .build();
        newClients.add(client);
        return client;
      }
    }

    throw new IllegalArgumentException("Could not find replica with nodename=" + node);
  }
  
  public HttpSolrClient getClient(String collection, String url) {
    String baseUrl = url + "/" + collection;
    HttpSolrClient client = new HttpSolrClient.Builder(baseUrl)
        .withConnectionTimeout(15000)
        .withSocketTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    newClients.add(client);
    return client;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "_default", numShards, numReplicas)
        .setMaxShardsPerNode(10)
        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr, String routerField) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "_default", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setRouterField(routerField)
        .setCreateNodeSet(createNodeSetStr)
        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr, String routerField, String conf) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, conf, numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setRouterField(routerField)

        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "_default", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setCreateNodeSet(createNodeSetStr)
        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected void index(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    indexDoc(doc);
  }
  
  protected void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);

    SolrClient client = clients.get(serverNumber);
    client.add(doc);
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

  protected int getReplicaPort(Replica replica) {
    String replicaNode = replica.getNodeName();
    String tmp = replicaNode.substring(replicaNode.indexOf(':')+1);
    if (tmp.indexOf('_') != -1)
      tmp = tmp.substring(0,tmp.indexOf('_'));
    return Integer.parseInt(tmp);
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
      Thread.sleep(300);
    }
    assertNotNull("Could not find active leader for " + shardId + " of " +
        testCollectionName + " after "+timeoutSecs+" secs;", leader);

    return leader;
  }
  
  protected JettySolrRunner getJettyOnPort(int port) {
    JettySolrRunner theJetty = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (port == jetty.getLocalPort()) {
        theJetty = jetty;
        break;
      }
    }

    if (createControl) {
      if (theJetty == null) {
        if (controlCluster.getJettySolrRunner(0).getLocalPort() == port) {
          theJetty = controlCluster.getJettySolrRunner(0);
        }
      }
    }
    if (theJetty == null)
      fail("Not able to find JettySolrRunner for port: "+port);

    return theJetty;
  }
  
  public static void commit() throws SolrServerException, IOException {
    if (controlClient != null) controlClient.commit();
    cloudClient.commit();
  }
  
  protected int getShardCount() {
    return numJettys;
  }
  
  public static abstract class RandVal {
    public static Set uniqueValues = new HashSet();

    public abstract Object val();

    public Object uval() {
      for (; ;) {
        Object v = val();
        if (uniqueValues.add(v)) return v;
      }
    }
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


  protected void setDistributedParams(ModifiableSolrParams params) {
    params.set("shards", getShardsString());
  }
  
  protected QueryResponse query(SolrParams p) throws Exception {
    return query(true, p);
  }
  
  protected QueryResponse query(boolean setDistribParams, SolrParams p) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams(p);

    // TODO: look into why passing true causes fails
    //params.set("distrib", "false");
    //final QueryResponse controlRsp = controlClient.query(params);
    //validateControlData(controlRsp);

    //params.remove("distrib");
    if (setDistribParams) setDistributedParams(params);

    QueryResponse rsp = queryServer(params);

    //compareResponses(rsp, controlRsp);

    return rsp;
  }
  
  protected QueryResponse query(boolean setDistribParams, Object[] q) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    return query(setDistribParams, params);
  }
  
  protected QueryResponse queryServer(ModifiableSolrParams params) throws Exception {
    return cloudClient.query(params);
  }
  
  protected QueryResponse query(Object... q) throws Exception {
    return query(true, q);
  }
  
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    addRandFields(doc);
    indexDoc(doc);
  }
  
  protected UpdateResponse indexDoc(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateResponse specificRsp = add(cloudClient, params, sdocs);
    return specificRsp;
  }

  protected UpdateResponse add(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(new ModifiableSolrParams(params));
    for (SolrInputDocument sdoc : sdocs) {
      ureq.add(sdoc);
    }
    return ureq.process(client);
  }
  
  protected static void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }

  public static Object[] getRandFields(String[] fields, RandVal[] randVals) {
    Object[] o = new Object[fields.length * 2];
    for (int i = 0; i < fields.length; i++) {
      o[i * 2] = fields[i];
      o[i * 2 + 1] = randVals[i].uval();
    }
    return o;
  }
  
  protected SolrInputDocument addRandFields(SolrInputDocument sdoc) {
    addFields(sdoc, getRandFields(fieldNames, randVals));
    return sdoc;
  }
  
  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }
  
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    if (controlClient != null) controlClient.add(doc);
    cloudClient.add(doc);
  }
  
  protected void del(String query) throws SolrServerException, IOException {
    if (controlClient != null) controlClient.deleteByQuery(query);
    cloudClient.deleteByQuery(query);
  }

  protected void waitForRecoveriesToFinish(String collectionName) throws InterruptedException, TimeoutException {
    cloudClient.getZkStateReader().waitForState(collectionName, 30, TimeUnit.SECONDS, new AllActive());
  }
  
  protected void waitForRecoveriesToFinish() throws InterruptedException, TimeoutException {
    waitForRecoveriesToFinish(DEFAULT_COLLECTION);
  }
  
  protected Replica getLeaderUrlFromZk(String collection, String slice) {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Replica leader = clusterState.getCollection(collection).getLeader(slice);
    if (leader == null) {
      throw new RuntimeException("Could not find leader:" + collection + " " + slice);
    }
    return leader;
  }
  
  /**
   * Create a collection in single node
   */
  protected void createCollectionInOneInstance(final SolrClient client, String nodeName,
                                               ExecutorService executor, final String collection,
                                               final int numShards, int numReplicas) {
    assertNotNull(nodeName);
    try {
      assertEquals(0, CollectionAdminRequest.createCollection(collection, "_default", numShards, 1)
          .setCreateNodeSet("")
          .process(client).getStatus());
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < numReplicas; i++) {
      final int freezeI = i;
      executor.execute(() -> {
        try {
          CollectionAdminRequest.addReplicaToShard(collection, "shard"+((freezeI%numShards)+1))
              .setCoreName(collection + freezeI)
              .setNode(nodeName).process(client);
        } catch (SolrServerException | IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
  
  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {

    String coreName = replica.getName();
    boolean reloadedOk = false;
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      long leaderCoreStartTime = statusResp.getStartTime(coreName).getTime();

      // send reload command for the collection
      log.info("Sending RELOAD command for "+testCollectionName);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.RELOAD.toString());
      params.set("name", testCollectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

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
  
  protected void setupRestTestHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(() -> ((HttpSolrClient) client).getBaseURL(), cluster.getSolrClient().getHttpClient());
      restTestHarnesses.add(harness);
    }
  }

  protected static void closeRestTestHarnesses() throws IOException {
    synchronized (restTestHarnesses) {
      for (RestTestHarness h : restTestHarnesses) {
        h.close();
      }
      restTestHarnesses.clear();
    }
  }

  protected static RestTestHarness randomRestTestHarness() {
    return restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
  }

  protected static RestTestHarness randomRestTestHarness(Random random) {
    return restTestHarnesses.get(random.nextInt(restTestHarnesses.size()));
  }

  protected static void forAllRestTestHarnesses(Consumer<RestTestHarness> op) {
    restTestHarnesses.forEach(op);
  }
  
  public static class AllActive implements CollectionStatePredicate {

    @Override
    public boolean matches(Set<String> liveNodes, DocCollection coll) {
      if (coll == null) return false;
      Collection<Slice> slices = coll.getActiveSlices();
      if (slices == null) return false;
      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          if (!replica.getState().equals(State.ACTIVE)) return false;
        }
      }

      return true;
    }
    
  }

  public static RandVal rint = new RandVal() {
    @Override
    public Object val() {
      return random().nextInt();
    }
  };

  public static RandVal rlong = new RandVal() {
    @Override
    public Object val() {
      return random().nextLong();
    }
  };

  public static RandVal rfloat = new RandVal() {
    @Override
    public Object val() {
      return random().nextFloat();
    }
  };

  public static RandVal rdouble = new RandVal() {
    @Override
    public Object val() {
      return random().nextDouble();
    }
  };
  
  public static class RandDate extends RandVal {
    @Override
    public Object val() {
      long v = random().nextLong();
      Date d = new Date(v);
      return d.toInstant().toString();
    }
  }
  
  protected static RandVal[] randVals = new RandVal[]{rint, rfloat, rfloat, rdouble, rdouble, rlong, rlong, rdate, rdate};
}