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
package org.apache.solr.update;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.MockStreamingSolrClients.Exp;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.apache.solr.update.SolrCmdDistributor.ForwardNode;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.LeaderRequestReplicationTracker;
import org.apache.solr.update.processor.DistributedUpdateProcessor.RollupRequestReplicationTracker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class SolrCmdDistributorTest extends BaseDistributedSearchTestCase {
  
  private static enum NodeType {FORWARD, STANDARD};
  
  private AtomicInteger id = new AtomicInteger();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // we can't use the Randomized merge policy because the test depends on
    // being able to call optimize to have all deletes expunged.
    systemSetPropertySolrTestsMergePolicyFactory(LogDocMergePolicyFactory.class.getName());
  }

  @AfterClass
  public static void afterClass() {
    systemClearPropertySolrTestsMergePolicyFactory();
  }

  private UpdateShardHandler updateShardHandler;
  
  public SolrCmdDistributorTest() throws ParserConfigurationException, IOException, SAXException {
    updateShardHandler = new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);
    
    stress = 0;
  }

  public static String getSchemaFile() {
    return "schema.xml";
  }
  
  public static  String getSolrConfigFile() {
    // use this because it has /update and is minimal
    return "solrconfig-tlog.xml";
  }
  
  // TODO: for now we redefine this method so that it pulls from the above
  // we don't get helpful override behavior due to the method being static
  @Override
  protected void createServers(int numShards) throws Exception {

    System.setProperty("configSetBaseDir", TEST_HOME());

    File controlHome = testDir.toPath().resolve("control").toFile();

    seedSolrHome(controlHome);
    writeCoreProperties(controlHome.toPath().resolve("cores").resolve(DEFAULT_TEST_CORENAME), DEFAULT_TEST_CORENAME);
    controlJetty = createJetty(controlHome, testDir + "/control/data", null, getSolrConfigFile(), getSchemaFile());
    controlJetty.start();
    controlClient = createNewSolrClient(controlJetty.getLocalPort());

    shardsArr = new String[numShards];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      String shardname = "shard" + i;
      Path shardHome = testDir.toPath().resolve(shardname);
      seedSolrHome(shardHome.toFile());
      Path coresPath = shardHome.resolve("cores");
      writeCoreProperties(coresPath.resolve(DEFAULT_TEST_CORENAME), DEFAULT_TEST_CORENAME);
      JettySolrRunner j = createJetty(shardHome.toFile(),
          testDir + "/shard" + i + "/data", null, getSolrConfigFile(),
          getSchemaFile());
      j.start();
      jettys.add(j);
      clients.add(createNewSolrClient(j.getLocalPort()));
      String shardStr = buildUrl(j.getLocalPort());
      shardsArr[i] = shardStr;
      sb.append(shardStr);
    }

    shards = sb.toString();
  }

  @SuppressWarnings("unchecked")
  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    del("*:*");

    ModifiableSolrParams params = new ModifiableSolrParams();
    List<Node> nodes = new ArrayList<>();
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    List<Error> errors;
    CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
    long numFound;
    HttpSolrClient client;
    ZkNodeProps nodeProps;

    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler)) {

      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
          ((HttpSolrClient) controlClient).getBaseURL(),
          ZkStateReader.CORE_NAME_PROP, "");
      nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));

      // add one doc to controlClient
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      params = new ModifiableSolrParams();

      cmdDistrib.distribAdd(cmd, nodes, params);

      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      cmdDistrib.distribCommit(ccmd, nodes, params);
      cmdDistrib.finish();


      errors = cmdDistrib.getErrors();

      assertEquals(errors.toString(), 0, errors.size());

      numFound = controlClient.query(new SolrQuery("*:*")).getResults()
          .getNumFound();
      assertEquals(1, numFound);

      client = (HttpSolrClient) clients.get(0);
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
          client.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));
    }
    int id2;
    // add another 2 docs to control and 3 to client
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler)) {
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      cmdDistrib.distribAdd(cmd, nodes, params);

      id2 = id.incrementAndGet();
      AddUpdateCommand cmd2 = new AddUpdateCommand(null);
      cmd2.solrDoc = sdoc("id", id2);

      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      cmdDistrib.distribAdd(cmd2, nodes, params);

      AddUpdateCommand cmd3 = new AddUpdateCommand(null);
      cmd3.solrDoc = sdoc("id", id.incrementAndGet());

      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      cmdDistrib.distribAdd(cmd3, Collections.singletonList(nodes.get(1)), params);

      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      cmdDistrib.distribCommit(ccmd, nodes, params);
      cmdDistrib.finish();
      errors = cmdDistrib.getErrors();
    }
    assertEquals(errors.toString(), 0, errors.size());

    SolrDocumentList results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 3, numFound);

    numFound = client.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(3, numFound);

    // now delete doc 2 which is on both control and client1

    DeleteUpdateCommand dcmd = new DeleteUpdateCommand(null);
    dcmd.id = Integer.toString(id2);


    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler)) {

      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);

      cmdDistrib.distribDelete(dcmd, nodes, params);

      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);

      cmdDistrib.distribCommit(ccmd, nodes, params);
      cmdDistrib.finish();

      errors = cmdDistrib.getErrors();
    }

    assertEquals(errors.toString(), 0, errors.size());


    results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 2, numFound);

    numFound = client.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(results.toString(), 2, numFound);

    for (SolrClient c : clients) {
      c.optimize();
      //System.out.println(clients.get(0).request(new LukeRequest()));
    }

    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler)) {

      int cnt = atLeast(303);
      for (int i = 0; i < cnt; i++) {
        nodes.clear();
        for (SolrClient c : clients) {
          if (random().nextBoolean()) {
            continue;
          }
          HttpSolrClient httpClient = (HttpSolrClient) c;
          nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
              httpClient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
          nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));

        }
        AddUpdateCommand c = new AddUpdateCommand(null);
        c.solrDoc = sdoc("id", id.incrementAndGet());
        if (nodes.size() > 0) {
          params = new ModifiableSolrParams();
          cmdDistrib.distribAdd(c, nodes, params);
        }
      }

      nodes.clear();

      for (SolrClient c : clients) {
        HttpSolrClient httpClient = (HttpSolrClient) c;
        nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
            httpClient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");

        nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));
      }

      final AtomicInteger commits = new AtomicInteger();
      for (JettySolrRunner jetty : jettys) {
        CoreContainer cores = jetty.getCoreContainer();
        try (SolrCore core = cores.getCore("collection1")) {
          core.getUpdateHandler().registerCommitCallback(new SolrEventListener() {
            @Override
            public void init(NamedList args) {
            }

            @Override
            public void postSoftCommit() {
            }

            @Override
            public void postCommit() {
              commits.incrementAndGet();
            }

            @Override
            public void newSearcher(SolrIndexSearcher newSearcher,
                                    SolrIndexSearcher currentSearcher) {
            }
          });
        }
      }
      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);

      cmdDistrib.distribCommit(ccmd, nodes, params);

      cmdDistrib.finish();

      assertEquals(getShardCount(), commits.get());

      for (SolrClient c : clients) {
        NamedList<Object> resp = c.request(new LukeRequest());
        assertEquals("SOLR-3428: We only did adds - there should be no deletes",
            ((NamedList<Object>) resp.get("index")).get("numDocs"),
            ((NamedList<Object>) resp.get("index")).get("maxDoc"));
      }
    }
    
    testMaxRetries(NodeType.FORWARD);
    testMaxRetries(NodeType.STANDARD);
    testOneRetry(NodeType.FORWARD);
    testOneRetry(NodeType.STANDARD);
    testRetryNodeAgainstBadAddress();
    testStdNodeRetriesSocketError();
    testForwardNodeWontRetrySocketError();
    testNodeWontRetryBadRequest(NodeType.FORWARD);
    testNodeWontRetryBadRequest(NodeType.STANDARD);
    testMinRfOnRetries(NodeType.FORWARD);
    testMinRfOnRetries(NodeType.STANDARD);
    testDistribOpenSearcher();
    testReqShouldRetryNoRetries();
    testReqShouldRetryMaxRetries();
    testReqShouldRetryBadRequest();
    testReqShouldRetryNotFound();
    testReqShouldRetryDBQ();
    testDeletes(false, true);
    testDeletes(false, false);
    testDeletes(true, true);
    testDeletes(true, false);
    getRfFromResponseShouldNotCloseTheInputStream();
  }
  
  private void testDeletes(boolean dbq, boolean withFailures) throws Exception {
    final HttpSolrClient solrclient = (HttpSolrClient) clients.get(0);
    solrclient.commit(true, true);
    long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    final MockStreamingSolrClients streamingClients = new MockStreamingSolrClients(updateShardHandler);
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(streamingClients, 0)) {
      if (withFailures) {
        streamingClients.setExp(Exp.CONNECT_EXCEPTION);
      }
      ArrayList<Node> nodes = new ArrayList<>();

      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
          ZkStateReader.CORE_NAME_PROP, "");

      final AtomicInteger retries = new AtomicInteger();
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      Node retryNode = new StdNode(new ZkCoreNodeProps(nodeProps), "collection1", "shard1", 5) {
        @Override
        public boolean checkRetry(Error err) {
          streamingClients.setExp(null);
          retries.incrementAndGet();
          return super.checkRetry(err);
        }
      };


      nodes.add(retryNode);

      for (int i = 0 ; i < 5 ; i++) {
        AddUpdateCommand cmd = new AddUpdateCommand(null);
        int currentId = id.incrementAndGet();
        cmd.solrDoc = sdoc("id", currentId);
        ModifiableSolrParams params = new ModifiableSolrParams();
        cmdDistrib.distribAdd(cmd, nodes, params);
        DeleteUpdateCommand dcmd = new DeleteUpdateCommand(null);
        if (dbq) {
          dcmd.setQuery("id:" + currentId);
        } else {
          dcmd.setId(String.valueOf(currentId));
        }
        cmdDistrib.distribDelete(dcmd, nodes, params, false, null, null);
      }
      

      CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
      cmdDistrib.distribCommit(ccmd, nodes, new ModifiableSolrParams());
      cmdDistrib.finish();
      
      int expectedRetryCount = 0;
      if (withFailures) {
        if (dbq) {
          expectedRetryCount = 1; // just the first cmd would be retried
        } else {
          expectedRetryCount = 10;
        }
      }
      assertEquals(expectedRetryCount, retries.get());


      long numFoundAfter = solrclient.query(new SolrQuery("*:*")).getResults()
          .getNumFound();

      // we will get java.net.ConnectException which we retry on
      assertEquals(numFoundBefore, numFoundAfter);
      assertEquals(0, cmdDistrib.getErrors().size());
    }
  }

  private void testMinRfOnRetries(NodeType nodeType) throws Exception {
    final HttpSolrClient solrclient = (HttpSolrClient) clients.get(0);
    final MockStreamingSolrClients streamingClients = new MockStreamingSolrClients(updateShardHandler);
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(streamingClients, 0)) {
      streamingClients.setExp(Exp.CONNECT_EXCEPTION);
      ArrayList<Node> nodes = new ArrayList<>();

      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
          ZkStateReader.CORE_NAME_PROP, "");

      final AtomicInteger retries = new AtomicInteger();
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      if (nodeType == NodeType.FORWARD) {
        nodes.add(new ForwardNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", 5) {
          @Override
          public boolean checkRetry(Error err) {
            if (retries.incrementAndGet() >= 3) {
              streamingClients.setExp(null);
            }
            return super.checkRetry(err);
          }
        });
      } else {
        nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps), "collection1", "shard1", 5) {
          @Override
          public boolean checkRetry(Error err) {
            if (retries.incrementAndGet() >= 3) {
              streamingClients.setExp(null);
            }
            return super.checkRetry(err);
          }
        });
      }


      AddUpdateCommand cmd = new AddUpdateCommand(null);
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      ModifiableSolrParams params = new ModifiableSolrParams();
      RollupRequestReplicationTracker rollupReqTracker = new RollupRequestReplicationTracker();
      LeaderRequestReplicationTracker leaderReqTracker = new LeaderRequestReplicationTracker("shard1");

      cmdDistrib.distribAdd(cmd, nodes, params, false, rollupReqTracker, leaderReqTracker);
      cmdDistrib.finish();
      assertEquals(3, retries.get());
      assertEquals(2, leaderReqTracker.getAchievedRf());// "2" here is because one would be the leader, that creates the instance of LeaderRequestReplicationTracker, the second one is the node

      assertEquals(0, cmdDistrib.getErrors().size());
    }
  }

  private void testMaxRetries(NodeType nodeType) throws IOException {
    final MockStreamingSolrClients streamingClients = new MockStreamingSolrClients(updateShardHandler);
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(streamingClients, 0)) {
      streamingClients.setExp(Exp.CONNECT_EXCEPTION);
      ArrayList<Node> nodes = new ArrayList<>();
      final HttpSolrClient solrclient1 = (HttpSolrClient) clients.get(0);

      final AtomicInteger retries = new AtomicInteger();
      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient1.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      Node retryNode;
      if (nodeType == NodeType.FORWARD) {
        retryNode = new ForwardNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", 6) {
          @Override
          public boolean checkRetry(Error err) {
            retries.incrementAndGet();
            return super.checkRetry(err);
          }
        };
      } else {
        retryNode = new StdNode(new ZkCoreNodeProps(nodeProps), "collection1", "shard1", 6) {
          @Override
          public boolean checkRetry(Error err) {
            retries.incrementAndGet();
            return super.checkRetry(err);
          }
        };
      }
      

      nodes.add(retryNode);

      AddUpdateCommand cmd = new AddUpdateCommand(null);
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      ModifiableSolrParams params = new ModifiableSolrParams();

      cmdDistrib.distribAdd(cmd, nodes, params);
      cmdDistrib.finish();

      assertEquals(7, retries.get());

      assertEquals(1, cmdDistrib.getErrors().size());
    }
  }
  
  private void testReqShouldRetryNoRetries() {
    Error err = getError(new SocketException()); 
    SolrCmdDistributor.Req req = new SolrCmdDistributor.Req(null, new StdNode(null, "collection1", "shard1", 0), new UpdateRequest(), true);
    assertFalse(req.shouldRetry(err));
  }
  
  private void testReqShouldRetryDBQ() {
    Error err = getError(new SocketException()); 
    UpdateRequest dbqReq = new UpdateRequest();
    dbqReq.deleteByQuery("*:*");
    SolrCmdDistributor.Req req = new SolrCmdDistributor.Req(null, new StdNode(null, "collection1", "shard1", 1), dbqReq, true);
    assertFalse(req.shouldRetry(err));
  }

  public void getRfFromResponseShouldNotCloseTheInputStream() {
    UpdateRequest dbqReq = new UpdateRequest();
    dbqReq.deleteByQuery("*:*");
    SolrCmdDistributor.Req req = new SolrCmdDistributor.Req(null, new StdNode(null, "collection1", "shard1", 1), dbqReq, true);
    AtomicBoolean isClosed = new AtomicBoolean(false);
    ByteArrayInputStream is = new ByteArrayInputStream(new byte[100]) {
      @Override
      public void close() throws IOException {
        isClosed.set(true);
        super.close();
      }
    };
    req.trackRequestResult(null, is, true);
    assertFalse("Underlying stream should not be closed!", isClosed.get());
  }
  
  private void testReqShouldRetryMaxRetries() {
    Error err = getError(new SocketException()); 
    SolrCmdDistributor.Req req = new SolrCmdDistributor.Req(null, new StdNode(null, "collection1", "shard1", 1), new UpdateRequest(), true);
    assertTrue(req.shouldRetry(err));
    req.retries++;
    assertFalse(req.shouldRetry(err));
  }
  
  private void testReqShouldRetryBadRequest() {
    Error err = getError(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "bad request")); 
    SolrCmdDistributor.Req req = new SolrCmdDistributor.Req(null, new StdNode(null, "collection1", "shard1", 1), new UpdateRequest(), true);
    assertFalse(req.shouldRetry(err));
  }
  
  private void testReqShouldRetryNotFound() {
    Error err = getError(new SolrException(SolrException.ErrorCode.NOT_FOUND, "not found"));
    SolrCmdDistributor.Req req = new SolrCmdDistributor.Req(null, new StdNode(null, "collection1", "shard1", 1), new UpdateRequest(), true);
    assertTrue(req.shouldRetry(err));
  }
  
  private Error getError(Exception e) {
    Error err = new Error();
    err.e = e;
    if (e instanceof SolrException) {
      err.statusCode = ((SolrException)e).code();
    }
    return err;
  }
  
  private void testOneRetry(NodeType nodeType) throws Exception {
    final HttpSolrClient solrclient = (HttpSolrClient) clients.get(0);
    long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    final MockStreamingSolrClients streamingClients = new MockStreamingSolrClients(updateShardHandler);
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(streamingClients, 0)) {
      streamingClients.setExp(Exp.CONNECT_EXCEPTION);
      ArrayList<Node> nodes = new ArrayList<>();

      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
          ZkStateReader.CORE_NAME_PROP, "");

      final AtomicInteger retries = new AtomicInteger();
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      Node retryNode;
      if (nodeType == NodeType.FORWARD) {
        retryNode = new ForwardNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", 5) {
          @Override
          public boolean checkRetry(Error err) {
            streamingClients.setExp(null);
            retries.incrementAndGet();
            return super.checkRetry(err);
          }
        };
      } else {
        retryNode = new StdNode(new ZkCoreNodeProps(nodeProps), "collection1", "shard1", 5) {
          @Override
          public boolean checkRetry(Error err) {
            streamingClients.setExp(null);
            retries.incrementAndGet();
            return super.checkRetry(err);
          }
        };
      }


      nodes.add(retryNode);

      AddUpdateCommand cmd = new AddUpdateCommand(null);
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      ModifiableSolrParams params = new ModifiableSolrParams();

      CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
      cmdDistrib.distribAdd(cmd, nodes, params);
      cmdDistrib.distribCommit(ccmd, nodes, params);
      cmdDistrib.finish();

      assertEquals(1, retries.get());


      long numFoundAfter = solrclient.query(new SolrQuery("*:*")).getResults()
          .getNumFound();

      // we will get java.net.ConnectException which we retry on
      assertEquals(numFoundBefore + 1, numFoundAfter);
      assertEquals(0, cmdDistrib.getErrors().size());
    }
  }

  private void testNodeWontRetryBadRequest(NodeType nodeType) throws Exception {
    ignoreException("Bad Request");
    final HttpSolrClient solrclient = (HttpSolrClient) clients.get(0);
    long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    final MockStreamingSolrClients streamingClients = new MockStreamingSolrClients(updateShardHandler);
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(streamingClients, 0)) {
      streamingClients.setExp(Exp.BAD_REQUEST);
      ArrayList<Node> nodes = new ArrayList<>();
      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
          ZkStateReader.CORE_NAME_PROP, "");

      final AtomicInteger retries = new AtomicInteger();
      Node retryNode;
      if (nodeType == NodeType.FORWARD) {
        retryNode = new ForwardNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", 5) {
          @Override
          public boolean checkRetry(Error err) {
            retries.incrementAndGet();
            return super.checkRetry(err);
          }
        };
      } else {
        retryNode = new StdNode(new ZkCoreNodeProps(nodeProps), "collection1", "shard1", 5) {
          @Override
          public boolean checkRetry(Error err) {
            retries.incrementAndGet();
            return super.checkRetry(err);
          }
        };
      }
      nodes.add(retryNode);

      AddUpdateCommand cmd = new AddUpdateCommand(null);
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      ModifiableSolrParams params = new ModifiableSolrParams();

      CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
      cmdDistrib.distribAdd(cmd, nodes, params);

      streamingClients.setExp(null);
      cmdDistrib.distribCommit(ccmd, nodes, params);
      cmdDistrib.finish();

      // it will checkRetry, but not actually do it...
      assertEquals(1, retries.get());


      long numFoundAfter = solrclient.query(new SolrQuery("*:*")).getResults()
          .getNumFound();

      // we will get java.net.SocketException: Network is unreachable, which we don't retry on
      assertEquals(numFoundBefore, numFoundAfter);
      assertEquals(1, cmdDistrib.getErrors().size());
      unIgnoreException("Bad Request");
    }
  }
  
  private void testForwardNodeWontRetrySocketError() throws Exception {
    final HttpSolrClient solrclient = (HttpSolrClient) clients.get(0);
    long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    final MockStreamingSolrClients streamingClients = new MockStreamingSolrClients(updateShardHandler);
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(streamingClients, 0)) {
      streamingClients.setExp(Exp.SOCKET_EXCEPTION);
      ArrayList<Node> nodes = new ArrayList<>();

      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
          ZkStateReader.CORE_NAME_PROP, "");

      final AtomicInteger retries = new AtomicInteger();
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      ForwardNode retryNode = new ForwardNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", 5) {
        @Override
        public boolean checkRetry(Error err) {
          retries.incrementAndGet();
          return super.checkRetry(err);
        }
      };


      nodes.add(retryNode);

      AddUpdateCommand cmd = new AddUpdateCommand(null);
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      ModifiableSolrParams params = new ModifiableSolrParams();

      CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
      cmdDistrib.distribAdd(cmd, nodes, params);

      streamingClients.setExp(null);
      cmdDistrib.distribCommit(ccmd, nodes, params);
      cmdDistrib.finish();

      // it will checkRetry, but not actually do it...
      assertEquals(1, retries.get());


      long numFoundAfter = solrclient.query(new SolrQuery("*:*")).getResults()
          .getNumFound();

      // we will get java.net.SocketException: Network is unreachable, which we don't retry on
      assertEquals(numFoundBefore, numFoundAfter);
      assertEquals(1, cmdDistrib.getErrors().size());
    }
  }
  
  private void testStdNodeRetriesSocketError() throws Exception {
    final HttpSolrClient solrclient = (HttpSolrClient) clients.get(0);
    final MockStreamingSolrClients streamingClients = new MockStreamingSolrClients(updateShardHandler);
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(streamingClients, 0)) {
      streamingClients.setExp(Exp.SOCKET_EXCEPTION);
      ArrayList<Node> nodes = new ArrayList<>();

      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
          ZkStateReader.CORE_NAME_PROP, "");

      final AtomicInteger retries = new AtomicInteger();
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      Node retryNode = new StdNode(new ZkCoreNodeProps(nodeProps), "collection1", "shard1", 5) {
        @Override
        public boolean checkRetry(Error err) {
          retries.incrementAndGet();
          return super.checkRetry(err);
        }
      };


      nodes.add(retryNode);

      AddUpdateCommand cmd = new AddUpdateCommand(null);
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      ModifiableSolrParams params = new ModifiableSolrParams();

      cmdDistrib.distribAdd(cmd, nodes, params);
      cmdDistrib.finish();

      // it will checkRetry, but not actually do it...
      assertEquals(6, retries.get());
    }
  }

  private void testRetryNodeAgainstBadAddress() throws SolrServerException, IOException {
    // Test RetryNode
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler)) {
      final HttpSolrClient solrclient = (HttpSolrClient) clients.get(0);
      long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
          .getNumFound();

      ArrayList<Node> nodes = new ArrayList<>();

      ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, "[ff01::114]:33332" + context, ZkStateReader.CORE_NAME_PROP, "");
      ForwardNode retryNode = new ForwardNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", 5) {
        @Override
        public boolean checkRetry(Error err) {
          ZkNodeProps leaderProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
              ZkStateReader.CORE_NAME_PROP, "");
          this.nodeProps = new ZkCoreNodeProps(leaderProps);

          return super.checkRetry(err);
        }
      };


      nodes.add(retryNode);


      AddUpdateCommand cmd = new AddUpdateCommand(null);
      cmd.solrDoc = sdoc("id", id.incrementAndGet());
      ModifiableSolrParams params = new ModifiableSolrParams();

      cmdDistrib.distribAdd(cmd, nodes, params);

      CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
      params = new ModifiableSolrParams();
      params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      cmdDistrib.distribCommit(ccmd, nodes, params);
      cmdDistrib.finish();

      long numFoundAfter = solrclient.query(new SolrQuery("*:*")).getResults()
          .getNumFound();

      // different OS's will throw different exceptions for the bad address above
      if (numFoundBefore != numFoundAfter) {
        assertEquals(0, cmdDistrib.getErrors().size());
        assertEquals(numFoundBefore + 1, numFoundAfter);
      } else {
        // we will get java.net.SocketException: Network is unreachable and not retry
        assertEquals(numFoundBefore, numFoundAfter);

        assertEquals(1, cmdDistrib.getErrors().size());
      }
    }
  }
  
  @Override
  public void distribTearDown() throws Exception {
    updateShardHandler.close();
    super.distribTearDown();
  }

  private void testDistribOpenSearcher() {
    try (SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler)) {
      UpdateRequest updateRequest = new UpdateRequest();

      CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);

      //test default value (should be true)
      cmdDistrib.addCommit(updateRequest, ccmd);
      boolean openSearcher = updateRequest.getParams().getBool(UpdateParams.OPEN_SEARCHER, false);
      assertTrue(openSearcher);

      //test openSearcher = false
      ccmd.openSearcher = false;

      cmdDistrib.addCommit(updateRequest, ccmd);
      openSearcher = updateRequest.getParams().getBool(UpdateParams.OPEN_SEARCHER, true);
      assertFalse(openSearcher);
    }
  }
}
