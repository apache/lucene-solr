package org.apache.solr.update;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoresLocator;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.MockStreamingSolrServers.Exp;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.RetryNode;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.BeforeClass;
import org.xml.sax.SAXException;

public class SolrCmdDistributorTest extends BaseDistributedSearchTestCase {
  
  private AtomicInteger id = new AtomicInteger();
  
  @BeforeClass
  public static void beforeClass() throws Exception {

    // we can't use the Randomized merge policy because the test depends on
    // being able to call optimize to have all deletes expunged.
    System.setProperty("solr.tests.mergePolicy", LogDocMergePolicy.class.getName());
  }
  private UpdateShardHandler updateShardHandler;
  
  public SolrCmdDistributorTest() throws ParserConfigurationException, IOException, SAXException {
    updateShardHandler = new UpdateShardHandler(new ConfigSolr() {

      @Override
      public CoresLocator getCoresLocator() {
        return null;
      }

      @Override
      protected String getShardHandlerFactoryConfigPath() {
        return null;
      }

      @Override
      public boolean isPersistent() {
        return false;
      }});
    
    fixShardCount = true;
    shardCount = 4;
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
    controlJetty = createJetty(new File(getSolrHome()), testDir + "/control/data", null, getSolrConfigFile(), getSchemaFile());

    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    shardsArr = new String[numShards];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner j = createJetty(new File(getSolrHome()),
          testDir + "/shard" + i + "/data", null, getSolrConfigFile(),
          getSchemaFile());
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      String shardStr = "127.0.0.1:" + j.getLocalPort() + context;
      shardsArr[i] = shardStr;
      sb.append(shardStr);
    }

    shards = sb.toString();
  }
  
  @Override
  public void doTest() throws Exception {
    del("*:*");
    
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler);
    
    ModifiableSolrParams params = new ModifiableSolrParams();

    List<Node> nodes = new ArrayList<Node>();

    ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        ((HttpSolrServer) controlClient).getBaseURL(),
        ZkStateReader.CORE_NAME_PROP, "");
    nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));

    // add one doc to controlClient
    
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = sdoc("id", id.incrementAndGet());
    params = new ModifiableSolrParams();

    cmdDistrib.distribAdd(cmd, nodes, params);
    
    CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();

    
    List<Error> errors = cmdDistrib.getErrors();
    
    assertEquals(errors.toString(), 0, errors.size());
    
    long numFound = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(1, numFound);
    
    HttpSolrServer client = (HttpSolrServer) clients.get(0);
    nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        client.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
    nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));
    
    // add another 2 docs to control and 3 to client
    cmdDistrib = new SolrCmdDistributor(updateShardHandler);
    cmd.solrDoc = sdoc("id", id.incrementAndGet());
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    cmdDistrib.distribAdd(cmd, nodes, params);
    
    int id2 = id.incrementAndGet();
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
    

    cmdDistrib = new SolrCmdDistributor(updateShardHandler);
    
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    
    cmdDistrib.distribDelete(dcmd, nodes, params);
    
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();

    errors = cmdDistrib.getErrors();
    
    assertEquals(errors.toString(), 0, errors.size());
    
    
    results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 2, numFound);
    
    numFound = client.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(results.toString(), 2, numFound);
    
    for (SolrServer c : clients) {
      c.optimize();
      //System.out.println(clients.get(0).request(new LukeRequest()));
    }
    
    cmdDistrib = new SolrCmdDistributor(updateShardHandler);
    
    int cnt = atLeast(303);
    for (int i = 0; i < cnt; i++) {
      nodes.clear();
      for (SolrServer c : clients) {
        if (random().nextBoolean()) {
          continue;
        }
        HttpSolrServer httpClient = (HttpSolrServer) c;
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
    
    for (SolrServer c : clients) {
      HttpSolrServer httpClient = (HttpSolrServer) c;
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
          httpClient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      
      nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));
    }
    
    final AtomicInteger commits = new AtomicInteger();
    for(JettySolrRunner jetty : jettys) {
      CoreContainer cores = ((SolrDispatchFilter) jetty.getDispatchFilter().getFilter()).getCores();
      SolrCore core = cores.getCore("collection1");
      try {
        core.getUpdateHandler().registerCommitCallback(new SolrEventListener() {
          @Override
          public void init(NamedList args) {}
          @Override
          public void postSoftCommit() {}
          @Override
          public void postCommit() {
            commits.incrementAndGet();
          }
          @Override
          public void newSearcher(SolrIndexSearcher newSearcher,
              SolrIndexSearcher currentSearcher) {}
        });
      } finally {
        core.close();
      }
    }
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);

    cmdDistrib.distribCommit(ccmd, nodes, params);
    
    cmdDistrib.finish();

    assertEquals(shardCount, commits.get());
    
    for (SolrServer c : clients) {
      NamedList<Object> resp = c.request(new LukeRequest());
      assertEquals("SOLR-3428: We only did adds - there should be no deletes",
          ((NamedList<Object>) resp.get("index")).get("numDocs"),
          ((NamedList<Object>) resp.get("index")).get("maxDoc"));
    }
    
    
    testMaxRetries();
    testOneRetry();
    testRetryNodeAgainstBadAddress();
    testRetryNodeWontRetrySocketError();
  }

  private void testMaxRetries() throws IOException {
    final MockStreamingSolrServers ss = new MockStreamingSolrServers(updateShardHandler);
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(ss, 5, 0);
    ss.setExp(Exp.CONNECT_EXCEPTION);
    ArrayList<Node> nodes = new ArrayList<Node>();
    final HttpSolrServer solrclient1 = (HttpSolrServer) clients.get(0);
    
    final AtomicInteger retries = new AtomicInteger();
    ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient1.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
    RetryNode retryNode = new RetryNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", "locahost") {
      @Override
      public boolean checkRetry() {
        retries.incrementAndGet();
        return true;
      }
    };
    
    nodes.add(retryNode);
    
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = sdoc("id", id.incrementAndGet());
    ModifiableSolrParams params = new ModifiableSolrParams();

    cmdDistrib.distribAdd(cmd, nodes, params);
    cmdDistrib.finish();
    
    assertEquals(6, retries.get());
    
    assertEquals(1, cmdDistrib.getErrors().size());
  }
  
  private void testOneRetry() throws Exception {
    final HttpSolrServer solrclient = (HttpSolrServer) clients.get(0);
    long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    final MockStreamingSolrServers ss = new MockStreamingSolrServers(updateShardHandler);
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(ss, 5, 0);
    ss.setExp(Exp.CONNECT_EXCEPTION);
    ArrayList<Node> nodes = new ArrayList<Node>();

    ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
        ZkStateReader.CORE_NAME_PROP, "");

    final AtomicInteger retries = new AtomicInteger();
    nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
    RetryNode retryNode = new RetryNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", "locahost") {
      @Override
      public boolean checkRetry() {
        ss.setExp(null);
        retries.incrementAndGet();
        return true;
      }
    };
    

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

  private void testRetryNodeWontRetrySocketError() throws Exception {
    final HttpSolrServer solrclient = (HttpSolrServer) clients.get(0);
    long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    final MockStreamingSolrServers ss = new MockStreamingSolrServers(updateShardHandler);
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(ss, 5, 0);
    ss.setExp(Exp.SOCKET_EXCEPTION);
    ArrayList<Node> nodes = new ArrayList<Node>();

    ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
        ZkStateReader.CORE_NAME_PROP, "");

    final AtomicInteger retries = new AtomicInteger();
    nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
    RetryNode retryNode = new RetryNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", "locahost") {
      @Override
      public boolean checkRetry() {
        retries.incrementAndGet();
        return true;
      }
    };
    

    nodes.add(retryNode);
    
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = sdoc("id", id.incrementAndGet());
    ModifiableSolrParams params = new ModifiableSolrParams();

    CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
    cmdDistrib.distribAdd(cmd, nodes, params);
    
    ss.setExp(null);
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

  private void testRetryNodeAgainstBadAddress() throws SolrServerException, IOException {
    // Test RetryNode
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(updateShardHandler);
    final HttpSolrServer solrclient = (HttpSolrServer) clients.get(0);
    long numFoundBefore = solrclient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    
    ArrayList<Node> nodes = new ArrayList<Node>();

    ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, "[ff01::114]:33332" + context, ZkStateReader.CORE_NAME_PROP, "");
    RetryNode retryNode = new RetryNode(new ZkCoreNodeProps(nodeProps), null, "collection1", "shard1", "locahost") {
      @Override
      public boolean checkRetry() {
        ZkNodeProps leaderProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, solrclient.getBaseURL(),
            ZkStateReader.CORE_NAME_PROP, "");
        this.nodeProps = new ZkCoreNodeProps(leaderProps);

        return true;
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
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    updateShardHandler.close();
    super.tearDown();
  }
}
