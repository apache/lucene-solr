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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.Response;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.util.DefaultSolrThreadFactory;

public class SolrCmdDistributorTest extends BaseDistributedSearchTestCase {
  private ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5,
      TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("cmdDistribExecutor"));
  
  public SolrCmdDistributorTest() {
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
      String shardStr = "localhost:" + j.getLocalPort() + context;
      shardsArr[i] = shardStr;
      sb.append(shardStr);
    }

    shards = sb.toString();
  }
  
  @Override
  public void doTest() throws Exception {
    del("*:*");
    
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(8, executor);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    List<Node> nodes = new ArrayList<Node>();

    ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        ((HttpSolrServer) controlClient).getBaseURL(),
        ZkStateReader.CORE_NAME_PROP, "");
    nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));

    // add one doc to controlClient
    
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = sdoc("id", 1);
    cmdDistrib.distribAdd(cmd, nodes, params);
    
    CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();

    Response response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    long numFound = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(1, numFound);
    
    HttpSolrServer client = (HttpSolrServer) clients.get(0);
    nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        client.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
    nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));
    
    // add another 2 docs to control and 3 to client
    cmdDistrib = new SolrCmdDistributor(8, executor);
    cmd.solrDoc = sdoc("id", 2);
    cmdDistrib.distribAdd(cmd, nodes, params);
    
    AddUpdateCommand cmd2 = new AddUpdateCommand(null);
    cmd2.solrDoc = sdoc("id", 3);

    cmdDistrib.distribAdd(cmd2, nodes, params);
    
    AddUpdateCommand cmd3 = new AddUpdateCommand(null);
    cmd3.solrDoc = sdoc("id", 4);
    
    cmdDistrib.distribAdd(cmd3, Collections.singletonList(nodes.get(1)), params);
    
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();
    response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    SolrDocumentList results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 3, numFound);
    
    numFound = client.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(3, numFound);
    
    // now delete doc 2 which is on both control and client1
    
    DeleteUpdateCommand dcmd = new DeleteUpdateCommand(null);
    dcmd.id = "2";
    
    cmdDistrib = new SolrCmdDistributor(8, executor);
    cmdDistrib.distribDelete(dcmd, nodes, params);
    
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();

    response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    
    results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 2, numFound);
    
    numFound = client.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(results.toString(), 2, numFound);
    
    // debug stuff
    for (SolrServer c : clients) {
      c.optimize();
      // distrib optimize is not working right yet, so call it on each client
      //System.out.println(clients.get(0).request(new LukeRequest()));
    }
    
    int id = 5;
    
    cmdDistrib = new SolrCmdDistributor(8, executor);
    
    nodes.clear();
    int cnt = atLeast(200);
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
      
      cmd.solrDoc = sdoc("id", id++);
      cmdDistrib.distribAdd(cmd, nodes, params);
    }

    cmdDistrib.finish();
    
    cmdDistrib.distribCommit(ccmd, nodes, params);
    
    for (SolrServer c : clients) {
      NamedList<Object> resp = c.request(new LukeRequest());
      System.out.println(resp);
      assertEquals("SOLR-3428: We only did adds - there should be no deletes",
          ((NamedList<Object>) resp.get("index")).get("numDocs"),
          ((NamedList<Object>) resp.get("index")).get("maxDoc"));
    }

  }
}
