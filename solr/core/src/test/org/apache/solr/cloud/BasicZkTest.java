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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test is not fully functional - the port registered is illegal - 
 * so you cannot hit this with http - a nice side benifit is that it will
 * detect if a node is trying to do an update to itself with http - it shouldn't
 * do that.
 */
@Slow
public class BasicZkTest extends AbstractZkTestCase {
  
  @BeforeClass
  public static void beforeClass() {

  }
  
  @Test
  public void testBasic() throws Exception {
    
    // test using ZooKeeper
    assertTrue("Not using ZooKeeper", h.getCoreContainer().isZooKeeperAware());
    
    // for the really slow/busy computer, we wait to make sure we have a leader before starting
    h.getCoreContainer().getZkController().getZkStateReader().getLeaderUrl("collection1", "shard1", 30000);
    
    ZkController zkController = h.getCoreContainer().getZkController();

    SolrCore core = h.getCore();

    // test that we got the expected config, not just hardcoded defaults
    assertNotNull(core.getRequestHandler("mock"));

    lrf.args.put(CommonParams.VERSION, "2.2");
    assertQ("test query on empty index", request("qlkciyopsbgzyvkylsjhchghjrdf"),
        "//result[@numFound='0']");

    // test escaping of ";"
    assertU("deleting 42 for no reason at all", delI("42"));
    assertU("adding doc#42", adoc("id", "42", "val_s", "aa;bb"));
    assertU("does commit work?", commit());

    assertQ("backslash escaping semicolon", request("id:42 AND val_s:aa\\;bb"),
        "//*[@numFound='1']", "//int[@name='id'][.='42']");

    assertQ("quote escaping semicolon", request("id:42 AND val_s:\"aa;bb\""),
        "//*[@numFound='1']", "//int[@name='id'][.='42']");

    assertQ("no escaping semicolon", request("id:42 AND val_s:aa"),
        "//*[@numFound='0']");

    assertU(delI("42"));
    assertU(commit());
    assertQ(request("id:42"), "//*[@numFound='0']");

    // test overwrite default of true

    assertU(adoc("id", "42", "val_s", "AAA"));
    assertU(adoc("id", "42", "val_s", "BBB"));
    assertU(commit());
    assertQ(request("id:42"), "//*[@numFound='1']", "//str[.='BBB']");
    assertU(adoc("id", "42", "val_s", "CCC"));
    assertU(adoc("id", "42", "val_s", "DDD"));
    assertU(commit());
    assertQ(request("id:42"), "//*[@numFound='1']", "//str[.='DDD']");

    // test deletes
    String[] adds = new String[] { add(doc("id", "101"), "overwrite", "true"),
        add(doc("id", "101"), "overwrite", "true"),
        add(doc("id", "105"), "overwrite", "false"),
        add(doc("id", "102"), "overwrite", "true"),
        add(doc("id", "103"), "overwrite", "false"),
        add(doc("id", "101"), "overwrite", "true"), };
    for (String a : adds) {
      assertU(a, a);
    }
    assertU(commit());
    int zkPort = zkServer.getPort();

    zkServer.shutdown();
    
    Thread.sleep(300);
    
    // try a reconnect from disconnect
    zkServer = new ZkTestServer(zkDir, zkPort);
    zkServer.run();
    
    Thread.sleep(300);
    
    // ensure zk still thinks node is up
    assertTrue(
        zkController.getClusterState().getLiveNodes().toString(),
        zkController.getClusterState().liveNodesContain(
            zkController.getNodeName()));

    // test maxint
    assertQ(request("q", "id:[100 TO 110]", "rows", "2147483647"),
        "//*[@numFound='4']");

    // test big limit
    assertQ(request("q", "id:[100 TO 111]", "rows", "1147483647"),
        "//*[@numFound='4']");

    assertQ(request("id:[100 TO 110]"), "//*[@numFound='4']");
    assertU(delI("102"));
    assertU(commit());
    assertQ(request("id:[100 TO 110]"), "//*[@numFound='3']");
    assertU(delI("105"));
    assertU(commit());
    assertQ(request("id:[100 TO 110]"), "//*[@numFound='2']");
    assertU(delQ("id:[100 TO 110]"));
    assertU(commit());
    assertQ(request("id:[100 TO 110]"), "//*[@numFound='0']");



    // SOLR-2651: test that reload still gets config files from zookeeper 
    zkController.getZkClient().setData("/configs/conf1/solrconfig.xml", new byte[0], true);
 
    // we set the solrconfig to nothing, so this reload should fail
    try {
      ignoreException("solrconfig.xml");
      h.getCoreContainer().reload(h.getCore().getName());
      fail("The reloaded SolrCore did not pick up configs from zookeeper");
    } catch(SolrException e) {
      resetExceptionIgnores();
      assertTrue(e.getMessage().contains("Unable to reload core [collection1]"));
      assertTrue(e.getCause().getMessage().contains("Error loading solr config from solrconfig.xml"));
    }
    
    // test stats call
    NamedList stats = core.getStatistics();
    assertEquals("collection1", stats.get("coreName"));
    assertEquals("collection1", stats.get("collection"));
    assertEquals("shard1", stats.get("shard"));
    assertTrue(stats.get("refCount") != null);

    //zkController.getZkClient().printLayoutToStdOut();
  }
  
  public SolrQueryRequest request(String... q) {
    LocalSolrQueryRequest req = lrf.makeRequest(q);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(req.getParams());
    params.set("distrib", false);
    req.setParams(params);
    return req;
  }
  
  @AfterClass
  public static void afterClass() {

  }
}
