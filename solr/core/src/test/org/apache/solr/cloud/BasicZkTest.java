package org.apache.solr.cloud;

/**
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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXParseException;

/**
 *
 */
public class BasicZkTest extends AbstractZkTestCase {
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("CLOUD_UPDATE_DELAY", "1");
  }
  
  @Test
  public void testBasic() throws Exception {
    // test using ZooKeeper
    assertTrue("Not using ZooKeeper", h.getCoreContainer().isZooKeeperAware());
    
    ZkController zkController = h.getCoreContainer().getZkController();
    
    // test merge factor picked up
    SolrCore core = h.getCore();

    IndexWriter writer = ((DirectUpdateHandler2)core.getUpdateHandler()).getIndexWriterProvider().getIndexWriter(core);

    assertEquals("Mergefactor was not picked up", ((LogMergePolicy)writer.getConfig().getMergePolicy()).getMergeFactor(), 8);
    
    lrf.args.put(CommonParams.VERSION, "2.2");
    assertQ("test query on empty index", req("qlkciyopsbgzyvkylsjhchghjrdf"),
        "//result[@numFound='0']");

    // test escaping of ";"
    assertU("deleting 42 for no reason at all", delI("42"));
    assertU("adding doc#42", adoc("id", "42", "val_s", "aa;bb"));
    assertU("does commit work?", commit());

    assertQ("backslash escaping semicolon", req("id:42 AND val_s:aa\\;bb"),
        "//*[@numFound='1']", "//int[@name='id'][.='42']");

    assertQ("quote escaping semicolon", req("id:42 AND val_s:\"aa;bb\""),
        "//*[@numFound='1']", "//int[@name='id'][.='42']");

    assertQ("no escaping semicolon", req("id:42 AND val_s:aa"),
        "//*[@numFound='0']");

    assertU(delI("42"));
    assertU(commit());
    assertQ(req("id:42"), "//*[@numFound='0']");

    // test overwrite default of true

    assertU(adoc("id", "42", "val_s", "AAA"));
    assertU(adoc("id", "42", "val_s", "BBB"));
    assertU(commit());
    assertQ(req("id:42"), "//*[@numFound='1']", "//str[.='BBB']");
    assertU(adoc("id", "42", "val_s", "CCC"));
    assertU(adoc("id", "42", "val_s", "DDD"));
    assertU(commit());
    assertQ(req("id:42"), "//*[@numFound='1']", "//str[.='DDD']");

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
        zkController.getCloudState().getLiveNodes().toString(),
        zkController.getCloudState().liveNodesContain(
            zkController.getNodeName()));

    // test maxint
    assertQ(req("q", "id:[100 TO 110]", "rows", "2147483647"),
        "//*[@numFound='4']");

    // test big limit
    assertQ(req("q", "id:[100 TO 111]", "rows", "1147483647"),
        "//*[@numFound='4']");

    assertQ(req("id:[100 TO 110]"), "//*[@numFound='4']");
    assertU(delI("102"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]"), "//*[@numFound='3']");
    assertU(delI("105"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]"), "//*[@numFound='2']");
    assertU(delQ("id:[100 TO 110]"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]"), "//*[@numFound='0']");
    
   
    
    // SOLR-2651: test that reload still gets config files from zookeeper 
    zkController.getZkClient().setData("/configs/conf1/solrconfig.xml", new byte[0]);
 
    // we set the solrconfig to nothing, so this reload should fail
    try {
      h.getCoreContainer().reload(h.getCore().getName());
      fail("The reloaded SolrCore did not pick up configs from zookeeper");
    } catch(SAXParseException e) {
      
    }
    
  }
  
  @AfterClass
  public static void afterClass() {
    System.clearProperty("CLOUD_UPDATE_DELAY");
  }
}
