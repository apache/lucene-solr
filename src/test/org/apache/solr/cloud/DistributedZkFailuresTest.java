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

import java.util.HashSet;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;

/**
 * nocommit: 
 *
 */
public class DistributedZkFailuresTest extends AbstractDistributedZkTestCase {
  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }
  
  protected String getSolrConfigFilename() {
    return "solr.lowZkTimeout.xml";
  }
  
  
  String t1="a_t";
  String i1="a_si";
  String nint = "n_i";
  String tint = "n_ti";
  String nfloat = "n_f";
  String tfloat = "n_tf";
  String ndouble = "n_d";
  String tdouble = "n_td";
  String nlong = "n_l";
  String tlong = "n_tl";
  String ndate = "n_dt";
  String tdate = "n_tdt";
  
  String oddField="oddField_s";
  String missingField="missing_but_valid_field_t";
  String invalidField="invalid_field_not_in_schema";
  

  JettySolrRunner jetty;
  int port = 0;
  
  @Override
  public void setUp() throws Exception {
    createEmbeddedCore = true;
    super.setUp();
    
    jetty = new JettySolrRunner(context, 0, getSolrConfigFilename());
    jetty.start();
    port = jetty.getLocalPort();
   
  }

  
  protected void createServers(int numShards) throws Exception {
    controlJetty = createJetty(testDir, "control", "shard2", getSolrConfigFilename());
    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner j = createJetty(testDir, "shard" + i, "shard" + (i + 2), getSolrConfigFilename());
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      sb.append("localhost:").append(j.getLocalPort()).append(context);
    }

    shards = sb.toString();
  }
  
  public void testDistribSearch() throws Exception {
    for (int nServers = 3; nServers < 4; nServers++) {
      createServers(nServers);
      RandVal.uniqueValues = new HashSet(); //reset random values
      doTest();
      jetty.stop();
      destroyServers();
    }
  }

  @Override
  public void doTest() throws Exception {
    ZkController zkController = h.getCoreContainer().getZkController();
    del("*:*");
    
    // nocommit : test too long
    System.out.println("gc block");
    for(int i = 0; i < 100; i++) {
      // try and timeout
      System.gc();
    }
    System.out.println("done gc block");
    
    // ensure zk still thinks node is up
    assertTrue(zkController.getCloudState().liveNodesContain(zkController.getNodeName()));
    
    // index the same document to two servers and make sure things
    // don't blow up.
    if (clients.size()>=2) {
      index(id,100, i1, 107 ,t1,"oh no, a duplicate!");
      for (int i=0; i<clients.size(); i++) {
        index_specific(i, id,100, i1, 107 ,t1,"oh no, a duplicate!");
      }
      commit();
      query("q","duplicate", "hl","true", "hl.fl", t1);
      query("q","fox duplicate horses", "hl","true", "hl.fl", t1);
      query("q","*:*", "rows",100);
    }

  }
}
