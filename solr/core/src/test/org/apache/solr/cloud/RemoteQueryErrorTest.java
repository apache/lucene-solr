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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Verify that remote (proxied) queries return proper error messages
 */

@Slow
public class RemoteQueryErrorTest extends AbstractFullDistribZkTestBase {
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
  }
  
  @AfterClass
  public static void afterSuperClass() {
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }

  public RemoteQueryErrorTest() {
    super();
    sliceCount = 1;
    shardCount = random().nextBoolean() ? 3 : 4;
  }

  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(15);

    del("*:*");
    
    createCollection("collection2", 2, 1, 10);
    
    List<Integer> numShardsNumReplicaList = new ArrayList<Integer>(2);
    numShardsNumReplicaList.add(2);
    numShardsNumReplicaList.add(1);
    checkForCollection("collection2", numShardsNumReplicaList, null);
    waitForRecoveriesToFinish("collection2", true);

    HttpSolrServer solrServer = null;
    for (JettySolrRunner jetty : jettys) {
      int port = port = jetty.getLocalPort();
      solrServer = new HttpSolrServer("http://127.0.0.1:" + port + context + "/collection2");
      try {
        SolrInputDocument emptyDoc = new SolrInputDocument();
        solrServer.add(emptyDoc);
        fail("Expected unique key exceptoin");
      } catch (Exception ex) {
        assert(ex.getMessage().contains("Document is missing mandatory uniqueKey field: id"));
      } finally {
        solrServer.shutdown();
      }
    }
  }
}