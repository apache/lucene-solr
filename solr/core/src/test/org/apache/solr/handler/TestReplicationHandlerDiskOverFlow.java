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

package org.apache.solr.handler;

import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.ReplicationHandler.CMD_FETCH_INDEX;
import static org.apache.solr.handler.ReplicationHandler.CMD_GET_FILE_LIST;
import static org.apache.solr.handler.TestReplicationHandler.createAndStartJetty;
import static org.apache.solr.handler.TestReplicationHandler.createNewSolrClient;
import static org.apache.solr.handler.TestReplicationHandler.invokeReplicationCommand;

@LogLevel("org.apache.solr.handler.IndexFetcher=DEBUG")
@SolrTestCaseJ4.SuppressSSL
public class TestReplicationHandlerDiskOverFlow extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());



  JettySolrRunner masterJetty, slaveJetty;
  SolrClient masterClient, slaveClient;
  TestReplicationHandler.SolrInstance master = null, slave = null;

  static String context = "/solr";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    String factory = random().nextInt(100) < 75 ? "solr.NRTCachingDirectoryFactory" : "solr.StandardDirectoryFactory"; // test the default most of the time
    System.setProperty("solr.directoryFactory", factory);
    master = new TestReplicationHandler.SolrInstance(createTempDir("solr-instance").toFile(), "master", null);
    master.setUp();
    masterJetty = createAndStartJetty(master);
    masterClient = createNewSolrClient(masterJetty.getLocalPort());

    slave = new TestReplicationHandler.SolrInstance(createTempDir("solr-instance").toFile(), "slave", masterJetty.getLocalPort());
    slave.setUp();
    slaveJetty = createAndStartJetty(slave);
    slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

    System.setProperty("solr.indexfetcher.sotimeout2", "45000");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    masterJetty.stop();
    slaveJetty.stop();
    masterJetty = slaveJetty = null;
    master = slave = null;
    masterClient.close();
    slaveClient.close();
    masterClient = slaveClient = null;
    System.clearProperty("solr.indexfetcher.sotimeout");
  }

  @Test
  public void testDiskOverFlow() throws Exception {
    invokeReplicationCommand(slaveJetty.getLocalPort(), "disablepoll");
    //index docs
    System.out.println("MASTER");
    int docsInMaster = 1000;
    long szMaster = indexDocs(masterClient, docsInMaster, 0);
    System.out.println("SLAVE");
    long szSlave = indexDocs(slaveClient, 1200, 1000);


    Function<String, Long> originalDiskSpaceprovider = IndexFetcher.usableDiskSpaceProvider;
    IndexFetcher.usableDiskSpaceProvider = new Function<String, Long>() {
      @Override
      public Long apply(String s) {
        return szMaster;
      }
    };
    QueryResponse response;
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean searchDisabledFound = new AtomicBoolean(false);
    try {
      IndexFetcher.testWait = new BooleanSupplier() {
        @Override
        public boolean getAsBoolean() {
          try {
            latch.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {

          }
          return true;
        }
      };


      new Thread(() -> {
        for (int i = 0; i < 20; i++) {
          try {
            QueryResponse rsp = slaveClient.query(new SolrQuery()
                .setQuery("*:*")
                .setRows(0));
            Thread.sleep(100);
          } catch (Exception e) {
            if (e.getMessage().contains("Search is temporarily disabled")) {
              searchDisabledFound.set(true);
            }
            latch.countDown();
            break;
          }
        }
      }).start();

      response = slaveClient.query(new SolrQuery()
          .add("qt", "/replication")
          .add("command", CMD_FETCH_INDEX)
          .add("wait", "true")
      );

    } finally {
      IndexFetcher.usableDiskSpaceProvider = originalDiskSpaceprovider;
    }
    assertTrue(searchDisabledFound.get());
    assertEquals("OK", response._getStr("status", null));
//    System.out.println("MASTER INDEX: " + szMaster);
//    System.out.println("SLAVE INDEX: " + szSlave);

    response = slaveClient.query(new SolrQuery().setQuery("*:*").setRows(0));
    assertEquals(docsInMaster, response.getResults().getNumFound());

    response = slaveClient.query(new SolrQuery()
        .add("qt", "/replication")
        .add("command", ReplicationHandler.CMD_DETAILS)
    );
    System.out.println("DETAILS" + Utils.writeJson(response, new StringWriter(), true).toString());
    assertEquals("true", response._getStr("details/slave/clearedLocalIndexFirst", null));
  }

  private long indexDocs(SolrClient client, int totalDocs, int start) throws Exception {
    for (int i = 0; i < totalDocs; i++)
      TestReplicationHandler.index(client, "id", i + start, "name", TestUtil.randomSimpleString(random(), 1000, 5000));
    client.commit(true, true);
    QueryResponse response = client.query(new SolrQuery()
        .add("qt", "/replication")
        .add("command", "filelist")
        .add("generation", "-1"));

    long totalSize = 0;
    for (Map map : (List<Map>) response.getResponse().get(CMD_GET_FILE_LIST)) {
      Long sz = (Long) map.get(ReplicationHandler.SIZE);
      totalSize += sz;
    }
    return totalSize;
  }

}
