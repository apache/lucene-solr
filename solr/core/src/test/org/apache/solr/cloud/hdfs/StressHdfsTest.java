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

package org.apache.solr.cloud.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.BasicDistributedZkTest;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@Slow
@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
public class StressHdfsTest extends BasicDistributedZkTest {

  private static final String DELETE_DATA_DIR_COLLECTION = "delete_data_dir";
  private static MiniDFSCluster dfsCluster;
  
  @BeforeClass
  public static void setupClass() throws Exception {

    dfsCluster = HdfsTestUtil.setupClass(new File(TEMP_DIR,
        HdfsBasicDistributedZk2Test.class.getName() + "_"
            + System.currentTimeMillis()).getAbsolutePath());
    System.setProperty("solr.hdfs.home", dfsCluster.getURI().toString() + "/solr");
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    dfsCluster = null;
  }

  
  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }
  
  public StressHdfsTest() {
    super();
    sliceCount = 1;
    shardCount = TEST_NIGHTLY ? 13 : random().nextInt(3) + 1;
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }
  
  @Override
  public void doTest() throws Exception {
    int cnt = random().nextInt(2) + 1;
    for (int i = 0; i < cnt; i++) {
      createAndDeleteCollection();
    }
  }

  private void createAndDeleteCollection() throws SolrServerException,
      IOException, Exception, KeeperException, InterruptedException,
      URISyntaxException {
    
    boolean overshard = random().nextBoolean();
    if (overshard) {
      createCollection(DELETE_DATA_DIR_COLLECTION, shardCount * 2, 1, 2);
    } else {
      int rep = shardCount / 2;
      if (rep == 0) rep = 1;
      createCollection(DELETE_DATA_DIR_COLLECTION, rep, 2, 1);
    }

    waitForRecoveriesToFinish(DELETE_DATA_DIR_COLLECTION, false);
    cloudClient.setDefaultCollection(DELETE_DATA_DIR_COLLECTION);
    cloudClient.getZkStateReader().updateClusterState(true);
    
    
    // collect the data dirs
    List<String> dataDirs = new ArrayList<String>();
    
    int i = 0;
    for (SolrServer client : clients) {
      HttpSolrServer c = new HttpSolrServer(getBaseUrl(client)
          + "/delete_data_dir");
      try {
        c.add(getDoc("id", i++));
        if (random().nextBoolean()) c.add(getDoc("id", i++));
        if (random().nextBoolean()) c.add(getDoc("id", i++));
        if (random().nextBoolean()) {
          c.commit();
        } else {
          c.commit(true, true, true);
        }
        
        c.query(new SolrQuery("id:" + i));
        c.setSoTimeout(60000);
        c.setConnectionTimeout(30000);
        NamedList<Object> response = c.query(
            new SolrQuery().setRequestHandler("/admin/system")).getResponse();
        NamedList<Object> coreInfo = (NamedList<Object>) response.get("core");
        String dataDir = (String) ((NamedList<Object>) coreInfo
            .get("directory")).get("data");
        dataDirs.add(dataDir);
      } finally {
        c.shutdown();
      }
    }
    
    if (random().nextBoolean()) {
      cloudClient.deleteByQuery("*:*");
      cloudClient.commit();
      
      assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", DELETE_DATA_DIR_COLLECTION);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cloudClient.request(request);
    
    // check that all dirs are gone
    for (String dataDir : dataDirs) {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.newInstance(new URI(dataDir), conf);
      assertFalse(
          "Data directory exists after collection removal : " + dataDir,
          fs.exists(new Path(dataDir)));
      fs.close();
    }
  }
}
