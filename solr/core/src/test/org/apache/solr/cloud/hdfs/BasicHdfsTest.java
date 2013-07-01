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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.BasicDistributedZkTest;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@Slow
@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
public class BasicHdfsTest extends BasicDistributedZkTest {

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
  
  public BasicHdfsTest() {
    super();
    sliceCount = 1;
    shardCount = 1;
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }
  
  @Override
  public void doTest() throws Exception {
    createCollection("delete_data_dir", 1, 1, 1);
    waitForRecoveriesToFinish("delete_data_dir", false);
    cloudClient.setDefaultCollection("delete_data_dir");
    cloudClient.getZkStateReader().updateClusterState(true);
    NamedList<Object> response = cloudClient.query(
        new SolrQuery().setRequestHandler("/admin/system")).getResponse();
    NamedList<Object> coreInfo = (NamedList<Object>) response.get("core");
    String dataDir = (String) ((NamedList<Object>) coreInfo.get("directory"))
        .get("data");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "delete_data_dir");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cloudClient.request(request);
    
    Configuration conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    FileSystem fs = FileSystem.newInstance(new URI(dataDir), conf);
    assertFalse(
        "Data directory exists after collection removal : "
            + dataDir, fs.exists(new Path(dataDir)));
    fs.close();
  }
}
