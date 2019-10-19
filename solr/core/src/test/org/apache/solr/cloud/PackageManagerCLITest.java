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

package org.apache.solr.cloud;

import java.nio.file.Path;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.util.PackageTool;
import org.apache.solr.util.SolrCLI;
import org.junit.BeforeClass;
import org.junit.Test;

public class PackageManagerCLITest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("enable.packages", "true");

    configureCluster(1)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testUpconfig() throws Exception {
    // Use a full, explicit path for configset.

    Path configSet = TEST_PATH().resolve("configsets");
    Path srcPathCheck = configSet.resolve("cloud-subdirs").resolve("conf");
    AbstractDistribZkTestBase.copyConfigUp(configSet, "cloud-subdirs", "upconfig1", cluster.getZkServer().getZkAddress());

    // Now just use a name in the configsets directory, do we find it?
    configSet = TEST_PATH().resolve("configsets");

    PackageTool tool = new PackageTool();
    String solrUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString();
    int res = run(tool, new String[] {"-solrUrl", solrUrl, "list"});
    assertEquals("tool should have returned 0 for success ", 0, res);
    
    res = run(tool, new String[] {"-solrUrl", solrUrl, "add-repo", "fullstory",  "http://localhost:8081"});
    assertEquals("tool should have returned 0 for success ", 0, res);

    res = run(tool, new String[] {"-solrUrl", solrUrl, "list-available"});
    assertEquals("tool should have returned 0 for success ", 0, res);

    res = run(tool, new String[] {"-solrUrl", solrUrl, "install", "question-answer", "1.0.0"}); // no-commit (change to pkg:ver syntax)
    assertEquals("tool should have returned 0 for success ", 0, res);
    
    res = run(tool, new String[] {"-solrUrl", solrUrl, "list"});
    assertEquals("tool should have returned 0 for success ", 0, res);

    CollectionAdminRequest
      .createCollection("abc", "conf1", 2, 1)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());

    res = run(tool, new String[] {"-solrUrl", solrUrl, "deploy", "question-answer", "-collections", "abc", "-p", "RH-HANDLER-PATH=/mypath2"});
    assertEquals("tool should have returned 0 for success ", 0, res);
    
    res = run(tool, new String[] {"-solrUrl", solrUrl, "update", "question-answer"});
    assertEquals("tool should have returned 0 for success ", 0, res);

  }

  private int run(PackageTool tool, String[] args) throws Exception {
    int res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    return res;
  }
}
