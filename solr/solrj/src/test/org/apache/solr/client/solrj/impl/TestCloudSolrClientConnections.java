package org.apache.solr.client.solrj.impl;

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrException;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class TestCloudSolrClientConnections extends SolrTestCaseJ4 {

  @Test
  public void testCloudClientCanConnectAfterClusterComesUp() throws Exception {

    // Start by creating a cluster with no jetties

    File solrXml = getFile("solrj").toPath().resolve("solr/solr.xml").toFile();
    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(0, createTempDir().toFile(), solrXml, buildJettyConfig("/solr"));
    try {

      CloudSolrClient client = cluster.getSolrClient();
      CollectionAdminRequest.List listReq = new CollectionAdminRequest.List();

      try {
        client.request(listReq);
        fail("Requests to a non-running cluster should throw a SolrException");
      }
      catch (SolrException e) {
        assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("cluster not found/not ready"));
      }

      cluster.startJettySolrRunner();
      client.connect(20, TimeUnit.SECONDS);

      // should work now!
      client.request(listReq);

    }
    finally {
      cluster.shutdown();
    }

  }

}
