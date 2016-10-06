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

import java.net.URL;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

@SuppressSSL
public class TestRequestForwarding extends SolrTestCaseJ4 {

  private MiniSolrCloudCluster solrCluster;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    solrCluster = new MiniSolrCloudCluster(3, createTempDir(), buildJettyConfig("/solr"));
    solrCluster.uploadConfigSet(TEST_PATH().resolve("collection1/conf"), "conf1");
  }

  @Override
  public void tearDown() throws Exception {
    solrCluster.shutdown();
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");

    super.tearDown();
  }

  @Test
  public void testMultiCollectionQuery() throws Exception {
    createCollection("collection1", "conf1");
    // Test against all nodes (two of them host the collection, one of them will 
    // forward the query)
    for (JettySolrRunner jettySolrRunner : solrCluster.getJettySolrRunners()) {
      String queryStrings[] = {
          "q=cat%3Afootball%5E2", // URL encoded 
          "q=cat:football^2" // No URL encoding, contains disallowed character ^
      };
      for (String q: queryStrings) {
        try {
          URL url = new URL(jettySolrRunner.getBaseUrl().toString()+"/collection1/select?"+q);
          url.openStream(); // Shouldn't throw any errors
        } catch (Exception ex) {
          throw new RuntimeException("Query '" + q + "' failed, ",ex);
        }
      }
    }
  }

  private void createCollection(String name, String config) throws Exception {
    CollectionAdminResponse response;
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    create.setConfigName(config);
    create.setCollectionName(name);
    create.setNumShards(2);
    create.setReplicationFactor(1);
    create.setMaxShardsPerNode(1);
    response = create.process(solrCluster.getSolrClient());
    
    if (response.getStatus() != 0 || response.getErrorMessages() != null) {
      fail("Could not create collection. Response" + response.toString());
    }
    ZkStateReader zkStateReader = solrCluster.getSolrClient().getZkStateReader();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(name, zkStateReader, false, true, 100);
  }
}
