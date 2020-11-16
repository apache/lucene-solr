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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExclusionRuleCollectionAccess extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void doTest() throws Exception {

    CollectionAdminRequest.createCollection("css33", "conf", 1, 1).process(cluster.getSolrClient());

    new UpdateRequest()
        .add("id", "1")
        .commit(cluster.getSolrClient(), "css33");

    assertEquals("Should have returned 1 result", 1,
        cluster.getSolrClient().query("css33", params("q", "*:*", "collection", "css33")).getResults().getNumFound());

  }
  
}
