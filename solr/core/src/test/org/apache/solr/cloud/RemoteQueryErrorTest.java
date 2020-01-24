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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

/**
 * Verify that remote (proxied) queries return proper error messages
 */
public class RemoteQueryErrorTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  // TODO add test for CloudSolrClient as well

  @Test
  public void test() throws Exception {

    CollectionAdminRequest.createCollection("collection", "conf", 2, 1).process(cluster.getSolrClient());

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      try (SolrClient client = jetty.newClient()) {
        SolrException e = expectThrows(SolrException.class, () -> {
          client.add("collection", new SolrInputDocument());
        });
        assertThat(e.getMessage(), containsString("Document is missing mandatory uniqueKey field: id"));
      }
    }

  }
}
