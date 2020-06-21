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
package org.apache.solr.client.solrj.embedded;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.NodeConfig;
import org.junit.Test;

public class TestEmbeddedSolrServerConstructors extends SolrTestCaseJ4 {

  @Test
  @SuppressWarnings({"try"})
  public void testPathConstructor() throws IOException {
    Path path = Paths.get(TEST_HOME());
    try (EmbeddedSolrServer server = new EmbeddedSolrServer(path, "collection1")) {

    }
  }

  @Test
  public void testNodeConfigConstructor() throws Exception {
    Path path = createTempDir();

    NodeConfig config = new NodeConfig.NodeConfigBuilder("testnode", path)
        .setConfigSetBaseDirectory(Paths.get(TEST_HOME()).resolve("configsets").toString())
        .build();

    try (EmbeddedSolrServer server = new EmbeddedSolrServer(config, "newcore")) {

      CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
      createRequest.setCoreName("newcore");
      createRequest.setConfigSet("minimal");
      server.request(createRequest);

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("articleid", "test");
      server.add("newcore", doc);
      server.commit();

      assertEquals(1, server.query(new SolrQuery("*:*")).getResults().getNumFound());
      assertEquals(1, server.query("newcore", new SolrQuery("*:*")).getResults().getNumFound());

    }
  }

}
