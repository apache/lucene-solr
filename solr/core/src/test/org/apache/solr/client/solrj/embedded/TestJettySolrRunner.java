package org.apache.solr.client.solrj.embedded;

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

import com.google.common.base.Charsets;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class TestJettySolrRunner extends SolrTestCaseJ4 {

  @Test
  public void testPassSolrHomeToRunner() throws Exception {

    // We set a non-standard coreRootDirectory, create a core, and check that it has been
    // built in the correct place

    Path solrHome = createTempDir();
    Path coresDir = createTempDir("crazy_path_to_cores");

    Path configsets = Paths.get(TEST_HOME()).resolve("configsets");

    String solrxml
        = "<solr><str name=\"configSetBaseDir\">CONFIGSETS</str><str name=\"coreRootDirectory\">COREROOT</str></solr>"
        .replace("CONFIGSETS", configsets.toString())
        .replace("COREROOT", coresDir.toString());
    Files.write(solrHome.resolve("solr.xml"), solrxml.getBytes(Charsets.UTF_8));

    JettyConfig jettyConfig = buildJettyConfig("/solr");

    JettySolrRunner runner = new JettySolrRunner(solrHome.toString(), new Properties(), jettyConfig);
    try {
      runner.start();

      SolrClient client = new HttpSolrClient(runner.getBaseUrl().toString());

      CoreAdminRequest.Create createReq = new CoreAdminRequest.Create();
      createReq.setCoreName("newcore");
      createReq.setConfigSet("minimal");

      client.request(createReq);

      assertTrue(Files.exists(coresDir.resolve("newcore").resolve("core.properties")));

    }
    finally {
      runner.stop();
    }

  }

}
