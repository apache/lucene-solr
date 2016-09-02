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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.junit.Test;

import static org.hamcrest.core.IsNot.not;

public class JettySolrRunnerTest extends SolrTestCaseJ4 {

  @Test
  public void testRestartPorts() throws Exception {

    Path solrHome = createTempDir();
    Files.write(solrHome.resolve("solr.xml"), MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.getBytes(Charset.defaultCharset()));

    JettyConfig config = JettyConfig.builder().build();

    JettySolrRunner jetty = new JettySolrRunner(solrHome.toString(), config);
    try {
      jetty.start();

      URL url = jetty.getBaseUrl();
      int usedPort = url.getPort();

      jetty.stop();
      jetty.start();

      assertEquals("After restart, jetty port should be the same", usedPort, jetty.getBaseUrl().getPort());

      jetty.stop();
      jetty.start(false);

      assertThat("After restart, jetty port should be different", jetty.getBaseUrl().getPort(), not(usedPort));
    }
    finally {
      if (jetty.isRunning())
        jetty.stop();
    }

  }

}
