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

package org.apache.solr.common.cloud;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Test;

public class TestNodesSysPropsCacher extends SolrCloudTestCase {

  @Test
  public void testSysProps() throws Exception {
    System.setProperty("metricsEnabled", "true");
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
            .configure();

    System.clearProperty("metricsEnabled");
    NodesSysPropsCacher nodesSysPropsCacher =
        cluster.getRandomJetty(random()).getCoreContainer().getZkController().getSysPropsCacher();

    try {
      for (JettySolrRunner j : cluster.getJettySolrRunners()) {
        List<String> tags = Arrays.asList("file.encoding", "java.vm.version");
        Map<String, Object> props = nodesSysPropsCacher.getSysProps(j.getNodeName(), tags);
        for (String tag : tags) assertNotNull(props.get(tag));
        tags = Arrays.asList("file.encoding", "java.vm.version", "os.arch");
        props = nodesSysPropsCacher.getSysProps(j.getNodeName(), tags);
        for (String tag : tags) assertNotNull(props.get(tag));
      }
    } finally {
      cluster.shutdown();
    }
  }
}