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
package org.apache.solr.rest.schema;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.rest.SolrRestletTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUniqueKeyFieldResource extends SolrRestletTestBase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    JettyConfig jettyConfig = JettyConfig.builder()
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();
    jetty = createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }

  @After
  public void tearDown() throws Exception {
    if (jetty != null) {
      jetty.stop();
    }
    jetty = null;
    super.tearDown();
  }

  @Test
  public void testGetUniqueKey() throws Exception {
    assertQ("/schema/uniquekey?indent=on&wt=xml",
            "count(/response/str[@name='uniqueKey']) = 1",
            "/response/str[@name='uniqueKey'][.='id']");
  }
}

