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

package org.apache.solr.client.solrj;

import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrSchemalessExampleTests extends SolrExampleTestsBase {
  private static Logger log = LoggerFactory
      .getLogger(SolrSchemalessExampleTests.class);
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_SCHEMALESS_HOME, null, null);
  }
  
  @Override
  public SolrServer createNewSolrServer() {
    try {
      // setup the server...
      String url = jetty.getBaseUrl().toString() + "/collection1";
      HttpSolrServer s = new HttpSolrServer(url);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      s.setUseMultiPartPost(random().nextBoolean());
      
      if (random().nextBoolean()) {
        s.setParser(new BinaryResponseParser());
        s.setRequestWriter(new BinaryRequestWriter());
      }
      
      return s;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
