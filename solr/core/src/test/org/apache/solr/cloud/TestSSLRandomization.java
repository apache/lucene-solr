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

import java.lang.invoke.MethodHandles;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.SSLTestConfig;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A "test the test" method that verifies the SSL options randomized by {@link SolrTestCaseJ4} are 
 * correctly used in the various helper methods available from the test framework and
 * {@link MiniSolrCloudCluster}.
 *
 * @see TestMiniSolrCloudClusterSSL
 */
public class TestSSLRandomization extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void createMiniSolrCloudCluster() throws Exception {
    configureCluster(TestMiniSolrCloudClusterSSL.NUM_SERVERS).configure();
  }
  
  public void testRandomizedSslAndClientAuth() throws Exception {
    TestMiniSolrCloudClusterSSL.checkClusterWithCollectionCreations(cluster,sslConfig);
  }
  
  public void testBaseUrl() throws Exception {
    String url = buildUrl(6666, "/foo");
    assertEquals(sslConfig.isSSLMode() ? "https://127.0.0.1:6666/foo" : "http://127.0.0.1:6666/foo", url);
  }
}
