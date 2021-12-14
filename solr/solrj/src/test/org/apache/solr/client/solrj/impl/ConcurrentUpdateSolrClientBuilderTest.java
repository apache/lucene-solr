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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient.Builder;
import org.junit.Test;

/**
 * Unit tests for {@link Builder}.
 */
public class ConcurrentUpdateSolrClientBuilderTest extends SolrTestCase {

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsMissingBaseSolrUrl() {
    new Builder(null).build();
  }

  @Test
  @SuppressWarnings({"try"})
  public void testMissingQueueSize() {
    try (ConcurrentUpdateSolrClient client = new Builder("someurl").build()){
      // Do nothing as we just need to test that the only mandatory parameter for building the client
      // is the baseSolrUrl
    }
  }

  /**
   * Test that connection timeout information is passed to the HttpSolrClient that handles non add operations.
   */
  @LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-15848")
  @Test(timeout = 10000)
  public void testSocketTimeoutOnCommit() throws IOException, SolrServerException {
    InetAddress localHost = InetAddress.getLocalHost();
    try (ServerSocket server = new ServerSocket(0, 1, localHost);
         ConcurrentUpdateSolrClient client = new ConcurrentUpdateSolrClient.Builder(
             "http://" + localHost.getHostAddress() + ":" + server.getLocalPort() + "/noOneThere")
             .withSocketTimeout(1)
             .build()){
      // Expecting an exception
      client.commit();
      fail();
    }
    catch (SolrServerException e) {
      if (!(e.getCause() instanceof SocketTimeoutException)) {
        throw e;
      }
      // else test passses
    }
  }
}
