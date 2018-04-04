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
package org.apache.solr.handler.component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;

import org.junit.BeforeClass;
import org.junit.AfterClass;

/**
 * Tests specifying a custom ShardHandlerFactory
 */
public class TestHttpShardHandlerFactory extends SolrTestCaseJ4 {

  private static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE = "solr.tests.loadBalancerRequestsMinimumAbsolute";
  private static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION = "solr.tests.loadBalancerRequestsMaximumFraction";

  private static int   expectedLoadBalancerRequestsMinimumAbsolute = 0;
  private static float expectedLoadBalancerRequestsMaximumFraction = 1.0f;

  @BeforeClass
  public static void beforeTests() throws Exception {
    expectedLoadBalancerRequestsMinimumAbsolute = random().nextInt(3); // 0 .. 2
    expectedLoadBalancerRequestsMaximumFraction = (1+random().nextInt(10))/10f; // 0.1 .. 1.0
    System.setProperty(LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE, Integer.toString(expectedLoadBalancerRequestsMinimumAbsolute));
    System.setProperty(LOAD_BALANCER_REQUESTS_MAX_FRACTION, Float.toString(expectedLoadBalancerRequestsMaximumFraction));
  }

  @AfterClass
  public static void afterTests() {
    System.clearProperty(LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE);
    System.clearProperty(LOAD_BALANCER_REQUESTS_MAX_FRACTION);
  }

  public void testLoadBalancerRequestsMinMax() throws Exception {
    final Path home = Paths.get(TEST_HOME());
    CoreContainer cc = null;
    ShardHandlerFactory factory = null;
    try {
      cc = CoreContainer.createAndLoad(home, home.resolve("solr-shardhandler-loadBalancerRequests.xml"));
      factory = cc.getShardHandlerFactory();

      // test that factory is HttpShardHandlerFactory with expected url reserve fraction
      assertTrue(factory instanceof HttpShardHandlerFactory);
      final HttpShardHandlerFactory httpShardHandlerFactory = ((HttpShardHandlerFactory)factory);
      assertEquals(expectedLoadBalancerRequestsMinimumAbsolute, httpShardHandlerFactory.permittedLoadBalancerRequestsMinimumAbsolute, 0.0);
      assertEquals(expectedLoadBalancerRequestsMaximumFraction, httpShardHandlerFactory.permittedLoadBalancerRequestsMaximumFraction, 0.0);

      // create a dummy request and dummy url list
      final QueryRequest queryRequest = null;
      final List<String> urls = new ArrayList<>();
      for (int ii=0; ii<10; ++ii) {
        urls.add(null);
      }

      // create LBHttpSolrClient request
      final LBHttpSolrClient.Req req = httpShardHandlerFactory.newLBHttpSolrClientReq(queryRequest, urls);

      // actual vs. expected test
      final int actualNumServersToTry = req.getNumServersToTry().intValue();
      int expectedNumServersToTry = (int)Math.floor(urls.size() * expectedLoadBalancerRequestsMaximumFraction);
      if (expectedNumServersToTry < expectedLoadBalancerRequestsMinimumAbsolute) {
        expectedNumServersToTry = expectedLoadBalancerRequestsMinimumAbsolute;
      }
      assertEquals("wrong numServersToTry for"
          + " urls.size="+urls.size()
          + " expectedLoadBalancerRequestsMinimumAbsolute="+expectedLoadBalancerRequestsMinimumAbsolute
          + " expectedLoadBalancerRequestsMaximumFraction="+expectedLoadBalancerRequestsMaximumFraction,
          expectedNumServersToTry,
          actualNumServersToTry);

    } finally {
      if (factory != null) factory.close();
      if (cc != null) cc.shutdown();
    }
  }

}
