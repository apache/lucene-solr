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

package org.apache.solr.aqp;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleQueryTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CloudSolrClient solrClient;

  @Before
  public void doBefore() throws Exception {
    configureCluster(2).configure();
    solrClient = getCloudSolrClient(cluster);
    // log this to help debug potential causes of problems
    log.info("SolrClient: {}", solrClient);
    if (log.isInfoEnabled()) {
      log.info("ClusterStateProvider {}", solrClient.getClusterStateProvider());
    }
  }

  @After
  public void doAfter() throws Exception {
    solrClient.close();
    shutdownCluster();
  }

  @Test
  public void testSimpleQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(solrClient);

    assertUpdateResponse(solrClient.add(coll, sdoc("id", "1", "_text_", "foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "2", "_text_", "bar")));
    solrClient.commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = solrClient.query(coll, params("q", "foo"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    resp = solrClient.query(coll, params("q", "foo", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));
  }
  @Test
  public void testWordW() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(solrClient);

    // tokens that start with w must still work
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "1", "_text_", "wane")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "2", "_text_", "bane")));
    solrClient.commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = solrClient.query(coll, params("q", "wane"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    resp = solrClient.query(coll, params("q", "wane", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));
  }

  @SuppressWarnings("rawtypes")
  void assertUpdateResponse(UpdateResponse rsp) {
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors, errors == null || errors.isEmpty());
  }

}
