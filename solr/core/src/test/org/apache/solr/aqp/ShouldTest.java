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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShouldTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CloudSolrClient solrClient;

  @Before
  public void doBefore() throws Exception {
    configureCluster(2).configure();
    solrClient = getCloudSolrClient(cluster);
    //log this to help debug potential causes of problems
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
  public void testShouldQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(solrClient);

    assertUpdateResponse(solrClient.add(coll, sdoc("id", "1", "_text_", "foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "2", "_text_", "foo bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "3", "_text_", "bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "4", "_text_", "baz")));

    solrClient.commit(coll);

    //Test that ~A ~B returns the equivalent of a logical OR
    QueryResponse shouldAShouldBResponse = solrClient.query(coll, params("q", " ~foo ~bar", "defType", "advanced"));
    SolrDocumentList shouldAShouldBResults = shouldAShouldBResponse.getResults();
    String[] shouldAShouldBExpcectedIds = new String[]{"1", "2", "3"};
    List<String> shouldAShouldBExpectedResultList = Arrays.asList(shouldAShouldBExpcectedIds);

    //~foo ~bar returns exactly three results
    assertEquals(3, shouldAShouldBResults.getNumFound());
    //~foo ~bar returns only results with at least one of those terms
    for (int i = 0; i < shouldAShouldBResults.getNumFound(); i++) {
      assertTrue(shouldAShouldBExpectedResultList.contains(shouldAShouldBResults.get(i).get("id")));
    }
    //Test that ~(A B) returns the equivalent of a logical OR
    shouldAShouldBResponse = solrClient.query(coll, params("q", " ~(foo bar)", "defType", "advanced"));
    shouldAShouldBResults = shouldAShouldBResponse.getResults();
    shouldAShouldBExpcectedIds = new String[]{"1", "2", "3"};
    shouldAShouldBExpectedResultList = Arrays.asList(shouldAShouldBExpcectedIds);

    //~(foo bar) returns exactly three results
    assertEquals(3, shouldAShouldBResults.getNumFound());
    //~(foo bar) returns only results with at least one of those terms
    for (int i = 0; i < shouldAShouldBResults.getNumFound(); i++) {
      assertTrue(shouldAShouldBExpectedResultList.contains(shouldAShouldBResults.get(i).get("id")));
    }

    //Test that ~(A B) returns the equivalent of a logical OR in the presence of default "and"
    shouldAShouldBResponse = solrClient.query(coll, params("q", " ~(foo bar)", "defType", "advanced", "q.op", "and"));
    shouldAShouldBResults = shouldAShouldBResponse.getResults();
    shouldAShouldBExpcectedIds = new String[]{"1", "2", "3"};
    shouldAShouldBExpectedResultList = Arrays.asList(shouldAShouldBExpcectedIds);

    //~foo ~bar returns exactly three results
    assertEquals(3, shouldAShouldBResults.getNumFound());
    //~foo ~bar returns only results with at least one of those terms
    for (int i = 0; i < shouldAShouldBResults.getNumFound(); i++) {
      assertTrue(shouldAShouldBExpectedResultList.contains(shouldAShouldBResults.get(i).get("id")));
    }

    //Test that +A ~B returns a super-set of documents returned by +A
    //(must (+), being the default, is not explicitly included in the queries)
    QueryResponse mustAShouldBResponse = solrClient.query(coll, params("q", "foo ~bar", "defType", "advanced"));
    SolrDocumentList mustAShouldBResults = mustAShouldBResponse.getResults();

    QueryResponse mustAResponse = solrClient.query(coll, params("q", "foo", "defType", "advanced"));
    SolrDocumentList mustAResults = mustAResponse.getResults();

    long mustAShouldBResultsSize = mustAShouldBResults.getNumFound();
    long mustAResultsSize = mustAResults.getNumFound();

    //+A  returns FEWER documents than +A ~B
    assertTrue(mustAResultsSize < mustAShouldBResultsSize);

    //+A returns exactly three documents
    assertEquals(3, mustAShouldBResultsSize);

    //Test that all documents returned by +A are present in +A ~B
    //build the list of mustAShouldBIds
    ArrayList<String> mustAShouldBIds = new ArrayList<String>();
    for (int i = 0; i < mustAShouldBResultsSize; i++) {
      mustAShouldBIds.add((String) mustAShouldBResults.get(i).get("id"));
    }
    //records in +foo must also be in +foo ~bar
    for (int i = 0; i < mustAResultsSize; i++) {
      assertTrue(mustAShouldBIds.contains((String) mustAResults.get(i).get("id")));
    }

    //Test that  +A ~B ranks documents containing B ahead of those that do not,
    //meaning that id:2 is returned first
    assertEquals("2", mustAShouldBResults.get(0).get("id"));

    QueryResponse mustAMustBResponse = solrClient.query(coll, params("q", "foo bar", "defType", "advanced"));
    SolrDocumentList mustAMustBResults = mustAMustBResponse.getResults();
    int mustAMustBResultsSize = mustAMustBResults.size();

    //Test that all documents returned by +A +B are present in +A ~B
    for (int i = 0; i < mustAMustBResultsSize; i++) {
      assertTrue(mustAShouldBIds.contains((String) mustAMustBResults.get(i).get("id")));
    }

  }


  @SuppressWarnings("rawtypes")
  void assertUpdateResponse(UpdateResponse rsp) {
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors, errors == null || errors.isEmpty());
  }

}
