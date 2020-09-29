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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * base class for all test classes. startup/shutdown solr cluster, utility
 * methods such as checking if response contains the desired ids
 */
public abstract class AbstractAqpTestCase extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CloudSolrClient solrClient;

  @Before
  public void doBefore() throws Exception {
    configureCluster(2)
        .configure();
    Path path = TEST_PATH();
    Path configSet = path.resolve("configsets");
    String zkAddr = cluster.getZkServer().getZkAddress();
    AbstractDistribZkTestBase.copyConfigUp(configSet, "aqp", "aqp", zkAddr);
    solrClient = cluster.getSolrClient();
    // log this to help debug potential causes of problems
    log.info("SolrClient: {}", solrClient);
    if (log.isInfoEnabled()) {
      log.info("ClusterStateProvider {}", solrClient.getClusterStateProvider());
    }
    ConfigSetAdminResponse.List list = new ConfigSetAdminRequest.List().process(getSolrClient());
    assertTrue(
        list.getConfigSets()
            .contains("aqp")
    );
  }

  @After
  public void doAfter() throws Exception {
    solrClient.close();
    shutdownCluster();
  }

  public CloudSolrClient getSolrClient() {
    return this.solrClient;
  }

  void assertUpdateResponse(UpdateResponse rsp) {
    @SuppressWarnings("rawtypes")
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors, errors == null || errors.isEmpty());
  }

  /**
   * ids from query response are all in the provided set
   *
   * @param resp the response to test
   * @param ids  the ids that should be in the response
   */
  public void haveAll(QueryResponse resp, String... ids) {
    Set<String> idsFromResp = resp.getResults().stream().map((d) -> (String) d.getFirstValue("id"))
        .collect(Collectors.toSet());
    // sort the id lists so that the failure message is easy to compare.
    idsFromResp = new TreeSet<>(idsFromResp);
    List<String> idList = Arrays.asList(ids);
    Collections.sort(idList);
    assertTrue("ids from query response (" + idsFromResp + ") should all be in the provided set (" + Arrays.toString(ids) + ")",
        idsFromResp.containsAll(idList));
  }

  /**
   * none of the ids from the query response is in the provided set
   *
   * @param resp the response to test
   * @param ids  the ids that should not be in the response
   */
  public void haveNone(QueryResponse resp, String... ids) {
    resp.getResults().forEach(doc ->
        assertFalse("none of the ids from the query response should be in the provided set",
            Arrays.asList(ids).contains((String) doc.get("id"))));
  }

  /**
   * ids from query response are all in the provided set, and set only contains these ids - exactly matching
   *
   * @param resp the response to test
   * @param ids  the ids that should match the ids in the response
   */
  public void expectMatchExactlyTheseDocs(QueryResponse resp, String... ids) {
    List<String> idsFromResp = resp.getResults().stream().map((d) -> (String) d.getFirstValue("id"))
        .collect(Collectors.toList());
    assertEquals("Wrong number of result documents, expected " + ids.length + " (" + Arrays.toString(ids) + ")" +
        " got " + idsFromResp.size() + " (" + idsFromResp + ")", ids.length, resp.getResults().size());
    haveAll(resp, ids);
  }

  void aqpSearch(String collName, String searchTerm, String... matches)
      throws SolrServerException, IOException {
    QueryResponse resp = getSolrClient().query(collName, params(
        "q", searchTerm,
        "deftype", "advanced",
        "sort", "id asc",
        "rows", "100"));
    expectMatchExactlyTheseDocs(resp, matches);
  }
}
