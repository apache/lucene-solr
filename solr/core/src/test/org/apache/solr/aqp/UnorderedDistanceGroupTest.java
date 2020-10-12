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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.search.spans.SpanQuery;
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

public class UnorderedDistanceGroupTest extends SolrCloudTestCase {
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

  /**
   * Test that un-ordered near queries work. This is essentially exposing spanNear functionality.
   */
  @Test
  public void testBinaryNearQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(solrClient);

    assertUpdateResponse(solrClient.add(coll, sdoc("id", "1", "_text_", "foo bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "2", "_text_", "foo baz bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "3", "_text_", "foo baz bam bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "4", "_text_", "foo baz bam buz bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "5", "_text_", "bar foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "6", "_text_", "bar baz foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "7", "_text_", "bar baz bam foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "8", "_text_", "bar baz bam buz foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "9", "_text_", "foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "10", "_text_", "bar")));
    solrClient.commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = solrClient.query(coll, params("q", "foo", "sort", "id asc"));
    assertEquals(9, resp.getResults().size());

    // now use the advanced query parser
    resp = solrClient.query(coll, params("q", "N/3(foo bar)", "defType", "advanced"));

    Set<String> expectedIds = new HashSet<>(Arrays.asList("1", "2", "3", "5", "6", "7"));
    expectExactly(expectedIds, resp);

  }

  /**
   * Test that un-ordered near queries work. This is essentially exposing spanNear functionality for cases
   * with more than two clauses.
   *
   * @see org.apache.lucene.search.spans.SpanNearQuery.Builder#addClause(SpanQuery)
   */
  @Test
  public void testTernaryNearQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(solrClient);

    //unordered matches
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "11", "_text_", "foo bar bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "12", "_text_", "foo bap bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "13", "_text_", "bar foo bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "14", "_text_", "bar bap foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "15", "_text_", "bap foo bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "16", "_text_", "bap bar foo")));

    //unordered with one interleaved matches
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "21", "_text_", "foo baz bar bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "22", "_text_", "foo baz bap bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "23", "_text_", "bar baz foo bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "24", "_text_", "bar baz bap foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "25", "_text_", "bap baz foo bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "26", "_text_", "bap baz bar foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "27", "_text_", "foo bar baz bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "28", "_text_", "foo bap baz bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "29", "_text_", "bar foo baz bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "210", "_text_", "bar bap baz foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "211", "_text_", "bap foo baz bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "212", "_text_", "bap bar baz foo")));

    //unordered with two does not match (last one is too far from first one)
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "31", "_text_", "foo baz bar bam bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "32", "_text_", "foo baz bap bam bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "33", "_text_", "bar baz foo bam bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "34", "_text_", "bar baz bap bam foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "35", "_text_", "bap baz foo bam bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "36", "_text_", "bap baz bar bam foo")));

    solrClient.commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = solrClient.query(coll, params("q", "foo", "rows","100"));
    assertEquals(24, resp.getResults().size());

    // now use the advanced query parser
    resp = solrClient.query(coll, params("q", "N/3(foo bar bap)", "defType", "advanced", "rows", "100"));

    // this feature merely intends to expose the solr span query functionality, so we only need to test that
    // it gets the same result.
    String xml = "<SpanNear slop=\"2\" inOrder=\"false\" >" +
        "<SpanTerm fieldName=\"_text_\"><Term>foo</Term></SpanTerm>" +
        "<SpanTerm fieldName=\"_text_\"><Term>bar</Term></SpanTerm>" +
        "<SpanTerm fieldName=\"_text_\"><Term>bap</Term></SpanTerm>" +
        "</SpanNear>";

    resp = solrClient.query(coll, params("q", xml, "defType", "xmlparser", "rows", "100"));

    Set<String> expectedIds = resp.getResults().stream()
        .map((doc) -> (String) doc.getFirstValue("id")).collect(Collectors.toSet());

    expectExactly(expectedIds, resp);

  }

  /**
   * Testing cases where the sub-clauses are more complex. Specifically other unordered near queries. Again
   * this should not differ from SpanNearQuery funcitonality
   *
   * @throws Exception when it gets grumpy.
   */
  @Test
  public void testNestedNearQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(solrClient);

    //unordered matches all but those marked with //x should match... the //x ones violate the n/2(bap bag) part
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "11", "_text_", "foo bar bap bag")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "12", "_text_", "foo bap bar bag")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "13", "_text_", "bar foo bap bag")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "14", "_text_", "bar bap foo bag")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "15", "_text_", "bap foo bar bag")));//x
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "16", "_text_", "bap bar foo bag")));//x
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "21", "_text_", "foo bar bag bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "22", "_text_", "foo bap bag bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "23", "_text_", "bar foo bag bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "24", "_text_", "bar bap bag foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "25", "_text_", "bap foo bag bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "26", "_text_", "bap bar bag foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "31", "_text_", "foo bag bar bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "32", "_text_", "foo bag bap bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "33", "_text_", "bar bag foo bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "34", "_text_", "bar bag bap foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "35", "_text_", "bap bag foo bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "36", "_text_", "bap bag bar foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "41", "_text_", "bag foo bar bap")));//x
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "42", "_text_", "bag foo bap bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "43", "_text_", "bag bar foo bap")));//x
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "44", "_text_", "bag bar bap foo")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "45", "_text_", "bag bap foo bar")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "46", "_text_", "bag bap bar foo")));

    // using capitals to make it easier to see filler terms that are not part of the query below

    assertUpdateResponse(solrClient.add(coll, sdoc("id", "51", "_text_", "foo bar BAM bag bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "52", "_text_", "foo bag BAM bap bar")));

    assertUpdateResponse(solrClient.add(coll, sdoc("id", "61", "_text_", "foo BAM BAZ bag bar bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "62", "_text_", "foo BAM BAZ bap bar bag")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "63", "_text_", "bar BAM BAZ bag foo bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "64", "_text_", "bar BAM BAZ bap foo bag")));

    // now for some longish matches...
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "71", "_text_", "foo BAM BAZ BAT bar bag BUZ bap")));
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "72", "_text_", "foo BAM BAZ BAT bar BOG bag BUZ bap")));

    // this is the maximum spread that could match...
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "73", "_text_", "foo BAM BAZ BAT bar BOG BIZ bag BUZ bap")));

    // outer N/3() violated, no match
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "81", "_text_", "foo BAM BAZ BAT bar BOG BIZ BOY bag BUZ bap")));//x

    // inner N/4 violated, no match
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "82", "_text_", "foo BAM BAZ BAT BOY bar BOG BIZ bag BUZ bap")));//x

    // inner N/2 violated, no match
    assertUpdateResponse(solrClient.add(coll, sdoc("id", "83", "_text_", "foo BAM BAZ BAT bar BOG BIZ bag BUZ BOY bap")));//x

    solrClient.commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = solrClient.query(coll, params("q", "foo", "rows","100"));
    assertEquals(36, resp.getResults().size());

    // now use the advanced query parser
    resp = solrClient.query(coll, params("q", "N/3(N/4(foo bar) N/2(bap bag))", "defType", "advanced", "rows", "100"));

    Set<String> expectedIds = new HashSet<>(Arrays.asList(
        "11", "12", "13", "14",
        "21", "22", "23", "24", "25", "26",
        "31", "32", "33", "34", "35", "36",
        "42", "44", "45", "46",
        "51", "52",
        "61", "62", "63", "64",
        "71", "72", "73"
        // none of the 8x's should match
    ));
    expectExactly(expectedIds, resp);

  }

  public void idInResp(String id, QueryResponse resp) {
    haveAll(Collections.singleton(id), resp);
  }

  public void haveAll(Set<String> ids, QueryResponse resp) {
    assertTrue("response did not contain all ids",
        resp.getResults().stream()
            .map((doc) -> (String) doc.get("id"))
            .collect(Collectors.toSet())
            .containsAll(ids));
  }

  public void expectExactly(Set<String> ids, QueryResponse resp) {
    assertEquals(ids.size(), resp.getResults().size());
    haveAll(ids, resp);
  }


  @SuppressWarnings("rawtypes")
  void assertUpdateResponse(UpdateResponse rsp) {
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors, errors == null || errors.isEmpty());
  }

}
