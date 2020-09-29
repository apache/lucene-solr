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

import java.util.Set;

import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

public class OrderedDistanceGroupTest extends AbstractAqpTestCase {

  /**
   * Test that un-ordered near queries work. This is essentially exposing spanNear functionality.
   */
  @Test
  public void testBinaryNearQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "foo bar", "text_ss", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "foo baz bar", "text_ss", "foo baz bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "foo baz bam bar", "text_ss", "foo baz bam bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", "foo baz bam buz bar", "text_ss", "foo baz bam buz bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "_text_", "bar foo", "text_ss", "bar foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "_text_", "bar baz foo", "text_ss", "bar baz foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "_text_", "bar baz bam foo", "text_ss", "bar baz bam foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "_text_", "bar baz bam buz foo", "text_ss", "bar baz bam buz foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "9", "_text_", "foo", "text_ss", "foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "10", "_text_", "bar", "text_ss", "bar")));//x

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "11", "bill_text_txt", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "12", "bill_text_txt", "foo baz bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "13", "bill_text_txt", "foo baz bam bar")));
    getSolrClient().commit(coll);


    //Thread.sleep(50000000);
    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "foo"));
    assertEquals(9, resp.getResults().size());

    // now use the advanced query parser
    resp = getSolrClient().query(coll, params("q", "W/3(foo bar)", "defType", "advanced"));

    Set<String> expectedIds;
    expectMatchExactlyTheseDocs(resp, "1", "2", "3");

    resp = getSolrClient().query(coll, params("q", "bill_text_txt:W/3(foo bar)", "defType", "advanced"));

    expectMatchExactlyTheseDocs(resp, "11", "12", "13");

    try {
      getSolrClient().query(coll, params("q", "W/3(bill_text_txt:foo _text_:bar)", "defType", "advanced"));
      fail("switching fields within a proximity query makes no sense, and so specifying a field should throw an error");
    } catch (BaseHttpSolrClient.RemoteSolrException e) {
      // success
    }

  }

  /**
   * Test that un-ordered near queries work. This is essentially exposing spanNear functionality.
   */
  @Test
  public void testBinaryPhraseNearQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "foo bar", "text_ss", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "foo baz bar", "text_ss", "foo baz bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "foo baz bam bar", "text_ss", "foo baz bam bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", "foo baz bam buz bar", "text_ss", "foo baz bam buz bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "_text_", "bar foo", "text_ss", "bar foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "_text_", "bar baz foo", "text_ss", "bar baz foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "_text_", "bar baz bam foo", "text_ss", "bar baz bam foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "_text_", "bar baz bam buz foo", "text_ss", "bar baz bam buz foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "9", "_text_", "foo", "text_ss", "foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "10", "_text_", "bar", "text_ss", "bar")));//x

    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "foo"));
    assertEquals(9, resp.getResults().size());

    // now use the advanced query parser
    resp = getSolrClient().query(coll, params("q", "W/3(\"foo\" \"bar\")", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1", "2", "3");

    // ensure we handle phrases as terms in near queries (previous bug threw NPE).
    resp = getSolrClient().query(coll, params("q", "W/3(\"foo baz\" \"bar\")", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "2", "3", "4");

    resp = getSolrClient().query(coll, params("q", "W/3(\"foo\" \"baz bar\")", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "2");

    try {
      getSolrClient().query(coll, params("q", "W/3(bill_text_txt:foo _text_:bar)", "defType", "advanced"));
      fail("switching fields within a proximity query makes no sense, and so specifying a field should throw an error");
    } catch (BaseHttpSolrClient.RemoteSolrException e) {
      // success
    }

  }
  /**
   * Test that un-ordered near queries work. This is essentially exposing spanNear functionality.
   */
  @Test
  public void testSynonymsInsideNearQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "billTextContent_aqp", "foobar bar antifoo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "billTextContent_aqp", "foobaz bar anti-foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "billTextContent_aqp", "foocar bam bar anti foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "billTextContent_aqp", "foohaz bam bar")));

    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "bar","df","billTextContent_aqp"));
    assertEquals(4, resp.getResults().size());

    // now use the advanced query parser - phrase causes span queries
    resp = getSolrClient().query(coll, params("q", "+billTextContent_aqp:(\"anti-foo\")","df","billTextContent_aqp", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "2");

    resp = getSolrClient().query(coll, params("q", "+billTextContent_aqp:n/1(anti-foo bar)","df","billTextContent_aqp", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "2");

  }
  /**
   * Test that un-ordered near queries work. This is essentially exposing spanNear functionality.
   */
  @Test
  public void testBinarySubGroupNearQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1)

        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "11", "_text_", "foo bar bap", "text_ss", "foo bar bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "12", "_text_", "foo bap bar", "text_ss", "foo bap bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "13", "_text_", "bar foo bap", "text_ss", "bar foo bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "14", "_text_", "bar bap foo", "text_ss", "bar bap foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "15", "_text_", "bap foo bar", "text_ss", "bap foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "16", "_text_", "bap bar foo", "text_ss", "bap bar foo")));

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "21", "_text_", "foo baz bar bap", "text_ss", "foo baz bar bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "22", "_text_", "foo baz bap bar", "text_ss", "foo baz bap bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "23", "_text_", "bar baz foo bap", "text_ss", "bar baz foo bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "24", "_text_", "bar baz bap foo", "text_ss", "bar baz bap foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "25", "_text_", "bap baz foo bar", "text_ss", "bap baz foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "26", "_text_", "bap baz bar foo", "text_ss", "bap baz bar foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "27", "_text_", "foo bar baz bap", "text_ss", "foo bar baz bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "28", "_text_", "foo bap baz bar", "text_ss", "foo bap baz bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "29", "_text_", "bar foo baz bap", "text_ss", "bar foo baz bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "210", "_text_", "bar bap baz foo", "text_ss", "bar bap baz foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "211", "_text_", "bap foo baz bar", "text_ss", "bap foo baz bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "212", "_text_", "bap bar baz foo", "text_ss", "bap bar baz foo")));

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "31", "_text_", "foo baz bar bam bap", "text_ss", "foo baz bar bam bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "311", "_text_", "foo baz bam bar bap", "text_ss", "foo baz bam bar bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3111", "_text_", "foo baz bam bag bar bap", "text_ss", "foo baz bam bag bar bap")));// x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "31111", "_text_", "foo baz bam bar bag bad bap", "text_ss", "foo baz bam bar bag bad bap")));// x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "32", "_text_", "foo baz bap bam bar", "text_ss", "foo baz bap bam bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "33", "_text_", "bar baz foo bam bap", "text_ss", "bar baz foo bam bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "34", "_text_", "bar baz bap bam foo", "text_ss", "bar baz bap bam foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "35", "_text_", "bap baz foo bam bar", "text_ss", "bap baz foo bam bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "36", "_text_", "bap baz bar bam foo", "text_ss", "bap baz bar bam foo")));

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "41", "_text_", "foo bam bap bad bar", "text_ss", "bap baz bar bam foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "41", "_text_", "baz bam bap bad bar", "text_ss", "bap baz bar bam foo")));

    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "foo", "rows", "100"));
    assertEquals(27, resp.getResults().size());

    // now use the advanced query parser
    resp = getSolrClient().query(coll, params("q", "W/3(\"foo\" \"bar\")", "rows", "100", "defType", "advanced", "sort", "id asc"));
    expectMatchExactlyTheseDocs(resp, "11", "12", "15", "21", "211", "22", "25", "27", "28", "31", "311", "31111", "35");

    // ensure we handle phrases as terms in near queries (previous bug threw NPE).
    resp = getSolrClient().query(coll, params("q", "W/3(~(foo baz) bar)", "rows", "100", "defType", "advanced", "sort", "id asc"));
    expectMatchExactlyTheseDocs(resp, "11", "12", "15", "21", "211", "22", "25", "26", "27", "28", "31", "311", "3111", "31111", "32", "35", "36");

    // proximity queries are naturally "MUST" and thus adding + has no effect.
    resp = getSolrClient().query(coll, params("q", "W/3(foo +(baz bar))", "defType", "advanced"));
    QueryResponse resp2 = getSolrClient().query(coll, params("q", "W/3(foo baz bar)", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, resp2.getResults().stream().map((d) ->(String) d.getFieldValue("id")).toArray(String[]::new));

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

        .process(getSolrClient());

    //ordered matches
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "11", "_text_", "foo bar bap", "text_ss", "foo bar bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "12", "_text_", "foo bap bar", "text_ss", "foo bap bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "13", "_text_", "bar foo bap", "text_ss", "bar foo bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "14", "_text_", "bar bap foo", "text_ss", "bar bap foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "15", "_text_", "bap foo bar", "text_ss", "bap foo bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "16", "_text_", "bap bar foo", "text_ss", "bap bar foo")));//x

    //ordered with one interleaved matches
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "21", "_text_", "foo baz bar bap", "text_ss", "foo baz bar bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "22", "_text_", "foo baz bap bar", "text_ss", "foo baz bap bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "23", "_text_", "bar baz foo bap", "text_ss", "bar baz foo bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "24", "_text_", "bar baz bap foo", "text_ss", "bar baz bap foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "25", "_text_", "bap baz foo bar", "text_ss", "bap baz foo bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "26", "_text_", "bap baz bar foo", "text_ss", "bap baz bar foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "27", "_text_", "foo bar baz bap", "text_ss", "foo bar baz bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "28", "_text_", "foo bap baz bar", "text_ss", "foo bap baz bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "29", "_text_", "bar foo baz bap", "text_ss", "bar foo baz bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "210", "_text_", "bar bap baz foo", "text_ss", "bar bap baz foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "211", "_text_", "bap foo baz bar", "text_ss", "bap foo baz bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "212", "_text_", "bap bar baz foo", "text_ss", "bap bar baz foo")));//x

    //ordered with two does not match (last one is too far from first one)
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "31", "_text_", "foo baz bar bam bap", "text_ss", "foo baz bar bam bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "311", "_text_", "foo baz bam bar bap", "text_ss", "foo baz bam bar bap")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3111", "_text_", "foo baz bam bag bar bap", "text_ss", "foo baz bam bag bar bap")));// x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "31111", "_text_", "foo baz bam bar bag bad bap", "text_ss", "foo baz bam bar bag bad bap")));// x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "32", "_text_", "foo baz bap bam bar", "text_ss", "foo baz bap bam bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "33", "_text_", "bar baz foo bam bap", "text_ss", "bar baz foo bam bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "34", "_text_", "bar baz bap bam foo", "text_ss", "bar baz bap bam foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "35", "_text_", "bap baz foo bam bar", "text_ss", "bap baz foo bam bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "36", "_text_", "bap baz bar bam foo", "text_ss", "bap baz bar bam foo")));//x

    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "foo", "rows", "100"));
    assertEquals(27, resp.getResults().size());

    // this functionality should be identical to:

//        SpanTermQuery stq1 = new SpanTermQuery(new Term("foo"));
//        SpanTermQuery stq2 = new SpanTermQuery(new Term("bar"));
//        SpanTermQuery stq3 = new SpanTermQuery(new Term("bap"));
//        SpanQuery[] clauses = new SpanQuery[]{stq1,stq2,stq3};
//        SpanNearQuery snq = new SpanNearQuery(clauses, 2,true);
//

    String xml = "<SpanNear slop=\"2\" inOrder=\"true\" >" +
        "<SpanTerm fieldName=\"_text_\"><Term>foo</Term></SpanTerm>" +
        "<SpanTerm fieldName=\"_text_\"><Term>bar</Term></SpanTerm>" +
        "<SpanTerm fieldName=\"_text_\"><Term>bap</Term></SpanTerm>" +
        "</SpanNear>";

    resp = getSolrClient().query(coll, params("q", xml, "defType", "xmlparser", "rows", "100"));

    String[] expectedIds = resp.getResults().stream()
        .map((doc) -> (String) doc.getFirstValue("id")).toArray(String[]::new);

    expectMatchExactlyTheseDocs(resp, expectedIds);

    // now use the advanced query parser
    resp = getSolrClient().query(coll, params("q", "W/3(foo bar bap)", "defType", "advanced"));

    // the above works out to [11, 311, 27, 31, 21]
    expectMatchExactlyTheseDocs(resp, expectedIds);

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

        .process(getSolrClient());

    //unordered matches all but those marked with //x should match... the //x ones violate the n/2(bap bag) part
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "11", "_text_", "foo bar bap bag", "text_ss", "foo bar bap bag")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "12", "_text_", "foo bap bar bag", "text_ss", "foo bap bar bag")));//x (overlap not allowed)
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "13", "_text_", "bar foo bap bag", "text_ss", "bar foo bap bag")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "14", "_text_", "bar bap foo bag", "text_ss", "bar bap foo bag")));//x

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "15", "_text_", "bap foo bar bag", "text_ss", "bap foo bar bag")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "16", "_text_", "bap bar foo bag", "text_ss", "bap bar foo bag")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "21", "_text_", "foo bar bag bap", "text_ss", "foo bar bag bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "22", "_text_", "foo bap bag bar", "text_ss", "foo bap bag bar")));//x (overlap not allowed)

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "23", "_text_", "bar foo bag bap", "text_ss", "bar foo bag bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "24", "_text_", "bar bap bag foo", "text_ss", "bar bap bag foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "25", "_text_", "bap foo bag bar", "text_ss", "bap foo bag bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "26", "_text_", "bap bar bag foo", "text_ss", "bap bar bag foo")));//x

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "31", "_text_", "foo bag bar bap", "text_ss", "foo bag bar bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "32", "_text_", "foo bag bap bar", "text_ss", "foo bag bap bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "33", "_text_", "bar bag foo bap", "text_ss", "bar bag foo bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "34", "_text_", "bar bag bap foo", "text_ss", "bar bag bap foo")));//x

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "35", "_text_", "bap bag foo bar", "text_ss", "bap bag foo bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "36", "_text_", "bap bag bar foo", "text_ss", "bap bag bar foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "41", "_text_", "bag foo bar bap", "text_ss", "bag foo bar bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "42", "_text_", "bag foo bap bar", "text_ss", "bag foo bap bar")));//x

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "43", "_text_", "bag bar foo bap", "text_ss", "bag bar foo bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "44", "_text_", "bag bar bap foo", "text_ss", "bag bar bap foo")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "45", "_text_", "bag bap foo bar", "text_ss", "bag bap foo bar")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "46", "_text_", "bag bap bar foo", "text_ss", "bag bap bar foo")));//x

    // using capitals to make it easier to see filler terms that are not part of the query below

    // Note that vs the unordered test, some of these docs below have bap/bag in opposite order to facilitate a match
    // in this test. Not all counter (non-matching) cases are enumerated on the assumption that the above
    // permutations have explored the order requirement well already.

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "51", "_text_", "foo bar BAM bap bag", "text_ss", "foo bar BAM bap bag")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "52", "_text_", "foo bap BAM bag bar", "text_ss", "foo bap BAM bag bar")));//x (overlap not allowed)
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "53", "_text_", "foo bar BAM bag bap", "text_ss", "foo bar BAM bag bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "54", "_text_", "foo bag BAM bap bar", "text_ss", "foo bag BAM bap bar")));//x

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "61", "_text_", "foo BAM BAZ bag bar bap", "text_ss", "foo BAM BAZ bag bar bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "62", "_text_", "foo BAM BAZ bap bar bag", "text_ss", "foo BAM BAZ bap bar bag")));//x (overlap not allowed)
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "63", "_text_", "bar BAM BAZ bag foo bap", "text_ss", "bar BAM BAZ bag foo bap")));//x
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "64", "_text_", "bar BAM BAZ bap foo bag", "text_ss", "bar BAM BAZ bap foo bag")));//x

    // now for some longish matches...
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "71", "_text_", "foo BAM BAZ BAT bar bap BUZ bag", "text_ss", "foo BAM BAZ BAT bar bap BUZ bag")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "72", "_text_", "foo BAM BAZ BAT bar BOG bap BUZ bag", "text_ss", "foo BAM BAZ BAT bar BOG bap BUZ bag")));

    // this is the maximum spread that could match...
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "73", "_text_", "foo BAM BAZ BAT bar BOG BIZ bap BUZ bag", "text_ss", "foo BAM BAZ BAT bar BOG BIZ bap BUZ bag")));

    // outer N/3() violated, no match
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "81", "_text_", "foo BAM BAZ BAT bar BOG BIZ BOY bap BUZ bag", "text_ss", "foo BAM BAZ BAT bar BOG BIZ BOY bap BUZ bag")));//x

    // inner N/4 violated, no match
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "82", "_text_", "foo BAM BAZ BAT BOY bar BOG BIZ bap BUZ bag", "text_ss", "foo BAM BAZ BAT BOY bar BOG BIZ bap BUZ bag")));//x

    // inner N/2 violated, no match
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "83", "_text_", "foo BAM BAZ BAT bar BOG BIZ bap BUZ BOY bag", "text_ss", "foo BAM BAZ BAT bar BOG BIZ bap BUZ BOY bag")));//x

    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "foo", "rows", "100"));
    assertEquals(38, resp.getResults().size());

    // now use the advanced query parser
    resp = getSolrClient().query(coll, params("q", "W/3(W/4(foo bar) W/2(bap bag))", "defType", "advanced"));

    // none of the 8x's should match
    expectMatchExactlyTheseDocs(resp,  "11", "51", "71", "72", "73");

  }

  public void idInResp(String id, QueryResponse resp) {
    haveAll(resp, id);
  }

}