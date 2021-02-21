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

package org.apache.solr.search.facet;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;


public class TestJsonFacetErrors extends SolrTestCaseHS {

  private static SolrInstances servers;  // for distributed testing

  @SuppressWarnings("deprecation")
  @BeforeClass
  public static void beforeTestJsonFacetErrors() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
    JSONTestUtil.failRepeatedKeys = true;

    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");

    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }

  /**
   * Start all servers for cluster if they don't already exist
   */
  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
    }
  }

  @SuppressWarnings("deprecation")
  @AfterClass
  public static void afterTestJsonFacetErrors() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
    deleteCore();
  }

  public void indexSimple(Client client) throws Exception {
    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY", "num_d", "4", "num_i", "2",
        "num_is", "4", "num_is", "2",
        "val_b", "true", "sparse_s", "one"), null);
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ", "num_d", "-9", "num_i", "-5",
        "num_is", "-9", "num_is", "-5",
        "val_b", "false"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ", "num_d", "2", "num_i", "3",
        "num_is", "2", "num_is", "3"), null);
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ", "num_d", "11", "num_i", "7",
        "num_is", "11", "num_is", "7",
        "sparse_s", "two"),null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY", "num_d", "-5", "num_i", "-5",
        "num_is", "-5"),null);
    client.commit();
  }

  @Test
  public void testErrors() throws Exception {
    doTestErrors(Client.localClient());
  }

  public void doTestErrors(Client client) throws Exception {
    client.deleteByQuery("*:*", null);

    try {
      client.testJQ(params("ignore_exception", "true", "q", "*:*"
          , "json.facet", "{f:{type:ignore_exception_aaa, field:bbbbbb}}"
          )
      );
    } catch (SolrException e) {
      assertTrue( e.getMessage().contains("ignore_exception_aaa") );
    }

  }

  @Test
  public void testDomainErrors() throws Exception {
    Client client = Client.localClient();
    client.deleteByQuery("*:*", null);
    indexSimple(client);

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{cat_s:{type:terms,field:cat_s,domain:{filter:'{!query v=$NOfilt}'}}}")) {
      // using assertQEx so that, status code and error message can be asserted
      assertQEx("Should Fail as filter with qparser in domain becomes null", "QParser yields null, perhaps unresolved parameter reference in: {!query v=$NOfilt}", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{cat_s:{type:terms,field:cat_s,domain:{filter:'{!v=$NOfilt}'}}}")) {
      assertQEx("Should Fail as filter in domain becomes null", "QParser yields null, perhaps unresolved parameter reference in: {!v=$NOfilt}", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,domain:bleh}}")) {
      // when domain type is invalid
      assertQEx("Should Fail as domain not of type map", "Expected Map for 'domain', received String=bleh , path=facet/cat_s", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    // when domain = null, should not throw exception
    assertQ("Should pass as no domain is specified",
        req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s}}"));

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,domain:{blockChildren:{}}}}")) {
      // when blockChildren or blockParent is passed but not of string
      assertQEx("Should Fail as blockChildren is of type map", "Expected string type for param 'blockChildren' but got LinkedHashMap = {} , path=facet/cat_s", req,
          SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,domain:{blockParent:{}}}}")) {
      assertQEx("Should Fail as blockParent is of type map", "Expected string type for param 'blockParent' but got LinkedHashMap = {} , path=facet/cat_s", req, SolrException.ErrorCode.BAD_REQUEST);
    }
  }

  @Test
  public void testRangeFacetsErrorCases() throws Exception {
    Client client = Client.localClient();
    client.deleteByQuery("*:*", null);
    indexSimple(client);

    SolrParams params = params("q", "*:*", "rows", "0");
    SolrException ex;
    // invalid format for ranges
    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i,start:-10,end:10,gap:2," +
        "ranges:[{key:\"0-200\", to:200}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));

      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertEquals("Cannot set gap/start/end and ranges params together", ex.getMessage());
    }

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," +
        "ranges:bleh}}")){
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Expected List for ranges but got String"));

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," +
        "ranges:[bleh]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Expected Map for range but got String"));

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{from:0, to:200, inclusive_to:bleh}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Expected boolean type for param 'inclusive_to' but got String"));

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{from:0, to:200, inclusive_from:bleh}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Expected boolean type for param 'inclusive_from' but got String"));

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{from:bleh, to:200}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertEquals("Can't parse value bleh for field: num_i", ex.getMessage());

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{from:0, to:bleh}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertEquals("Can't parse value bleh for field: num_i", ex.getMessage());

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{from:200, to:0}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertEquals("'from' is higher than 'to' in range for key: [200,0)", ex.getMessage());

    // with old format
    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{range:\"\"}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("empty facet range"));

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{range:\"bl\"}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Invalid start character b in facet range bl"));

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{range:\"(bl\"}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Invalid end character l in facet range (bl"));

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{range:\"(bleh,12)\"}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertEquals("Can't parse value bleh for field: num_i", ex.getMessage());

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{range:\"(12,bleh)\"}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertEquals("Can't parse value bleh for field: num_i", ex.getMessage());

    try (SolrQueryRequest req = req(params, "json.facet", "{price:{type :range, field : num_i," + "ranges:[{range:\"(200,12)\"}]}}")) {
      ex = SolrTestCaseUtil.expectThrows(SolrException.class, () -> h.query(req));
    }
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertEquals("'start' is higher than 'end' in range for key: (200,12)", ex.getMessage());
  }

  @Test
  public void testOtherErrorCases() throws Exception {
    Client client = Client.localClient();
    client.deleteByQuery("*:*", null);
    indexSimple(client);

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,sort:[\"count desc\"]}}")) {
      // test for sort
      assertQEx("Should fail as sort is of type list", "Expected string/map for 'sort', received ArrayList=[count desc]", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "[{}]")) {
      assertQEx("Should fail as facet is not of type map", "Expected Map for 'facet', received ArrayList=[{}]", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.queries", "[{}]")) {
      assertQEx("Should fail as queries is not of type map", "Expected Map for 'queries', received [{}]", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("json", "{query:\"*:*\", queries:null}")) {
      assertQEx("Should fail as queries are null in JSON", "Expected Map for 'queries', received null", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{f:{type:range, field:num_d, start:10, end:12, gap:1, other:{}}}")) {
      // range facets
      assertQEx("Should fail as 'other' is of type Map", "Expected list of string or comma separated string values for 'other', " + "received LinkedHashMap={} , path=facet/f", req,
          SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{f:{type:range, field:num_d, start:10, end:12, gap:1, include:{}}}")) {
      assertQEx("Should fail as 'include' is of type Map", "Expected list of string or comma separated string values for 'include', " + "received LinkedHashMap={} , path=facet/f", req,
          SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{f:{type:range, field:num_d}}")) {
      // missing start parameter
      assertQEx("Should Fail with missing field error", "Missing required parameter: 'start' , path=facet/f", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{f:{type:range, field:num_d, start:10}}")) {
      // missing end parameter
      assertQEx("Should Fail with missing field error", "Missing required parameter: 'end' , path=facet/f", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{f:{type:range, field:num_d, start:10, end:12}}")) {
      // missing gap parameter
      assertQEx("Should Fail with missing field error", "Missing required parameter: 'gap' , path=facet/f", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet.facet.field", "2")) {
      // invalid value for facet field
      assertQEx("Should Fail as args is of type long", "Expected string/map for facet field, received Long=2 , path=facet/facet", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet.facet.query", "2")) {
      // invalid value for facet query
      assertQEx("Should Fail as args is of type long for query", "Expected string/map for facet query, received Long=2 , path=facet/facet", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    // valid facet field
    assertQ("Should pass as this is valid query", req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s}}"));

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,perSeg:2}}")) {
      // invalid perSeg
      assertQEx("Should fail as perSeg is not of type boolean", "Expected boolean type for param 'perSeg' but got Long = 2 , path=facet/cat_s", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,sort:bleh}}")) {
      assertQEx("Should fail as sort is invalid", "Invalid sort option 'bleh' for field 'cat_s'", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,sort:{count: bleh}}}")) {
      assertQEx("Should fail as sort order is invalid", "Unknown Sort direction 'bleh'", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,prelim_sort:bleh}}")) {
      // test for prelim_sort
      assertQEx("Should fail as prelim_sort is invalid", "Invalid prelim_sort option 'bleh' for field 'cat_s'", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,prelim_sort:{bleh:desc}}}")) {
      assertQEx("Should fail as prelim_sort map is invalid", "Invalid prelim_sort option '{bleh=desc}' for field 'cat_s'", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    try (SolrQueryRequest req = req("q", "*:*", "rows", "0", "json.facet", "{cat_s:{type:terms,field:cat_s,sort:bleh,facet:" + "{bleh:\"unique(cat_s)\",id:{type:terms,field:id,sort:bleh}}}}")) {
      // with nested facet
      assertQEx("Should fail as prelim_sort is invalid", "Invalid sort option 'bleh' for field 'id'", req, SolrException.ErrorCode.BAD_REQUEST);
    }

    assertQ("Should pass as sort is proper", req("q", "*:*", "rows", "0", "json.facet",
        "{cat_s:{type:terms,field:cat_s,sort:bleh,facet:" + "{bleh:\"unique(cat_s)\",id:{type:terms,field:id,sort:{bleh:desc},facet:{bleh:\"unique(id)\"}}}}}"));
  }

  @Test
  public void testAggErrors() {
    ignoreException("aggregation");

    SolrException e;
    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{bleh:'div(2,4)'}")) {
      e = SolrTestCaseUtil.expectThrows(SolrException.class, () -> {
        h.query(req);
      });
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertThat(e.getMessage(), containsString("Expected multi-doc aggregation from 'div' but got per-doc function in input ('div(2,4)"));
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{b:'agg(div(2,4))'}")) {
      e = SolrTestCaseUtil.expectThrows(SolrException.class, () -> {
        h.query(req);
      });
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertThat(e.getMessage(), containsString("Expected multi-doc aggregation from 'div' but got per-doc function in input ('agg(div(2,4))"));
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{b:'agg(bleh(2,4))'}")) {
      e = SolrTestCaseUtil.expectThrows(SolrException.class, () -> {
        h.query(req);
      });
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertThat(e.getMessage(), containsString("Unknown aggregation 'bleh' in input ('agg(bleh(2,4))"));
    }

    try (SolrQueryRequest req = req("q", "*:*", "json.facet", "{b:'bleh(2,4)'}")) {
      e = SolrTestCaseUtil.expectThrows(SolrException.class, () -> {
        h.query(req);
      });
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertThat(e.getMessage(), containsString("Unknown aggregation 'bleh' in input ('bleh(2,4)"));
    }

    resetExceptionIgnores();
  }
}
