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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

/**
 * Test for distributed ExpandComponent
 *
 * @see org.apache.solr.handler.component.ExpandComponent
 */
public class DistributedExpandComponentTest extends BaseDistributedSearchTestCase {

  public DistributedExpandComponentTest() {
    stress = 0;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCore("solrconfig-collapseqparser.xml", "schema11.xml");
  }

  @ShardsFixed(num = 3)
  public void test() throws Exception {
    _test("group_s", "g1", "g2", "g3", "g4");
    _test("group_s_dv", "g1", "g2", "g3", "g4");
    _test("group_i", "1", "0", "3", "-1"); // NOTE: using 0 to explicitly confim we don't assume null
    _test("group_ti_dv", "1", "-2", "0", "4"); // NOTE: using 0 to explicitly confim we don't assume null
  }
  
  private void _test(final String group,
                     final String aaa, final String bbb, final String ccc, final String ddd) throws Exception {
    
    del("*:*");

    index_specific(0,"id","1", "term_s", "YYYY", group, aaa, "test_i", "5",  "test_l", "10", "test_f", "2000");
    index_specific(0,"id","2", "term_s", "YYYY", group, aaa, "test_i", "50", "test_l", "100", "test_f", "200");
    index_specific(1,"id","5", "term_s", "YYYY", group, bbb, "test_i", "4",  "test_l", "10", "test_f", "2000");
    index_specific(1,"id","6", "term_s", "YYYY", group, bbb, "test_i", "10", "test_l", "100", "test_f", "200");
    index_specific(0,"id","7", "term_s", "YYYY", group, aaa, "test_i", "1",  "test_l", "100000", "test_f", "2000");
    index_specific(1,"id","8", "term_s", "YYYY", group, bbb, "test_i", "2",  "test_l", "100000", "test_f", "200");
    index_specific(2,"id","9", "term_s", "YYYY", group, ccc, "test_i", "1000", "test_l", "1005", "test_f", "3000");
    index_specific(2,"id","10","term_s", "YYYY", group, ccc, "test_i", "1500", "test_l", "1001", "test_f", "3200");

    // NOTE: nullPolicy=collapse will only be viable because all null docs are in collocated in shard #2
    index_specific(2,"id","88", "test_i", "1001", "test_l", "1001", "test_f", "3200");
    index_specific(2,"id","99", "test_i", "11", "test_l", "100", "test_f", "200");

    index_specific(2,"id","11","term_s", "YYYY", group, ccc, "test_i", "1300", "test_l", "1002", "test_f", "3300");
    index_specific(1,"id","12","term_s", "YYYY", group, ddd, "test_i", "15",  "test_l", "10", "test_f", "2000");
    index_specific(1,"id","13","term_s", "YYYY", group, ddd, "test_i", "16",  "test_l", "9", "test_f", "2000");
    index_specific(1,"id","14","term_s", "YYYY", group, ddd, "test_i", "1",  "test_l", "20", "test_f", "2000");


    commit();


    handle.put("explain", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("wt", SKIP);
    handle.put("distrib", SKIP);
    handle.put("shards.qt", SKIP);
    handle.put("shards", SKIP);
    handle.put("q", SKIP);
    handle.put("maxScore", SKIPVAL);
    handle.put("_version_", SKIP);
    handle.put("expanded", UNORDERED);

    query("q", "*:*", "fq", "{!collapse field="+group+"}", "defType", "edismax", "bf", "field(test_i)", "expand", "true", "fl","*,score");
    query("q", "*:*", "fq", "{!collapse field="+group+"}", "defType", "edismax", "bf", "field(test_i)", "expand", "true", "expand.sort", "test_l desc", "fl","*,score");
    query("q", "*:*", "fq", "{!collapse field="+group+"}", "defType", "edismax", "bf", "field(test_i)", "expand", "true", "expand.sort", "test_l desc", "expand.rows", "1", "fl","*,score");
    //Test no expand results
    query("q", "test_i:5", "fq", "{!collapse field="+group+"}", "defType", "edismax", "bf", "field(test_i)", "expand", "true", "expand.sort", "test_l desc", "expand.rows", "1", "fl","*,score");
    //Test zero results
    query("q", "test_i:5434343", "fq", "{!collapse field="+group+"}", "defType", "edismax", "bf", "field(test_i)", "expand", "true", "expand.sort", "test_l desc", "expand.rows", "1", "fl","*,score");
    //Test page 2
    query("q", "*:*", "start","1", "rows", "1", "fq", "{!collapse field="+group+"}", "defType", "edismax", "bf", "field(test_i)", "expand", "true", "fl","*,score");

    // multiple collapse and equal cost
    ModifiableSolrParams baseParams = params("q", "term_s:YYYY", "defType", "edismax", "expand", "true", "fl", "*,score",
        "bf", "field(test_i)", "expand.sort", "id asc");
    baseParams.set("fq", "{!collapse field="+group+"}", "{!collapse field=test_i}");
    query(baseParams);

    // multiple collapse and unequal cost case1
    baseParams.set("fq", "{!collapse cost=1000 field="+group+"}", "{!collapse cost=2000 field=test_i}");
    query(baseParams);

    // multiple collapse and unequal cost case2
    baseParams.set("fq", "{!collapse cost=1000 field="+group+"}", "{!collapse cost=200 field=test_i}");
    query(baseParams);

    ignoreException("missing expand field");
    SolrException e = expectThrows(SolrException.class, () -> query("q", "*:*", "expand", "true"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage().contains("missing expand field"));
    resetExceptionIgnores();

    // Since none of these queries will match any doc w/null in the group field, it shouldn't matter what nullPolicy is used...
    for (String np : Arrays.asList("", " nullPolicy=ignore", " nullPolicy=expand", " nullPolicy=collapse")) {
      
      //First basic test case.
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "term_s:YYYY");
      params.add("fq", "{!collapse field="+group+np+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      
      setDistributedParams(params);
      QueryResponse rsp = queryServer(params);
      assertCountAndOrder(4, rsp.getResults(), "10" /* c */, "2" /* a */, "13" /* d */, "6" /* b */);
      Map<String, SolrDocumentList> results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 2, results, "1", "7");
      assertExpandGroupCountAndOrder(bbb, 2, results, "5", "8");
      assertExpandGroupCountAndOrder(ccc, 2, results, "11", "9");
      assertExpandGroupCountAndOrder(ddd, 2, results, "12", "14");
      
      
      //Test expand.sort
      
      params = new ModifiableSolrParams();
      params.add("q", "term_s:YYYY");
      params.add("fq", "{!collapse field="+group+np+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      params.add("expand.sort", "test_l desc");
      setDistributedParams(params);
      rsp = queryServer(params);
      assertCountAndOrder(4, rsp.getResults(), "10" /* c */, "2" /* a */, "13" /* d */, "6" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 2, results, "7", "1");
      assertExpandGroupCountAndOrder(bbb, 2, results, "8", "5");
      assertExpandGroupCountAndOrder(ccc, 2, results, "9", "11");
      assertExpandGroupCountAndOrder(ddd, 2, results, "14", "12");


      //Test expand.rows
      
      params = new ModifiableSolrParams();
      params.add("q", "term_s:YYYY");
      params.add("fq", "{!collapse field="+group+np+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      params.add("expand.sort", "test_l desc");
      params.add("expand.rows", "1");
      setDistributedParams(params);
      rsp = queryServer(params);
      assertCountAndOrder(4, rsp.getResults(), "10" /* c */, "2" /* a */, "13" /* d */, "6" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 1, results, "7");
      assertExpandGroupCountAndOrder(bbb, 1, results, "8");
      assertExpandGroupCountAndOrder(ccc, 1, results, "9");
      assertExpandGroupCountAndOrder(ddd, 1, results, "14");
      
      //Test expand.rows = 0 - no docs only expand count
      
      params = new ModifiableSolrParams();
      params.add("q", "term_s:YYYY");
      params.add("fq", "{!collapse field="+group+np+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      params.add("expand.rows", "0");
      params.add("fl", "id");
      setDistributedParams(params);
      rsp = queryServer(params);
      assertCountAndOrder(4, rsp.getResults(), "10" /* c */, "2" /* a */, "13" /* d */, "6" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 0, results);
      assertExpandGroupCountAndOrder(bbb, 0, results);
      assertExpandGroupCountAndOrder(ccc, 0, results);
      assertExpandGroupCountAndOrder(ddd, 0, results);
      
      //Test expand.rows = 0 with expand.field
      
      params = new ModifiableSolrParams();
      params.add("q", "term_s:YYYY");
      params.add("fq", "test_l:10");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      params.add("expand.fq", "test_f:2000");
      params.add("expand.field", group);
      params.add("expand.rows", "0");
      params.add("fl", "id,score");
      setDistributedParams(params);
      rsp = queryServer(params);
      assertCountAndOrder(3, rsp.getResults(), "12" /* d */, "1" /* a */, "5" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, ddd);
      assertExpandGroupCountAndOrder(aaa, 0, results);
      assertExpandGroupCountAndOrder(ddd, 0, results);
      
      //Test key-only fl
      
      params = new ModifiableSolrParams();
      params.add("q", "term_s:YYYY");
      params.add("fq", "{!collapse field="+group+np+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      params.add("fl", "id");
      
      setDistributedParams(params);
      rsp = queryServer(params);
      assertCountAndOrder(4, rsp.getResults(), "10" /* c */, "2" /* a */, "13" /* d */, "6" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 2, results, "1", "7");
      assertExpandGroupCountAndOrder(bbb, 2, results, "5", "8");
      assertExpandGroupCountAndOrder(ccc, 2, results, "11", "9");
      assertExpandGroupCountAndOrder(ddd, 2, results, "12", "14");
      
      //Test distrib.singlePass true
      
      params = new ModifiableSolrParams();
      params.add("q", "term_s:YYYY");
      params.add("fq", "{!collapse field="+group+np+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      params.add("distrib.singlePass", "true");
      
      setDistributedParams(params);
      rsp = queryServer(params);
      assertCountAndOrder(4, rsp.getResults(), "10" /* c */, "2" /* a */, "13" /* d */, "6" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 2, results, "1", "7");
      assertExpandGroupCountAndOrder(bbb, 2, results, "5", "8");
      assertExpandGroupCountAndOrder(ccc, 2, results, "11", "9");
      assertExpandGroupCountAndOrder(ddd, 2, results, "12", "14");
    }

    { // queries matching all docs to test null groups from collapse and how it affects expand

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("expand", "true");
      setDistributedParams(params);

      // nullPolicy=expand
      params.add("fq", "{!collapse field="+group+" nullPolicy=expand}");
      
      QueryResponse rsp = queryServer(params);
      assertCountAndOrder(6, rsp.getResults(), "10" /* c */, "88" /* null */, "2" /* a */, "13" /* d */, "99" /* null */, "6" /* b */);
      Map<String, SolrDocumentList> results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 2, results, "1", "7");
      assertExpandGroupCountAndOrder(bbb, 2, results, "5", "8");
      assertExpandGroupCountAndOrder(ccc, 2, results, "11", "9");
      assertExpandGroupCountAndOrder(ddd, 2, results, "12", "14");

      // nullPolicy=collapse
      params.set("fq", "{!collapse field="+group+" nullPolicy=collapse}");
      
      rsp = queryServer(params);
      assertCountAndOrder(5, rsp.getResults(), "10" /* c */, "88" /* null */, "2" /* a */, "13" /* d */, "6" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd);
      assertExpandGroupCountAndOrder(aaa, 2, results, "1", "7");
      assertExpandGroupCountAndOrder(bbb, 2, results, "5", "8");
      assertExpandGroupCountAndOrder(ccc, 2, results, "11", "9");
      assertExpandGroupCountAndOrder(ddd, 2, results, "12", "14");

      // nullPolicy=collapse w/ expand.nullGroup=true...
      params.set("fq", "{!collapse field="+group+" nullPolicy=collapse}");
      params.set("expand.nullGroup", "true");
      
      rsp = queryServer(params);
      assertCountAndOrder(5, rsp.getResults(), "10" /* c */, "88" /* null */, "2" /* a */, "13" /* d */, "6" /* b */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, bbb, ccc, ddd, null);
      assertExpandGroupCountAndOrder(aaa, 2, results, "1", "7");
      assertExpandGroupCountAndOrder(bbb, 2, results, "5", "8");
      assertExpandGroupCountAndOrder(ccc, 2, results, "11", "9");
      assertExpandGroupCountAndOrder(ddd, 2, results, "12", "14");
      assertExpandGroupCountAndOrder(null, 1, results, "99");

      // nullPolicy=expand w/ expand.nullGroup=true (use small rows to ensure null expanded group)
      params.set("fq", "{!collapse field="+group+" nullPolicy=expand}");
      params.set("rows", "3");
      
      rsp = queryServer(params);
      assertCountAndOrder(3, rsp.getResults(), "10" /* c */, "88" /* null */, "2" /* a */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, ccc, null);
      assertExpandGroupCountAndOrder(aaa, 2, results, "1", "7");
      assertExpandGroupCountAndOrder(ccc, 2, results, "11", "9");
      assertExpandGroupCountAndOrder(null, 1, results, "99");

      // nullPolicy=expand w/ expand.nullGroup=true & expand.rows = 0 
      params.set("expand.rows", "0");
      
      rsp = queryServer(params);
      assertCountAndOrder(3, rsp.getResults(), "10" /* c */, "88" /* null */, "2" /* a */);
      results = rsp.getExpandedResults();
      assertExpandGroups(results, aaa, ccc, null);
      assertExpandGroupCountAndOrder(aaa, 0, results);
      assertExpandGroupCountAndOrder(ccc, 0, results);
      assertExpandGroupCountAndOrder(null, 0, results);
      
    }
    
  }

  private void assertExpandGroups(Map<String, SolrDocumentList> expandedResults, String... groups) throws Exception {
    for(int i=0; i<groups.length; i++) {
      if(!expandedResults.containsKey(groups[i])) {
        throw new Exception("Expanded Group Not Found:"+groups[i]+", Found:"+exportGroups(expandedResults));
      }
    }
  }

  private String exportGroups(Map<String, SolrDocumentList> groups) {
    StringBuilder buf = new StringBuilder();
    Iterator<String> it = groups.keySet().iterator();
    while(it.hasNext()) {
      String group = it.next();
      buf.append(group);
      if(it.hasNext()) {
        buf.append(",");
      }
    }
    return buf.toString();
  }

  private void assertExpandGroupCountAndOrder(final String group, final int count,
                                              final Map<String, SolrDocumentList>expandedResults,
                                              final String... docs) throws Exception {
    SolrDocumentList results = expandedResults.get(group);
    if(results == null) {
      throw new Exception("Group Not Found:"+group);
    }
    assertCountAndOrder(count, results, docs);
  }
  private void assertCountAndOrder(final int count, final SolrDocumentList results,
                                   final String... docs) throws Exception {
    assertEquals(results.toString(), count, results.size());

    for(int i=0; i<docs.length;i++) {
      String id = docs[i];
      SolrDocument doc = results.get(i);
      assertEquals("Id not in results or out of order", id, doc.getFieldValue("id").toString());
    }
  }
}
