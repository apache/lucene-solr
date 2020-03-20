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

import java.util.Iterator;
import java.util.Map;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

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

  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    final String group = (random().nextBoolean() ? "group_s" : "group_s_dv");
    
    del("*:*");

    index_specific(0,"id","1", "term_s", "YYYY", group, "group1", "test_i", "5",  "test_l", "10", "test_f", "2000");
    index_specific(0,"id","2", "term_s", "YYYY", group, "group1", "test_i", "50", "test_l", "100", "test_f", "200");
    index_specific(1,"id","5", "term_s", "YYYY", group, "group2", "test_i", "4",  "test_l", "10", "test_f", "2000");
    index_specific(1,"id","6", "term_s", "YYYY", group, "group2", "test_i", "10", "test_l", "100", "test_f", "200");
    index_specific(0,"id","7", "term_s", "YYYY", group, "group1", "test_i", "1",  "test_l", "100000", "test_f", "2000");
    index_specific(1,"id","8", "term_s", "YYYY", group, "group2", "test_i", "2",  "test_l", "100000", "test_f", "200");
    index_specific(2,"id","9", "term_s", "YYYY", group, "group3", "test_i", "1000", "test_l", "1005", "test_f", "3000");
    index_specific(2, "id", "10", "term_s", "YYYY", group, "group3", "test_i", "1500", "test_l", "1001", "test_f", "3200");
    index_specific(2,"id", "11",  "term_s", "YYYY", group, "group3", "test_i", "1300", "test_l", "1002", "test_f", "3300");
    index_specific(1,"id","12", "term_s", "YYYY", group, "group4", "test_i", "15",  "test_l", "10", "test_f", "2000");
    index_specific(1,"id","13", "term_s", "YYYY", group, "group4", "test_i", "16",  "test_l", "9", "test_f", "2000");
    index_specific(1,"id","14", "term_s", "YYYY", group, "group4", "test_i", "1",  "test_l", "20", "test_f", "2000");


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


    ignoreException("missing expand field");
    SolrException e = expectThrows(SolrException.class, () -> query("q", "*:*", "expand", "true"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage().contains("missing expand field"));
    resetExceptionIgnores();

    //First basic test case.
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");

    setDistributedParams(params);
    QueryResponse rsp = queryServer(params);
    Map<String, SolrDocumentList> results = rsp.getExpandedResults();
    assertExpandGroups(results, "group1","group2", "group3", "group4");
    assertExpandGroupCountAndOrder("group1", 2, results, "1", "7");
    assertExpandGroupCountAndOrder("group2", 2, results, "5", "8");
    assertExpandGroupCountAndOrder("group3", 2, results, "11", "9");
    assertExpandGroupCountAndOrder("group4", 2, results, "12", "14");


    //Test expand.sort

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.sort", "test_l desc");
    setDistributedParams(params);
    rsp = queryServer(params);
    results = rsp.getExpandedResults();
    assertExpandGroups(results, "group1","group2", "group3", "group4");
    assertExpandGroupCountAndOrder("group1", 2, results, "7", "1");
    assertExpandGroupCountAndOrder("group2", 2, results, "8", "5");
    assertExpandGroupCountAndOrder("group3", 2, results, "9", "11");
    assertExpandGroupCountAndOrder("group4", 2, results, "14", "12");


    //Test expand.rows

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.sort", "test_l desc");
    params.add("expand.rows", "1");
    setDistributedParams(params);
    rsp = queryServer(params);
    results = rsp.getExpandedResults();
    assertExpandGroups(results, "group1","group2", "group3", "group4");
    assertExpandGroupCountAndOrder("group1", 1, results, "7");
    assertExpandGroupCountAndOrder("group2", 1, results, "8");
    assertExpandGroupCountAndOrder("group3", 1, results, "9");
    assertExpandGroupCountAndOrder("group4", 1, results, "14");


    //Test key-only fl

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("fl", "id");

    setDistributedParams(params);
    rsp = queryServer(params);
    results = rsp.getExpandedResults();
    assertExpandGroups(results, "group1","group2", "group3", "group4");
    assertExpandGroupCountAndOrder("group1", 2, results, "1", "7");
    assertExpandGroupCountAndOrder("group2", 2, results, "5", "8");
    assertExpandGroupCountAndOrder("group3", 2, results, "11", "9");
    assertExpandGroupCountAndOrder("group4", 2, results, "12", "14");

    //Test distrib.singlePass true

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("distrib.singlePass", "true");

    setDistributedParams(params);
    rsp = queryServer(params);
    results = rsp.getExpandedResults();
    assertExpandGroups(results, "group1","group2", "group3", "group4");
    assertExpandGroupCountAndOrder("group1", 2, results, "1", "7");
    assertExpandGroupCountAndOrder("group2", 2, results, "5", "8");
    assertExpandGroupCountAndOrder("group3", 2, results, "11", "9");
    assertExpandGroupCountAndOrder("group4", 2, results, "12", "14");

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

  private void assertExpandGroupCountAndOrder(String group, int count, Map<String, SolrDocumentList>expandedResults, String... docs) throws Exception {
    SolrDocumentList results = expandedResults.get(group);
    if(results == null) {
      throw new Exception("Group Not Found:"+group);
    }

    if(results.size() != count) {
      throw new Exception("Expected Count "+results.size()+" Not Found:"+count);
    }

    for(int i=0; i<docs.length;i++) {
      String id = docs[i];
      SolrDocument doc = results.get(i);
      if(!doc.getFieldValue("id").toString().equals(id)) {
        throw new Exception("Id not in results or out of order:"+id+"!="+doc.getFieldValue("id"));
      }
    }
  }
}
