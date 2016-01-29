package org.apache.solr.search.join;

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphQueryTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeTests() throws Exception {
    
    initCore("solrconfig.xml","schema-graph.xml");
  }
  
  @Test
  public void testGraph() throws Exception {
    // 1 -> 2 -> 3 -> ( 4 5 )
    // 7 -> 1
    // 8 -> ( 1 2 )
    assertU(adoc("id", "doc_1", "node_id", "1", "edge_id", "2", "text", "foo", "title", "foo10"));
    assertU(adoc("id", "doc_2", "node_id", "2", "edge_id", "3", "text", "foo"));
    assertU(commit());
    assertU(adoc("id", "doc_3", "node_id", "3", "edge_id", "4", "edge_id", "5", "table", "foo"));
    assertU(adoc("id", "doc_4", "node_id", "4", "table", "foo"));
    assertU(commit());
    assertU(adoc("id", "doc_5", "node_id", "5", "edge_id", "7", "table", "bar"));
    assertU(adoc("id", "doc_6", "node_id", "6", "edge_id", "3" ));
    assertU(adoc("id", "doc_7", "node_id", "7", "edge_id", "1" ));
    assertU(adoc("id", "doc_8", "node_id", "8", "edge_id", "1", "edge_id", "2" ));
    assertU(adoc("id", "doc_9", "node_id", "9"));
    assertU(commit());
    // update docs so they're in a new segment.
    assertU(adoc("id", "doc_1", "node_id", "1", "edge_id", "2", "text", "foo"));
    assertU(adoc("id", "doc_2", "node_id", "2", "edge_id", "3", "edge_id", "9", "text", "foo11"));
    assertU(commit());
    // a graph for testing traversal filter 10 - 11 -> (12 | 13)
    assertU(adoc("id", "doc_10", "node_id", "10", "edge_id", "11", "title", "foo"));
    assertU(adoc("id", "doc_11", "node_id", "11", "edge_id", "12", "edge_id", "13", "text", "foo11"));
    assertU(adoc("id", "doc_12", "node_id", "12", "text", "foo10"));
    assertU(adoc("id", "doc_13", "node_id", "13", "edge_id", "12", "text", "foo10"));  
    assertU(commit());
    // Now we have created a simple graph
    // start traversal from node id to edge id
    String gQuery = "{!graph from=\"node_id\" to=\"edge_id\"}id:doc_1";
    SolrQueryRequest qr = createRequest(gQuery);
    assertQ(qr,"//*[@numFound='7']");
    
    String g2Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=\"true\" returnOnlyLeaf=\"false\"}id:doc_8";
    qr = createRequest(g2Query);    
    assertQ(qr,"//*[@numFound='8']");

    String g3Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=\"false\" returnOnlyLeaf=\"false\"}id:doc_8";
    qr = createRequest(g3Query);    
    assertQ(qr,"//*[@numFound='7']");
    
    String g4Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=\"true\" returnOnlyLeaf=\"false\" traversalFilter=\"text:foo11\"}id:doc_8";
    qr = createRequest(g4Query);    
    assertQ(qr,"//*[@numFound='2']");
    
    String g5Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=\"true\" returnOnlyLeaf=\"false\" maxDepth=0}id:doc_8";
    qr = createRequest(g5Query);    
    assertQ(qr,"//*[@numFound='1']");  

    String g6Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=\"true\" returnOnlyLeaf=\"false\" maxDepth=1}id:doc_8";
    qr = createRequest(g6Query);    
    assertQ(qr,"//*[@numFound='3']");
    
    String g7Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=\"false\" returnOnlyLeaf=\"false\" maxDepth=1}id:doc_8";
    qr = createRequest(g7Query);    
    assertQ(qr,"//*[@numFound='2']");

    String g8Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=\"false\" returnOnlyLeaf=\"true\" maxDepth=2}id:doc_8";
    qr = createRequest(g8Query);    
    assertQ(qr,"//*[@numFound='1']");

    String g9Query = "{!graph from=\"node_id\" to=\"edge_id\" maxDepth=1}id:doc_1";
    qr = createRequest(g9Query);    
    assertQ(qr,"//*[@numFound='2']");
    
    String g10Query = "{!graph from=\"node_id\" to=\"edge_id\" returnRoot=false maxDepth=1}id:doc_1";
    qr = createRequest(g10Query);    
    assertQ(qr,"//*[@numFound='1']");
  }

  private SolrQueryRequest createRequest(String query) {
    SolrQueryRequest qr = req(query);
    NamedList<Object> par = qr.getParams().toNamedList();
    par.add("debug", "true");
    par.add("rows", "10");
    par.add("fl", "id,node_id,edge_id");
    par.remove("qt");
    SolrParams newp = SolrParams.toSolrParams(par);
    qr.setParams(newp);
    return qr;
  }
  
}
