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
package org.apache.solr.search.join;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphQueryTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeTests() throws Exception {
    
    initCore("solrconfig.xml","schema_latest.xml");
  }

  @Test
  public void testGraph() throws Exception {
    doGraph( params("node_id","node_s", "edge_id","edge_ss") );
    // TODO: try with numeric fields...   doGraph( params("node_id","node_i", "edge_id","edge_is") );
  }

  public void doGraph(SolrParams p) throws Exception {
    String node_id = p.get("node_id");
    String edge_id = p.get("edge_id");
    
    // 1 -> 2 -> 3 -> ( 4 5 )
    // 7 -> 1
    // 8 -> ( 1 2 )
    assertU(adoc("id", "doc_1", node_id, "1", edge_id, "2", "text", "foo", "title", "foo10" ));
    assertU(adoc("id", "doc_2", node_id, "2", edge_id, "3", "text", "foo" ));
    assertU(commit());
    assertU(adoc("id", "doc_3", node_id, "3", edge_id, "4", edge_id, "5"));
    assertU(adoc("id", "doc_4", node_id, "4" ));
    assertU(commit());
    assertU(adoc("id", "doc_5", node_id, "5", edge_id, "7" ));
    assertU(adoc("id", "doc_6", node_id, "6", edge_id, "3" ));
    assertU(adoc("id", "doc_7", node_id, "7", edge_id, "1" ));
    assertU(adoc("id", "doc_8", node_id, "8", edge_id, "1", edge_id, "2" ));
    assertU(adoc("id", "doc_9", node_id, "9"));
    assertU(commit());
    // update docs so they're in a new segment.
    assertU(adoc("id", "doc_1", node_id, "1", edge_id, "2", "text", "foo"));
    assertU(adoc("id", "doc_2", node_id, "2", edge_id, "3", edge_id, "9", "text", "foo11"));
    assertU(commit());
    // a graph for testing traversal filter 10 - 11 -> (12 | 13)
    assertU(adoc("id", "doc_10", node_id, "10", edge_id, "11", "title", "foo"));
    assertU(adoc("id", "doc_11", node_id, "11", edge_id, "12", edge_id, "13", "text", "foo11"));
    assertU(adoc("id", "doc_12", node_id, "12", "text", "foo10"));
    assertU(adoc("id", "doc_13", node_id, "13", edge_id, "12", "text", "foo10"));  
    assertU(commit());
    // Now we have created a simple graph
    // start traversal from node id to edge id

    // TODO: assert which documents actually come back
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id}}id:doc_1")
        , "/response/numFound==7"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=true returnOnlyLeaf=false}id:doc_8")
        , "/response/numFound==8"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=false returnOnlyLeaf=false}id:doc_8")
        , "/response/numFound==7"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=true returnOnlyLeaf=false traversalFilter='text:foo11'}id:doc_8")
        , "/response/numFound==2"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=true returnOnlyLeaf=false maxDepth=0}id:doc_8")
        , "/response/numFound==1"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=true returnOnlyLeaf=false maxDepth=1}id:doc_8")
        , "/response/numFound==3"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=false returnOnlyLeaf=false maxDepth=1}id:doc_8")
        , "/response/numFound==2"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=false returnOnlyLeaf=true maxDepth=2}id:doc_8")
        , "/response/numFound==1"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} maxDepth=1}id:doc_1")
        , "/response/numFound==2"
    );
    assertJQ(req(p, "q","{!graph from=${node_id} to=${edge_id} returnRoot=false maxDepth=1}id:doc_1")
        , "/response/numFound==1"
    );
  }
  
}
