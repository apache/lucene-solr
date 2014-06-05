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

package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import com.carrotsearch.hppc.IntOpenHashSet;

import java.io.IOException;
import java.util.*;
import java.util.Random;

public class TestReRankQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-collapseqparser.xml", "schema11.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testReRankQueries() throws Exception {
    String[] doc = {"id","1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "term_s","YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));

    String[] doc4 = {"id","5", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "term_s","YYYY", "group_s", "group2", "test_ti", "10", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc5));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=200}");
    params.add("q", "term_s:YYYY");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='3.0']",
        "//result/doc[2]/float[@name='id'][.='4.0']",
        "//result/doc[3]/float[@name='id'][.='2.0']",
        "//result/doc[4]/float[@name='id'][.='6.0']",
        "//result/doc[5]/float[@name='id'][.='1.0']",
        "//result/doc[6]/float[@name='id'][.='5.0']"
    );

    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='2.0']",
        "//result/doc[2]/float[@name='id'][.='6.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']",
        "//result/doc[4]/float[@name='id'][.='4.0']",
        "//result/doc[5]/float[@name='id'][.='3.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );

    //Test with sort by score.
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("sort", "score desc");
    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='2.0']",
        "//result/doc[2]/float[@name='id'][.='6.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']",
        "//result/doc[4]/float[@name='id'][.='4.0']",
        "//result/doc[5]/float[@name='id'][.='3.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );


    //Test with compound sort.
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("sort", "score desc,test_ti asc");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='2.0']",
        "//result/doc[2]/float[@name='id'][.='6.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']",
        "//result/doc[4]/float[@name='id'][.='4.0']",
        "//result/doc[5]/float[@name='id'][.='3.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );


    //Test with elevation

    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6 reRankWeight=50}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "1");
    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='1.0']",
        "//result/doc[2]/float[@name='id'][.='2.0']",
        "//result/doc[3]/float[@name='id'][.='6.0']",
        "//result/doc[4]/float[@name='id'][.='5.0']",
        "//result/doc[5]/float[@name='id'][.='4.0']",
        "//result/doc[6]/float[@name='id'][.='3.0']"

    );


    //Test TermQuery rqq
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='2.0']",
        "//result/doc[2]/float[@name='id'][.='6.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']",
        "//result/doc[4]/float[@name='id'][.='4.0']",
        "//result/doc[5]/float[@name='id'][.='3.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );


    //Test Elevation
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt","/elevate");
    params.add("elevateIds", "1,4");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='1.0']", //Elevated
        "//result/doc[2]/float[@name='id'][.='4.0']", //Elevated
        "//result/doc[3]/float[@name='id'][.='2.0']", //Boosted during rerank.
        "//result/doc[4]/float[@name='id'][.='6.0']",
        "//result/doc[5]/float[@name='id'][.='5.0']",
        "//result/doc[6]/float[@name='id'][.='3.0']"
    );


    //Test Elevation swapped
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt","/elevate");
    params.add("elevateIds", "4,1");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='4.0']", //Elevated
        "//result/doc[2]/float[@name='id'][.='1.0']", //Elevated
        "//result/doc[3]/float[@name='id'][.='2.0']", //Boosted during rerank.
        "//result/doc[4]/float[@name='id'][.='6.0']",
        "//result/doc[5]/float[@name='id'][.='5.0']",
        "//result/doc[6]/float[@name='id'][.='3.0']"
    );



    //Pass in reRankDocs lower then the length being collected.
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=0 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='2.0']",
        "//result/doc[2]/float[@name='id'][.='6.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']",
        "//result/doc[4]/float[@name='id'][.='4.0']",
        "//result/doc[5]/float[@name='id'][.='3.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );


    //Test reRankWeight of 0, reranking will have no effect.
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6 reRankWeight=0}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "5");

    assertQ(req(params), "*[count(//doc)=5]",
        "//result/doc[1]/float[@name='id'][.='6.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']",
        "//result/doc[4]/float[@name='id'][.='3.0']",
        "//result/doc[5]/float[@name='id'][.='2.0']"
    );

    //Test with start beyond reRankDocs
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=3 reRankWeight=2}");
    params.add("q", "*:*");
    params.add("rqq", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "5");

    assertQ(req(params), "*[count(//doc)=2]",
        "//result/doc[1]/float[@name='id'][.='2.0']",
        "//result/doc[2]/float[@name='id'][.='1.0']"
    );


  }
}
