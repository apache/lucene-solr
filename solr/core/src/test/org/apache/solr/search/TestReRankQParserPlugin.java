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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.util.RefCounted;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

    assertU(delQ("*:*"));
    assertU(commit());

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
    params.add("qt", "/elevate");
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




    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=4 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "4,1");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='4.0']", //Elevated
        "//result/doc[2]/float[@name='id'][.='1.0']", //Elevated
        "//result/doc[3]/float[@name='id'][.='6.0']",
        "//result/doc[4]/float[@name='id'][.='5.0']",
        "//result/doc[5]/float[@name='id'][.='3.0']",
        "//result/doc[6]/float[@name='id'][.='2.0']"  //Not in reRankeDocs
    );

    //Test Elevation with start beyond the rerank docs
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=3 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "4,1");

    assertQ(req(params), "*[count(//doc)=2]",
        "//result/doc[1]/float[@name='id'][.='3.0']",
        "//result/doc[2]/float[@name='id'][.='2.0']"  //Was not in reRankDocs
    );

    //Test Elevation with zero results
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=3 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}nada");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "4,1");

    assertQ(req(params), "*[count(//doc)=0]");



    //Pass in reRankDocs lower then the length being collected.
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=1 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='6.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']",
        "//result/doc[4]/float[@name='id'][.='3.0']",
        "//result/doc[5]/float[@name='id'][.='2.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );

    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=0 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='6.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']",
        "//result/doc[4]/float[@name='id'][.='3.0']",
        "//result/doc[5]/float[@name='id'][.='2.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );

    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=2 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:4^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='5.0']",
        "//result/doc[2]/float[@name='id'][.='6.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']",
        "//result/doc[4]/float[@name='id'][.='3.0']",
        "//result/doc[5]/float[@name='id'][.='2.0']",
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

    SolrInfoMBean info  = h.getCore().getInfoRegistry().get("queryResultCache");
    NamedList stats = info.getStatistics();

    long inserts = (Long) stats.get("inserts");

    assertTrue(inserts > 0);

    //Test range query
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6}");
    params.add("q", "test_ti:[0 TO 2000]");
    params.add("rqq", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "6");

    assertQ(req(params), "*[count(//doc)=5]",
        "//result/doc[1]/float[@name='id'][.='6.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']",
        "//result/doc[4]/float[@name='id'][.='2.0']",
        "//result/doc[5]/float[@name='id'][.='1.0']"
    );


    info  = h.getCore().getInfoRegistry().get("queryResultCache");
    stats = info.getStatistics();

    long inserts1 = (Long) stats.get("inserts");

    //Last query was added to the cache
    assertTrue(inserts1 > inserts);

    //Run same query and see if it was cached. This tests the query result cache hit with rewritten queries
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6}");
    params.add("q", "test_ti:[0 TO 2000]");
    params.add("rqq", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "6");

    assertQ(req(params), "*[count(//doc)=5]",
        "//result/doc[1]/float[@name='id'][.='6.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']",
        "//result/doc[4]/float[@name='id'][.='2.0']",
        "//result/doc[5]/float[@name='id'][.='1.0']"
    );

    info  = h.getCore().getInfoRegistry().get("queryResultCache");
    stats = info.getStatistics();
    long inserts2 = (Long) stats.get("inserts");
    //Last query was NOT added to the cache
    assertTrue(inserts1 == inserts2);


    //Test range query embedded in larger query
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6}");
    // function query for predictible scores (relative to id) independent of similarity
    params.add("q", "{!func}id");
    // constant score for each clause (unique per doc) for predictible scores independent of similarity
    // NOTE: biased in favor of doc id == 2
    params.add("rqq", "id:1^=10 id:2^=40 id:3^=30 id:4^=40 id:5^=50 id:6^=60");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "6");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='6.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']",
        "//result/doc[4]/float[@name='id'][.='2.0']", // reranked out of orig order
        "//result/doc[5]/float[@name='id'][.='3.0']",
        "//result/doc[6]/float[@name='id'][.='1.0']"
    );


    //Test with start beyond reRankDocs
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=3 reRankWeight=2}");
    params.add("q", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "id:1^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "5");

    assertQ(req(params), "*[count(//doc)=2]",
        "//result/doc[1]/float[@name='id'][.='2.0']",
        "//result/doc[2]/float[@name='id'][.='1.0']"
    );


    //Test ReRankDocs > docs returned

    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=6 reRankWeight=2}");
    params.add("q", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50");
    params.add("rqq", "id:1^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "1");

    assertQ(req(params), "*[count(//doc)=1]",
        "//result/doc[1]/float[@name='id'][.='1.0']"
    );



    //Test with zero results
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=3 reRankWeight=2}");
    params.add("q", "term_s:NNNN");
    params.add("rqq", "id:1^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "5");

    assertQ(req(params), "*[count(//doc)=0]");

  }

  @Test
  public void testOverRank() throws Exception {

    assertU(delQ("*:*"));
    assertU(commit());

    //Test the scenario that where we rank more documents then we return.

    String[] doc = {"id","1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    String[] doc1 = {"id","2", "term_s","YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));


    String[] doc4 = {"id","5", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc4));

    String[] doc5 = {"id","6", "term_s","YYYY", "group_s", "group2", "test_ti", "10", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc5));

    String[] doc6 = {"id","7", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc6));


    String[] doc7 = {"id","8", "term_s","YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc7));

    String[] doc8 = {"id","9", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc8));
    String[] doc9 = {"id","10", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc9));

    String[] doc10 = {"id","11", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc10));
    assertU(commit());


    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=11 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60 id:7^70 id:8^80 id:9^90 id:10^100 id:11^110");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "2");

    assertQ(req(params), "*[count(//doc)=2]",
        "//result/doc[1]/float[@name='id'][.='8.0']",
        "//result/doc[2]/float[@name='id'][.='2.0']"
    );

    //Test Elevation
    params = new ModifiableSolrParams();
    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=11 reRankWeight=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60 id:7^70 id:8^80 id:9^90 id:10^100 id:11^110");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "3");
    params.add("qt","/elevate");
    params.add("elevateIds", "1,4");

    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='1.0']", //Elevated
        "//result/doc[2]/float[@name='id'][.='4.0']", //Elevated
        "//result/doc[3]/float[@name='id'][.='8.0']"); //Boosted during rerank.
  }

  @Test
  public void testRerankQueryParsingShouldFailWithoutMandatoryReRankQueryParameter() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());

    String[] doc = {"id", "1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id", "2", "term_s", "YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("rq", "{!rerank reRankQuery=$rqq reRankDocs=200}");
    params.add("q", "term_s:YYYY");
    params.add("start", "0");
    params.add("rows", "2");

    try {
      h.query(req(params));
      fail("A syntax error should be thrown when reRankQuery parameter is not specified");
    } catch (SolrException e) {
      assertTrue(e.code() == SolrException.ErrorCode.BAD_REQUEST.code);
    }
  }

}
