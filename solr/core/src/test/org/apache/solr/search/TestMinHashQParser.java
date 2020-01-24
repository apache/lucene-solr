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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMinHashQParser extends SolrTestCaseJ4 {

  /**
   * Initializes core and does some sanity checking of schema
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minhash.xml", "schema-minhash.xml");
  }

  @After
  public void afterTest() {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testBandSize() {
    // Examples from mining massive data sets
    assertEquals(5, MinHashQParser.computeBandSize(100, 0.8, 0.9995));
    assertEquals(5, MinHashQParser.computeBandSize(100, 0.7, 0.974));
    assertEquals(5, MinHashQParser.computeBandSize(100, 0.6, 0.8));
    assertEquals(5, MinHashQParser.computeBandSize(100, 0.5, 0.465));
    assertEquals(5, MinHashQParser.computeBandSize(100, 0.4, 0.185));
    assertEquals(5, MinHashQParser.computeBandSize(100, 0.3, 0.046));
    assertEquals(5, MinHashQParser.computeBandSize(100, 0.2, 0.005));
  }


  @Test
  public void testAnalysedMinHash() {
    assertU(adoc("id", "doc_1", "min_hash_analysed", "Min Hashing is great for spotted strings of exact matching words"));
    assertU(adoc("id", "doc_2", "min_hash_analysed", "Min Hashing is great for rabbits who like to spot strings of exact matching words"));
    assertU(commit());

    String gQuery = "*:*";
    SolrQueryRequest qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']");

    gQuery = "{!minhash field=\"min_hash_analysed\"}Min Hashing is great for spotted strings of exact matching words";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=512.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=255.0]");

    gQuery = "{!minhash field=\"min_hash_analysed\"}Min Hashing is great for";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=512.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=512.0]");

    gQuery = "{!minhash field=\"min_hash_analysed\" sim=\"0.9\" tp=\"0.9\"}Min Hashing is great for spotted strings of exact matching words";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=23.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=10.0]");

    gQuery = "{!minhash field=\"min_hash_analysed\" sim=\"0.9\"}Min Hashing is great for spotted strings of exact matching words";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=512.0]");

    gQuery = "{!minhash field=\"min_hash_analysed\" sim=\"0.9\" analyzer_field=\"min_hash_analysed\"}Min Hashing is great for spotted strings of exact matching words";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=512.0]");

    gQuery = "{!minhash field=\"min_hash_analysed\" sim=\"0.9\" analyzer_field=\"min_hash_string\"}Min Hashing is great for spotted strings of exact matching words";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='0']");
  }

  @Test
  public void testPreAnalysedMinHash() {
    assertU(adoc("id", "doc_1", "min_hash_string", "HASH1", "min_hash_string", "HASH2", "min_hash_string", "HASH3"));
    assertU(adoc("id", "doc_2", "min_hash_string", "HASH1", "min_hash_string", "HASH2", "min_hash_string", "HASH4"));
    assertU(commit());

    String gQuery = "*:*";
    SolrQueryRequest qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=1.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=1.0]");

    gQuery = "{!minhash field=\"min_hash_string\"}HASH1";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=1.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=1.0]");


    gQuery = "{!minhash field=\"min_hash_string\" sep=\",\"}HASH1,HASH2,HASH3";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=3.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=2.0]");
  }

  @Test
  public void testNestedQuery() {

    assertU(adoc("id", "doc_1", "min_hash_string", "HASH1", "min_hash_string", "HASH2", "min_hash_string", "HASH3"));
    assertU(adoc("id", "doc_2", "min_hash_string", "HASH1", "min_hash_string", "HASH2", "min_hash_string", "HASH4"));
    assertU(commit());

    String gQuery = "*:*";
    SolrQueryRequest qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=1.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=1.0]");

    gQuery = "*:* AND _query_:{!minhash field=\"min_hash_string\" sep=\",\"}HASH3";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=2.0]");

    gQuery = "*:* AND _query_:{!minhash field=\"min_hash_string\" sep=\",\" sep=\"0.9\" tp=\"0.9\"}HASH3";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=2.0]");

    gQuery = "*:* AND _query_:{!minhash field=\"min_hash_string\" sep=\",\" sep=\"0.1\" tp=\"0.1\"}HASH3";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=2.0]");

  }

  @Test
  public void testBasic() {

    assertU(adoc("id", "doc_1", "min_hash_analysed", "woof woof woof woof woof"));
    assertU(adoc("id", "doc_2", "min_hash_analysed", "woof woof woof woof woof puff"));
    assertU(adoc("id", "doc_3", "min_hash_analysed", "woof woof woof woof puff"));
    assertU(commit());

    String gQuery = "*:*";
    SolrQueryRequest qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=1.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=1.0]",
        "//result/doc[3]/str[@name='id'][.='doc_3']",
        "//result/doc[3]/float[@name='score'][.=1.0]");


    gQuery = "{!minhash field=\"min_hash_analysed\"}woof woof woof woof woof puff";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='doc_2']",
        "//result/doc[1]/float[@name='score'][.=512.0]",
        "//result/doc[2]/str[@name='id'][.='doc_1']",
        "//result/doc[2]/float[@name='score'][.=295.0]",
        "//result/doc[3]/str[@name='id'][.='doc_3']",
        "//result/doc[3]/float[@name='score'][.=217.0]");

    gQuery = "{!minhash field=\"min_hash_analysed\" sep=\",\"}℁팽徭聙↝ꇁ홱杯,跻\uF7E1ꠅ�찼薷\uE24Eꔾ";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='doc_2']",
        "//result/doc[1]/float[@name='score'][.=2.0]",
        "//result/doc[2]/str[@name='id'][.='doc_1']",
        "//result/doc[2]/float[@name='score'][.=1.0]",
        "//result/doc[3]/str[@name='id'][.='doc_3']",
        "//result/doc[3]/float[@name='score'][.=1.0]");

    gQuery = "{!minhash field=\"min_hash_analysed\" analyzer_field=\"min_hash_string\"}℁팽徭聙↝ꇁ홱杯";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='doc_1']",
        "//result/doc[1]/float[@name='score'][.=1.0]",
        "//result/doc[2]/str[@name='id'][.='doc_2']",
        "//result/doc[2]/float[@name='score'][.=1.0]");

  }


  @Test
  public void test() {

    String[] parts = new String[]{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};

    for (int i = 0; i < parts.length; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < parts.length - i; j++) {
        if (builder.length() > 0) {
          builder.append(" ");
        }
        builder.append(parts[i + j]);
        if (j >= 5 - 1) {
          assertU(adoc("id", "doc_" + i + "_" + j, "min_hash_analysed", builder.toString()));
        }
      }
    }

    assertU(commit());

    String gQuery = "*:*";
    SolrQueryRequest qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='21']");

    gQuery = "{!minhash field=\"min_hash_analysed\"}one two three four five";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='6']");

    gQuery = "{!minhash field=\"min_hash_analysed\"}two three four five six";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='10']");

    gQuery = "{!minhash field=\"min_hash_analysed\"}three four five six seven";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='12']");

    gQuery = "{!minhash field=\"min_hash_analysed\"}four five six seven eight";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='12']");

    gQuery = "{!minhash field=\"min_hash_analysed\"}five six seven eight nine";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='10']");

    gQuery = "{!minhash field=\"min_hash_analysed\"}six seven eight nine ten";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='6']");


    gQuery = "{!minhash field=\"min_hash_analysed\"}one two three four five six seven eight nine ten";
    qr = createRequest(gQuery);
    assertQ(qr, "//*[@numFound='21']",
        "//result/doc[1]/str[@name='id'][.='doc_0_9']",
        "//result/doc[1]/float[@name='score'][.=512.0]",
        "//result/doc[2]/str[@name='id'][.='doc_1_8']",
        "//result/doc[2]/float[@name='score'][.=425.0]",
        "//result/doc[3]/str[@name='id'][.='doc_0_8']",
        "//result/doc[3]/float[@name='score'][.=341.0]",
        "//result/doc[4]/str[@name='id'][.='doc_2_7']",
        "//result/doc[4]/float[@name='score'][.=331.0]",
        "//result/doc[5]/str[@name='id'][.='doc_0_7']",
        "//result/doc[5]/float[@name='score'][.=305.0]",
        "//result/doc[6]/str[@name='id'][.='doc_3_6']",
        "//result/doc[6]/float[@name='score'][.=274.0]",
        "//result/doc[7]/str[@name='id'][.='doc_1_7']",
        "//result/doc[7]/float[@name='score'][.=254.0]",
        "//result/doc[8]/str[@name='id'][.='doc_0_6']",
        "//result/doc[8]/float[@name='score'][.=238.0]",
        "//result/doc[9]/str[@name='id'][.='doc_1_6']",
        "//result/doc[9]/float[@name='score'][.=218.0]",
        "//result/doc[10]/str[@name='id'][.='doc_4_5']",
        "//result/doc[10]/float[@name='score'][.=207.0]",
        "//result/doc[11]/str[@name='id'][.='doc_0_5']",
        "//result/doc[11]/float[@name='score'][.=181.0]",

        "//result/doc[12]/str[@name='id'][.='doc_5_4']",
        "//result/doc[12]/float[@name='score'][.=171.0]",
        "//result/doc[13]/str[@name='id'][.='doc_2_6']",
        "//result/doc[13]/float[@name='score'][.=160.0]",
        "//result/doc[14]/str[@name='id'][.='doc_1_5']",
        "//result/doc[14]/float[@name='score'][.=151.0]",
        "//result/doc[15]/str[@name='id'][.='doc_2_5']",
        "//result/doc[15]/float[@name='score'][.=124.0]",
        "//result/doc[16]/str[@name='id'][.='doc_3_5']",
        "//result/doc[16]/float[@name='score'][.=103.0]",
        "//result/doc[17]/str[@name='id'][.='doc_1_4']",
        "//result/doc[17]/float[@name='score'][.=94.0]",
        "//result/doc[18]/str[@name='id'][.='doc_0_4']",
        "//result/doc[18]/float[@name='score'][.=87.0]",
        "//result/doc[19]/str[@name='id'][.='doc_3_4']",
        "//result/doc[19]/float[@name='score'][.=67.0]",
        "//result/doc[20]/str[@name='id'][.='doc_2_4']",
        "//result/doc[20]/float[@name='score'][.=57.0]"
        //     "//result/doc[21]/str[@name='id'][.='doc_0_8']",
        //     "//result/doc[21]/float[@name='score'][.=341.0]"
    );
  }

  @Test
  public void testBandsWrap() throws SyntaxError {

    NamedList<Object> par = new NamedList<>();
    par.add("sim", "0.8");
    par.add("tp", "0.694");
    par.add("sep", ",");
    par.add("debug", "false");

    QParser qparser = h.getCore().getQueryPlugin("minhash").createParser("1, 2, 3, 4, 5, 6, 7, 8, 9, 10", SolrParams.toSolrParams(par), null, null);
    Query query = qparser.getQuery();

    BooleanQuery bq = (BooleanQuery)query;
    assertEquals(4, bq.clauses().size());
    for(BooleanClause clause : bq.clauses()) {
      assertEquals(3, ((BooleanQuery)((ConstantScoreQuery)clause.getQuery()).getQuery())  .clauses().size());
    }

  }

  private SolrQueryRequest createRequest(String query) {
    SolrQueryRequest qr = req(query);
    NamedList<Object> par = qr.getParams().toNamedList();
    par.add("debug", "false");
    par.add("rows", "30");
    par.add("fl", "id,score");
    par.remove("qt");
    SolrParams newp = SolrParams.toSolrParams(par);
    qr.setParams(newp);
    return qr;
  }
}
