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

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.CollapsingQParserPlugin.GroupHeadSelector;
import org.apache.solr.search.CollapsingQParserPlugin.GroupHeadSelectorType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

//We want codecs that support DocValues, and ones supporting blank/empty values.
@SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42"})
public class TestCollapseQParserPlugin extends SolrTestCaseJ4 {

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

  public void testMultiSort() throws Exception {
    assertU(adoc("id", "1", "group_s", "group1", "test_ti", "5", "test_tl", "10"));
    assertU(commit());
    assertU(adoc("id", "2", "group_s", "group1", "test_ti", "5", "test_tl", "1000"));
    assertU(adoc("id", "3", "group_s", "group1", "test_ti", "5", "test_tl", "1000"));
    assertU(adoc("id", "4", "group_s", "group1", "test_ti", "10", "test_tl", "100"));
    //
    assertU(adoc("id", "5", "group_s", "group2", "test_ti", "5", "test_tl", "10", "term_s", "YYYY"));
    assertU(commit());
    assertU(adoc("id", "6", "group_s", "group2", "test_ti", "5", "test_tl","1000"));
    assertU(adoc("id", "7", "group_s", "group2", "test_ti", "5", "test_tl","1000", "term_s", "XXXX"));
    assertU(adoc("id", "8", "group_s", "group2", "test_ti", "10","test_tl", "100"));
    assertU(commit());
    
    ModifiableSolrParams params;
    
    // group heads are selected using the same sort that is then applied to the final groups
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort=$sort}");
    params.add("sort", "test_ti asc, test_tl desc, id desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='7.0']"
            ,"//result/doc[2]/float[@name='id'][.='3.0']"
            );
    
    // group heads are selected using a complex sort, simpler sort used for final groups
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='test_ti asc, test_tl desc, id desc'}");
    params.add("sort", "id asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='3.0']"
            ,"//result/doc[2]/float[@name='id'][.='7.0']"
            );

    // diff up the sort directions, only first clause matters with our data
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='test_ti desc, test_tl asc, id asc'}");
    params.add("sort", "id desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='8.0']"
            ,"//result/doc[2]/float[@name='id'][.='4.0']"
            );
    
    // tie broken by index order
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='test_tl desc'}");
    params.add("sort", "id desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='6.0']"
            ,"//result/doc[2]/float[@name='id'][.='2.0']"
            );

    // score, then tiebreakers; note top level sort by score ASCENDING (just for weirdness)
    params = new ModifiableSolrParams();
    params.add("q", "*:* term_s:YYYY");
    params.add("fq", "{!collapse field=group_s sort='score desc, test_tl desc, test_ti asc, id asc'}");
    params.add("sort", "score asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='2.0']"
            ,"//result/doc[2]/float[@name='id'][.='5.0']"
            );

    // score, then tiebreakers; note no score in top level sort/fl to check needsScores logic
    params = new ModifiableSolrParams();
    params.add("q", "*:* term_s:YYYY");
    params.add("fq", "{!collapse field=group_s sort='score desc, test_tl desc, test_ti asc, id asc'}");
    params.add("sort", "id desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='5.0']"
            ,"//result/doc[2]/float[@name='id'][.='2.0']"
            );
    
    // term_s desc -- term_s is missing from many docs, and uses sortMissingLast=true
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s desc, test_tl asc'}");
    params.add("sort", "id asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='1.0']"
            ,"//result/doc[2]/float[@name='id'][.='5.0']"
            );

    // term_s asc -- term_s is missing from many docs, and uses sortMissingLast=true
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s asc, test_tl asc'}");
    params.add("sort", "id asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='1.0']"
            ,"//result/doc[2]/float[@name='id'][.='7.0']"
            );

    // collapse on int field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=test_ti sort='term_s asc, group_s asc'}");
    params.add("sort", "id asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/float[@name='id'][.='4.0']"
            ,"//result/doc[2]/float[@name='id'][.='7.0']"
            );
    
    // collapse on term_s (very sparse) with nullPolicy=collapse
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=term_s nullPolicy=collapse sort='test_ti asc, test_tl desc, id asc'}");
    params.add("sort", "test_tl asc, id asc");
    assertQ(req(params)
            , "*[count(//doc)=3]"
            ,"//result/doc[1]/float[@name='id'][.='5.0']"
            ,"//result/doc[2]/float[@name='id'][.='2.0']"
            ,"//result/doc[3]/float[@name='id'][.='7.0']"
            );
    
    // sort local param + elevation
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s desc, test_tl asc'}");
    params.add("sort", "test_tl asc");
    params.add("qt", "/elevate");
    params.add("forceElevation", "true");
    params.add("elevateIds", "4.0");
    assertQ(req(params),
            "*[count(//doc)=2]",
            "//result/doc[1]/float[@name='id'][.='4.0']",
            "//result/doc[2]/float[@name='id'][.='5.0']");
    //
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s desc, test_tl asc'}");
    params.add("sort", "test_tl asc");
    params.add("qt", "/elevate");
    params.add("forceElevation", "true");
    params.add("elevateIds", "7.0");
    assertQ(req(params),
            "*[count(//doc)=2]",
            "//result/doc[1]/float[@name='id'][.='7.0']",
            "//result/doc[2]/float[@name='id'][.='1.0']");

  }

  @Test
  public void testStringCollapse() throws Exception {
    for (final String hint : new String[] {"", " hint="+CollapsingQParserPlugin.HINT_TOP_FC}) {
      testCollapseQueries("group_s", hint, false);
      testCollapseQueries("group_s_dv", hint, false);
    }
  }

  @Test
  public void testNumericCollapse() throws Exception {
    final String hint = "";
    testCollapseQueries("group_i", hint, true);
    testCollapseQueries("group_ti_dv", hint, true);
    testCollapseQueries("group_f", hint, true);
    testCollapseQueries("group_tf_dv", hint, true);
  }

  @Test
  public void testFieldValueCollapseWithNegativeMinMax() throws Exception {
    String[] doc = {"id","1", "group_i", "-1000", "test_ti", "5", "test_tl", "-10", "test_tf", "2000.32"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "group_i", "-1000", "test_ti", "50", "test_tl", "-100", "test_tf", "2000.33"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "group_i", "-1000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));

    String[] doc4 = {"id","5", "group_i", "-1000", "test_ti", "4", "test_tl", "10", "test_tf", "2000.31"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "group_i", "-1000", "test_ti", "10", "test_tl", "100", "test_tf", "-2000.12"};
    assertU(adoc(doc5));
    assertU(commit());

    String[] doc6 = {"id","7", "group_i", "-1000", "test_ti", "8", "test_tl", "-50", "test_tf", "-100.2"};
    assertU(adoc(doc6));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_i min=test_tf}");
    assertQ(req(params), "*[count(//doc)=1]",
        "//result/doc[1]/float[@name='id'][.='6.0']");

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_i max=test_tf}");
    assertQ(req(params), "*[count(//doc)=1]",
        "//result/doc[1]/float[@name='id'][.='2.0']");

  }

  @Test
  public void testMergeBoost() throws Exception {

    Set<Integer> boosted = new HashSet();
    Set<Integer> results = new HashSet();

    for(int i=0; i<200; i++) {
      boosted.add(random().nextInt(1000));
    }

    for(int i=0; i<200; i++) {
      results.add(random().nextInt(1000));
    }

    int[] boostedArray = new int[boosted.size()];
    int[] resultsArray = new int[results.size()];

    Iterator<Integer> boostIt = boosted.iterator();
    int index = 0;
    while(boostIt.hasNext()) {
      boostedArray[index++] = boostIt.next();
    }

    Iterator<Integer> resultsIt = results.iterator();
    index = 0;
    while(resultsIt.hasNext()) {
      resultsArray[index++] = resultsIt.next();
    }

    Arrays.sort(boostedArray);
    Arrays.sort(resultsArray);

    CollapsingQParserPlugin.MergeBoost mergeBoost = new CollapsingQParserPlugin.MergeBoost(boostedArray);

    List<Integer> boostedResults = new ArrayList();

    for(int i=0; i<resultsArray.length; i++) {
      int result = resultsArray[i];
      if(mergeBoost.boost(result)) {
        boostedResults.add(result);
      }
    }

    List<Integer> controlResults = new ArrayList();

    for(int i=0; i<resultsArray.length; i++) {
      int result = resultsArray[i];
      if(Arrays.binarySearch(boostedArray, result) > -1) {
        controlResults.add(result);
      }
    }

    if(boostedResults.size() == controlResults.size()) {
      for(int i=0; i<boostedResults.size(); i++) {
        if(!boostedResults.get(i).equals(controlResults.get(i).intValue())) {
          throw new Exception("boosted results do not match control results, boostedResults size:"+boostedResults.toString()+", controlResults size:"+controlResults.toString());
        }
      }
    } else {
      throw new Exception("boosted results do not match control results, boostedResults size:"+boostedResults.toString()+", controlResults size:"+controlResults.toString());
    }
  }



  private void testCollapseQueries(String group, String hint, boolean numeric) throws Exception {

    String[] doc = {"id","1", "term_s", "YYYY", group, "1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "term_s","YYYY", group, "1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));



    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));


    String[] doc4 = {"id","5", "term_s", "YYYY", group, "2", "test_ti", "4", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "term_s","YYYY", group, "2", "test_ti", "10", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc5));
    assertU(commit());

    String[] doc6 = {"id","7", "term_s", "YYYY", group, "1", "test_ti", "8", "test_tl", "50", "test_tf", "300"};
    assertU(adoc(doc6));
    assertU(commit());


    //Test collapse by score and following sort by score
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+""+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=2]",
                       "//result/doc[1]/float[@name='id'][.='2.0']",
                       "//result/doc[2]/float[@name='id'][.='6.0']"
        );


    // SOLR-5544 test ordering with empty sort param
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=expand min=test_tf"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("sort","");
    assertQ(req(params), "*[count(//doc)=4]",
        "//result/doc[1]/float[@name='id'][.='3.0']",
        "//result/doc[2]/float[@name='id'][.='4.0']",
        "//result/doc[3]/float[@name='id'][.='2.0']",
        "//result/doc[4]/float[@name='id'][.='6.0']"
    );

    // Test value source collapse criteria
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=field(test_ti)"+hint+"}");
    params.add("sort", "test_ti desc");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='4.0']",
        "//result/doc[2]/float[@name='id'][.='1.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']"
    );

    // Test value source collapse criteria with cscore function
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=cscore()"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='4.0']",
        "//result/doc[2]/float[@name='id'][.='1.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']"
    );
    
    // Test value source collapse criteria with cscore function but no top level score sort
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=cscore()"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("fl", "id");
    params.add("sort", "id desc");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='5.0']",
        "//result/doc[2]/float[@name='id'][.='4.0']",
        "//result/doc[3]/float[@name='id'][.='1.0']"
    );

    // Test value source collapse criteria with compound cscore function
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=sum(cscore(),field(test_ti))"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='4.0']",
        "//result/doc[2]/float[@name='id'][.='1.0']",
        "//result/doc[3]/float[@name='id'][.='5.0']"
    );

    //Test collapse by score with elevation

    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    assertQ(req(params), "*[count(//doc)=4]",
                         "//result/doc[1]/float[@name='id'][.='1.0']",
                         "//result/doc[2]/float[@name='id'][.='2.0']",
                         "//result/doc[3]/float[@name='id'][.='3.0']",
                         "//result/doc[4]/float[@name='id'][.='6.0']");

    //Test SOLR-5773 with score collapse criteria
    // try both default & sort localparams as alternate ways to ask for max score
    for (String maxscore : new String[] {"  ", " sort='score desc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "YYYY");
      params.add("fq", "{!collapse field="+group + maxscore + " nullPolicy=collapse"+hint+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_ti)");
      params.add("qf", "term_s");
      params.add("qt", "/elevate");
      params.add("elevateIds", "1,5");
      assertQ(req(params), "*[count(//doc)=3]",
              "//result/doc[1]/float[@name='id'][.='1.0']",
              "//result/doc[2]/float[@name='id'][.='5.0']",
              "//result/doc[3]/float[@name='id'][.='3.0']");
    }
    
    //Test SOLR-5773 with max field collapse criteria
    // try both max & sort localparams as alternate ways to ask for max group head
    for (String max : new String[] {" max=test_ti ", " sort='test_ti desc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "YYYY");
      params.add("fq", "{!collapse field=" + group + max + "nullPolicy=collapse"+hint+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_ti)");
      params.add("qf", "term_s");
      params.add("qt", "/elevate");
      params.add("elevateIds", "1,5");
      assertQ(req(params), "*[count(//doc)=3]",
              "//result/doc[1]/float[@name='id'][.='1.0']",
              "//result/doc[2]/float[@name='id'][.='5.0']",
              "//result/doc[3]/float[@name='id'][.='3.0']");
    }
    
    //Test SOLR-5773 with min field collapse criteria
    // try both min & sort localparams as alternate ways to ask for min group head
    for (String min : new String[] {" min=test_ti ", " sort='test_ti asc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "YYYY");
      params.add("fq", "{!collapse field=" + group + min + "nullPolicy=collapse"+hint+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_ti)");
      params.add("qf", "term_s");
      params.add("qt", "/elevate");
      params.add("elevateIds", "1,5");
      assertQ(req(params), "*[count(//doc)=3]",
              "//result/doc[1]/float[@name='id'][.='1.0']",
              "//result/doc[2]/float[@name='id'][.='5.0']",
              "//result/doc[3]/float[@name='id'][.='4.0']");
    }
    
    //Test SOLR-5773 elevating documents with null group
    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field="+group+""+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    params.add("elevateIds", "3,4");
    assertQ(req(params), "*[count(//doc)=4]",
        "//result/doc[1]/float[@name='id'][.='3.0']",
        "//result/doc[2]/float[@name='id'][.='4.0']",
        "//result/doc[3]/float[@name='id'][.='2.0']",
        "//result/doc[4]/float[@name='id'][.='6.0']");


    // Non trivial sort local param for picking group head
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse sort='term_s asc, test_ti asc' "+hint+"}");
    params.add("sort", "id desc");
    assertQ(req(params),
            "*[count(//doc)=3]",
            "//result/doc[1]/float[@name='id'][.='5.0']",
            "//result/doc[2]/float[@name='id'][.='4.0']",
            "//result/doc[3]/float[@name='id'][.='1.0']"
    );
    // 
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse sort='term_s asc, test_ti desc' "+hint+"}");
    params.add("sort", "id desc");
    assertQ(req(params),
            "*[count(//doc)=3]",
            "//result/doc[1]/float[@name='id'][.='6.0']",
            "//result/doc[2]/float[@name='id'][.='3.0']",
            "//result/doc[3]/float[@name='id'][.='2.0']"
    );
    


    // Test collapse by min int field and top level sort
    // try both min & sort localparams as alternate ways to ask for min group head
    for (String min : new String[] {" min=test_ti ", " sort='test_ti asc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "id desc");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/float[@name='id'][.='5.0']",
              "//result/doc[2]/float[@name='id'][.='1.0']");

      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "id asc");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/float[@name='id'][.='1.0']",
              "//result/doc[2]/float[@name='id'][.='5.0']");
      
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "test_tl asc,id desc");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/float[@name='id'][.='5.0']",
              "//result/doc[2]/float[@name='id'][.='1.0']");

      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "score desc,id asc");
      params.add("defType", "edismax");
      params.add("bf", "field(id)");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/float[@name='id'][.='5.0']",
              "//result/doc[2]/float[@name='id'][.='1.0']");
    }


    //Test collapse by max int field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_ti"+hint+"}");
    params.add("sort", "test_ti asc");
    assertQ(req(params), "*[count(//doc)=2]",
                         "//result/doc[1]/float[@name='id'][.='6.0']",
                         "//result/doc[2]/float[@name='id'][.='2.0']"
        );

    try {
      //Test collapse by min long field
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group+" min=test_tl"+hint+"}");
      params.add("sort", "test_ti desc");
      assertQ(req(params), "*[count(//doc)=2]",
          "//result/doc[1]/float[@name='id'][.='1.0']",
          "//result/doc[2]/float[@name='id'][.='5.0']");


      //Test collapse by max long field
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group+" max=test_tl"+hint+"}");
      params.add("sort", "test_ti desc");
      assertQ(req(params), "*[count(//doc)=2]",
                           "//result/doc[1]/float[@name='id'][.='2.0']",
                           "//result/doc[2]/float[@name='id'][.='6.0']");
    } catch (Exception e) {
      if(!numeric) {
        throw e;
      }
    }


    //Test collapse by min float field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" min=test_tf"+hint+"}");
    params.add("sort", "test_ti desc");
    assertQ(req(params), "*[count(//doc)=2]",
                         "//result/doc[1]/float[@name='id'][.='2.0']",
                         "//result/doc[2]/float[@name='id'][.='6.0']");

    //Test collapse by min float field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_tf"+hint+"}");
    params.add("sort", "test_ti asc");
    assertQ(req(params), "*[count(//doc)=2]",
                         "//result/doc[1]/float[@name='id'][.='5.0']",
                         "//result/doc[2]/float[@name='id'][.='1.0']");

    //Test collapse by min float field sort by score
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_tf"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(id)");
    params.add("fl", "score, id");
    params.add("facet","true");
    params.add("fq", "{!tag=test}term_s:YYYY");
    params.add("facet.field", "{!ex=test}term_s");

    assertQ(req(params), "*[count(//doc)=2]",
        "//result/doc[1]/float[@name='id'][.='5.0']",
        "//result/doc[2]/float[@name='id'][.='1.0']");
    
    // Test collapse using selector field in no docs
    // tie selector in all of these cases
    for (String selector : new String[] {
        " min=bogus_ti ", " sort='bogus_ti asc' ",
        " max=bogus_ti ", " sort='bogus_ti desc' ",
        " min=bogus_tf ", " sort='bogus_tf asc' ",
        " max=bogus_tf ", " sort='bogus_tf desc' ",
        " sort='bogus_td asc' ", " sort='bogus_td desc' ",
        " sort='bogus_s asc' ", " sort='bogus_s desc' ", 
      }) {
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + selector + hint+"}");
      params.add("sort", group + " asc");
      assertQ(req(params),
              "*[count(//doc)=2]",
              // since selector is bogus, group head is undefined
              // (should be index order, but don't make absolute assumptions: segments may be re-ordered)
              // key assertion is that there is one doc from each group & groups are in order
              "//result/doc[1]/*[@name='"+group+"'][starts-with(.,'1')]",
              "//result/doc[2]/*[@name='"+group+"'][starts-with(.,'2')]");
    }
    
    // attempting to use cscore() in sort local param should fail
    assertQEx("expected error trying to sort on a function that includes cscore()",
              req(params("q", "{!func}sub(sub(test_tl,1000),id)",
                         "fq", "{!collapse field="+group+" sort='abs(cscore()) asc, id asc'}",
                         "sort", "score asc")),
              SolrException.ErrorCode.BAD_REQUEST);
    
    // multiple params for picking groupHead should all fail
    for (String bad : new String[] {
        "{!collapse field="+group+" min=test_tf max=test_tf}",
        "{!collapse field="+group+" min=test_tf sort='test_tf asc'}",
        "{!collapse field="+group+" max=test_tf sort='test_tf asc'}" }) {
      assertQEx("Expected error: " + bad, req(params("q", "*:*", "fq", bad)),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    // multiple params for picking groupHead should work as long as only one is non-null
    // sort used
    for (SolrParams collapse : new SolrParams[] {
        // these should all be equivilently valid
        params("fq", "{!collapse field="+group+" nullPolicy=collapse sort='test_ti asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse min='' sort='test_ti asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse max='' sort='test_ti asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse min=$x sort='test_ti asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse min=$x sort='test_ti asc'"+hint+"}",
               "x",""),
      }) {
      
      assertQ(req(collapse, "q", "*:*", "sort", "test_ti desc"),
              "*[count(//doc)=3]",
              "//result/doc[1]/float[@name='id'][.='4.0']",
              "//result/doc[2]/float[@name='id'][.='1.0']",
              "//result/doc[3]/float[@name='id'][.='5.0']");
    }
    

    //Test nullPolicy expand
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_tf nullPolicy=expand"+hint+"}");
    params.add("sort", "id desc");
    assertQ(req(params), "*[count(//doc)=4]",
        "//result/doc[1]/float[@name='id'][.='5.0']",
        "//result/doc[2]/float[@name='id'][.='4.0']",
        "//result/doc[3]/float[@name='id'][.='3.0']",
        "//result/doc[4]/float[@name='id'][.='1.0']");

    //Test nullPolicy collapse
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_tf nullPolicy=collapse"+hint+"}");
    params.add("sort", "id desc");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='5.0']",
        "//result/doc[2]/float[@name='id'][.='4.0']",
        "//result/doc[3]/float[@name='id'][.='1.0']");


    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("fq","{!tag=test_ti}id:5");
    params.add("facet","true");
    params.add("facet.field","{!ex=test_ti}test_ti");
    params.add("facet.mincount", "1");
    assertQ(req(params), "*[count(//doc)=1]", "*[count(//lst[@name='facet_fields']/lst[@name='test_ti']/int)=2]");

    // SOLR-5230 - ensure CollapsingFieldValueCollector.finish() is called
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("group", "true");
    params.add("group.field", "id");
    assertQ(req(params), "*[count(//doc)=2]");


    // delete the elevated docs, confirm collapsing still works
    assertU(delI("1"));
    assertU(delI("2"));
    assertU(commit());
    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field="+group+hint+" nullPolicy=collapse}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    assertQ(req(params), "*[count(//doc)=3]",
                         "//result/doc[1]/float[@name='id'][.='3.0']",
                         "//result/doc[2]/float[@name='id'][.='6.0']",
                         "//result/doc[3]/float[@name='id'][.='7.0']");


  }

  @Test
  public void testMissingFieldParam() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse}");
    assertQEx("It should respond with a bad request when the 'field' param is missing", req(params),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testEmptyCollection() throws Exception {
    // group_s is docValues=false and group_dv_s is docValues=true
    String group = (random().nextBoolean() ? "group_s" : "group_s_dv");

    // min-or-max is for CollapsingScoreCollector vs. CollapsingFieldValueCollector
    String optional_min_or_max = (random().nextBoolean() ? "" : (random().nextBoolean() ? "min=field(test_ti)" : "max=field(test_ti)"));
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" "+optional_min_or_max+"}");
    assertQ(req(params), "*[count(//doc)=0]");
  }

  public void testNoDocsHaveGroupField() throws Exception {
    // as unlikely as this test seems, it's important for the possibility that a segment exists w/o
    // any live docs that have DocValues for the group field -- ie: every doc in segment is in null group.
    
    assertU(adoc("id", "1", "group_s", "group1", "test_ti", "5", "test_tl", "10"));
    assertU(commit());
    assertU(adoc("id", "2", "group_s", "group1", "test_ti", "5", "test_tl", "1000"));
    assertU(adoc("id", "3", "group_s", "group1", "test_ti", "5", "test_tl", "1000"));
    assertU(adoc("id", "4", "group_s", "group1", "test_ti", "10", "test_tl", "100"));
    //
    assertU(adoc("id", "5", "group_s", "group2", "test_ti", "5", "test_tl", "10", "term_s", "YYYY"));
    assertU(commit());
    assertU(adoc("id", "6", "group_s", "group2", "test_ti", "5", "test_tl","1000"));
    assertU(adoc("id", "7", "group_s", "group2", "test_ti", "5", "test_tl","1000", "term_s", "XXXX"));
    assertU(adoc("id", "8", "group_s", "group2", "test_ti", "10","test_tl", "100"));
    assertU(commit());
    
    // none of these grouping fields are in any doc
    for (String group : new String[] {
        "field=bogus_s", "field=bogus_s_dv",
        "field=bogus_s hint=top_fc", // alternative docvalues codepath w/ hint
        "field=bogus_s_dv hint=top_fc", // alternative docvalues codepath w/ hint
        "field=bogus_ti", "field=bogus_tf" }) {
      
      // for any of these selectors, behavior of these checks should be consistent
      for (String selector : new String[] {
          "", " sort='score desc' ",
          " min=test_ti ", " max=test_ti ", " sort='test_ti asc' ",  " sort='test_ti desc' ",
          " min=test_tf ", " max=test_tf ", " sort='test_tf asc' ",  " sort='test_tf desc' ",
          " sort='group_s asc' ",  " sort='group_s desc' ",
          // fields that don't exist
          " min=bogus_sort_ti ", " max=bogus_sort_ti ",
          " sort='bogus_sort_ti asc' ",  " sort='bogus_sort_ti desc' ",
          " sort='bogus_sort_s asc' ",  " sort='bogus_sort_s desc' ",
        }) {
          
          
        ModifiableSolrParams params = null;

        // w/default nullPolicy, no groups found
        params = new ModifiableSolrParams();
        params.add("q", "*:*");
        params.add("sort", "id desc");
        params.add("fq", "{!collapse "+group+" "+selector+"}");
        assertQ(req(params), "*[count(//doc)=0]");

        // w/nullPolicy=expand, every doc found
        params = new ModifiableSolrParams();
        params.add("q", "*:*");
        params.add("sort", "id desc");
        params.add("fq", "{!collapse field="+group+" nullPolicy=expand "+selector+"}");
        assertQ(req(params)
                , "*[count(//doc)=8]"
                ,"//result/doc[1]/float[@name='id'][.='8.0']"
                ,"//result/doc[2]/float[@name='id'][.='7.0']"
                ,"//result/doc[3]/float[@name='id'][.='6.0']"
                ,"//result/doc[4]/float[@name='id'][.='5.0']"
                ,"//result/doc[5]/float[@name='id'][.='4.0']"
                ,"//result/doc[6]/float[@name='id'][.='3.0']"
                ,"//result/doc[7]/float[@name='id'][.='2.0']"
                ,"//result/doc[8]/float[@name='id'][.='1.0']"
                );

        
      }
    }
  }

  public void testGroupHeadSelector() {
    GroupHeadSelector s;
    
    try {
      s = GroupHeadSelector.build(params("sort", "foo_s asc", "min", "bar_s"));
      fail("no exception with multi criteria");
    } catch (SolrException e) {
      // expected
    }
    
    s = GroupHeadSelector.build(params("min", "foo_s"));
    assertEquals(GroupHeadSelectorType.MIN, s.type);
    assertEquals("foo_s", s.selectorText);

    s = GroupHeadSelector.build(params("max", "foo_s"));
    assertEquals(GroupHeadSelectorType.MAX, s.type);
    assertEquals("foo_s", s.selectorText);
    assertFalse(s.equals(GroupHeadSelector.build(params("min", "foo_s", "other", "stuff"))));

    s = GroupHeadSelector.build(params());
    assertEquals(GroupHeadSelectorType.SCORE, s.type);
    assertNotNull(s.selectorText);
    assertEquals(GroupHeadSelector.build(params()), s);
    assertFalse(s.equals(GroupHeadSelector.build(params("min", "BAR_s"))));

    s = GroupHeadSelector.build(params("sort", "foo_s asc"));
    assertEquals(GroupHeadSelectorType.SORT, s.type);
    assertEquals("foo_s asc", s.selectorText);
    assertEquals(GroupHeadSelector.build(params("sort", "foo_s asc")),
                 s);
    assertFalse(s.equals(GroupHeadSelector.build(params("sort", "BAR_s asc"))));
    assertFalse(s.equals(GroupHeadSelector.build(params("min", "BAR_s"))));
    assertFalse(s.equals(GroupHeadSelector.build(params())));

    assertEquals(GroupHeadSelector.build(params("sort", "foo_s asc")).hashCode(),
                 GroupHeadSelector.build(params("sort", "foo_s asc",
                                                "other", "stuff")).hashCode());
    
  }

}
