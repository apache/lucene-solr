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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.CollapsingQParserPlugin.GroupHeadSelector;
import org.apache.solr.search.CollapsingQParserPlugin.GroupHeadSelectorType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCollapseQParserPlugin extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
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
    assertU(adoc("id", "1", "group_s", "group1", "test_i", "5", "test_l", "10"));
    assertU(commit());
    assertU(adoc("id", "2", "group_s", "group1", "test_i", "5", "test_l", "1000"));
    assertU(adoc("id", "3", "group_s", "group1", "test_i", "5", "test_l", "1000"));
    assertU(adoc("id", "4", "group_s", "group1", "test_i", "10", "test_l", "100"));
    //
    assertU(adoc("id", "5", "group_s", "group2", "test_i", "5", "test_l", "10", "term_s", "YYYY"));
    assertU(commit());
    assertU(adoc("id", "6", "group_s", "group2", "test_i", "5", "test_l","1000"));
    assertU(adoc("id", "7", "group_s", "group2", "test_i", "5", "test_l","1000", "term_s", "XXXX"));
    assertU(adoc("id", "8", "group_s", "group2", "test_i", "10","test_l", "100"));
    assertU(commit());
    
    ModifiableSolrParams params;
    
    // group heads are selected using the same sort that is then applied to the final groups
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort=$sort}");
    params.add("sort", "test_i asc, test_l desc, id_i desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='7']"
            ,"//result/doc[2]/str[@name='id'][.='3']"
            );
    
    // group heads are selected using a complex sort, simpler sort used for final groups
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='test_i asc, test_l desc, id_i desc'}");
    params.add("sort", "id_i asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='3']"
            ,"//result/doc[2]/str[@name='id'][.='7']"
            );

    // diff up the sort directions, only first clause matters with our data
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='test_i desc, test_l asc, id_i asc'}");
    params.add("sort", "id_i desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='8']"
            ,"//result/doc[2]/str[@name='id'][.='4']"
            );
    
    // tie broken by index order
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='test_l desc'}");
    params.add("sort", "id_i desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='6']"
            ,"//result/doc[2]/str[@name='id'][.='2']"
            );

    // score, then tiebreakers; note top level sort by score ASCENDING (just for weirdness)
    params = new ModifiableSolrParams();
    params.add("q", "*:* term_s:YYYY");
    params.add("fq", "{!collapse field=group_s sort='score desc, test_l desc, test_i asc, id_i asc'}");
    params.add("sort", "score asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='2']"
            ,"//result/doc[2]/str[@name='id'][.='5']"
            );

    // score, then tiebreakers; note no score in top level sort/fl to check needsScores logic
    params = new ModifiableSolrParams();
    params.add("q", "*:* term_s:YYYY");
    params.add("fq", "{!collapse field=group_s sort='score desc, test_l desc, test_i asc, id_i asc'}");
    params.add("sort", "id_i desc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='5']"
            ,"//result/doc[2]/str[@name='id'][.='2']"
            );
    
    // term_s desc -- term_s is missing from many docs, and uses sortMissingLast=true
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s desc, test_l asc'}");
    params.add("sort", "id_i asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='1']"
            ,"//result/doc[2]/str[@name='id'][.='5']"
            );

    // term_s asc -- term_s is missing from many docs, and uses sortMissingLast=true
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s asc, test_l asc'}");
    params.add("sort", "id_i asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='1']"
            ,"//result/doc[2]/str[@name='id'][.='7']"
            );

    // collapse on int field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=test_i sort='term_s asc, group_s asc'}");
    params.add("sort", "id_i asc");
    assertQ(req(params)
            , "*[count(//doc)=2]"
            ,"//result/doc[1]/str[@name='id'][.='4']"
            ,"//result/doc[2]/str[@name='id'][.='7']"
            );
    
    // collapse on term_s (very sparse) with nullPolicy=collapse
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=term_s nullPolicy=collapse sort='test_i asc, test_l desc, id_i asc'}");
    params.add("sort", "test_l asc, id_i asc");
    assertQ(req(params)
            , "*[count(//doc)=3]"
            ,"//result/doc[1]/str[@name='id'][.='5']"
            ,"//result/doc[2]/str[@name='id'][.='2']"
            ,"//result/doc[3]/str[@name='id'][.='7']"
            );
    
    // sort local param + elevation
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s desc, test_l asc'}");
    params.add("sort", "test_l asc");
    params.add("qt", "/elevate");
    params.add("forceElevation", "true");
    params.add("elevateIds", "4");
    assertQ(req(params),
            "*[count(//doc)=2]",
            "//result/doc[1]/str[@name='id'][.='4']",
            "//result/doc[2]/str[@name='id'][.='5']");
    //
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s sort='term_s desc, test_l asc'}");
    params.add("sort", "test_l asc");
    params.add("qt", "/elevate");
    params.add("forceElevation", "true");
    params.add("elevateIds", "7");
    assertQ(req(params),
            "*[count(//doc)=2]",
            "//result/doc[1]/str[@name='id'][.='7']",
            "//result/doc[2]/str[@name='id'][.='1']");

  }

  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-11974")
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
    String[] doc = {"id","1", "group_i", "-1000", "test_i", "5", "test_l", "-10", "test_f", "2000.32"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "group_i", "-1000", "test_i", "50", "test_l", "-100", "test_f", "2000.33"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "group_i", "-1000", "test_l", "100", "test_f", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "test_i", "500", "test_l", "1000", "test_f", "2000"};
    assertU(adoc(doc3));

    String[] doc4 = {"id","5", "group_i", "-1000", "test_i", "4", "test_l", "10", "test_f", "2000.31"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "group_i", "-1000", "test_i", "10", "test_l", "100", "test_f", "-2000.12"};
    assertU(adoc(doc5));
    assertU(commit());

    String[] doc6 = {"id","7", "group_i", "-1000", "test_i", "8", "test_l", "-50", "test_f", "-100.2"};
    assertU(adoc(doc6));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_i min=test_f}");
    assertQ(req(params), "*[count(//doc)=1]",
        "//result/doc[1]/str[@name='id'][.='6']");

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_i max=test_f}");
    assertQ(req(params), "*[count(//doc)=1]",
        "//result/doc[1]/str[@name='id'][.='2']");

  }

  @Test // https://issues.apache.org/jira/browse/SOLR-9494
  public void testNeedsScoreBugFixed() throws Exception {
    String[] doc = {"id","1", "group_s", "xyz", "text_ws", "hello xxx world"};
    assertU(adoc(doc));
    assertU(commit());

    ModifiableSolrParams params = params(
        "q", "{!surround df=text_ws} 2W(hello, world)", // a SpanQuery that matches
        "fq", "{!collapse field=group_s}", // collapse on some field
        // note: rows= whatever; doesn't matter
        "facet", "true", // facet on something
        "facet.field", "group_s"
    );
    assertQ(req(params));
    assertQ(req(params)); // fails *second* time!
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
        if(!boostedResults.get(i).equals(controlResults.get(i))) {
          throw new Exception("boosted results do not match control results, boostedResults size:"+boostedResults.toString()+", controlResults size:"+controlResults.toString());
        }
      }
    } else {
      throw new Exception("boosted results do not match control results, boostedResults size:"+boostedResults.toString()+", controlResults size:"+controlResults.toString());
    }
  }



  private void testCollapseQueries(String group, String hint, boolean numeric) throws Exception {

    String[] doc = {"id","1", "term_s", "YYYY", group, "1", "test_i", "5", "test_l", "10", "test_f", "2000"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "term_s","YYYY", group, "1", "test_i", "50", "test_l", "100", "test_f", "200"};
    assertU(adoc(doc1));



    String[] doc2 = {"id","3", "term_s", "YYYY", "test_i", "5000", "test_l", "100", "test_f", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_i", "500", "test_l", "1000", "test_f", "2000"};
    assertU(adoc(doc3));


    String[] doc4 = {"id","5", "term_s", "YYYY", group, "2", "test_i", "4", "test_l", "10", "test_f", "2000"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "term_s","YYYY", group, "2", "test_i", "10", "test_l", "100", "test_f", "200"};
    assertU(adoc(doc5));
    assertU(commit());

    String[] doc6 = {"id","7", "term_s", "YYYY", group, "1", "test_i", "8", "test_l", "50", "test_f", "300"};
    assertU(adoc(doc6));
    assertU(commit());


    //Test collapse by score and following sort by score
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+""+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=2]",
                       "//result/doc[1]/str[@name='id'][.='2']",
                       "//result/doc[2]/str[@name='id'][.='6']"
        );


    // SOLR-5544 test ordering with empty sort param
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=expand min=test_f"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("sort","");
    assertQ(req(params), "*[count(//doc)=4]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='6']"
    );

    // Test value source collapse criteria
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=field(test_i)"+hint+"}");
    params.add("sort", "test_i desc");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='5']"
    );

    // Test value source collapse criteria with cscore function
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=cscore()"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='5']"
    );
    
    // Test value source collapse criteria with cscore function but no top level score sort
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=cscore()"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("fl", "id");
    params.add("sort", "id_i desc");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/str[@name='id'][.='5']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='1']"
    );

    // Test value source collapse criteria with compound cscore function
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse min=sum(cscore(),field(test_i))"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='5']"
    );

    //Test collapse by score with elevation

    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    assertQ(req(params), "*[count(//doc)=4]",
                         "//result/doc[1]/str[@name='id'][.='1']",
                         "//result/doc[2]/str[@name='id'][.='2']",
                         "//result/doc[3]/str[@name='id'][.='3']",
                         "//result/doc[4]/str[@name='id'][.='6']");

    //Test SOLR-5773 with score collapse criteria
    // try both default & sort localparams as alternate ways to ask for max score
    for (String maxscore : new String[] {"  ", " sort='score desc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "YYYY");
      params.add("fq", "{!collapse field="+group + maxscore + " nullPolicy=collapse"+hint+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("qf", "term_s");
      params.add("qt", "/elevate");
      params.add("elevateIds", "1,5");
      assertQ(req(params), "*[count(//doc)=3]",
              "//result/doc[1]/str[@name='id'][.='1']",
              "//result/doc[2]/str[@name='id'][.='5']",
              "//result/doc[3]/str[@name='id'][.='3']");
    }
    
    //Test SOLR-5773 with max field collapse criteria
    // try both max & sort localparams as alternate ways to ask for max group head
    for (String max : new String[] {" max=test_i ", " sort='test_i desc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "YYYY");
      params.add("fq", "{!collapse field=" + group + max + "nullPolicy=collapse"+hint+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("qf", "term_s");
      params.add("qt", "/elevate");
      params.add("elevateIds", "1,5");
      assertQ(req(params), "*[count(//doc)=3]",
              "//result/doc[1]/str[@name='id'][.='1']",
              "//result/doc[2]/str[@name='id'][.='5']",
              "//result/doc[3]/str[@name='id'][.='3']");
    }
    
    //Test SOLR-5773 with min field collapse criteria
    // try both min & sort localparams as alternate ways to ask for min group head
    for (String min : new String[] {" min=test_i ", " sort='test_i asc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "YYYY");
      params.add("fq", "{!collapse field=" + group + min + "nullPolicy=collapse"+hint+"}");
      params.add("defType", "edismax");
      params.add("bf", "field(test_i)");
      params.add("qf", "term_s");
      params.add("qt", "/elevate");
      params.add("elevateIds", "1,5");
      assertQ(req(params), "*[count(//doc)=3]",
              "//result/doc[1]/str[@name='id'][.='1']",
              "//result/doc[2]/str[@name='id'][.='5']",
              "//result/doc[3]/str[@name='id'][.='4']");
    }
    
    //Test SOLR-5773 elevating documents with null group
    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field="+group+""+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    params.add("elevateIds", "3,4");
    assertQ(req(params), "*[count(//doc)=4]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='6']");


    // Non trivial sort local param for picking group head
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse sort='term_s asc, test_i asc' "+hint+"}");
    params.add("sort", "id_i desc");
    assertQ(req(params),
            "*[count(//doc)=3]",
            "//result/doc[1]/str[@name='id'][.='5']",
            "//result/doc[2]/str[@name='id'][.='4']",
            "//result/doc[3]/str[@name='id'][.='1']"
    );
    // 
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse sort='term_s asc, test_i desc' "+hint+"}");
    params.add("sort", "id_i desc");
    assertQ(req(params),
            "*[count(//doc)=3]",
            "//result/doc[1]/str[@name='id'][.='6']",
            "//result/doc[2]/str[@name='id'][.='3']",
            "//result/doc[3]/str[@name='id'][.='2']"
    );
    


    // Test collapse by min int field and top level sort
    // try both min & sort localparams as alternate ways to ask for min group head
    for (String min : new String[] {" min=test_i ", " sort='test_i asc' "}) {
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "id_i desc");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/str[@name='id'][.='5']",
              "//result/doc[2]/str[@name='id'][.='1']");

      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "id_i asc");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/str[@name='id'][.='1']",
              "//result/doc[2]/str[@name='id'][.='5']");
      
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "test_l asc,id_i desc");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/str[@name='id'][.='5']",
              "//result/doc[2]/str[@name='id'][.='1']");

      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group + min + hint+"}");
      params.add("sort", "score desc,id_i asc");
      params.add("defType", "edismax");
      params.add("bf", "field(id_i)");
      assertQ(req(params),
              "*[count(//doc)=2]",
              "//result/doc[1]/str[@name='id'][.='5']",
              "//result/doc[2]/str[@name='id'][.='1']");
    }


    //Test collapse by max int field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_i"+hint+"}");
    params.add("sort", "test_i asc");
    assertQ(req(params), "*[count(//doc)=2]",
                         "//result/doc[1]/str[@name='id'][.='6']",
                         "//result/doc[2]/str[@name='id'][.='2']"
        );

    try {
      //Test collapse by min long field
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group+" min=test_l"+hint+"}");
      params.add("sort", "test_i desc");
      assertQ(req(params), "*[count(//doc)=2]",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='5']");


      //Test collapse by max long field
      params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("fq", "{!collapse field="+group+" max=test_l"+hint+"}");
      params.add("sort", "test_i desc");
      assertQ(req(params), "*[count(//doc)=2]",
                           "//result/doc[1]/str[@name='id'][.='2']",
                           "//result/doc[2]/str[@name='id'][.='6']");
    } catch (Exception e) {
      if(!numeric) {
        throw e;
      }
    }


    //Test collapse by min float field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" min=test_f"+hint+"}");
    params.add("sort", "test_i desc");
    assertQ(req(params), "*[count(//doc)=2]",
                         "//result/doc[1]/str[@name='id'][.='2']",
                         "//result/doc[2]/str[@name='id'][.='6']");

    //Test collapse by min float field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_f"+hint+"}");
    params.add("sort", "test_i asc");
    assertQ(req(params), "*[count(//doc)=2]",
                         "//result/doc[1]/str[@name='id'][.='5']",
                         "//result/doc[2]/str[@name='id'][.='1']");

    //Test collapse by min float field sort by score
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_f"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(id_i)");
    params.add("fl", "score, id");
    params.add("facet","true");
    params.add("fq", "{!tag=test}term_s:YYYY");
    params.add("facet.field", "{!ex=test}term_s");

    assertQ(req(params), "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='5']",
        "//result/doc[2]/str[@name='id'][.='1']");
    
    // Test collapse using selector field in no docs
    // tie selector in all of these cases
    for (String selector : new String[] {
        " min=bogus_i ", " sort='bogus_i asc' ",
        " max=bogus_i ", " sort='bogus_i desc' ",
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
              req(params("q", "{!func}sub(sub(test_l,1000),id_i)",
                         "fq", "{!collapse field="+group+" sort='abs(cscore()) asc, id_i asc'}",
                         "sort", "score asc")),
              SolrException.ErrorCode.BAD_REQUEST);
    
    // multiple params for picking groupHead should all fail
    for (String bad : new String[] {
        "{!collapse field="+group+" min=test_f max=test_f}",
        "{!collapse field="+group+" min=test_f sort='test_f asc'}",
        "{!collapse field="+group+" max=test_f sort='test_f asc'}" }) {
      assertQEx("Expected error: " + bad, req(params("q", "*:*", "fq", bad)),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    // multiple params for picking groupHead should work as long as only one is non-null
    // sort used
    for (SolrParams collapse : new SolrParams[] {
        // these should all be equally valid
        params("fq", "{!collapse field="+group+" nullPolicy=collapse sort='test_i asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse min='' sort='test_i asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse max='' sort='test_i asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse min=$x sort='test_i asc'"+hint+"}"),
        params("fq", "{!collapse field="+group+" nullPolicy=collapse min=$x sort='test_i asc'"+hint+"}",
               "x",""),
      }) {
      
      assertQ(req(collapse, "q", "*:*", "sort", "test_i desc"),
              "*[count(//doc)=3]",
              "//result/doc[1]/str[@name='id'][.='4']",
              "//result/doc[2]/str[@name='id'][.='1']",
              "//result/doc[3]/str[@name='id'][.='5']");
    }
    

    //Test nullPolicy expand
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_f nullPolicy=expand"+hint+"}");
    params.add("sort", "id_i desc");
    assertQ(req(params), "*[count(//doc)=4]",
        "//result/doc[1]/str[@name='id'][.='5']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='1']");

    //Test nullPolicy collapse
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" max=test_f nullPolicy=collapse"+hint+"}");
    params.add("sort", "id_i desc");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/str[@name='id'][.='5']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='1']");


    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("fq","{!tag=test_i}id:5");
    params.add("facet","true");
    params.add("facet.field","{!ex=test_i}test_i");
    params.add("facet.mincount", "1");
    assertQ(req(params), "*[count(//doc)=1]", "*[count(//lst[@name='facet_fields']/lst[@name='test_i']/int)=2]");

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
    params.add("bf", "field(test_i)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    assertQ(req(params), "*[count(//doc)=3]",
                         "//result/doc[1]/str[@name='id'][.='3']",
                         "//result/doc[2]/str[@name='id'][.='6']",
                         "//result/doc[3]/str[@name='id'][.='7']");


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
    String optional_min_or_max = (random().nextBoolean() ? "" : (random().nextBoolean() ? "min=field(test_i)" : "max=field(test_i)"));
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" "+optional_min_or_max+"}");
    assertQ(req(params), "*[count(//doc)=0]");

    // if a field is uninvertible=false, it should behave the same as a field that is indexed=false
    // this is currently ok on fields that don't exist on any docs in the index
    for (String f : Arrays.asList("not_indexed_sS", "indexed_s_not_uninvert")) {
      for (String hint : Arrays.asList("", " hint=top_fc")) {
        SolrException e = expectThrows(SolrException.class,
            () -> h.query(req("q", "*:*", "fq", "{!collapse field="+f + hint +"}")));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue("unexpected Message: " + e.getMessage(),
            e.getMessage().contains("Collapsing field '" + f + "' " +
                "should be either docValues enabled or indexed with uninvertible enabled"));
      }
    }
  }

  public void testNoDocsHaveGroupField() throws Exception {
    // as unlikely as this test seems, it's important for the possibility that a segment exists w/o
    // any live docs that have DocValues for the group field -- ie: every doc in segment is in null group.
    
    assertU(adoc("id", "1", "group_s", "group1", "test_i", "5", "test_l", "10"));
    assertU(commit());
    assertU(adoc("id", "2", "group_s", "group1", "test_i", "5", "test_l", "1000"));
    assertU(adoc("id", "3", "group_s", "group1", "test_i", "5", "test_l", "1000"));
    assertU(adoc("id", "4", "group_s", "group1", "test_i", "10", "test_l", "100"));
    //
    assertU(adoc("id", "5", "group_s", "group2", "test_i", "5", "test_l", "10", "term_s", "YYYY"));
    assertU(commit());
    assertU(adoc("id", "6", "group_s", "group2", "test_i", "5", "test_l","1000"));
    assertU(adoc("id", "7", "group_s", "group2", "test_i", "5", "test_l","1000", "term_s", "XXXX"));
    assertU(adoc("id", "8", "group_s", "group2", "test_i", "10","test_l", "100"));
    assertU(commit());
    
    // none of these grouping fields are in any doc
    for (String group : new String[] {
        "field=bogus_s", "field=bogus_s_dv",
        "field=bogus_s hint=top_fc", // alternative docvalues codepath w/ hint
        "field=bogus_s_dv hint=top_fc", // alternative docvalues codepath w/ hint
        "field=bogus_i", "field=bogus_tf" }) {
      
      // for any of these selectors, behavior of these checks should be consistent
      for (String selector : new String[] {
          "", " sort='score desc' ",
          " min=test_i ", " max=test_i ", " sort='test_i asc' ",  " sort='test_i desc' ",
          " min=test_f ", " max=test_f ", " sort='test_f asc' ",  " sort='test_f desc' ",
          " sort='group_s asc' ",  " sort='group_s desc' ",
          // fields that don't exist
          " min=bogus_sort_i ", " max=bogus_sort_i ",
          " sort='bogus_sort_i asc' ",  " sort='bogus_sort_i desc' ",
          " sort='bogus_sort_s asc' ",  " sort='bogus_sort_s desc' ",
        }) {
          
          
        ModifiableSolrParams params = null;

        // w/default nullPolicy, no groups found
        params = new ModifiableSolrParams();
        params.add("q", "*:*");
        params.add("sort", "id_i desc");
        params.add("fq", "{!collapse "+group+" "+selector+"}");
        assertQ(req(params), "*[count(//doc)=0]");

        // w/nullPolicy=expand, every doc found
        params = new ModifiableSolrParams();
        params.add("q", "*:*");
        params.add("sort", "id_i desc");
        params.add("fq", "{!collapse field="+group+" nullPolicy=expand "+selector+"}");
        assertQ(req(params)
                , "*[count(//doc)=8]"
                ,"//result/doc[1]/str[@name='id'][.='8']"
                ,"//result/doc[2]/str[@name='id'][.='7']"
                ,"//result/doc[3]/str[@name='id'][.='6']"
                ,"//result/doc[4]/str[@name='id'][.='5']"
                ,"//result/doc[5]/str[@name='id'][.='4']"
                ,"//result/doc[6]/str[@name='id'][.='3']"
                ,"//result/doc[7]/str[@name='id'][.='2']"
                ,"//result/doc[8]/str[@name='id'][.='1']"
                );

        
      }
    }
  }

  public void testGroupHeadSelector() {
    GroupHeadSelector s;

    expectThrows(SolrException.class, "no exception with multi criteria",
        () -> GroupHeadSelector.build(params("sort", "foo_s asc", "min", "bar_s"))
    );
    
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

  @Test
  public void testForNotSupportedCases() {
    String[] doc = {"id","3", "term_s", "YYYY", "test_ii", "5000", "test_l", "100", "test_f", "200",
                    "not_indexed_sS", "zzz", "indexed_s_not_uninvert", "zzz"};
    assertU(adoc(doc));
    assertU(commit());

    // collapsing on multivalued field
    assertQEx("Should Fail with Bad Request", "Collapsing not supported on multivalued fields",
        req("q","*:*", "fq","{!collapse field=test_ii}"), SolrException.ErrorCode.BAD_REQUEST);

    // collapsing on unknown field
    assertQEx("Should Fail with Bad Request", "org.apache.solr.search.SyntaxError: undefined field: \"bleh\"",
        req("q","*:*", "fq","{!collapse field=bleh}"), SolrException.ErrorCode.BAD_REQUEST);

    // if a field is uninvertible=false, it should behave the same as a field that is indexed=false ...
    // this also tests docValues=false along with indexed=false or univertible=false
    for (String f : Arrays.asList("not_indexed_sS", "indexed_s_not_uninvert")) {
      {
        SolrException e = expectThrows(SolrException.class,
                                    () -> h.query(req(params("q", "*:*",
                                                             "fq", "{!collapse field="+f+"}"))));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue("unexpected Message: " + e.getMessage(),
                   e.getMessage().contains("Collapsing field '" + f + "' " +
                       "should be either docValues enabled or indexed with uninvertible enabled"));
      }
      {
        SolrException e = expectThrows(SolrException.class,
            () -> h.query(req("q", "*:*", "fq", "{!collapse field="+f+" hint=top_fc}")));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue("unexpected Message: " + e.getMessage(),
            e.getMessage().contains("Collapsing field '" + f + "' " +
                "should be either docValues enabled or indexed with uninvertible enabled"));
      }
      
    }
  }

  @Test
  public void test64BitCollapseFieldException() {
    assertQEx("Should Fail For collapsing on Long fields", "Collapsing field should be of either String, Int or Float type",
        req("q", "*:*", "fq", "{!collapse field=group_l}"), SolrException.ErrorCode.BAD_REQUEST);

    assertQEx("Should Fail For collapsing on Double fields", "Collapsing field should be of either String, Int or Float type",
        req("q", "*:*", "fq", "{!collapse field=group_d}"), SolrException.ErrorCode.BAD_REQUEST);

    assertQEx("Should Fail For collapsing on Date fields", "Collapsing field should be of either String, Int or Float type",
        req("q", "*:*", "fq", "{!collapse field=group_dt}"), SolrException.ErrorCode.BAD_REQUEST);
  }
}
