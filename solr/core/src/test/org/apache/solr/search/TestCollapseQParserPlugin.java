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

  @Test
  public void testStringCollapse() throws Exception {
    List<String> types = new ArrayList();
    types.add("group_s");
    types.add("group_s_dv");
    Collections.shuffle(types, random());
    String group = types.get(0);
    String hint = (random().nextBoolean() ? " hint="+CollapsingQParserPlugin.HINT_TOP_FC : "");
    testCollapseQueries(group, hint, false);
  }


  @Test
  public void testNumericCollapse() throws Exception {
    List<String> types = new ArrayList();
    types.add("group_i");
    types.add("group_ti_dv");
    types.add("group_f");
    types.add("group_tf_dv");
    Collections.shuffle(types, random());
    String group = types.get(0);
    String hint = "";
    testCollapseQueries(group, hint, true);
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
    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field="+group+" nullPolicy=collapse"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    params.add("elevateIds", "1,5");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='1.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='3.0']");

    //Test SOLR-5773 with max field collapse criteria
    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field="+group+" min=test_ti nullPolicy=collapse"+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    params.add("elevateIds", "1,5");
    assertQ(req(params), "*[count(//doc)=3]",
        "//result/doc[1]/float[@name='id'][.='1.0']",
        "//result/doc[2]/float[@name='id'][.='5.0']",
        "//result/doc[3]/float[@name='id'][.='4.0']");


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



    //Test collapse by min int field and sort
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" min=test_ti"+hint+"}");
    params.add("sort", "id desc");
    assertQ(req(params), "*[count(//doc)=2]",
                           "//result/doc[1]/float[@name='id'][.='5.0']",
                           "//result/doc[2]/float[@name='id'][.='1.0']");

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" min=test_ti"+hint+"}");
    params.add("sort", "id asc");
    assertQ(req(params), "*[count(//doc)=2]",
                         "//result/doc[1]/float[@name='id'][.='1.0']",
                         "//result/doc[2]/float[@name='id'][.='5.0']");

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" min=test_ti"+hint+"}");
    params.add("sort", "test_tl asc,id desc");
    assertQ(req(params), "*[count(//doc)=2]",
        "//result/doc[1]/float[@name='id'][.='5.0']",
        "//result/doc[2]/float[@name='id'][.='1.0']");



    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+" min=test_ti"+hint+"}");
    params.add("sort", "score desc,id asc");
    params.add("defType", "edismax");
    params.add("bf", "field(id)");
    assertQ(req(params), "*[count(//doc)=2]",
                          "//result/doc[1]/float[@name='id'][.='5.0']",
                          "//result/doc[2]/float[@name='id'][.='1.0']");




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


}
