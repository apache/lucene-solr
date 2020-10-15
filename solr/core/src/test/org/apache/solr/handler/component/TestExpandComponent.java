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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExpandComponent extends SolrTestCaseJ4 {

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
  public void testExpand() throws Exception {
    List<String> groups = new ArrayList<>();
    groups.add("group_s");
    groups.add("group_s_dv");

    Collections.shuffle(groups, random());
    String floatAppend = "";

    String hint = (random().nextBoolean() ? " hint="+ CollapsingQParserPlugin.HINT_TOP_FC : "");

    _testExpand(groups.get(0), floatAppend, hint);
  }

  @Test
  public void testNumericExpand() throws Exception {
    List<String> groups = new ArrayList<>();
    groups.add("group_i");
    groups.add("group_ti_dv");
    groups.add("group_f");
    groups.add("group_tf_dv");
    Collections.shuffle(groups, random());
    String floatAppend = "";
    if(groups.get(0).indexOf("f") > -1) {
      floatAppend = "."+random().nextInt(100);  //Append the float
      floatAppend = Float.toString(Float.parseFloat(floatAppend)); //Create a proper float out of the string.
      floatAppend = floatAppend.substring(1);  //Drop off the leading 0, leaving just the decimal
    }

    String hint = "";

    _testExpand(groups.get(0), floatAppend, hint);
  }

  private void _testExpand(String group, String floatAppend, String hint) throws Exception {
    String[][] docs = {
        {"id","1", "term_s", "YYYY", group, "1"+floatAppend, "test_i", "5", "test_l", "10", "test_f", "2000", "type_s", "parent"},
        {"id","2", "term_s","YYYY", group, "1"+floatAppend, "test_i", "50", "test_l", "100", "test_f", "200", "type_s", "child"},
        {"id","3", "term_s", "YYYY", "test_i", "5000", "test_l", "100", "test_f", "200"},
        {"id","4", "term_s", "YYYY", "test_i", "500", "test_l", "1000", "test_f", "2000"},
        {"id","5", "term_s", "YYYY", group, "2"+floatAppend, "test_i", "4", "test_l", "10", "test_f", "2000", "type_s", "parent"},
        {"id","6", "term_s","YYYY", group, "2"+floatAppend, "test_i", "10", "test_l", "100", "test_f", "200", "type_s", "child"},
        {"id","7", "term_s", "YYYY", group, "1"+floatAppend, "test_i", "1", "test_l", "100000", "test_f", "2000", "type_s", "child"},
        {"id","8", "term_s","YYYY", group, "2"+floatAppend, "test_i", "2", "test_l",  "100000", "test_f", "200", "type_s", "child"}
    };
    createIndex(docs);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");

    //First basic test case.
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='8']"
    );

    //Basic test case page 2

    assertQ(req(params, "rows", "1", "start", "1"), "*[count(/response/result/doc)=1]",
        "*[count(/response/lst[@name='expanded']/result)=1]",
        "/response/result/doc[1]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='8']"
    );

    //Test expand.sort
    //the "sub()" just testing function queries
    assertQ(req(params,"expand.sort", "test_l desc, sub(1,1) asc"),
        "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='5']"
    );

    //Test with nullPolicy, ExpandComponent should ignore docs with null values in the collapse fields.
    //Main result set should include the doc with null value in the collapse field.
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+" nullPolicy=collapse}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.sort", "test_l desc");
    assertQ(req(params), "*[count(/response/result/doc)=3]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='3']",
        "/response/result/doc[2]/str[@name='id'][.='2']",
        "/response/result/doc[3]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='5']"
    );


    //Test override expand.q
    params = new ModifiableSolrParams();
    params.add("q", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.q", "type_s:child");
    params.add("expand.field", group);
    params.add("expand.sort", "test_l desc");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='1']",
        "/response/result/doc[2]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='2']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='6']"
    );


    //Test override expand.fq
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.fq", "type_s:child");
    params.add("expand.field", group);
    params.add("expand.sort", "test_l desc");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='1']",
        "/response/result/doc[2]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='2']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='6']"
    );

    //Test override expand.fq and expand.q
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.q", "type_s:child");
    params.add("expand.fq", "*:*");
    params.add("expand.field", group);
    params.add("expand.sort", "test_l desc");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='1']",
        "/response/result/doc[2]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='2']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='6']"
    );

    //Test expand.rows
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.sort", "test_l desc");
    params.add("expand.rows", "1");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "*[count(/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc)=1]",
        "*[count(/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc)=1]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']"
    );

    //Test expand.rows = 0 - no docs only expand count
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.rows", "0");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
            "*[count(/response/lst[@name='expanded']/result)=2]",
            "*[count(/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc)=0]",
            "*[count(/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc)=0]",
            "/response/result/doc[1]/str[@name='id'][.='2']",
            "/response/result/doc[2]/str[@name='id'][.='6']"
    );

    //Test expand.rows = 0 with expand.field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.fq", "type_s:child");
    params.add("expand.field", group);
    params.add("expand.rows", "0");
    assertQ(req(params, "fl", "id"), "*[count(/response/result/doc)=2]",
            "*[count(/response/lst[@name='expanded']/result)=2]",
            "*[count(/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc)=0]",
            "*[count(/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc)=0]",
            "/response/result/doc[1]/str[@name='id'][.='1']",
            "/response/result/doc[2]/str[@name='id'][.='5']"
    );

    //Test score with expand.rows = 0
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.fq", "*:*");
    params.add("expand.field", group);
    params.add("expand.rows", "0");
    assertQ(req(params, "fl", "id,score"), "*[count(/response/result/doc)=2]",
            "*[count(/response/lst[@name='expanded']/result)=2]",
            "*[count(/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc)=0]",
            "*[count(/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc)=0]",
            "*[count(/response/lst[@name='expanded']/result[@maxScore])=0]", //maxScore should not be available
            "/response/result/doc[1]/str[@name='id'][.='1']",
            "/response/result/doc[2]/str[@name='id'][.='5']",
            "count(//*[@name='score' and .='NaN'])=0"

    );

    //Test no group results
    params = new ModifiableSolrParams();
    params.add("q", "test_i:5");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.sort", "test_l desc");
    params.add("expand.rows", "1");
    assertQ(req(params), "*[count(/response/result/doc)=1]",
        "*[count(/response/lst[@name='expanded']/result)=0]"
    );

    //Test zero results
    params = new ModifiableSolrParams();
    params.add("q", "test_i:5532535");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");
    params.add("expand.sort", "test_l desc");
    params.add("expand.rows", "1");
    assertQ(req(params), "*[count(/response/result/doc)=0]",
        "*[count(/response/lst[@name='expanded']/result)=0]"
    );

    //Test key-only fl
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_i)");
    params.add("expand", "true");

    assertQ(req(params, "fl", "id"),
        "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='8']",
        "count(//*[@name='score'])=0" // score shouldn't be returned when not requested
    );

    //Test key-only fl with score but no sorting
    assertQ(req(params, "fl", "id,score"), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='8']",
        "count(//*[@name='score' and .='NaN'])=0"
    );


    // Test with fl and sort=score desc
    assertQ(req(params, "expand.sort", "score desc", "fl", "id,score"),
        "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='8']",
        "count(//*[@name='score' and .='NaN'])=0"
    );

    //Test fl with score, sort by non-score
    assertQ(req(params, "expand.sort", "test_l desc", "fl", "id,test_i,score"),
        "*[count(/response/result/doc)=2]",
        "count(/response/lst[@name='expanded']/result)=2",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        // note that the expanded docs are score descending order (score is 1 test_i)
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='5']",
        "count(//*[@name='score' and .='NaN'])=0",
        "count(/response/lst[@name='expanded']/result/doc[number(*/@name='score')!=number(*/@name='test_i')])=0"
    );

    //Test fl with score with multi-sort
    assertQ(req(params, "expand.sort", "test_l desc, score asc", "fl", "id,test_i,score"),
        "*[count(/response/result/doc)=2]",
        "count(/response/lst[@name='expanded']/result)=2",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/result/doc[2]/str[@name='id'][.='6']",
        // note that the expanded docs are score descending order (score is 1 test_i)
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='5']",
        "count(//*[@name='score' and .='NaN'])=0",
        "count(/response/lst[@name='expanded']/result/doc[number(*/@name='score')!=number(*/@name='test_i')])=0"
    );

    // Test for expand with collapse
    // when matched docs have fewer unique values
    params = params("q", "*:*", "sort", "id asc", "fl", "id", "rows", "6", "expand", "true", "expand.sort", "id asc");
    assertQ(req(params, "expand.field", "term_s"),
        "*[count(/response/result/doc)=6]",
        "/response/lst[@name='expanded']/result[@name='YYYY']/doc[1]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='YYYY']/doc[2]/str[@name='id'][.='8']",
        "count(//*[@name='score'])=0"
    );
    assertQ(req(params, "expand.field", "test_f"),
        "*[count(/response/result/doc)=6]",
        "/response/lst[@name='expanded']/result[@name='200.0']/doc[1]/str[@name='id'][.='8']",
        "/response/lst[@name='expanded']/result[@name='2000.0']/doc[1]/str[@name='id'][.='7']",
        "count(//*[@name='score'])=0"
    );

    // Support expand enabled without previous collapse
    assertQ(req("q", "type_s:child", "sort", group+" asc, test_l desc", "defType", "edismax",
        "expand", "true", "expand.q", "type_s:parent", "expand.field", group),
        "*[count(/response/result/doc)=4]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/str[@name='id'][.='7']",
        "/response/result/doc[2]/str[@name='id'][.='2']",
        "/response/result/doc[3]/str[@name='id'][.='8']",
        "/response/result/doc[4]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='5']"
    );

    // With multiple collapse

    // with different cost
    params = params("q", "*:*", "defType", "edismax", "expand", "true",  "bf", "field(test_i)", "expand.sort", "id asc");
    params.set("fq", "{!collapse cost=1000 field="+group+"}", "{!collapse cost=2000 field=test_f}");
    assertQ(req(params),
        "*[count(/response/result/doc)=1]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']"
    );

    // with same cost (default cost)
    params.set("fq", "{!collapse field="+group+"}", "{!collapse field=test_f}");
    assertQ(req(params),
        "*[count(/response/result/doc)=1]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']"
    );

    // with different cost but choose the test_f
    params.set("fq", "{!collapse cost=3000 field="+group+"}", "{!collapse cost=2000 field=test_f}");
    assertQ(req(params),
        "*[count(/response/result/doc)=1]",
        "/response/result/doc[1]/str[@name='id'][.='2']",
        "/response/lst[@name='expanded']/result[@name='200.0']/doc[1]/str[@name='id'][.='3']",
        "/response/lst[@name='expanded']/result[@name='200.0']/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='200.0']/doc[3]/str[@name='id'][.='8']"
    );

    // with different cost and nullPolicy
    params.set("bf", "ord(id)");
    params.set("fq", "{!collapse cost=1000 field="+group+" nullPolicy=collapse}", "{!collapse cost=2000 field=test_f}");
    assertQ(req(params),
        "*[count(/response/result/doc)=2]",
        "/response/result/doc[1]/str[@name='id'][.='8']",
        "/response/result/doc[2]/str[@name='id'][.='7']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/str[@name='id'][.='5']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/str[@name='id'][.='6']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/str[@name='id'][.='1']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/str[@name='id'][.='2']"
    );
  }

  @Test
  public void testExpandWithEmptyIndexReturnsZeroResults() {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s}");
    params.add("expand" ,"true");
    params.add("expand.rows", "10");

    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void testErrorCases() {
    String[] doc = {"id","1", "term_s", "YYYY", "text_t", "bleh bleh", "test_i", "5000", "test_l", "100", "test_f", "200"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "term_s", "YYYY", "text_t", "bleh bleh", "test_i", "500", "test_l", "1000", "test_f", "2000"};
    assertU(adoc(doc1));

    ignoreException("missing expand field");
    ignoreException("Expected identifier at pos 2");
    ignoreException("Can't determine a Sort Order");
    ignoreException("Expand not supported for fieldType:'text'");

    // expand with grouping
    SolrException e = expectThrows(SolrException.class, () -> {
      h.query(req("q", "*:*", "expand", "true", "expand.field", "id", "group", "true", "group.field", "id"));
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertEquals("Can not use expand with Grouping enabled", e.getMessage());

    // no expand field
    e = expectThrows(SolrException.class,  () -> h.query(req("q", "*:*", "expand", "true")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertEquals("missing expand field", e.getMessage());

    // query and filter syntax errors
    e = expectThrows(SolrException.class,  () -> h.query(req("q", "*:*", "expand", "true",
        "expand.field", "term_s", "expand.q", "{!")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage().contains("Expected identifier at pos 2 str='{!'"));

    e = expectThrows(SolrException.class,  () -> h.query(req("q", "*:*", "expand", "true",
        "expand.field", "term_s", "expand.q", "*:*", "expand.fq", "{!")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage().contains("Expected identifier at pos 2 str='{!'"));

    e = expectThrows(SolrException.class,  () -> h.query(req("q", "*:*", "expand", "true",
        "expand.field", "term_s", "expand.q", "*:*", "expand.fq", "{!")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage().contains("Expected identifier at pos 2 str='{!'"));

    e = expectThrows(SolrException.class,  () -> h.query(req("q", "*:*", "expand", "true",
        "expand.field", "term_s", "expand.q", "*:*", "expand.sort", "bleh")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage().contains("Can't determine a Sort Order (asc or desc) in sort spec 'bleh'"));

    e = expectThrows(SolrException.class,  () -> h.query(req("q", "*:*", "expand", "true",
        "expand.field", "text_t", "expand.q", "*:*")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertEquals("Expand not supported for fieldType:'text'", e.getMessage());

    resetExceptionIgnores();
  }

  /**
   * randomize addition of docs into bunch of segments
   * TODO: there ought to be a test utility to do this; even add in batches
   */
  private void createIndex(String[][] docs) {
    Collections.shuffle(Arrays.asList(docs), random());
    for (String[] doc : docs) {
      assertU(adoc(doc));
      if (random().nextBoolean()) {
        assertU(commit());
      }
    }
    assertU(commit());
  }
}
