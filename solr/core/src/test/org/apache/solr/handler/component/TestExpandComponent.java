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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

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
    List<String> groups = new ArrayList();
    groups.add("group_s");
    groups.add("group_s_dv");

    Collections.shuffle(groups, random());
    String floatAppend = "";

    String hint = (random().nextBoolean() ? " hint="+ CollapsingQParserPlugin.HINT_TOP_FC : "");

     _testExpand(groups.get(0), floatAppend, hint);
  }

  @Test
  public void testNumericExpand() throws Exception {
    List<String> groups = new ArrayList();
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

    String[] doc = {"id","1", "term_s", "YYYY", group, "1"+floatAppend, "test_ti", "5", "test_tl", "10", "test_tf", "2000", "type_s", "parent"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "term_s","YYYY", group, "1"+floatAppend, "test_ti", "50", "test_tl", "100", "test_tf", "200", "type_s", "child"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));


    String[] doc4 = {"id","5", "term_s", "YYYY", group, "2"+floatAppend, "test_ti", "4", "test_tl", "10", "test_tf", "2000", "type_s", "parent"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "term_s","YYYY", group, "2"+floatAppend, "test_ti", "10", "test_tl", "100", "test_tf", "200", "type_s", "child"};
    assertU(adoc(doc5));
    assertU(commit());

    String[] doc6 = {"id","7", "term_s", "YYYY", group, "1"+floatAppend, "test_ti", "1", "test_tl", "100000", "test_tf", "2000", "type_s", "child"};
    assertU(adoc(doc6));
    assertU(commit());
    String[] doc7 = {"id","8", "term_s","YYYY", group, "2"+floatAppend, "test_ti", "2", "test_tl", "100000", "test_tf", "200", "type_s", "child"};
    assertU(adoc(doc7));

    assertU(commit());

    //First basic test case.
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/float[@name='id'][.='2.0']",
        "/response/result/doc[2]/float[@name='id'][.='6.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='1.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='5.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='8.0']"
    );

    //Basic test case page 2

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("rows", "1");
    params.add("start", "1");
    assertQ(req(params), "*[count(/response/result/doc)=1]",
        "*[count(/response/lst[@name='expanded']/result)=1]",
        "/response/result/doc[1]/float[@name='id'][.='6.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='5.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='8.0']"
    );

    //Test expand.sort
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.sort", "test_tl desc, sub(1,1) asc");//the "sub()" just testing function queries
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/float[@name='id'][.='2.0']",
        "/response/result/doc[2]/float[@name='id'][.='6.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/float[@name='id'][.='1.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='8.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='5.0']"
    );

    //Test with nullPolicy, ExpandComponent should ignore docs with null values in the collapse fields.
    //Main result set should include the doc with null value in the collapse field.
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+" nullPolicy=collapse}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.sort", "test_tl desc");
    assertQ(req(params), "*[count(/response/result/doc)=3]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/float[@name='id'][.='3.0']",
        "/response/result/doc[2]/float[@name='id'][.='2.0']",
        "/response/result/doc[3]/float[@name='id'][.='6.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/float[@name='id'][.='1.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='8.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='5.0']"
    );


    //Test overide expand.q

    params = new ModifiableSolrParams();
    params.add("q", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.q", "type_s:child");
    params.add("expand.field", group);
    params.add("expand.sort", "test_tl desc");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/float[@name='id'][.='1.0']",
        "/response/result/doc[2]/float[@name='id'][.='5.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/float[@name='id'][.='2.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='8.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='6.0']"
    );


    //Test overide expand.fq

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.fq", "type_s:child");
    params.add("expand.field", group);
    params.add("expand.sort", "test_tl desc");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/float[@name='id'][.='1.0']",
        "/response/result/doc[2]/float[@name='id'][.='5.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/float[@name='id'][.='2.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='8.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='6.0']"
    );

    //Test overide expand.fq and expand.q

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "type_s:parent");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.q", "type_s:child");
    params.add("expand.fq", "*:*");
    params.add("expand.field", group);
    params.add("expand.sort", "test_tl desc");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/float[@name='id'][.='1.0']",
        "/response/result/doc[2]/float[@name='id'][.='5.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/float[@name='id'][.='2.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='8.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='6.0']"
    );

    //Test expand.rows

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.sort", "test_tl desc");
    params.add("expand.rows", "1");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "*[count(/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc)=1]",
        "*[count(/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc)=1]",
        "/response/result/doc[1]/float[@name='id'][.='2.0']",
        "/response/result/doc[2]/float[@name='id'][.='6.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='8.0']"
    );


    //Test no group results

    params = new ModifiableSolrParams();
    params.add("q", "test_ti:5");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.sort", "test_tl desc");
    params.add("expand.rows", "1");
    assertQ(req(params), "*[count(/response/result/doc)=1]",
        "*[count(/response/lst[@name='expanded']/result)=0]"
    );

    //Test zero results

    params = new ModifiableSolrParams();
    params.add("q", "test_ti:5532535");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("expand.sort", "test_tl desc");
    params.add("expand.rows", "1");
    assertQ(req(params), "*[count(/response/result/doc)=0]",
        "*[count(/response/lst[@name='expanded']/result)=0]"
    );

    //Test key-only fl

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field="+group+hint+"}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("expand", "true");
    params.add("fl", "id");
    assertQ(req(params), "*[count(/response/result/doc)=2]",
        "*[count(/response/lst[@name='expanded']/result)=2]",
        "/response/result/doc[1]/float[@name='id'][.='2.0']",
        "/response/result/doc[2]/float[@name='id'][.='6.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[1]/float[@name='id'][.='1.0']",
        "/response/lst[@name='expanded']/result[@name='1"+floatAppend+"']/doc[2]/float[@name='id'][.='7.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[1]/float[@name='id'][.='5.0']",
        "/response/lst[@name='expanded']/result[@name='2"+floatAppend+"']/doc[2]/float[@name='id'][.='8.0']"
    );
  }

  @Test
  public void testExpandWithEmptyIndexReturnsZeroResults() {
    //We make sure the index is cleared

    clearIndex();
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s}");
    params.add("expand" ,"true");
    params.add("expand.rows", "10");

    assertQ(req(params), "*[count(//doc)=0]");
  }
}
