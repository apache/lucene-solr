package org.apache.solr.search.function;
/**
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

import org.apache.solr.util.AbstractSolrTestCase;


/**
 *
 *
 **/
public class SortByFunctionTest extends AbstractSolrTestCase {
  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  public void test() throws Exception {
    assertU(adoc("id", "1", "x_td", "0", "y_td", "2", "w_td1", "25", "z_td", "5", "f_t", "ipod"));
    assertU(adoc("id", "2", "x_td", "2", "y_td", "2", "w_td1", "15", "z_td", "5", "f_t", "ipod ipod ipod ipod ipod"));
    assertU(adoc("id", "3", "x_td", "3", "y_td", "2", "w_td1", "55", "z_td", "5", "f_t", "ipod ipod ipod ipod ipod ipod ipod ipod ipod"));
    assertU(adoc("id", "4", "x_td", "4", "y_td", "2", "w_td1", "45", "z_td", "5", "f_t", "ipod ipod ipod ipod ipod ipod ipod"));
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "*:*"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/int[@name='id'][.='1']",
            "//result/doc[2]/int[@name='id'][.='2']",
            "//result/doc[3]/int[@name='id'][.='3']",
            "//result/doc[4]/int[@name='id'][.='4']"
    );
    assertQ(req("fl", "*,score", "q", "*:*", "sort", "score desc"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/int[@name='id'][.='1']",
            "//result/doc[2]/int[@name='id'][.='2']",
            "//result/doc[3]/int[@name='id'][.='3']",
            "//result/doc[4]/int[@name='id'][.='4']"
    );
    assertQ(req("fl", "id,score", "q", "f_t:ipod", "sort", "score desc"),
            "//*[@numFound='4']",
            "//result/doc[1]/int[@name='id'][.='1']",
            "//result/doc[2]/int[@name='id'][.='4']",
            "//result/doc[3]/int[@name='id'][.='2']",
            "//result/doc[4]/int[@name='id'][.='3']"
    );


    assertQ(req("fl", "*,score", "q", "*:*", "sort", "sum(x_td, y_td) desc"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/int[@name='id'][.='4']",
            "//result/doc[2]/int[@name='id'][.='3']",
            "//result/doc[3]/int[@name='id'][.='2']",
            "//result/doc[4]/int[@name='id'][.='1']"
    );
    assertQ(req("fl", "*,score", "q", "*:*", "sort", "sum(x_td, y_td) asc"),
            "//*[@numFound='4']",
            "//float[@name='score']='1.0'",
            "//result/doc[1]/int[@name='id'][.='1']",
            "//result/doc[2]/int[@name='id'][.='2']",
            "//result/doc[3]/int[@name='id'][.='3']",
            "//result/doc[4]/int[@name='id'][.='4']"
    );
    //the function is equal, w_td1 separates
    assertQ(req("q", "*:*", "fl", "id", "sort", "sum(z_td, y_td) asc, w_td1 asc"),
            "//*[@numFound='4']",
            "//result/doc[1]/int[@name='id'][.='2']",
            "//result/doc[2]/int[@name='id'][.='1']",
            "//result/doc[3]/int[@name='id'][.='4']",
            "//result/doc[4]/int[@name='id'][.='3']"
    );
  }
}

/*
<lst name="responseHeader"><int name="status">0</int><int name="QTime">93</int></lst><result name="response" numFound="4" start="0" maxScore="1.0"><doc><float name="score">1.0</float><int name="id">4</int><int name="intDefault">42</int><arr name="multiDefault"><str>muLti-Default</str></arr><date name="timestamp">2009-12-12T12:59:46.412Z</date><arr name="x_td"><double>4.0</double></arr><arr name="y_td"><double>2.0</double></arr></doc><doc><float name="score">1.0</float><int name="id">3</int><int name="intDefault">42</int><arr name="multiDefault"><str>muLti-Default</str></arr><date name="timestamp">2009-12-12T12:59:46.409Z</date><arr name="x_td"><double>3.0</double></arr><arr name="y_td"><double>2.0</double></arr></doc><doc><float name="score">1.0</float><int name="id">2</int><int name="intDefault">42</int><arr name="multiDefault"><str>muLti-Default</str></arr><date name="timestamp">2009-12-12T12:59:46.406Z</date><arr name="x_td"><double>2.0</double></arr><arr name="y_td"><double>2.0</double></arr></doc><doc><float name="score">1.0</float><int name="id">1</int><int name="intDefault">42</int><arr name="multiDefault"><str>muLti-Default</str></arr><date name="timestamp">2009-12-12T12:59:46.361Z</date><arr name="x_td"><double>0.0</double></arr><arr name="y_td"><double>2.0</double></arr></doc></result>
*/
