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

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

//We want codecs that support DocValues, and ones supporting blank/empty values.
@SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42"})
public class TestGraphTermsQParserPlugin extends SolrTestCaseJ4 {

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
  public void testQueries() throws Exception {

    String group = "group_s";

    String[] doc = {"id","1", "term_s", "YYYY", group, "1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    String[] doc1 = {"id","2", "term_s","YYYY", group, "1", "test_ti", "5", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));

    String[] doc4 = {"id","5", "term_s", "YYYY", group, "2", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "term_s","YYYY", group, "2", "test_ti", "10", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc5));
    assertU(commit());

    String[] doc6 = {"id","7", "term_s", "YYYY", group, "1", "test_ti", "10", "test_tl", "50", "test_tf", "300"};
    assertU(adoc(doc6));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!graphTerms f=group_s maxDocFreq=10}1,2");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=5]",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='6']",
        "//result/doc[5]/str[@name='id'][.='7']"
    );

    //Test without maxDocFreq param. Should default to Integer.MAX_VALUE and match all terms.
    params = new ModifiableSolrParams();
    params.add("q", "{!graphTerms f=group_s}1,2");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=5]",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='6']",
        "//result/doc[5]/str[@name='id'][.='7']"
    );

    params = new ModifiableSolrParams();
    params.add("q", "{!graphTerms f=group_s maxDocFreq=1}1,2");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=0]"
    );

    //Test with int field
    params = new ModifiableSolrParams();
    params.add("q", "{!graphTerms f=test_ti maxDocFreq=10}5,10");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=5]",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='6']",
        "//result/doc[5]/str[@name='id'][.='7']"
    );

    //Test with int field
    params = new ModifiableSolrParams();
    params.add("q", "{!graphTerms f=test_ti maxDocFreq=2}5,10");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='7']"
    );
  }
}
