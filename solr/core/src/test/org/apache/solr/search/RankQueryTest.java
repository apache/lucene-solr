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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RankQueryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-plugcollector.xml", "schema15.xml");
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
  public void testPluggableCollector() throws Exception {

    String[] doc = {"id","1", "sort_i", "100"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "sort_i", "50"};
    assertU(adoc(doc1));



    String[] doc2 = {"id","3", "sort_i", "1000"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "sort_i", "2000"};
    assertU(adoc(doc3));


    String[] doc4 = {"id","5", "sort_i", "2"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "sort_i","11"};
    assertU(adoc(doc5));
    assertU(commit());


    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", "*:*");
    params.add("rq", "{!rank}");
    params.add("sort","sort_i asc");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='3']",
        "//result/doc[3]/str[@name='id'][.='1']",
        "//result/doc[4]/str[@name='id'][.='2']",
        "//result/doc[5]/str[@name='id'][.='6']",
        "//result/doc[6]/str[@name='id'][.='5']"
    );

    params = new ModifiableSolrParams();
    params.add("q", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(sort_i)");
    params.add("rq", "{!rank collector=1}");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[6]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[1]/str[@name='id'][.='5']"
    );


    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("sort","sort_i asc");

    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[6]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[1]/str[@name='id'][.='5']"
    );

  }
}
