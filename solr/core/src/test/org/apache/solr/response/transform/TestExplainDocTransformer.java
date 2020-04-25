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

package org.apache.solr.response.transform;


import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExplainDocTransformer extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");

    assertU(add(doc("id", "1", "name_s", "john", "title_s", "Director", "dept_s","Engineering",
        "text_t","These guys develop stuff")));
    assertU(add(doc("id", "2", "name_s", "mark", "title_s", "VP", "dept_s","Marketing",
        "text_t","These guys make you look good")));
    assertU(add(doc("id", "3", "name_s", "nancy", "title_s", "MTS", "dept_s","Sales",
        "text_t","These guys sell stuff")));
    assertU(add(doc("id", "4", "name_s", "dave", "title_s", "MTS", "dept_s","Support",
        "text_t","These guys help customers")));
    assertU(add(doc("id", "5", "name_s", "tina", "title_s", "VP", "dept_s","Engineering",
        "text_t","These guys develop stuff")));
    assertU(commit());
  }

  @After
  public void cleanup() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testStyle() throws Exception {
    // this doesn't validate the explain response but checks if explain response is returned in expected format

    // when not style is passed then default style should be used
    assertQ(req("q", "*:*", "fl", "id,[explain]"), "//result/doc[1]/str[@name='id'][.='1']",
        "boolean(//result/doc[1]/str[@name='[explain]'])");

    // doc transformer defined in solrconfig without style
    assertQ(req("q", "*:*", "fl", "id,[explain1]"), "//result/doc[1]/str[@name='id'][.='1']",
        "boolean(//result/doc[1]/str[@name='[explain1]'])");

    // doc transformer defined in solrconfig with style=nl
    assertQ(req("q", "*:*", "fl", "id,[explainNL]"), "//result/doc[1]/str[@name='id'][.='1']",
        "boolean(//result/doc[1]/lst[@name='[explainNL]'])");

    // doc transformer defined in solrconfig with style=nl
    assertQ(req("q", "*:*", "fl", "id,[explainText]"), "//result/doc[1]/str[@name='id'][.='1']",
        "boolean(//result/doc[1]/str[@name='[explainText]'])");

    // passing style as parameter at runtime
    assertQ(req("q", "*:*", "fl", "id,[explainText style=nl]"), "//result/doc[1]/str[@name='id'][.='1']",
        "boolean(//result/doc[1]/lst[@name='[explainText]'])");
  }
}
