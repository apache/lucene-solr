package org.apache.solr.update.processor;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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

public class AtomicUpdatesTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "true");
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void before() {
    h.update("<delete><query>*:*</query></delete>");
    assertU(commit());
  }
  @Test
  public void testRemove() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", new String[]{"aaa", "bbb", "ccc", "ccc", "ddd"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "2");
    doc.setField("cat", new String[]{"aaa", "bbb", "bbb", "ccc", "ddd"});
    assertU(adoc(doc));


    doc = new SolrInputDocument();
    doc.setField("id", "20");
    doc.setField("cat", new String[]{"aaa", "ccc", "ddd"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "21");
    doc.setField("cat", new String[]{"aaa", "bbb", "ddd"});
    assertU(adoc(doc));


    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '3']");


    doc = new SolrInputDocument();
    doc.setField("id", "1");
    List<String> removeList = new ArrayList<String>();
    removeList.add("bbb");
    removeList.add("ccc");
    doc.setField("cat", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '2']");

    doc = new SolrInputDocument();
    doc.setField("id", "21");
    removeList = new ArrayList<String>();
    removeList.add("bbb");
    removeList.add("ccc");
    doc.setField("cat", ImmutableMap.of("remove", removeList)); //behavior when hitting Solr through ZK
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", ImmutableMap.of("remove", "aaa")); //behavior when hitting Solr directly

    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '4']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '3']");
  }

  @Test
  public void testAdd() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "3");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "4");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '0']");


    doc = new SolrInputDocument();
    doc.setField("id", "3");
    doc.setField("cat", ImmutableMap.of("add", "bbb"));
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testSet() throws Exception {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "5");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    doc = new SolrInputDocument();
    doc.setField("id", "6");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "cat:ccc", "indent", "true"), "//result[@numFound = '2']");


    doc = new SolrInputDocument();
    doc.setField("id", "5");
    doc.setField("cat", ImmutableMap.of("set", "bbb"));
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '2']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:ccc", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testInvalidOperation() {
    SolrInputDocument doc;

    doc = new SolrInputDocument();
    doc.setField("id", "7");
    doc.setField("cat", new String[]{"aaa", "ccc"});
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "7");
    doc.setField("cat", ImmutableMap.of("whatever", "bbb"));
    assertU(adoc(doc));
    assertU(commit());

    assertQ(req("q", "cat:*", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:aaa", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "cat:bbb", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "cat:ccc", "indent", "true"), "//result[@numFound = '1']");

  }
}
