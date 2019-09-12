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

import org.apache.lucene.search.*;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

public class TestOverriddenPrefixQueryForCustomFieldType extends SolrTestCaseJ4 {

  private static int[] counts= new int[2];
  private static int otherCounts;
  String[] otherTerms = {"this", "that", "those", "randomness"};

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.CustomIntFieldType",
                       (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)
                        ? "solr.IntPointPrefixActsAsRangeQueryFieldType"
                        : "solr.TrieIntPrefixActsAsRangeQueryFieldType"));
    initCore("solrconfig-basic.xml", "schema-customfield.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    clearIndex();
    assertU(commit());
    otherCounts=0;
    counts = new int[2];
  }

  public void createIndex(int nDocs) {
    Random r = random();

    for (int i=0; i<nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", ""+i);
      int t = r.nextInt(1000);
      if(t%3 == 0) {
        doc.addField("swap_foo_bar_in_prefix_query", "foo" + i);
        counts[0]++;
      } else if(t%3 == 1) {
        doc.addField("swap_foo_bar_in_prefix_query", "foo" + i);
        doc.addField("swap_foo_bar_in_prefix_query", "spam" + i);
        otherCounts++;
        counts[0]++;
      } else {
        doc.addField("swap_foo_bar_in_prefix_query", "bar" + i);
        counts[1]++;
      }
      //Randomly add noise

      doc.addField("int_prefix_as_range", i);
      doc.addField("intfield", i);

      assertU(adoc(doc));
    }
    assertU(commit());
  }

  @Test
  public void testPrefixQueries() throws Exception {
    createIndex(100);
    assertQ(req("fl", "id", "q", "*:*"), "//*[@numFound='100']");

    // Test that prefix query actually transforms foo <-> bar.
    assertQ(req("q", "swap_foo_bar_in_prefix_query:foo*"), "//*[@numFound='" + counts[1] + "']");

    assertQ(req("q", "swap_foo_bar_in_prefix_query:bar*"), "//*[@numFound='" + counts[0] + "']");
    assertQ(req("q", "swap_foo_bar_in_prefix_query:spam*"), "//*[@numFound='" + otherCounts + "']");

    //Custom field should query for the range [2,MAX_INT)
    assertQ(req("q", "int_prefix_as_range:2*"),"//*[@numFound='98']");

  }

  @Test
  public void testQuery() throws Exception {
    SolrQueryRequest req = req("myField","swap_foo_bar_in_prefix_query");

    try {
      assertQueryEquals(req,
          "{!simple qf=$myField}foo*",
          "{!simple qf=$myField}foo*",
          "{!prefix f=swap_foo_bar_in_prefix_query}foo",
          "{!lucene df=$myField v=foo*}",
          "{!lucene}swap_foo_bar_in_prefix_query:foo*");

      req.close();
      req = req("myField", "int_prefix_as_range");
      assertQueryEquals(req,
          "{!lucene}int_prefix_as_range:[42 TO 2147483647}",
          "{!lucene}int_prefix_as_range:42*",
          "{!prefix f=int_prefix_as_range}42",
          "{!simple qf=int_prefix_as_range}42*",
          "{!simple df=int_prefix_as_range}42*");

    } finally {
      req.close();
    }
  }

  /**
   * @see org.apache.lucene.search.QueryUtils#check
   * @see org.apache.lucene.search.QueryUtils#checkEqual
   */
  protected void assertQueryEquals(final SolrQueryRequest req,
                                   final String... inputs) throws Exception {

    final Query[] queries = new Query[inputs.length];

    try {
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      for (int i = 0; i < inputs.length; i++) {
        queries[i] = (QParser.getParser(inputs[i], req).getQuery());
      }
    } finally {
      SolrRequestInfo.clearRequestInfo();
    }

    for (int i = 0; i < queries.length; i++) {
      org.apache.lucene.search.QueryUtils.check(queries[i]);
      for (int j = i; j < queries.length; j++) {
        org.apache.lucene.search.QueryUtils.checkEqual(queries[i], queries[j]);
      }
    }
  }
}
