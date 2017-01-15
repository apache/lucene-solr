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

public class TestXmlQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-testxmlparser.xml", "schema-minimal.xml");
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
  public void testHelloQuery() throws Exception {
    final int numDocs = random().nextInt(10);
    implTestQuery(numDocs, "<HelloQuery/>", numDocs);
  }

  @Test
  public void testGoodbyeQuery() throws Exception {
    final int numDocs = random().nextInt(10);
    implTestQuery(numDocs, "<GoodbyeQuery/>", 0);
  }

  @Test
  public void testHandyQuery() throws Exception {
    final int numDocs = random().nextInt(10);
    final String q = "<HandyQuery><Left><HelloQuery/></Left><Right><GoodbyeQuery/></Right></HandyQuery>";
    implTestQuery(numDocs, q, numDocs);
  }

  public void implTestQuery(int numDocs, String q, int expectedCount) throws Exception {
    // add some documents
    for (int ii=1; ii<=numDocs; ++ii) {
      String[] doc = {"id",ii+"0"};
      assertU(adoc(doc));
      if (random().nextBoolean()) {
        assertU(commit());
      }
    }
    assertU(commit());
    // and then run the query
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("defType", "testxmlparser");
    params.add("q", q);
    assertQ(req(params), "*[count(//doc)="+expectedCount+"]");
  }

}
