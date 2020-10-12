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
package org.apache.solr.aqp;

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test simple group search using advanced query parser
 */
public class SimpleGroupTest extends AbstractAqpTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Test that (A B) returns identical results to A B
   */
  @Test
  public void testWordsInParentheses() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA + "    " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", wordB + "    " + wordA)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "foo bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", "foo bar " + wordA + "    " + wordB)));

    getSolrClient().commit(coll);

    QueryResponse resp1 = getSolrClient().query(coll,
        params("q", "(" + wordA + " " + wordB + ")", "deftype", "advanced"));
    int cnt1 = resp1.getResults().size();

    QueryResponse resp2 = getSolrClient().query(coll, params("q", wordA + " " + wordB, "deftype", "advanced"));
    int cnt2 = resp2.getResults().size();

    assertEquals(cnt1, cnt2);
    String[] ids = resp1.getResults().stream().map(doc -> (String) doc.get("id")).toArray(String[]::new);
    haveAll(resp2, ids);
  }

  /**
   * Test that A (B C) retuns identical results to A B C
   */
  @Test
  public void testMultipleWordsInParentheses() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";
    String wordC = "authority";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA + "    " + wordB + "    " + wordC)));
    // duplicate of id=1 data, intentionally
    getSolrClient().add(coll, sdoc("id", "2", "_text_", wordA + "    " + wordB + "    " + wordC));
    getSolrClient().add(coll, sdoc("id", "3", "_text_", wordB + "    " + wordC));
    getSolrClient().add(coll, sdoc("id", "4", "_text_", wordA + "    " + wordC + "    " + wordB));
    getSolrClient().commit(coll);

    QueryResponse resp1 = getSolrClient().query(coll,
        params("q", wordA + "(" + wordB + " " + wordC + ")", "deftype", "advanced"));
    int cnt1 = resp1.getResults().size();

    QueryResponse resp2 = getSolrClient().query(coll,
        params("q", wordA + " " + wordB + " " + wordC, "deftype", "advanced"));
    int cnt2 = resp2.getResults().size();

    assertEquals(cnt1, cnt2);
    String[] ids = resp1.getResults().stream().map(doc -> (String) doc.get("id")).toArray(String[]::new);
    haveAll(resp2, ids);
  }

  /**
   * test should contain and must contain combination
   */
  @Test
  public void testShouldContain() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";
    String wordC = "authority";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", wordA + "    " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "_text_", wordB + "    " + wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "_text_", wordA + "    " + wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "_text_", wordA + "    " + wordB + "    " + wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "_text_", "Fo(o(b)ar)")));
    getSolrClient().commit(coll);

    // test ~A ~(+B +C)
    QueryResponse resp
        = getSolrClient().query(coll,
        params("q", "~" + wordA + " ~(+" + wordB + " +" + wordC + ")", "defType", "advanced"));

    // contains all docs with A
    haveAll(resp, "1", "4", "6", "7");
    // contains all docs both B, C
    haveAll(resp, "5", "7");
    // contains both B, C, but not A
    haveAll(resp, "5");
    // No documents that contain only one of B and C but don't contain A
    haveNone(resp, "2", "3");

    // test ~A ~(B C) with default and is equivalent to ~A (~B ~C)
    resp = getSolrClient().query(coll, params("q", "~" + wordA + " ~(" + wordB + " " + wordC + ")", "defType", "advanced", "q.op", "and"));

    // contains all docs with A
    haveAll(resp, "1", "4", "6", "7");
    // contains all docs with B,
    haveAll(resp, "2", "4", "5", "7");
    // contains all docs with C,
    haveAll(resp, "3", "5", "6", "7");
    // No documents that contain only one of B and C
    haveNone(resp, "8");
  }

  /**
   * Test to verify our magical user friendly parenthesis detection and handling
   */
  @Test
  public void testParenMagic() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "_text_", "Fo(o(b)ar)")));
    getSolrClient().commit(coll);

    QueryResponse resp;

    // test handling of ending parens - this should include the first ) in the token and leave the last two )
    // for syntax the token will be Fo(o(b)ar) in this case
    resp = getSolrClient().query(coll, params("q", " ~(+(Fo(o(b)ar))))", "defType", "advanced", "q.op", "and"));

    expectMatchExactlyTheseDocs(resp, "8");

    resp = getSolrClient().query(coll, params("q", " ~(+(Fo(o(b)ar)))", "defType", "advanced", "q.op", "and"));

    expectMatchExactlyTheseDocs(resp, "8");

    // test handling of ending parens - this should leave the last two ) for syntax... here this will match because
    // we are using standard tokenizer setup. In other tests we'll test that it doesn't match once we have pattern
    // based synonyms - the token will be 'Fo(o(b)ar' in this case
    resp = getSolrClient().query(coll, params("q", " ~(+(Fo(o(b)ar))", "defType", "advanced", "q.op", "and"));

    expectMatchExactlyTheseDocs(resp, "8");

    try {
      getSolrClient().query(coll, params("q", " ~(+(Fo(o(b)ar)", "defType", "advanced", "q.op", "and"));
      fail("insufficient parenthesis should cause an error");
    } catch (Exception e) {
      // correct
    }

  }

  /**
   * test must operator +, must not operator !, and should operator ~
   */

  @Test
  public void testMustContain() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";
    String wordC = "authority";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", wordA + "    " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "_text_", wordB + "    " + wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "_text_", wordA + "    " + wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "_text_", wordA + "    " + wordB + "    " + wordC)));
    getSolrClient().commit(coll);

    // test +A !(~B ~C)
    QueryResponse resp1 = getSolrClient().query(coll,
        params("q", "+" + wordA + " !(~" + wordB + " ~" + wordC + ")", "defType", "advanced"));
    int cnt1st = resp1.getResults().size();

    // only documents with A but not B or C
    expectMatchExactlyTheseDocs(resp1, "1");

    // test +A !B !C
    QueryResponse resp2 = getSolrClient().query(coll,
        params("q", "+" + wordA + " !" + wordB + " !" + wordC, "defType", "advanced"));

    // only documents with A but not B or C
    expectMatchExactlyTheseDocs(resp2, "1");

  }
  /**
   * test subgroup must/should logic
   */
  @Test
  public void testGroupingLogic() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";
    String wordC = "authority";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", wordA + "    " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "_text_", wordB + "    " + wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "_text_", wordA + "    " + wordC)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "_text_", wordA + "    " + wordB + "    " + wordC)));
    getSolrClient().commit(coll);

    // test +A +(~B ~C)
    QueryResponse resp = getSolrClient().query(coll,
        params("q", "+" + wordA + " +(~" + wordB + " ~" + wordC + ")", "defType", "advanced"));
    int cnt1st = resp.getResults().size();

    // only documents with A and AT LEAST one of B or C
    expectMatchExactlyTheseDocs(resp, "4", "6", "7");

    // above test within an should. The entire top level should, ought to be overridden by the internal operators
    resp = getSolrClient().query(coll,
        params("q", "~(+" + wordA + " +(~" + wordB + " ~" + wordC + "))", "defType", "advanced"));

    // only documents with A but not B or C
    expectMatchExactlyTheseDocs(resp, "4", "6", "7");

  }
}
