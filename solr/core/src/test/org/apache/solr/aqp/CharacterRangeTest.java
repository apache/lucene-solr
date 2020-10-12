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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

/**
 * <pre>
 * Test Range searches with chars
 *   something_f:[a TO z] - a, b, c, d, e, f, g, h
 *   something_f:{a TO z] - b, c, d, e, f, g, h
 *   something_f:[cat TO concave] - cat, charcoal, clasp, commit, concave
 *   something_f:{cat TO concave} - charcoal, clasp, commit
 * </pre>
 *
 *
 */
public class CharacterRangeTest extends AbstractAqpTestCase {

  @Test
  public void testCharRanges() throws Exception {

    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "a")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "b")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "c")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "cat")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "congress", "charcoal")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "congress", "clasp")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "congress", "commit")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "congress", "concave")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "9", "congress", "d")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "10", "congress", "e")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[a TO z]", "defType", "advanced"));
    assertEquals(10, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5","6","7","8","9","10");

    resp = getSolrClient().query(coll, params("q", "congress:{a TO z]", "defType", "advanced"));
    assertEquals(9, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "2","3","4","5","6","7","8","9","10");

    resp = getSolrClient().query(coll, params("q", "congress:[a TO e}", "defType", "advanced"));
    assertEquals(9, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5","6","7","8","9");

    resp = getSolrClient().query(coll, params("q", "congress:{a TO e}", "defType", "advanced"));
    assertEquals(8, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "2","3","4","5","6","7","8","9");

    resp = getSolrClient().query(coll, params("q", "congress:[cat TO concave]", "defType", "advanced"));
    assertEquals(5, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "4","5","6","7","8");

    resp = getSolrClient().query(coll, params("q", "congress:{cat TO concave]", "defType", "advanced"));
    assertEquals(4, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "5","6","7","8");

    resp = getSolrClient().query(coll, params("q", "congress:[cat TO concave}", "defType", "advanced"));
    assertEquals(4, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "4","5","6","7");

    resp = getSolrClient().query(coll, params("q", "congress:{cat TO con]", "defType", "advanced"));
    assertEquals(3, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "5","6","7");
  }

  @Test
  public void testOpenEndedRanges() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "a")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "b")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "c")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "d")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "congress", "e")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[* TO *]", "defType", "advanced"));
    assertEquals(5, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:{* TO *]", "defType", "advanced"));
    assertEquals(5, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:[* TO *}", "defType", "advanced"));
    assertEquals(5, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:{* TO *}", "defType", "advanced"));
    assertEquals(5, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:[a TO *]", "defType", "advanced"));
    assertEquals(5, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:[* TO e]", "defType", "advanced"));
    assertEquals(5, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:{a TO *]", "defType", "advanced"));
    assertEquals(4, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:[* TO e}", "defType", "advanced"));
    assertEquals(4, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4");

    resp = getSolrClient().query(coll, params("q", "congress:{a TO *}", "defType", "advanced"));
    assertEquals(4, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "2","3","4","5");

    resp = getSolrClient().query(coll, params("q", "congress:{* TO e}", "defType", "advanced"));
    assertEquals(4, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4");
  }

  @Test
  public void testTextRange() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "ta")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "to")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "to")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "te")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[to TO to]", "defType", "advanced"));
    assertEquals(2, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "2","3");

  }

  @Test
  public void testPrefixInRange() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "sad")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "sadden")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "sadder")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "saddest")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[sad? TO sadder]", "defType", "advanced"));
    assertEquals(2, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "2","3");

    resp = getSolrClient().query(coll, params("q", "congress:{sad? TO sadder]", "defType", "advanced"));
    assertEquals(2, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "2","3");

    resp = getSolrClient().query(coll, params("q", "congress:[sad TO sadde*]", "defType", "advanced"));
    assertEquals(1, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1");
    //expectExactly("1","4", resp);

    resp = getSolrClient().query(coll, params("q", "congress:[sad TO sadde*}", "defType", "advanced"));
    assertEquals(1, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1");
  }

  @Test
  public void testShortPrefixInRange() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "sad")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "sadden")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "sadder")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "saddest")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[sa* TO sadder]", "defType", "advanced"));
    assertEquals(3, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3");
  }

  @Test
  public void testLeadingWildcardInRange() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "congress", "sad")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "congress", "sadden")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "congress", "sadder")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "congress", "saddest")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "congress:[*dde TO *]", "defType", "advanced"));
    assertEquals(4, resp.getResults().getNumFound());
    expectMatchExactlyTheseDocs(resp, "1","2","3","4");

  }

}
