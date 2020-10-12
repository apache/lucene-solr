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
import org.apache.solr.common.SolrException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 *
 * Test that querying for A with no qualifiers searches the default search field
 * Test that querying for A and passing df=bill_text is identical to querying for bill_text:A
 * Test that field_not_existing:A returns an error
 * Test that bill_text: A returns an error (illegal space after : )
 * Test that bill_text:* returns all documents that have a value for bill_text and none that have not indexed a value for that field.
 * Test that !bill_text:* returns all documents that do not have a value indexed for bill_text
 * Test that ~bill_text:A and +bill_text:B returns documents that have bill text always containing B, and sometimes containing A, with documents containing A given higher relevancy
 * Test that “bill_text:A” returns documents that have the words Bill Text an A in exactly that order (this is a valid query, but probably user error)
 * Test that bill_text:“A B” returns only documents with A and B in consecutive order.
 * Test compatibility with all other operators, grouping after the : and other operators before the field name examples include:
 *   bill_text:‘foo’
 *   bill_text:W/3(foo bar)
 *   +bill_text:foo
 *   -bill_text:foo
 *   ~bill_text:foo
 *   W/3(bill_text:foo bill_text:bar)
 *   W/3(bill_text:foo other_text:bar) - ERROR: distance across fields not possible
 *   bill_text:(A ~(B C))
 *   bill_text: (A ~(B C)) - ERROR: space after colon creates fielded search with no value and a default field search of (A ~(B C))
 *
 * </pre>
 *
 *
 *
 *
 */
public class FieldedSearchTest extends AbstractAqpTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testFieldSearchCombo() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "bill_text", "foo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "bill_text", "foo one two bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "bill_text", "foo one two three bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "bill_text", "no a word matches")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "other_text", "bar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "bill_text", "X")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "bill_text", "X B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "bill_text", "X C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "9", "bill_text", "X B C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "10", "other_text", "bar")));
    getSolrClient().commit(coll);

    QueryResponse resp;

    resp = getSolrClient().query(coll, params("q", "bill_text:\"foo\"", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1", "2", "3");

    resp = getSolrClient().query(coll, params("q", "~bill_text:foo", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1", "2", "3");

    try {
      getSolrClient().query(coll, params("q",
          "W/3(bill_text:foo other_text:bar) - ERROR: distance across fields not possible", "defType", "advanced"));
    } catch (SolrException e) {
      assertTrue(true);
    }

    resp = getSolrClient().query(coll, params("q", " bill_text:(X ~(B C))", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "6","7", "8", "9");

    try {
      getSolrClient().query(coll, params("q",
          "bill_text: (X ~(B C)) - ERROR: space after colon creates fielded search with no value and a default field search of (X ~(B C))",
          "defType", "advanced"));
    } catch (SolrException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testFieldSearchProximity() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 1, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "bill_text", "john adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "bill_text", "john bbb adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "bill_text", "john bbb bbb adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "bill_text", "john bbb bbb bbb adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "bill_text", "john bbb bbb bbb bbb adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "21", "other_text", "john adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "22", "other_text", "john bbb adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "23", "other_text", "john bbb bbb adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "24", "other_text", "john bbb bbb bbb adams")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "25", "other_text", "john bbb bbb bbb bbb adams")));
    getSolrClient().commit(coll);

    QueryResponse resp;

    resp = getSolrClient().query(coll, params("q", "bill_text:W/3(john adams)", "defType", "advanced", "q.op", "AND"));
    expectMatchExactlyTheseDocs(resp, "1", "2", "3");

    resp = getSolrClient().query(coll, params("q", "bill_text:W/3(john adams)", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1", "2", "3");

    resp = getSolrClient().query(coll, params("q", "bill_text:W/3(john)", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1", "2", "3","4","5");

    // both terms are stop words!
    resp = getSolrClient().query(coll, params("q", "bill_text:W/3( a a )", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp);
  }

  @Test
  public void testFieldSearchPhraseExactOrder() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "bill_text", "X C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "bill_text", "X B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "bill_text", "B X")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "bill_text", "X C B")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "bill_text:\"X B\"", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "2");
    haveNone(resp, "1", "3", "4");
  }


  @Test
  public void testMustAndShould() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "other_text", "X D")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "bill_text", "B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "bill_text", "B C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "bill_text", "X B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "other_text", "X C")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll,
        params("q", "~bill_text:X and +bill_text:B", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "2", "3", "4");
    haveNone(resp, "1", "5");

    // 1st id should be "4" as it has B, and also having A, which makes it rank
    // higher "B", "B C"
    assertEquals("4", resp.getResults().get(0).get("id"));
  }

  @Test
  public void testFieldValueWildcardValueNot() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "bill_text", "X")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "bill_text", "B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "bill_text", "C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "other_text", "D")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "other_text", "E")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "!bill_text:*", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "4", "5");
    haveNone(resp, "1", "2", "3");
  }

  @Test
  public void testFieldValueWildcardValue() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "bill_text", "X")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "bill_text", "B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "bill_text", "C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "other_text", "D")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "other_text", "E")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "bill_text:*", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1", "2", "3");
    haveNone(resp, "4", "5");
  }

  @Test(expected = SolrException.class)
  public void testNonExistingField() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "bill_text", "X")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "other_text", "B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "C")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "non_existing_field:x", "defType", "advanced"));
    assertEquals(0, resp.getResults().size());
  }

  @Test
  public void testSpecifyDefaultField() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "bill_text", "X")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "other_text", "B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "bill_text", "B")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "X", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1");

    resp = getSolrClient().query(coll, params("q", "C", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "3");
    resp = getSolrClient().query(coll, params("q", "C", "df", "bill_text", "defType", "advanced"));
    assertEquals(0, resp.getResults().size());
    resp = getSolrClient().query(coll, params("q", "X", "df", "bill_text", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1");
  }

  @Test
  public void testDefaultFieldSearch() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "X")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "bill_text", "B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "other_text", "C")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", "C")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "X", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1");
  }

}
