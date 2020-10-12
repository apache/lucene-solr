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

import org.apache.lucene.analysis.pattern.PatternTypingFilter;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test demonstrates a usage of AQP that leverages the punctuation handling to allow recognition of legal terms,
 * US phone numbers or hyphenation. This test integrates functionality from
 * {@link org.apache.lucene.analysis.miscellaneous.TypeAsSynonymFilter},
 * {@link PatternTypingFilter},
 * and {@link org.apache.lucene.analysis.miscellaneous.DropIfFlaggedFilter}
 */
public class TestPatternTypingSynonyms extends AbstractAqpTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testLegalIdentifierPatterns() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "spouse's 401(k) plan")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "qualifying 501(c)(3) organization")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "_text_", "401(k)")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "_text_", "501(c)(3)")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "401(k)", "defType", "advanced"));
    this.expectMatchExactlyTheseDocs(resp, "1", "5");

    resp = getSolrClient().query(coll, params("q", "501(c)(3)", "defType", "advanced"));
    this.expectMatchExactlyTheseDocs(resp, "2", "6");

    // search for parts of special exact terms and ensure those don't match
    resp = getSolrClient().query(coll, params("q", "401 k", "defType", "advanced"));
    assertEquals(0, resp.getResults().size());

    resp = getSolrClient().query(coll, params("q", "501 c 3", "defType", "advanced"));
    assertEquals("Found Unexpected Docs:" + resp.getResults().toString() , 0, resp.getResults().size());

  }

  @Test
  public void testPhoneNumbers() throws Exception {
    // here we demonstrate support for equivalency among common NANP phone number formats. Note that this does
    // not support 1- prefixes at this time
    // (555) 555-5555
    // 555-555-5555
    // 555.555.5555
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 1, 1)
        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "1", "_text_", "foo (555)555-5555 bar"))); // can't have space :(
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "2", "_text_", "foo 555-555-5555 bar")));
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "3", "_text_", "foo 555.555.5555 bar")));
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "4", "_text_", "foo 555 555 5555 bar"))); // not a supported format
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "5", "_text_", "foo (555) bar 555 5555 baz"))); // not a match
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "6", "_text_", "foo 555-867-5309 baz"))); // Jenny's number is not a match
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "6", "_text_", "foo (555) 555-5555 baz"))); // not a match
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "id:1"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    aqpSearch(coll, "555-555-5555", "1", "2", "3");
    aqpSearch(coll, "1.555.555.5555", "1", "2", "3");

  }

  @Test
  public void testHyphendatedWordTransitions() throws Exception {
    String coll = getSaferTestName();

    CollectionAdminRequest.createCollection(coll, "aqp", 1, 1)
        .process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "1", "_text_", "Foo123")));
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "2", "_text_", "foo-123")));
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "3", "_text_", "foo 123")));
    assertUpdateResponse(getSolrClient().add(coll,sdoc("id", "4", "_text_", "foo-bar")));
    getSolrClient().commit(coll);

    // verify that something is indexed with a regular standard query parser query
    QueryResponse resp = getSolrClient().query(coll, params("q", "id:1"));
    assertEquals(1, resp.getResults().size());
    assertEquals("1", resp.getResults().get(0).get("id"));

    // now use the advanced query parser
    aqpSearch(coll, "foo", "2", "3");
    // now use the advanced query parser
    aqpSearch(coll, "123", "2", "3");

    aqpSearch(coll, "foo123", "1");

    aqpSearch(coll, "foo-bar", "4"); // relies on patterns.txt and PatternTypingFilter!

    resp = getSolrClient().query(coll, params("q", "bar", "defType","advanced", "sort", "id asc"));
    assertEquals(0, resp.getResults().size());
  }

}
