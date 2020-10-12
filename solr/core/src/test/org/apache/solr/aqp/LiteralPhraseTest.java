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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.AnalyzerDefinition;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 *  - Test that ‘A B’ returns documents with A followed by B directly
 *  - Test that ‘A B’ does not return documents with a term in between A and B
 *  - Test that ‘A B’ does not return document with B followed by A
 *  - Test that ‘A B’ returns documents where A and B are separated by punctuation (e.g. “A.B”)
 *  - Test that ‘A B’ is not case sensitive
 *  - Test that ‘A B’ does not return A C where C is configured as a synonym for B
 *  - Test that ‘A B’ does not return A B’ where B’ is a word with the same stem as B
 * </pre>
 */
public class LiteralPhraseTest extends AbstractAqpTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test(expected = SolrException.class)
  public void testNoConfiguredLitField() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "other_text";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "A B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "B A")));

    getSolrClient().commit(coll);

    getSolrClient().query(coll, params("q", "'A B'", "defType", "advanced", "df", fieldName));

  }

  @Test
  public void testDefaultFieldAFollowBDirectly() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "bill_text";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "C B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "B C")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "C", "defType", "advanced", "df", fieldName + "_lit"));
    expectMatchExactlyTheseDocs(resp, "1", "2");

    resp = getSolrClient().query(coll, params("q", "B", "defType", "advanced", "df", fieldName + "_lit"));
    expectMatchExactlyTheseDocs(resp, "1", "2");

    resp = getSolrClient().query(coll, params("q", "\"C B\"", "defType", "advanced", "df", fieldName + "_lit"));
    expectMatchExactlyTheseDocs(resp, "1");

    resp = getSolrClient().query(coll, params("q", "'C B'", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1");
  }

  @Test
  public void testFieldedAFollowBDirectly() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "bill_text";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "C B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "B C")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", fieldName + ":'C B'", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1");
    haveNone(resp, "2");
  }

  @Test
  public void testATermB() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "_text_";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "Z B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "Z C B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "C D E")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", fieldName, "z b")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "'Z B'", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1", "4");
    haveNone(resp, "2", "3");
  }



  @Test
  public void testCaseInsensitive() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "_text_";

    // create a literal field with lowercase filter
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    Map<String, Object> filterConfig = new LinkedHashMap<String, Object>() {{ put("class", "solr.LowerCaseFilterFactory"); }};
    Map<String, Object> tokenizerAttributes = new LinkedHashMap<String, Object>() {{ put("class", "solr.KeywordTokenizerFactory"); }};
    analyzerDefinition.setTokenizer(tokenizerAttributes);
    analyzerDefinition.setFilters(Collections.singletonList(filterConfig));

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "Z b")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "z b")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "Z B")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", fieldName, "Z d")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", fieldName, "z d")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", fieldName, "b z")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", fieldName, "B Z")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "'Z B'", "defType", "advanced", "df", fieldName));

    expectMatchExactlyTheseDocs(resp, "1", "2", "3");
    haveNone(resp, "4", "5", "6", "7");
  }

  @Test
  public void testSynonymsSearch() throws Exception {
    // aqp configset includes some test synonyms we'll use for this test
    //    aaafoo => aaabar
    //    bbbfoo => bbbfoo bbbbar
    //    cccfoo => cccbar cccbaz
    //    fooaaa,baraaa,bazaaa

    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "_text_";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "aaafoo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "aaabar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "bbbfoo")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", fieldName, "bbbbar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", fieldName, "cccbar")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", fieldName, "cccbaz")));

    getSolrClient().commit(coll);

    // standard query will honor synonyms
    QueryResponse resp = getSolrClient().query(coll, params("q", "aaafoo", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "2");

    resp = getSolrClient().query(coll, params("q", "cccfoo", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "5", "6");

    // literal query will disregard synonyms (searches a different analyzer chain)
    resp = getSolrClient().query(coll, params("q", "'aaafoo'", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "1");

    resp = getSolrClient().query(coll, params("q", "'bbbfoo'", "defType", "advanced"));
    expectMatchExactlyTheseDocs(resp, "3");

    resp = getSolrClient().query(coll, params("q", "'cccfoo'", "defType", "advanced"));
    assertEquals(0, resp.getResults().size());
  }

  @Test
  public void testStemSearch() throws Exception {
    String coll = getSaferTestName();
    getSolrClient().setDefaultCollection(coll); // required by SchemaRequest api
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "_text_";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "dog")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "dogs")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "dog's")));

    getSolrClient().commit(coll);

//    QueryResponse resp = getSolrClient().query(coll, params("q", "dogs", "defType", "advanced", "df", fieldName));
//    assertEquals(3, resp.getResults().size());

    QueryResponse resp = getSolrClient().query(coll, params("q", "dogs", "defType", "advanced", "df", fieldName + "_lit"));
    expectMatchExactlyTheseDocs(resp, "2");

    resp = getSolrClient().query(coll, params("q", "'dogs'", "defType", "advanced", "df", fieldName));
    expectMatchExactlyTheseDocs(resp, "2");

    resp = getSolrClient().query(coll, params("q", "'dog'", "defType", "advanced", "df", fieldName));
    expectMatchExactlyTheseDocs(resp, "1");

    resp = getSolrClient().query(coll, params("q", "'dog\\'s'", "defType", "advanced", "df", fieldName));
    expectMatchExactlyTheseDocs(resp, "3");

  }



}
