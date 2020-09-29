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
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for phrase search using advanced query parser
 */
public class PhraseTest extends AbstractAqpTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Test that “A B” returns documents with A following B directly
   */
  @Test
  public void testAFollowBDirectly() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", wordA + "    " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", wordA + "  middle  " + wordB)));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll,
        params("q", "\"" + wordA + " " + wordB + "\"", "deftype", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("3", resp.getResults().get(0).get("id"));
  }

  /**
   * Test that “A B” does not return documents with a term in between A and B
   */
  @Test
  public void testNotABSeparatedByTerm() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", wordA + "  " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", wordA + "  middle  " + wordB)));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll,
        params("q", "\"" + wordA + " " + wordB + "\"", "deftype", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("3", resp.getResults().get(0).get("id"));
  }

  /**
   * Test that “A B” does not return document with B followed by A
   */
  @Test
  public void testNotBFollowA() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", wordA + "    " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", wordB + "    " + wordA)));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll,
        params("q", "\"" + wordA + " " + wordB + "\"", "deftype", "advanced"));
    assertEquals(1, resp.getResults().size());
    assertEquals("3", resp.getResults().get(0).get("id"));
  }

  /**
   * Test that “A B” is not case sensitive
   */
  @Test
  public void testABCaseInsensitive() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";

    assertUpdateResponse(
        getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA.toUpperCase(Locale.ROOT) + "  " + wordB)));
    assertUpdateResponse(
        getSolrClient().add(coll, sdoc("id", "2", "_text_", wordA + "  " + wordB.toUpperCase(Locale.ROOT))));
    assertUpdateResponse(
        getSolrClient().add(coll, sdoc("id", "3", "_text_", wordA.toLowerCase(Locale.ROOT) + "  " + wordB)));
    assertUpdateResponse(
        getSolrClient().add(coll, sdoc("id", "4", "_text_", wordA + "  " + wordB.toLowerCase(Locale.ROOT))));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll,
        params("q", "\"" + wordA + " " + wordB + "\"s", "deftype", "advanced"));
    assertEquals(4, resp.getResults().size());
  }

  /**
   * Test that “A B” returns A C where C is configured as a synonym for B
   */
  @Test
  public void testSynonymSearch() throws Exception {
    String synPath = "/configs/_default/synonyms.txt";
    String wordA = "utilizes";
    String wordB = "powerful";
    String wordC = "strong";

    // Note that with the synonym graph filter if you don't specify that a word is it's own synonym then
    // the word is effectively removed from the query. This can be ok for correcting common misspellings
    // when querying against a corpus not likely to contain misspellings for example teh => the.
    String synonyms =
        "#some test synonym mappings\n" +
            wordB + " => " + wordB + "\n" +
            wordB + " => " + wordC + "\n";
    cluster.getZkClient().setData(synPath, synonyms.getBytes(StandardCharsets.UTF_8), false);

    // Uncomment these two lines to check what you put in ZK
    // byte [] data = cluster.getZkClient().getData(synPath, null, null, false);
    // System.out.println(new String(data));

    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA + "  " + wordB, "text_s", wordA + "  " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", wordA + "  " + wordC, "text_s", wordA + "  " + wordC)));

    getSolrClient().commit(coll);

    // Uncomment below to verify synonym expansion directly
    //    CloudSolrClient solrClient = getSolrClient();
    //    FieldAnalysisRequest req = new FieldAnalysisRequest();
    //    req.addFieldName("_text_");
    //    req.setQuery(wordB);
    //    NamedList<Object> request = solrClient.request(req, coll);
    //    System.out.println("ANALYSIS:" + request.toString());

    QueryResponse respAll = getSolrClient().query(coll, params("q", "*:*"));
    assertEquals(2, respAll.getResults().size());

    QueryResponse resp = getSolrClient().query(coll, params("q", wordB));
    assertEquals(2, resp.getResults().size());

    resp = getSolrClient().query(coll,
        params("q", "\"" + wordA + " " + wordB + "\"", "deftype", "advanced"));
    assertEquals(2, resp.getResults().size());
  }

  /**
   * Test that “A B” returns A B’ where B’ is a word with the same stem as B
   */
  @Test
  public void testStemSearch() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String wordA = "utilizes";
    String wordB = "powerful";
    String wordC = "util";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", wordA + "  " + wordB)));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", wordC + "  " + wordB)));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll,
        // q="utilizes powerful" ---> matches both docs even though it's a phrase
        params("q", "\"" + wordA + " " + wordB + "\"", "deftype", "advanced"));
    assertEquals(2, resp.getResults().size());

    resp = getSolrClient().query(coll,
        // q="util powerful" ---> matches both docs even though it's a phrase
        params("q", "\"" + wordC + " " + wordB + "\"", "deftype", "advanced"));
    assertEquals(2, resp.getResults().size());

     resp = getSolrClient().query(coll,
        // q="powerful powerful" ---> matches no docs because it's a phrase
        params("q", "\"" + wordB + " " + wordB + "\"", "deftype", "advanced"));
    assertEquals(0, resp.getResults().size());

  }


  // duplicate of LiteralPhraseTest test method but with different expectations
  @Test
  public void testStemSearch2() throws Exception {
    String coll = getSaferTestName();
    getSolrClient().setDefaultCollection(coll); // required by SchemaRequest api
    CollectionAdminRequest.createCollection(coll, "aqp", 2, 1).process(getSolrClient());

    String fieldName = "_text_";

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "dog")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "dogs")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "dog's")));

    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "dogs", "defType", "advanced", "df", fieldName));
    assertEquals(3, resp.getResults().size());

    resp = getSolrClient().query(coll, params("q", "dogs", "defType", "advanced", "df", fieldName + "_lit"));
    expectMatchExactlyTheseDocs(resp, "2");

    resp = getSolrClient().query(coll, params("q", "\"dogs\"", "defType", "advanced", "df", fieldName));
    assertEquals(3, resp.getResults().size());

    resp = getSolrClient().query(coll, params("q", "\"dog\"", "defType", "advanced", "df", fieldName));
    assertEquals(3, resp.getResults().size());

    resp = getSolrClient().query(coll, params("q", "\"dog\\'s\"", "defType", "advanced", "df", fieldName));
    assertEquals(3, resp.getResults().size());

  }
}
