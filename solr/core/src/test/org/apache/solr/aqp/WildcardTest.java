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
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.AnalyzerDefinition;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <pre>
 * Test that supe*
 * - Matches super
 * - Matches Super
 * - Matches SUPER
 * - Matches superb
 * - Matches superfund
 * - Matches supercalifragilisticexpialidocious
 * - Does not match sup
 * - Does not match supine
 * Test that su* is an error (too short)
 * Test that *uper is an error (no prefixes) UNLESS a ReverseTokenFilterFactory is configured for the FieldType
 * Test that s*er is an error (no infixes) UNLESS a ReverseTokenFilterFactory is configured for the FieldType
 * Test that supe? Is an error (? Not a valid wild card) UNLESS a ReverseTokenFilterFactory is configured for the FieldType
 *
 * Allow queries normally disallowed due to length of prefix on fields configured with ReverseWildcardFilterFactory.
 * </pre>
 */
public class WildcardTest extends AbstractAqpTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testPrefixQuery() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    // matches these
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "SUPER")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "4", "_text_", "superb")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "5", "_text_", "superfund")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "6", "_text_", "supercalifragilisticexpialidocious")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "7", "_text_", "supe")));

    // does not match these
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "8", "_text_", "sup")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "9", "_text_", "supine")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "supe*", "defType", "advanced"));
    assertEquals(7, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1", "2", "3", "4", "5", "6", "7");
    haveNone(resp, "8", "9");
  }

  @Test(expected = SolrException.class)
  public void testPrefixQueryMinimumLengthUnmet() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Super")));
    getSolrClient().commit(coll);

    // Boom!
    getSolrClient().query(coll, params("q", "su*", "defType", "advanced"));
  }

  public void testPrefixQueryMinimumLengthMet() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Super")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "sup*", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
  }

  @Test(expected = SolrException.class)
  public void testWildcardDisallowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Super")));
    getSolrClient().commit(coll);

    // Boom!
    getSolrClient().query(coll, params("q", "supe?", "defType", "advanced"));
  }

  public void testWildcardWithReversedWildcardFilterFactoryAllowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String fieldName = coll + "_rev";
    configureTestField(coll, fieldName, createRevWildcardFilterConfig());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "Super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "Superate")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", fieldName + ":supe?", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
    haveNone(resp, "2", "3");
  }

  @Test(expected = SolrException.class)
  public void testLeadingWildcardDisallowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Super")));
    getSolrClient().commit(coll);

    // Boom!
    getSolrClient().query(coll, params("q", "*uper", "defType", "advanced"));
  }

  public void testLeadingWildcardWithReversedWildcardFilterFactoryAllowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String fieldName = coll + "_rev";
    configureTestField(coll, fieldName, createRevWildcardFilterConfig());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "Super")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", fieldName + ":*uper", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
  }

  @Test(expected = SolrException.class)
  public void testInfixWildcardDisallowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Super")));
    getSolrClient().commit(coll);

    // Boom!
    getSolrClient().query(coll, params("q", "s*er", "defType", "advanced"));
  }

  public void testInfixWildcardWithReversedWildcardFilterFactoryAllowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String fieldName = coll + "_rev";
    configureTestField(coll, fieldName, createRevWildcardFilterConfig());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "Super")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", fieldName + ":s*er", "defType", "advanced"));
    assertEquals(1, resp.getResults().size());
  }

  @Test(expected = SolrException.class)
  public void testPrefixQueryParserDisallowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Super")));
    getSolrClient().commit(coll);

    // Boom!
    getSolrClient().query(coll, params("q", "{!prefix f=_text_}su", "defType", "advanced"));
  }

  @Test(expected = SolrException.class)
  public void testPrefixQueryWithinComplexPhraseQueryDisallowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", "_text_", "super clubbing")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", "_text_", "Supper clubs")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", "_text_", "Supreme clubs")));
    getSolrClient().commit(coll);

    QueryResponse resp = getSolrClient().query(coll, params("q", "{!complexphrase inOrder=true}\"(supper super) club*\"", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    expectMatchExactlyTheseDocs(resp, "1", "2");
    haveNone(resp, "3");
  }



  public void testWildcardWithLenientReverseWildcardAllowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String fieldName = coll + "_rev";
    configureTestField(coll, fieldName, createRevWildcardFilterConfig(true, 6, 5, 0.50));
    // add some docs
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "Super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "superate")));
    getSolrClient().commit(coll);

    // should be allowed
    QueryResponse resp = getSolrClient().query(coll, params("q", fieldName + ":*upe?", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    haveNone(resp, "3");
  }

  public void testWildcardWithStrictReverseWildcardAllowed() throws Exception {
    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "_default", 2, 1).process(getSolrClient());

    String fieldName = coll + "_rev";
    configureTestField(coll, fieldName, createRevWildcardFilterConfig(true, 1, 1, 0.10));
    // add some docs
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "1", fieldName, "super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "2", fieldName, "Super")));
    assertUpdateResponse(getSolrClient().add(coll, sdoc("id", "3", fieldName, "superate")));
    getSolrClient().commit(coll);

    // should be allowed
    QueryResponse resp = getSolrClient().query(coll, params("q", fieldName + ":*upe?", "defType", "advanced"));
    assertEquals(2, resp.getResults().size());
    haveNone(resp, "3");
  }

  private void configureTestField(String coll, String fieldName, Map<String, Object> filterConfig) throws Exception {
    CloudSolrClient solrClient = getSolrClient();
    solrClient.setDefaultCollection(coll); // required by SchemaRequest api

    String fieldTypeName = coll + "_text_rev";

    // create the FieldType
    SchemaResponse.UpdateResponse addFieldTypeResponse = createFieldTypeWithFilters(fieldTypeName, Collections.singletonList(filterConfig));
    assertEquals("Response contained errors: " + addFieldTypeResponse.toString(), 0, addFieldTypeResponse.getStatus());
    // create the Field
    SchemaResponse.UpdateResponse addFieldResponse = createFieldWithFieldType(fieldName, fieldTypeName);
    assertEquals("Response contained errors: " + addFieldResponse.toString(), 0, addFieldTypeResponse.getStatus());

  }

  private Map<String, Object> createRevWildcardFilterConfig() {
    return createRevWildcardFilterConfig(true, 3, 2, 0.33);
  }

  private Map<String, Object> createRevWildcardFilterConfig(boolean withOriginal, int maxPosAsterix, int maxPosQuestion, double maxFractionAsterisk) {
    Map<String, Object> filterConfig = new LinkedHashMap<>();
    filterConfig.put("class", "solr.ReversedWildcardFilterFactory");
    filterConfig.put("withOriginal", withOriginal);
    filterConfig.put("maxPosAsterisk", maxPosAsterix);
    filterConfig.put("maxPosQuestion", maxPosQuestion);
    filterConfig.put("maxFractionAsterisk", maxFractionAsterisk);
    return filterConfig;
  }

  private SchemaResponse.UpdateResponse createFieldTypeWithFilters(String fieldTypeName, List<Map<String, Object>> filterConfigs) throws Exception {

    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();

    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    fieldTypeAttributes.put("name", fieldTypeName);
    fieldTypeAttributes.put("class", "solr.TextField");
    fieldTypeAttributes.put("indexed", true);
    fieldTypeAttributes.put("stored", false);
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);

    Map<String, Object> tokenizerAttributes = new LinkedHashMap<>();
    tokenizerAttributes.put("class", "solr.StandardTokenizerFactory");

    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    analyzerDefinition.setTokenizer(tokenizerAttributes);
    analyzerDefinition.setFilters(filterConfigs);
    fieldTypeDefinition.setIndexAnalyzer(analyzerDefinition);

    SchemaRequest.AddFieldType addFieldTypeRequest = new SchemaRequest.AddFieldType(fieldTypeDefinition);
    return addFieldTypeRequest.process(getSolrClient());

  }

  private SchemaResponse.UpdateResponse createFieldWithFieldType(String fieldName, String fieldTypeName) throws Exception {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", fieldTypeName);
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    fieldAttributes.put("required", true);
    SchemaRequest.AddField addFieldUpdateSchemaRequest = new SchemaRequest.AddField(fieldAttributes);
    return addFieldUpdateSchemaRequest.process(getSolrClient());
  }
}
