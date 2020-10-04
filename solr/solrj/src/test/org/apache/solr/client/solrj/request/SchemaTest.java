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
package org.apache.solr.client.solrj.request;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.schema.AnalyzerDefinition;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.FieldTypeRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the functionality (accuracy and failure) of the methods exposed by the classes
 * {@link SchemaRequest} and {@link SchemaResponse}.
 */
public class SchemaTest extends RestTestBase {
  private static void assertValidSchemaResponse(SolrResponseBase schemaResponse) {
    assertEquals("Response contained errors: " + schemaResponse.toString(), 0, schemaResponse.getStatus());
    assertNull("Response contained errors: " + schemaResponse.toString(), schemaResponse.getResponse().get("errors"));
  }
  
  private static void assertFailedSchemaResponse(ThrowingRunnable runnable, String expectedErrorMessage) {
    HttpSolrClient.RemoteExecutionException e = expectThrows(HttpSolrClient.RemoteExecutionException.class, runnable);
    SimpleOrderedMap errorMap = (SimpleOrderedMap)e.getMetaData().get("error");
    assertEquals("org.apache.solr.api.ApiBag$ExceptionWithErrObject",
        ((NamedList)errorMap.get("metadata")).get("error-class"));
    @SuppressWarnings({"rawtypes"})
    List details = (List)errorMap.get("details");
    assertTrue(((List)((Map)details.get(0)).get("errorMessages")).get(0).toString().contains(expectedErrorMessage));
  }

  private static void createStoredStringField(String fieldName, SolrClient solrClient) throws Exception {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", true);
    SchemaRequest.AddField addFieldRequest = new SchemaRequest.AddField(fieldAttributes);
    addFieldRequest.process(solrClient);
  }

  private static SchemaRequest.AddFieldType createFieldTypeRequest(String fieldTypeName) {
    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    fieldTypeAttributes.put("name", fieldTypeName);
    fieldTypeAttributes.put("class", "solr.TextField");
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);
    AnalyzerDefinition indexAnalyzerDefinition = new AnalyzerDefinition();
    Map<String, Object> iTokenizerAttributes = new LinkedHashMap<>();
    iTokenizerAttributes.put("class", "solr.PathHierarchyTokenizerFactory");
    iTokenizerAttributes.put("delimiter", "/");
    indexAnalyzerDefinition.setTokenizer(iTokenizerAttributes);
    fieldTypeDefinition.setIndexAnalyzer(indexAnalyzerDefinition);
    AnalyzerDefinition queryAnalyzerDefinition = new AnalyzerDefinition();
    Map<String, Object> qTokenizerAttributes = new LinkedHashMap<>();
    qTokenizerAttributes.put("class", "solr.KeywordTokenizerFactory");
    queryAnalyzerDefinition.setTokenizer(qTokenizerAttributes);
    fieldTypeDefinition.setQueryAnalyzer(queryAnalyzerDefinition);
    return new SchemaRequest.AddFieldType(fieldTypeDefinition);
  }

  @Before
  public void init() throws Exception {
    File tmpSolrHome = createTempDir().toFile();
    FileUtils.copyDirectory(new File(getFile("solrj/solr/collection1").getParent()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder, String> extraServlets = new TreeMap<>();

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema.xml",
        "/solr", true, extraServlets);
  }

  @After
  public void cleanup() throws Exception  {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;

  }

  @Test
  public void testSchemaRequestAccuracy() throws Exception {
    SchemaRequest schemaRequest = new SchemaRequest();
    SchemaResponse schemaResponse = schemaRequest.process(getSolrClient());
    assertValidSchemaResponse(schemaResponse);
    SchemaRepresentation schemaRepresentation = schemaResponse.getSchemaRepresentation();
    assertNotNull(schemaRepresentation);
    assertEquals("test", schemaRepresentation.getName());
    assertEquals(1.6, schemaRepresentation.getVersion(), 0.001f);
    assertEquals("id", schemaRepresentation.getUniqueKey());
    assertFalse(schemaRepresentation.getFields().isEmpty());
    assertFalse(schemaRepresentation.getDynamicFields().isEmpty());
    assertFalse(schemaRepresentation.getFieldTypes().isEmpty());
    assertFalse(schemaRepresentation.getCopyFields().isEmpty());
  }

  @Test
  public void testSchemaNameRequestAccuracy() throws Exception {
    SchemaRequest.SchemaName schemaNameRequest = new SchemaRequest.SchemaName();
    SchemaResponse.SchemaNameResponse schemaNameResponse = schemaNameRequest.process(getSolrClient());
    assertValidSchemaResponse(schemaNameResponse);
    assertEquals("test", schemaNameResponse.getSchemaName());
  }

  @Test
  public void testSchemaVersionRequestAccuracy() throws Exception {
    SchemaRequest.SchemaVersion schemaVersionRequest = new SchemaRequest.SchemaVersion();
    SchemaResponse.SchemaVersionResponse schemaVersionResponse = schemaVersionRequest.process(getSolrClient());
    assertValidSchemaResponse(schemaVersionResponse);
    assertEquals(1.6, schemaVersionResponse.getSchemaVersion(), 0.001);
  }

  @Test
  public void testGetFieldsAccuracy() throws Exception {
    SchemaRequest.Fields fieldsSchemaRequest = new SchemaRequest.Fields();
    SchemaResponse.FieldsResponse fieldsResponse = fieldsSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(fieldsResponse);
    List<Map<String, Object>> fields = fieldsResponse.getFields();
    assertThat(fields.isEmpty(), is(false));
  }

  @Test
  public void testGetFieldAccuracy() throws Exception {
    String fieldName = "signatureField";
    SchemaRequest.Field fieldSchemaRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse fieldResponse = fieldSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(fieldResponse);
    Map<String, Object> fieldAttributes = fieldResponse.getField();
    assertThat(fieldName, is(equalTo(fieldAttributes.get("name"))));
    assertThat("string", is(equalTo(fieldAttributes.get("type"))));
  }

  @Test
  public void testGetDynamicFieldsAccuracy() throws Exception {
    SchemaRequest.DynamicFields dynamicFieldsSchemaRequest =
        new SchemaRequest.DynamicFields();
    SchemaResponse.DynamicFieldsResponse dynamicFieldsResponse = dynamicFieldsSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(dynamicFieldsResponse);
    List<Map<String, Object>> fields = dynamicFieldsResponse.getDynamicFields();
    assertThat(fields.isEmpty(), is(false));
  }

  @Test
  public void testGetDynamicFieldAccuracy() throws Exception {
    String dynamicFieldName = "*_i";
    SchemaRequest.DynamicField dynamicFieldSchemaRequest =
        new SchemaRequest.DynamicField(dynamicFieldName);
    SchemaResponse.DynamicFieldResponse dynamicFieldResponse = dynamicFieldSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(dynamicFieldResponse);
    Map<String, Object> dynamicFieldAttributes = dynamicFieldResponse.getDynamicField();
    assertThat(dynamicFieldName, is(equalTo(dynamicFieldAttributes.get("name"))));
    assertThat("int", is(equalTo(dynamicFieldAttributes.get("type"))));
  }

  @Test
  public void testGetFieldTypesAccuracy() throws Exception {
    SchemaRequest.FieldTypes fieldTypesRequest =
        new SchemaRequest.FieldTypes();
    SchemaResponse.FieldTypesResponse fieldTypesResponse = fieldTypesRequest.process(getSolrClient());
    assertValidSchemaResponse(fieldTypesResponse);
    List<FieldTypeRepresentation> fieldTypes = fieldTypesResponse.getFieldTypes();
    assertThat(fieldTypes.isEmpty(), is(false));
  }

  @Test
  public void testGetFieldTypeAccuracy() throws Exception {
    String fieldType = "string";
    SchemaRequest.FieldType fieldTypeSchemaRequest =
        new SchemaRequest.FieldType(fieldType);
    SchemaResponse.FieldTypeResponse fieldTypeResponse = fieldTypeSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(fieldTypeResponse);
    FieldTypeRepresentation fieldTypeDefinition = fieldTypeResponse.getFieldType();
    assertThat(fieldType, is(equalTo(fieldTypeDefinition.getAttributes().get("name"))));
    assertThat("solr.StrField", is(equalTo(fieldTypeDefinition.getAttributes().get("class"))));
  }

  @Test
  public void testGetCopyFieldsAccuracy() throws Exception {
    SchemaRequest.CopyFields copyFieldsRequest =
        new SchemaRequest.CopyFields();
    SchemaResponse.CopyFieldsResponse copyFieldsResponse = copyFieldsRequest.process(getSolrClient());
    assertValidSchemaResponse(copyFieldsResponse);
    List<Map<String, Object>> copyFieldsAttributes = copyFieldsResponse.getCopyFields();
    assertThat(copyFieldsAttributes.isEmpty(), is(false));
  }

  @Test
  public void testGetUniqueKeyAccuracy() throws Exception {
    SchemaRequest.UniqueKey uniqueKeyRequest =
        new SchemaRequest.UniqueKey();
    SchemaResponse.UniqueKeyResponse uniqueKeyResponse = uniqueKeyRequest.process(getSolrClient());
    assertValidSchemaResponse(uniqueKeyResponse);
    assertEquals("id", uniqueKeyResponse.getUniqueKey());
  }

  @Test
  public void testGetGlobalSimilarityAccuracy() throws Exception {
    SchemaRequest.GlobalSimilarity globalSimilarityRequest =
        new SchemaRequest.GlobalSimilarity();
    SchemaResponse.GlobalSimilarityResponse globalSimilarityResponse = globalSimilarityRequest.process(getSolrClient());
    assertValidSchemaResponse(globalSimilarityResponse);
    assertEquals("org.apache.solr.search.similarities.SchemaSimilarityFactory",
        globalSimilarityResponse.getSimilarity().get("class"));
  }

  @Test
  public void testAddFieldAccuracy() throws Exception {
    SchemaRequest.Fields fieldsSchemaRequest = new SchemaRequest.Fields();
    SchemaResponse.FieldsResponse initialFieldsResponse = fieldsSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(initialFieldsResponse);
    List<Map<String, Object>> initialFields = initialFieldsResponse.getFields();

    String fieldName = "accuracyField";
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    fieldAttributes.put("default", "accuracy");
    fieldAttributes.put("required", true);
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldResponse);

    SchemaResponse.FieldsResponse currentFieldsResponse = fieldsSchemaRequest.process(getSolrClient());
    assertEquals(0, currentFieldsResponse.getStatus());
    List<Map<String, Object>> currentFields = currentFieldsResponse.getFields();
    assertEquals(initialFields.size() + 1, currentFields.size());


    SchemaRequest.Field fieldSchemaRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse newFieldResponse = fieldSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldResponse);
    Map<String, Object> newFieldAttributes = newFieldResponse.getField();
    assertThat(fieldName, is(equalTo(newFieldAttributes.get("name"))));
    assertThat("string", is(equalTo(newFieldAttributes.get("type"))));
    assertThat(false, is(equalTo(newFieldAttributes.get("stored"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("indexed"))));
    assertThat("accuracy", is(equalTo(newFieldAttributes.get("default"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("required"))));
  }

  @Test
  public void addFieldShouldntBeCalledTwiceWithTheSameName() throws Exception {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    String fieldName = "failureField"; 
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldFirstResponse = addFieldUpdateSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldFirstResponse);

    assertFailedSchemaResponse(() -> addFieldUpdateSchemaRequest.process(getSolrClient()),
        "Field '" + fieldName + "' already exists.");
  }

  @Test
  public void testDeleteFieldAccuracy() throws Exception {
    String fieldName = "fieldToBeDeleted";
    Map<String, Object> fieldAttributesRequest = new LinkedHashMap<>();
    fieldAttributesRequest.put("name", fieldName);
    fieldAttributesRequest.put("type", "string");
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributesRequest);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldResponse);

    SchemaRequest.Field fieldSchemaRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse initialFieldResponse = fieldSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(initialFieldResponse);
    Map<String, Object> fieldAttributesResponse = initialFieldResponse.getField();
    assertThat(fieldName, is(equalTo(fieldAttributesResponse.get("name"))));

    SchemaRequest.DeleteField deleteFieldRequest =
        new SchemaRequest.DeleteField(fieldName);
    SchemaResponse.UpdateResponse deleteFieldResponse = deleteFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(deleteFieldResponse);

    expectThrows(SolrException.class, () -> fieldSchemaRequest.process(getSolrClient()));
  }

  @Test
  public void deletingAFieldThatDoesntExistInTheSchemaShouldFail() {
    String fieldName = "fieldToBeDeleted"; 
    SchemaRequest.DeleteField deleteFieldRequest = new SchemaRequest.DeleteField(fieldName);
    assertFailedSchemaResponse(() -> deleteFieldRequest.process(getSolrClient()),
        "The field '" + fieldName + "' is not present in this schema, and so cannot be deleted.");
  }

  @Test
  public void testReplaceFieldAccuracy() throws Exception {
    // Given
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    String fieldName = "accuracyField";
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    fieldAttributes.put("required", true);
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldResponse);

    // When : update the field definition
    fieldAttributes.put("stored", true);
    fieldAttributes.put("indexed", false);
    SchemaRequest.ReplaceField replaceFieldRequest = new SchemaRequest.ReplaceField(fieldAttributes);
    SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(replaceFieldResponse);

    // Then
    SchemaRequest.Field fieldSchemaRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse newFieldResponse = fieldSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldResponse);
    Map<String, Object> newFieldAttributes = newFieldResponse.getField();
    assertThat(fieldName, is(equalTo(newFieldAttributes.get("name"))));
    assertThat("string", is(equalTo(newFieldAttributes.get("type"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("stored"))));
    assertThat(false, is(equalTo(newFieldAttributes.get("indexed"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("required"))));
  }

  @Test
  public void testAddDynamicFieldAccuracy() throws Exception {
    SchemaRequest.DynamicFields dynamicFieldsSchemaRequest =
        new SchemaRequest.DynamicFields();
    SchemaResponse.DynamicFieldsResponse initialDFieldsResponse = dynamicFieldsSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(initialDFieldsResponse);
    List<Map<String, Object>> initialDFields = initialDFieldsResponse.getDynamicFields();

    String dFieldName = "*_acc";
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", dFieldName);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    // Dynamic fields cannot be required or have a default value
    SchemaRequest.AddDynamicField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddDynamicField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldResponse);

    SchemaResponse.DynamicFieldsResponse currentDFieldsResponse = dynamicFieldsSchemaRequest.process(getSolrClient());
    assertEquals(0, currentDFieldsResponse.getStatus());
    List<Map<String, Object>> currentFields = currentDFieldsResponse.getDynamicFields();
    assertEquals(initialDFields.size() + 1, currentFields.size());


    SchemaRequest.DynamicField dFieldRequest = new SchemaRequest.DynamicField(dFieldName);
    SchemaResponse.DynamicFieldResponse newFieldResponse = dFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldResponse);
    Map<String, Object> newFieldAttributes = newFieldResponse.getDynamicField();
    assertThat(dFieldName, is(equalTo(newFieldAttributes.get("name"))));
    assertThat("string", is(equalTo(newFieldAttributes.get("type"))));
    assertThat(false, is(equalTo(newFieldAttributes.get("stored"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("indexed"))));
  }

  @Test
  public void addDynamicFieldShouldntBeCalledTwiceWithTheSameName() throws Exception {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    String dynamicFieldName = "*_failure";
    fieldAttributes.put("name", dynamicFieldName);
    fieldAttributes.put("type", "string");
    SchemaRequest.AddDynamicField addDFieldUpdateSchemaRequest =
        new SchemaRequest.AddDynamicField(fieldAttributes);
    SolrClient client = getSolrClient();
    SchemaResponse.UpdateResponse addDFieldFirstResponse = addDFieldUpdateSchemaRequest.process(client);
    assertValidSchemaResponse(addDFieldFirstResponse);

    assertFailedSchemaResponse(() -> addDFieldUpdateSchemaRequest.process(getSolrClient()),
        "[schema.xml] Duplicate DynamicField definition for '" + dynamicFieldName + "'");
  }

  @Test
  public void testDeleteDynamicFieldAccuracy() throws Exception {
    String dynamicFieldName = "*_del";
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", dynamicFieldName);
    fieldAttributes.put("type", "string");
    SchemaRequest.AddDynamicField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddDynamicField(fieldAttributes);
    SchemaResponse.UpdateResponse addDynamicFieldResponse = addFieldUpdateSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(addDynamicFieldResponse);

    SchemaRequest.DynamicField dynamicFieldSchemaRequest =
        new SchemaRequest.DynamicField(dynamicFieldName);
    SchemaResponse.DynamicFieldResponse initialDFieldResponse = dynamicFieldSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(initialDFieldResponse);
    Map<String, Object> fieldAttributesResponse = initialDFieldResponse.getDynamicField();
    assertThat(dynamicFieldName, is(equalTo(fieldAttributesResponse.get("name"))));

    SchemaRequest.DeleteDynamicField deleteFieldRequest =
        new SchemaRequest.DeleteDynamicField(dynamicFieldName);
    SchemaResponse.UpdateResponse deleteDynamicFieldResponse = deleteFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(deleteDynamicFieldResponse);

    expectThrows(SolrException.class, () -> dynamicFieldSchemaRequest.process(getSolrClient()));
  }

  @Test
  public void deletingADynamicFieldThatDoesntExistInTheSchemaShouldFail() throws Exception {
    String dynamicFieldName = "*_notexists";
    SchemaRequest.DeleteDynamicField deleteDynamicFieldRequest = new SchemaRequest.DeleteDynamicField(dynamicFieldName);
    assertFailedSchemaResponse(() -> deleteDynamicFieldRequest.process(getSolrClient()),
        "The dynamic field '" + dynamicFieldName + "' is not present in this schema, and so cannot be deleted.");
  }

  @Test
  public void testReplaceDynamicFieldAccuracy() throws Exception {
    // Given
    String fieldName = "*_replace";
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    SchemaRequest.AddDynamicField addDFieldUpdateSchemaRequest =
        new SchemaRequest.AddDynamicField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse = addDFieldUpdateSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldResponse);

    // When : update the field definition
    Map<String, Object> replaceFieldAttributes = new LinkedHashMap<>(fieldAttributes);
    replaceFieldAttributes.put("stored", true);
    replaceFieldAttributes.put("indexed", false);
    SchemaRequest.ReplaceDynamicField replaceFieldRequest =
        new SchemaRequest.ReplaceDynamicField(replaceFieldAttributes);
    SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(replaceFieldResponse);

    // Then
    SchemaRequest.DynamicField dynamicFieldSchemaRequest =
        new SchemaRequest.DynamicField(fieldName);
    SchemaResponse.DynamicFieldResponse newFieldResponse = dynamicFieldSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldResponse);
    Map<String, Object> newFieldAttributes = newFieldResponse.getDynamicField();
    assertThat(fieldName, is(equalTo(newFieldAttributes.get("name"))));
    assertThat("string", is(equalTo(newFieldAttributes.get("type"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("stored"))));
    assertThat(false, is(equalTo(newFieldAttributes.get("indexed"))));
  }

  @Test
  public void testAddFieldTypeAccuracy() throws Exception {
    SchemaRequest.FieldTypes fieldTypesRequest = new SchemaRequest.FieldTypes();
    SchemaResponse.FieldTypesResponse initialFieldTypesResponse = fieldTypesRequest.process(getSolrClient());
    assertValidSchemaResponse(initialFieldTypesResponse);
    List<FieldTypeRepresentation> initialFieldTypes = initialFieldTypesResponse.getFieldTypes();

    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    String fieldTypeName = "accuracyTextField";
    fieldTypeAttributes.put("name", fieldTypeName);
    fieldTypeAttributes.put("class", "solr.TextField");
    fieldTypeAttributes.put("positionIncrementGap", "100");
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);

    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    Map<String, Object> charFilterAttributes = new LinkedHashMap<>();
    charFilterAttributes.put("class", "solr.PatternReplaceCharFilterFactory");
    charFilterAttributes.put("replacement", "$1$1");
    charFilterAttributes.put("pattern", "([a-zA-Z])\\\\1+");
    analyzerDefinition.setCharFilters(Collections.singletonList(charFilterAttributes));
    Map<String, Object> tokenizerAttributes = new LinkedHashMap<>();
    tokenizerAttributes.put("class", "solr.WhitespaceTokenizerFactory");
    analyzerDefinition.setTokenizer(tokenizerAttributes);
    Map<String, Object> filterAttributes = new LinkedHashMap<>();
    filterAttributes.put("class", "solr.WordDelimiterGraphFilterFactory");
    filterAttributes.put("preserveOriginal", "0");
    analyzerDefinition.setFilters(Collections.singletonList(filterAttributes));
    fieldTypeDefinition.setAnalyzer(analyzerDefinition);

    SchemaRequest.AddFieldType addFieldTypeRequest =
        new SchemaRequest.AddFieldType(fieldTypeDefinition);
    SchemaResponse.UpdateResponse addFieldTypeResponse = addFieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldTypeResponse);

    SchemaResponse.FieldTypesResponse currentFieldTypesResponse = fieldTypesRequest.process(getSolrClient());
    assertEquals(0, currentFieldTypesResponse.getStatus());
    List<FieldTypeRepresentation> currentFieldTypes = currentFieldTypesResponse.getFieldTypes();
    assertEquals(initialFieldTypes.size() + 1, currentFieldTypes.size());

    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    String fieldName = "accuracyField";
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", fieldTypeName);
    SchemaRequest.AddField addFieldRequest =
        new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldResponse);

    Map<String, Object> dynamicFieldAttributes = new LinkedHashMap<>();
    String dynamicFieldName = "*_accuracy";
    dynamicFieldAttributes.put("name", dynamicFieldName);
    dynamicFieldAttributes.put("type", fieldTypeName);
    SchemaRequest.AddDynamicField addDynamicFieldRequest =
        new SchemaRequest.AddDynamicField(dynamicFieldAttributes);
    SchemaResponse.UpdateResponse addDynamicFieldResponse = addDynamicFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(addDynamicFieldResponse);

    SchemaRequest.FieldType fieldTypeRequest = new SchemaRequest.FieldType(fieldTypeName);
    SchemaResponse.FieldTypeResponse newFieldTypeResponse = fieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldTypeResponse);
    FieldTypeRepresentation newFieldTypeRepresentation = newFieldTypeResponse.getFieldType();
    assertThat(fieldTypeName, is(equalTo(newFieldTypeRepresentation.getAttributes().get("name"))));
    assertThat("solr.TextField", is(equalTo(newFieldTypeRepresentation.getAttributes().get("class"))));
    assertThat(analyzerDefinition.getTokenizer().get("class"),
        is(equalTo(newFieldTypeRepresentation.getAnalyzer().getTokenizer().get("class"))));
  }

  @Test
  public void addFieldTypeWithSimilarityAccuracy() throws Exception {
    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    String fieldTypeName = "fullClassNames";
    fieldTypeAttributes.put("name", fieldTypeName);
    fieldTypeAttributes.put("class", "org.apache.solr.schema.TextField");
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);

    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    Map<String, Object> charFilterAttributes = new LinkedHashMap<>();
    charFilterAttributes.put("class", "solr.PatternReplaceCharFilterFactory");
    charFilterAttributes.put("replacement", "$1$1");
    charFilterAttributes.put("pattern", "([a-zA-Z])\\\\1+");
    analyzerDefinition.setCharFilters(Collections.singletonList(charFilterAttributes));
    Map<String, Object> tokenizerAttributes = new LinkedHashMap<>();
    tokenizerAttributes.put("class", "solr.WhitespaceTokenizerFactory");
    analyzerDefinition.setTokenizer(tokenizerAttributes);
    fieldTypeDefinition.setAnalyzer(analyzerDefinition);
    Map<String, Object> similarityAttributes = new LinkedHashMap<>();
    similarityAttributes.put("class", "org.apache.lucene.misc.SweetSpotSimilarity");
    fieldTypeDefinition.setSimilarity(similarityAttributes);

    SchemaRequest.AddFieldType addFieldTypeRequest =
        new SchemaRequest.AddFieldType(fieldTypeDefinition);
    SchemaResponse.UpdateResponse addFieldTypeResponse = addFieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldTypeResponse);

    // similarity is not shown by default for the fieldType
    SchemaRequest.FieldType fieldTypeRequest = new SchemaRequest.FieldType(fieldTypeName);
    SchemaResponse.FieldTypeResponse newFieldTypeResponse = fieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldTypeResponse);
    FieldTypeRepresentation newFieldTypeRepresentation = newFieldTypeResponse.getFieldType();
    assertThat(fieldTypeName, is(equalTo(newFieldTypeRepresentation.getAttributes().get("name"))));
    assertThat(similarityAttributes.get("class"), is(equalTo(newFieldTypeRepresentation.getSimilarity().get("class"))));
  }

  @Test
  public void addFieldTypeWithAnalyzerClassAccuracy() throws Exception {
    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    String fieldTypeName = "nameText";
    fieldTypeAttributes.put("name", fieldTypeName);
    fieldTypeAttributes.put("class", "solr.TextField");

    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);
    Map<String, Object> analyzerAttributes = new LinkedHashMap<>();
    analyzerAttributes.put("class", "org.apache.lucene.analysis.core.WhitespaceAnalyzer");
    analyzerAttributes.put("luceneMatchVersion", "5.0.0");
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    analyzerDefinition.setAttributes(analyzerAttributes);
    fieldTypeDefinition.setAnalyzer(analyzerDefinition);


    SchemaRequest.AddFieldType addFieldTypeRequest =
        new SchemaRequest.AddFieldType(fieldTypeDefinition);
    SchemaResponse.UpdateResponse addFieldTypeResponse = addFieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldTypeResponse);

    SchemaRequest.FieldType fieldTypeRequest = new SchemaRequest.FieldType(fieldTypeName);
    SchemaResponse.FieldTypeResponse newFieldTypeResponse = fieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldTypeResponse);
    FieldTypeRepresentation newFieldTypeRepresentation = newFieldTypeResponse.getFieldType();
    assertThat(fieldTypeName, is(equalTo(newFieldTypeRepresentation.getAttributes().get("name"))));
    assertThat(analyzerAttributes.get("class"),
        is(equalTo(newFieldTypeRepresentation.getAnalyzer().getAttributes().get("class"))));
    assertThat(analyzerAttributes.get("luceneMatchVersion"),
        is(equalTo(newFieldTypeRepresentation.getAnalyzer().getAttributes().get("luceneMatchVersion"))));
  }

  @Test
  public void addFieldTypeShouldntBeCalledTwiceWithTheSameName() throws Exception {
    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    String fieldName = "failureInt";
    fieldTypeAttributes.put("name", fieldName);
    fieldTypeAttributes.put("class",  RANDOMIZED_NUMERIC_FIELDTYPES.get(Integer.class));
    fieldTypeAttributes.put("omitNorms", true);
    fieldTypeAttributes.put("positionIncrementGap", 0);
    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);
    SchemaRequest.AddFieldType addFieldTypeRequest =
        new SchemaRequest.AddFieldType(fieldTypeDefinition);
    SchemaResponse.UpdateResponse addFieldTypeFirstResponse = addFieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldTypeFirstResponse);

    assertFailedSchemaResponse(() -> addFieldTypeRequest.process(getSolrClient()),
        "Field type '" + fieldName + "' already exists.");
  }

  @Test
  public void testDeleteFieldTypeAccuracy() throws Exception {
    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    String fieldTypeName = "delInt";
    fieldTypeAttributes.put("name", fieldTypeName);
    fieldTypeAttributes.put("class",  RANDOMIZED_NUMERIC_FIELDTYPES.get(Integer.class));
    fieldTypeAttributes.put("omitNorms", true);
    fieldTypeAttributes.put("positionIncrementGap", 0);
    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);
    SchemaRequest.AddFieldType addFieldTypeRequest =
        new SchemaRequest.AddFieldType(fieldTypeDefinition);
    SolrClient c = getSolrClient();
    SchemaResponse.UpdateResponse addFieldTypeResponse = addFieldTypeRequest.process(c);
    assertValidSchemaResponse(addFieldTypeResponse);

    SchemaRequest.FieldType fieldTypeRequest = new SchemaRequest.FieldType(fieldTypeName);
    SchemaResponse.FieldTypeResponse initialFieldTypeResponse = fieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(initialFieldTypeResponse);
    FieldTypeRepresentation responseFieldTypeRepresentation = initialFieldTypeResponse.getFieldType();
    assertThat(fieldTypeName, is(equalTo(responseFieldTypeRepresentation.getAttributes().get("name"))));

    SchemaRequest.DeleteFieldType deleteFieldTypeRequest =
        new SchemaRequest.DeleteFieldType(fieldTypeName);
    SchemaResponse.UpdateResponse deleteFieldTypeResponse = deleteFieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(deleteFieldTypeResponse);

    try {
      fieldTypeRequest.process(getSolrClient());
      fail(String.format(Locale.ROOT, "after removal, the field type %s shouldn't be anymore available over Schema API",
          fieldTypeName));
    } catch (SolrException e) {
      //success
    }
  }

  @Test
  public void deletingAFieldTypeThatDoesntExistInTheSchemaShouldFail() throws Exception {
    String fieldType = "fieldTypeToBeDeleted"; 
    SchemaRequest.DeleteFieldType deleteFieldTypeRequest = new SchemaRequest.DeleteFieldType(fieldType);
    assertFailedSchemaResponse(() -> deleteFieldTypeRequest.process(getSolrClient()),
        "The field type '" + fieldType + "' is not present in this schema, and so cannot be deleted.");
  }

  @Test
  public void testReplaceFieldTypeAccuracy() throws Exception {
    // a fixed value for comparison after update, be contraian from the randomized 'default'
    final boolean useDv = Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP);
    
    // Given
    Map<String, Object> fieldTypeAttributes = new LinkedHashMap<>();
    String fieldTypeName = "replaceInt";
    fieldTypeAttributes.put("name", fieldTypeName);
    fieldTypeAttributes.put("class",  RANDOMIZED_NUMERIC_FIELDTYPES.get(Integer.class));
    fieldTypeAttributes.put("docValues", useDv);
    fieldTypeAttributes.put("omitNorms", true);
    fieldTypeAttributes.put("positionIncrementGap", 0);
    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);
    SchemaRequest.AddFieldType addFieldTypeRequest =
        new SchemaRequest.AddFieldType(fieldTypeDefinition);
    SchemaResponse.UpdateResponse addFieldTypeResponse = addFieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(addFieldTypeResponse);

    // When : update the field definition
    fieldTypeAttributes.put("positionIncrementGap", 42);
    fieldTypeAttributes.put("omitNorms", false);
    FieldTypeDefinition replaceFieldTypeDefinition = new FieldTypeDefinition();
    replaceFieldTypeDefinition.setAttributes(fieldTypeAttributes);
    SchemaRequest.ReplaceFieldType replaceFieldTypeRequest =
        new SchemaRequest.ReplaceFieldType(replaceFieldTypeDefinition);
    SchemaResponse.UpdateResponse replaceFieldTypeResponse = replaceFieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(replaceFieldTypeResponse);

    // Then
    SchemaRequest.FieldType fieldTypeRequest = new SchemaRequest.FieldType(fieldTypeName);
    SchemaResponse.FieldTypeResponse newFieldTypeResponse = fieldTypeRequest.process(getSolrClient());
    assertValidSchemaResponse(newFieldTypeResponse);
    FieldTypeRepresentation replacedFieldTypeRepresentation = newFieldTypeResponse.getFieldType();
    Map<String, Object> replacedFieldTypeAttributes = replacedFieldTypeRepresentation.getAttributes();
    assertThat(fieldTypeName, is(equalTo(replacedFieldTypeAttributes.get("name"))));
    assertThat( RANDOMIZED_NUMERIC_FIELDTYPES.get(Integer.class),
                is(equalTo(replacedFieldTypeAttributes.get("class"))));
    assertThat(false, is(equalTo(replacedFieldTypeAttributes.get("omitNorms"))));
    assertThat("42", is(equalTo(replacedFieldTypeAttributes.get("positionIncrementGap"))));
    // should be unchanged...
    assertThat(useDv, is(equalTo(replacedFieldTypeAttributes.get("docValues"))));
  }

  @Test
  public void testCopyFieldAccuracy() throws Exception {
    SchemaRequest.CopyFields copyFieldsSchemaRequest = new SchemaRequest.CopyFields();
    SchemaResponse.CopyFieldsResponse initialCopyFieldsResponse = copyFieldsSchemaRequest.process(getSolrClient());
    List<Map<String, Object>> initialCopyFieldsAttributes = initialCopyFieldsResponse.getCopyFields();

    String srcFieldName = "copyfield";
    String destFieldName1 = "destField1", destFieldName2 = "destField2";
    createStoredStringField(srcFieldName, getSolrClient());
    createStoredStringField(destFieldName1, getSolrClient());
    createStoredStringField(destFieldName2, getSolrClient());

    SchemaRequest.AddCopyField addCopyFieldRequest =
        new SchemaRequest.AddCopyField(srcFieldName,
            Arrays.asList(destFieldName1, destFieldName2));
    SchemaResponse.UpdateResponse addCopyFieldResponse = addCopyFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(addCopyFieldResponse);

    SchemaResponse.CopyFieldsResponse currentCopyFieldsResponse = copyFieldsSchemaRequest.process(getSolrClient());
    List<Map<String, Object>> currentCopyFields = currentCopyFieldsResponse.getCopyFields();
    assertEquals(initialCopyFieldsAttributes.size() + 2, currentCopyFields.size());
  }

  @Test
  public void testCopyFieldWithMaxCharsAccuracy() throws Exception {
    SchemaRequest.CopyFields copyFieldsSchemaRequest = new SchemaRequest.CopyFields();
    SchemaResponse.CopyFieldsResponse initialCopyFieldsResponse = copyFieldsSchemaRequest.process(getSolrClient());
    List<Map<String, Object>> initialCopyFieldsAttributes = initialCopyFieldsResponse.getCopyFields();

    String srcFieldName = "copyfield";
    String destFieldName1 = "destField1", destFieldName2 = "destField2";
    createStoredStringField(srcFieldName, getSolrClient());
    createStoredStringField(destFieldName1, getSolrClient());
    createStoredStringField(destFieldName2, getSolrClient());

    Integer maxChars = 200;
    SchemaRequest.AddCopyField addCopyFieldRequest =
        new SchemaRequest.AddCopyField(srcFieldName,
            Arrays.asList(destFieldName1, destFieldName2), maxChars);
    SchemaResponse.UpdateResponse addCopyFieldResponse = addCopyFieldRequest.process(getSolrClient());
    assertValidSchemaResponse(addCopyFieldResponse);

    SchemaResponse.CopyFieldsResponse currentCopyFieldsResponse = copyFieldsSchemaRequest.process(getSolrClient());
    List<Map<String, Object>> currentCopyFields = currentCopyFieldsResponse.getCopyFields();
    assertEquals(initialCopyFieldsAttributes.size() + 2, currentCopyFields.size());
    for (Map<String, Object> currentCopyField : currentCopyFields) {
      if (srcFieldName.equals(currentCopyField.get("source"))) {
        String currentDestFieldName = (String) currentCopyField.get("dest");
        int currentMaxChars = (Integer) currentCopyField.get("maxChars");
        assertThat(currentDestFieldName, anyOf(is(equalTo(destFieldName1)), is(equalTo(destFieldName2))));
        assertTrue(maxChars == currentMaxChars);
      }
    }
  }

  @Test
  public void copyFieldsShouldFailWhenOneOfTheFieldsDoesntExistInTheSchema() throws Exception {
    String srcFieldName = "srcnotexist";
    String destFieldName1 = "destNotExist1", destFieldName2 = "destNotExist2";

    SchemaRequest.AddCopyField addCopyFieldRequest 
        = new SchemaRequest.AddCopyField(srcFieldName, Arrays.asList(destFieldName1, destFieldName2));
    assertFailedSchemaResponse(() -> addCopyFieldRequest.process(getSolrClient()),
        "copyField source :'" + srcFieldName + "' is not a glob and doesn't match any explicit field or dynamicField.");
  }

  @Test
  public void testDeleteCopyFieldAccuracy() throws Exception {
    String srcFieldName = "copyfield";
    String destFieldName1 = "destField1", destFieldName2 = "destField2";
    createStoredStringField(srcFieldName, getSolrClient());
    createStoredStringField(destFieldName1, getSolrClient());
    createStoredStringField(destFieldName2, getSolrClient());

    SchemaRequest.AddCopyField addCopyFieldRequest =
        new SchemaRequest.AddCopyField(srcFieldName,
            Arrays.asList(destFieldName1, destFieldName2));
    SchemaResponse.UpdateResponse addCopyFieldResponse = addCopyFieldRequest.process(getSolrClient());
    System.out.println(addCopyFieldResponse);
    assertValidSchemaResponse(addCopyFieldResponse);

    SchemaRequest.DeleteCopyField deleteCopyFieldRequest1 =
        new SchemaRequest.DeleteCopyField(srcFieldName, Arrays.asList(destFieldName1));
    assertValidSchemaResponse(deleteCopyFieldRequest1.process(getSolrClient()));

    SchemaRequest.DeleteCopyField deleteCopyFieldRequest2 =
        new SchemaRequest.DeleteCopyField(srcFieldName, Arrays.asList(destFieldName2));
    assertValidSchemaResponse(deleteCopyFieldRequest2.process(getSolrClient()));
  }

  @Test
  public void deleteCopyFieldShouldFailWhenOneOfTheFieldsDoesntExistInTheSchema() throws Exception {
    String srcFieldName = "copyfield";
    String destFieldName1 = "destField1", destFieldName2 = "destField2";
    SchemaRequest.DeleteCopyField deleteCopyFieldsRequest =
        new SchemaRequest.DeleteCopyField(srcFieldName,
            Arrays.asList(destFieldName1, destFieldName2));
    assertFailedSchemaResponse(() -> deleteCopyFieldsRequest.process(getSolrClient()),
        "Copy field directive not found: '" + srcFieldName + "' -> '" + destFieldName1 + "'");
  }

  @Test
  public void testMultipleUpdateRequestAccuracy() throws Exception {
    String fieldTypeName = "accuracyTextField";
    SchemaRequest.AddFieldType addFieldTypeRequest = createFieldTypeRequest(fieldTypeName);

    String field1Name = "accuracyField1";
    String field2Name = "accuracyField2";
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", field1Name);
    fieldAttributes.put("type", fieldTypeName);
    fieldAttributes.put("stored", true);
    fieldAttributes.put("indexed", true);
    SchemaRequest.AddField addFieldName1Request = new SchemaRequest.AddField(fieldAttributes);
    fieldAttributes.put("name", field2Name);
    SchemaRequest.AddField addFieldName2Request = new SchemaRequest.AddField(fieldAttributes);

    List<SchemaRequest.Update> list = new ArrayList<>(3);
    list.add(addFieldTypeRequest);
    list.add(addFieldName1Request);
    list.add(addFieldName2Request);
    SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(list);
    SchemaResponse.UpdateResponse multipleUpdatesResponse = multiUpdateRequest.process(getSolrClient());
    assertValidSchemaResponse(multipleUpdatesResponse);

    SchemaRequest.FieldType fieldTypeSchemaRequest =
        new SchemaRequest.FieldType(fieldTypeName);
    SchemaResponse.FieldTypeResponse fieldTypeResponse = fieldTypeSchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(fieldTypeResponse);
    FieldTypeRepresentation fieldTypeRepresentation = fieldTypeResponse.getFieldType();
    assertThat(fieldTypeName, is(equalTo(fieldTypeRepresentation.getAttributes().get("name"))));

    SchemaRequest.Field field1SchemaRequest = new SchemaRequest.Field(field1Name);
    SchemaResponse.FieldResponse field1Response = field1SchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(field1Response);
    Map<String, ?> field1Attributes = field1Response.getField();
    assertThat(field1Name, is(equalTo(field1Attributes.get("name"))));
    assertThat(fieldTypeName, is(equalTo(field1Attributes.get("type"))));
    assertThat(true, is(equalTo(field1Attributes.get("stored"))));
    assertThat(true, is(equalTo(field1Attributes.get("indexed"))));

    SchemaRequest.Field field2SchemaRequest = new SchemaRequest.Field(field1Name);
    SchemaResponse.FieldResponse field2Response = field2SchemaRequest.process(getSolrClient());
    assertValidSchemaResponse(field2Response);
    Map<String, ?> field2Attributes = field2Response.getField();
    assertThat(field1Name, is(equalTo(field2Attributes.get("name"))));
    assertThat(fieldTypeName, is(equalTo(field2Attributes.get("type"))));
    assertThat(true, is(equalTo(field2Attributes.get("stored"))));
    assertThat(true, is(equalTo(field2Attributes.get("indexed"))));
  }
}
