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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.FieldTypeRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Test the functionality (accuracy and failure) of the methods exposed by the classes
 * {@link SchemaRequest} and {@link SchemaResponse}.
 */
public class Schema2Test extends RestTestBase {

  private static void assertValidSchemaResponse(SolrResponseBase schemaResponse) {
    assertEquals("Response contained errors: " + schemaResponse.toString(), 0, schemaResponse.getStatus());
    assertNull("Response contained errors: " + schemaResponse.toString(), schemaResponse.getResponse().get("errors"));
  }
  
  private static void assertFailedSchemaResponse(LuceneTestCase.ThrowingRunnable runnable, String expectedErrorMessage) {
    BaseHttpSolrClient.RemoteSolrException e = LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, runnable);
    assertTrue(e.getMessage(), e.getMessage().contains(expectedErrorMessage));
  }

  private static void createStoredStringField(String fieldName, SolrClient solrClient) throws Exception {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", true);
    SchemaRequest.AddField addFieldRequest = new SchemaRequest.AddField(fieldAttributes);
    addFieldRequest.process(solrClient);
  }

  @BeforeClass
  public static void beforeSolrExampleTestsBase() throws Exception {
    SolrTestCaseJ4.randomizeNumericTypesProperties();
    File tmpSolrHome = SolrTestUtil.createTempDir().toFile();
    FileUtils.copyDirectory(new File(SolrTestUtil.getFile("solrj/solr/collection1").getParent()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder, String> extraServlets = new TreeMap<>();

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    jetty = createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema.xml",
            "/solr", true, extraServlets);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception  {
    super.tearDown();
  }

  @Test
  public void testDeleteFieldAccuracy() throws Exception {
    String fieldName = "fieldToBeDeleted";
    Map<String, Object> fieldAttributesRequest = new LinkedHashMap<>();
    fieldAttributesRequest.put("name", fieldName);
    fieldAttributesRequest.put("type", "string");
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributesRequest);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(addFieldResponse);

    SchemaRequest.Field fieldSchemaRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse initialFieldResponse = fieldSchemaRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(initialFieldResponse);
    Map<String, Object> fieldAttributesResponse = initialFieldResponse.getField();
    assertThat(fieldName, is(equalTo(fieldAttributesResponse.get("name"))));

    SchemaRequest.DeleteField deleteFieldRequest =
        new SchemaRequest.DeleteField(fieldName);
    SchemaResponse.UpdateResponse deleteFieldResponse = deleteFieldRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(deleteFieldResponse);

    LuceneTestCase.expectThrows(SolrException.class, () -> fieldSchemaRequest.process(getSolrClient(jetty)));
  }

  @Test
  public void deletingAFieldThatDoesntExistInTheSchemaShouldFail() {
    String fieldName = "fieldToBeDeleted"; 
    SchemaRequest.DeleteField deleteFieldRequest = new SchemaRequest.DeleteField(fieldName);
    assertFailedSchemaResponse(() -> deleteFieldRequest.process(getSolrClient(jetty)),
        "The field '" + fieldName + "' is not present in this schema, and so cannot be deleted.");
  }

  @Test
  public void testReplaceFieldAccuracy() throws Exception {
    // Given
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    String fieldName = "accuracyFieldReplace";
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    fieldAttributes.put("required", true);
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(addFieldResponse);

    // When : update the field definition
    fieldAttributes.put("stored", true);
    fieldAttributes.put("indexed", false);
    SchemaRequest.ReplaceField replaceFieldRequest = new SchemaRequest.ReplaceField(fieldAttributes);
    SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(replaceFieldResponse);

    // Then
    SchemaRequest.Field fieldSchemaRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse newFieldResponse = fieldSchemaRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(newFieldResponse);
    Map<String, Object> newFieldAttributes = newFieldResponse.getField();
    assertThat(fieldName, is(equalTo(newFieldAttributes.get("name"))));
    assertThat("string", is(equalTo(newFieldAttributes.get("type"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("stored"))));
    assertThat(false, is(equalTo(newFieldAttributes.get("indexed"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("required"))));
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
    SchemaResponse.UpdateResponse addFieldResponse = addDFieldUpdateSchemaRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(addFieldResponse);

    // When : update the field definition
    Map<String, Object> replaceFieldAttributes = new LinkedHashMap<>(fieldAttributes);
    replaceFieldAttributes.put("stored", true);
    replaceFieldAttributes.put("indexed", false);
    SchemaRequest.ReplaceDynamicField replaceFieldRequest =
        new SchemaRequest.ReplaceDynamicField(replaceFieldAttributes);
    SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(replaceFieldResponse);

    // Then
    SchemaRequest.DynamicField dynamicFieldSchemaRequest =
        new SchemaRequest.DynamicField(fieldName);
    SchemaResponse.DynamicFieldResponse newFieldResponse = dynamicFieldSchemaRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(newFieldResponse);
    Map<String, Object> newFieldAttributes = newFieldResponse.getDynamicField();
    assertThat(fieldName, is(equalTo(newFieldAttributes.get("name"))));
    assertThat("string", is(equalTo(newFieldAttributes.get("type"))));
    assertThat(true, is(equalTo(newFieldAttributes.get("stored"))));
    assertThat(false, is(equalTo(newFieldAttributes.get("indexed"))));
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
    SolrClient c = getSolrClient(jetty);
    SchemaResponse.UpdateResponse addFieldTypeResponse = addFieldTypeRequest.process(c);
    assertValidSchemaResponse(addFieldTypeResponse);

    SchemaRequest.FieldType fieldTypeRequest = new SchemaRequest.FieldType(fieldTypeName);
    SchemaResponse.FieldTypeResponse initialFieldTypeResponse = fieldTypeRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(initialFieldTypeResponse);
    FieldTypeRepresentation responseFieldTypeRepresentation = initialFieldTypeResponse.getFieldType();
    assertThat(fieldTypeName, is(equalTo(responseFieldTypeRepresentation.getAttributes().get("name"))));

    SchemaRequest.DeleteFieldType deleteFieldTypeRequest =
        new SchemaRequest.DeleteFieldType(fieldTypeName);
    SchemaResponse.UpdateResponse deleteFieldTypeResponse = deleteFieldTypeRequest.process(getSolrClient(jetty));
    assertValidSchemaResponse(deleteFieldTypeResponse);

    try {
      fieldTypeRequest.process(getSolrClient(jetty));
      fail(String.format(Locale.ROOT, "after removal, the field type %s shouldn't be anymore available over Schema API",
          fieldTypeName));
    } catch (SolrException e) {
      //success
    }
  }

  @Test
  public void testDeleteCopyFieldAccuracy() throws Exception {
    String srcFieldName = "copyfield";
    String destFieldName1 = "destField1", destFieldName2 = "destField2";
    createStoredStringField(srcFieldName, getSolrClient(jetty));
    createStoredStringField(destFieldName1, getSolrClient(jetty));
    createStoredStringField(destFieldName2, getSolrClient(jetty));

    SchemaRequest.AddCopyField addCopyFieldRequest =
        new SchemaRequest.AddCopyField(srcFieldName,
            Arrays.asList(destFieldName1, destFieldName2));
    SchemaResponse.UpdateResponse addCopyFieldResponse = addCopyFieldRequest.process(getSolrClient(jetty));
    System.out.println(addCopyFieldResponse);
    assertValidSchemaResponse(addCopyFieldResponse);

    SchemaRequest.DeleteCopyField deleteCopyFieldRequest1 =
        new SchemaRequest.DeleteCopyField(srcFieldName, Arrays.asList(destFieldName1));
    assertValidSchemaResponse(deleteCopyFieldRequest1.process(getSolrClient(jetty)));

    SchemaRequest.DeleteCopyField deleteCopyFieldRequest2 =
        new SchemaRequest.DeleteCopyField(srcFieldName, Arrays.asList(destFieldName2));
    assertValidSchemaResponse(deleteCopyFieldRequest2.process(getSolrClient(jetty)));
  }

  @Test
  public void deleteCopyFieldShouldFailWhenOneOfTheFieldsDoesntExistInTheSchema() throws Exception {
    String srcFieldName = "copyfield";
    String destFieldName1 = "destField1", destFieldName2 = "destField2";
    SchemaRequest.DeleteCopyField deleteCopyFieldsRequest =
        new SchemaRequest.DeleteCopyField(srcFieldName,
            Arrays.asList(destFieldName1, destFieldName2));
    assertFailedSchemaResponse(() -> deleteCopyFieldsRequest.process(getSolrClient(jetty)),
        "Copy field directive not found: '" + srcFieldName + "' -> '" + destFieldName1 + "'");
  }
}
