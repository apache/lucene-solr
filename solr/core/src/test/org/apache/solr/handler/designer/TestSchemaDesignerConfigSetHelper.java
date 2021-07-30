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

package org.apache.solr.handler.designer;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.common.util.Utils.toJavabin;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.handler.designer.SchemaDesignerAPI.getMutableId;
import static org.apache.solr.schema.IndexSchema.NEST_PATH_FIELD_NAME;
import static org.apache.solr.schema.IndexSchema.ROOT_FIELD_NAME;

public class TestSchemaDesignerConfigSetHelper extends SolrCloudTestCase implements SchemaDesignerConstants {

  private CoreContainer cc;
  private SchemaDesignerConfigSetHelper helper;

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1).addConfig(DEFAULT_CONFIGSET_NAME, new File(ExternalPaths.DEFAULT_CONFIGSET).toPath()).configure();
    // SchemaDesignerConfigSetHelper depends on the blob store
    CollectionAdminRequest.createCollection(BLOB_STORE_ID, 1, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(BLOB_STORE_ID, 1, 1);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    if (cluster != null && cluster.getSolrClient() != null) {
      cluster.deleteAllCollections();
      cluster.deleteAllConfigSets();
    }
  }

  @Before
  public void setupTest() {
    assumeWorkingMockito();
    assertNotNull(cluster);
    cc = cluster.getJettySolrRunner(0).getCoreContainer();
    assertNotNull(cc);
    helper = new SchemaDesignerConfigSetHelper(cc, SchemaDesignerAPI.newSchemaSuggester(cc));
  }

  @Test
  public void testSetupMutable() throws Exception {
    String configSet = "testSetupMutable";
    String mutableId = getMutableId(configSet);
    // create our test config by copying from _default
    helper.copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    SolrConfig solrConfig = helper.loadSolrConfig(mutableId);
    assertNotNull(solrConfig);
    ManagedIndexSchema schema = helper.loadLatestSchema(solrConfig);
    assertNotNull(schema);
    int currentVersion = helper.getCurrentSchemaVersion(mutableId);
    assertEquals(schema.getSchemaZkVersion(), currentVersion);

    assertTrue(schema.persistManagedSchema(false));

    // create the temp collection
    helper.createCollection(mutableId, mutableId);

    List<String> collsForConfig = helper.listCollectionsForConfig(configSet);
    assertTrue(collsForConfig.isEmpty());

    helper.reloadTempCollection(mutableId, true);

    // version is incremented b/c we called persist on the schema above
    int version = helper.getCurrentSchemaVersion(mutableId);
    assertEquals(1, version);

    helper.checkSchemaVersion(mutableId, version, -1);

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Collections.emptyList(), true, DEFAULT_CONFIGSET_NAME);
    assertEquals(2, schema.getSchemaZkVersion());

    byte[] zipped = helper.downloadAndZipConfigSet(mutableId);
    assertTrue(zipped != null && zipped.length > 0);
  }

  @Test
  public void testEnableDisableOptions() throws Exception {
    String configSet = "testEnableDisableOptions";
    String mutableId = getMutableId(configSet);
    helper.copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Collections.singletonList("en"), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertNull(schema.getFieldTypeByName("text_fr"));

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Collections.singletonList("en"), false, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    assertNull(schema.getFieldTypeByName("text_fr"));

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Arrays.asList("en", "fr"), false, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    assertNotNull(schema.getFieldTypeByName("text_fr"));

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Arrays.asList("en", "fr"), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertTrue(cluster.getZkClient().exists(SchemaDesignerAPI.getConfigSetZkPath(mutableId, "lang/stopwords_en.txt"), true));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertNull(schema.getFieldOrNull("*_txt_ga"));

    // add a field that uses text_en and then try removing "en" from the lang set
    helper.createCollection(mutableId, mutableId); // need to create field
    Map<String, Object> addField = makeMap("name", "title", "type", "text_en");
    String addedFieldName = helper.addSchemaObject(configSet, Collections.singletonMap("add-field", addField));
    assertEquals("title", addedFieldName);

    schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertNotNull(schema.getField("title"));

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Collections.singletonList("fr"), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en")); // being used, so not removed
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertTrue(cluster.getZkClient().exists(SchemaDesignerAPI.getConfigSetZkPath(mutableId, "lang/stopwords_en.txt"), true));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertNull(schema.getFieldOrNull("*_txt_ga"));

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Collections.emptyList(), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertNotNull(schema.getFieldTypeByName("text_ga"));
    assertNotNull(schema.getFieldOrNull("*_txt_ga"));

    schema = helper.syncLanguageSpecificObjectsAndFiles(configSet, schema, Collections.emptyList(), false, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNull(schema.getFieldOrNull("*_txt_fr"));
    assertNotNull(schema.getFieldTypeByName("text_ga"));
    assertNull(schema.getFieldOrNull("*_txt_ga"));

    schema = helper.toggleNestedDocsFields(schema, true);
    assertTrue(schema.hasExplicitField(ROOT_FIELD_NAME));
    assertTrue(schema.hasExplicitField(NEST_PATH_FIELD_NAME));
    schema = helper.toggleNestedDocsFields(schema, false);
    assertFalse(schema.hasExplicitField(ROOT_FIELD_NAME));
    assertFalse(schema.hasExplicitField(NEST_PATH_FIELD_NAME));
    schema = helper.toggleNestedDocsFields(schema, true);
    assertTrue(schema.hasExplicitField(ROOT_FIELD_NAME));
    assertTrue(schema.hasExplicitField(NEST_PATH_FIELD_NAME));
  }

  @Test
  public void testPersistSampleDocs() throws Exception {
    String configSet = "testPersistSampleDocs";

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("author", "Ken Follet");
    doc.setField("title", "The Pillars of the Earth");
    doc.setField("series", "Kingsbridge Series");
    doc.setField("pages", 809);
    doc.setField("published_year", 1989);

    helper.postDataToBlobStore(cluster.getSolrClient(), configSet + "_sample",
        DefaultSampleDocumentsLoader.streamAsBytes(toJavabin(Collections.singletonList(doc))));

    List<SolrInputDocument> docs = helper.getStoredSampleDocs(configSet);
    assertTrue(docs != null && docs.size() == 1);
    assertEquals("1", docs.get(0).getFieldValue("id"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAnalyzeField() throws Exception {
    String configSet = "testAnalyzeField";

    String mutableId = getMutableId(configSet);
    helper.copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));
    helper.createCollection(mutableId, mutableId);

    Map<String, Object> addField = makeMap("name", "title", "type", "text_en");
    String addedFieldName = helper.addSchemaObject(configSet, Collections.singletonMap("add-field", addField));
    assertEquals("title", addedFieldName);

    Map<String, Object> analysis = helper.analyzeField(configSet, "title", "The Pillars of the Earth");

    Map<String, Object> title = (Map<String, Object>) ((Map<String, Object>) analysis.get("field_names")).get("title");
    assertNotNull(title);
    List<Object> index = (List<Object>) title.get("index");
    assertNotNull(index);
    assertFalse(index.isEmpty());
  }

  @Test
  public void testCopyFieldUpdates() throws Exception {
    String configSet = "testCopyFieldUpdates";
    String mutableId = getMutableId(configSet);
    helper.copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));
    helper.createCollection(mutableId, mutableId);

    // add / update field
    Map<String, Object> addField = makeMap("name", "author", "type", "string");
    String addedFieldName = helper.addSchemaObject(configSet, Collections.singletonMap("add-field", addField));
    assertEquals("author", addedFieldName);

    helper.addSchemaObject(configSet,
        Collections.singletonMap("add-field", makeMap("name", "_catch_all_", "type", "text_general")));

    Map<String, Object> updateField = makeMap("name", "author", "type", "string", "copyDest", "_text_");
    ManagedIndexSchema latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    latest.getField("_catch_all_");

    Map<String, Object> resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));

    updateField = makeMap("name", "author", "type", "string", "copyDest", "_text_,_catch_all_");
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));

    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(Collections.singletonList("author"), latest.getCopySources("_text_"));
    assertEquals(Collections.singletonList("author"), latest.getCopySources("_catch_all_"));

    updateField = makeMap("name", "author", "type", "string", "copyDest", "");
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertTrue(latest.getCopySources("_text_").isEmpty());
    assertTrue(latest.getCopySources("_catch_all_").isEmpty());
  }

  @Test
  public void testAddUpdateObjects() throws Exception {
    String configSet = "testAddUpdateObjects";
    String mutableId = getMutableId(configSet);
    helper.copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));
    helper.createCollection(mutableId, mutableId);

    // add / update field
    Map<String, Object> addField = makeMap("name", "author", "type", "string");
    String addedFieldName = helper.addSchemaObject(configSet, Collections.singletonMap("add-field", addField));
    assertEquals("author", addedFieldName);

    Map<String, Object> updateField = makeMap("name", "author", "type", "string", "required", true);
    ManagedIndexSchema latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    Map<String, Object> resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));

    SchemaField addedField = latest.getField("author");
    assertFalse(addedField.multiValued());
    assertTrue(addedField.hasDocValues());

    // an update that requires a full-rebuild
    updateField = makeMap("name", "author", "type", "string", "required", true, "docValues", true, "multiValued", true, "copyDest", "_text_");
    resp = helper.updateSchemaObject(configSet, updateField, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(true, resp.get("rebuild"));

    // did the copy field update get applied?
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(Collections.singletonList("author"), latest.getCopySources("_text_"));

    // switch the author field type to strings
    updateField = makeMap("name", "author", "type", "strings", "docValues", true, "copyDest", "_text_");
    resp = helper.updateSchemaObject(configSet, updateField, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild")); // tricky, we didn't actually change the field to multiValue (it already was)

    // add / update field type
    Map<String, Object> addType = makeMap("name", "testType", "class", "solr.StrField", "docValues", true);
    String addTypeName = helper.addSchemaObject(configSet, Collections.singletonMap("add-field-type", addType));
    assertEquals("testType", addTypeName);

    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    FieldType addedType = latest.getFieldTypeByName(addTypeName);
    assertNotNull(addedType);
    SimpleOrderedMap<Object> props = addedType.getNamedPropertyValues(false);
    assertTrue(props.getBooleanArg("docValues"));
    assertFalse(addedType.isMultiValued());

    Map<String, Object> updateType = makeMap("name", "testType", "class", "solr.StrField", "docValues", true, "multiValued", true);
    resp = helper.updateSchemaObject(configSet, updateType, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertNotNull(resp);
    assertEquals("type", resp.get("updateType"));
    assertEquals(true, resp.get("rebuild"));

    // add / update dynamic field
    Map<String, Object> addDynField = makeMap("name", "*_test", "type", "string");
    String addedDynFieldName = helper.addSchemaObject(configSet, Collections.singletonMap("add-dynamic-field", addDynField));
    assertEquals("*_test", addedDynFieldName);

    // update the dynamic field
    Map<String, Object> updateDynField = makeMap("name", "*_test", "type", "string", "docValues", false);
    resp = helper.updateSchemaObject(configSet, updateDynField, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertEquals("*_test", addedDynFieldName);
    assertNotNull(resp);
    assertEquals("dynamicField", resp.get("updateType"));
  }
}
