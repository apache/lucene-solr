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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.TestSampleDocumentsLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.JSONUtil;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.handler.designer.SchemaDesignerAPI.getMutableId;
import static org.apache.solr.response.RawResponseWriter.CONTENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSchemaDesignerAPI extends SolrCloudTestCase implements SchemaDesignerConstants {

  private CoreContainer cc;
  private SchemaDesignerAPI schemaDesignerAPI;

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1).addConfig(DEFAULT_CONFIGSET_NAME, new File(ExternalPaths.DEFAULT_CONFIGSET).toPath()).configure();
    // SchemaDesignerAPI depends on the blob store
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
    schemaDesignerAPI = new SchemaDesignerAPI(cc);
  }

  public void testTSV() throws Exception {
    String configSet = "testTSV";

    ModifiableSolrParams reqParams = new ModifiableSolrParams();

    // GET /schema-designer/info
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = mock(SolrQueryRequest.class);

    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(LANGUAGES_PARAM, "en");
    reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, false);
    when(req.getParams()).thenReturn(reqParams);

    String tsv = "id\tcol1\tcol2\n1\tfoo\tbar\n2\tbaz\tbah\n";

    // POST some sample TSV docs
    ContentStream stream = new ContentStreamBase.StringStream(tsv, "text/csv");
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/analyze
    schemaDesignerAPI.analyze(req, rsp);
    assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
    assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));
    assertEquals(2, rsp.getValues().get("numDocs"));

    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    rsp = new SolrQueryResponse();
    schemaDesignerAPI.cleanupTemp(req, rsp);

    String mutableId = getMutableId(configSet);
    assertFalse(cc.getZkController().getClusterState().hasCollection(mutableId));
    SolrZkClient zkClient = cc.getZkController().getZkClient();
    assertFalse(zkClient.exists("/configs/" + mutableId, true));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAddTechproductsProgressively() throws Exception {
    File docsDir = new File(ExternalPaths.SOURCE_HOME, "example/exampledocs");
    assertTrue(docsDir.getAbsolutePath() + " not found!", docsDir.isDirectory());
    File[] toAdd = docsDir.listFiles((dir, name) -> name.endsWith(".xml") || name.endsWith(".json") || name.endsWith(".csv") || name.endsWith(".jsonl"));
    assertNotNull("No test data files found in " + docsDir.getAbsolutePath(), toAdd);

    String configSet = "techproducts";

    ModifiableSolrParams reqParams = new ModifiableSolrParams();

    // GET /schema-designer/info
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.getInfo(req, rsp);
    // response should just be the default values
    Map<String, Object> expSettings = makeMap(
        ENABLE_DYNAMIC_FIELDS_PARAM, true,
        ENABLE_FIELD_GUESSING_PARAM, true,
        ENABLE_NESTED_DOCS_PARAM, false,
        LANGUAGES_PARAM, Collections.emptyList());
    assertDesignerSettings(expSettings, rsp.getValues());
    SolrParams rspData = rsp.getValues().toSolrParams();
    int schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);
    assertEquals(schemaVersion, -1); // shouldn't exist yet

    // Use the prep endpoint to prepare the new schema
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    rsp = new SolrQueryResponse();
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.prepNewSchema(req, rsp);
    assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
    assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));
    rspData = rsp.getValues().toSolrParams();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    for (File next : toAdd) {
      // Analyze some sample documents to refine the schema
      reqParams.clear();
      reqParams.set(CONFIG_SET_PARAM, configSet);
      reqParams.set(LANGUAGES_PARAM, "en");
      reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, false);
      reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
      req = mock(SolrQueryRequest.class);
      when(req.getParams()).thenReturn(reqParams);

      // POST some sample JSON docs
      ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(next);
      stream.setContentType(TestSampleDocumentsLoader.guessContentTypeFromFilename(next.getName()));
      when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));

      rsp = new SolrQueryResponse();

      // POST /schema-designer/analyze
      schemaDesignerAPI.analyze(req, rsp);

      assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
      assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));
      assertNotNull(rsp.getValues().get("fields"));
      assertNotNull(rsp.getValues().get("fieldTypes"));
      assertNotNull(rsp.getValues().get("docIds"));

      // capture the schema version for MVCC
      rspData = rsp.getValues().toSolrParams();
      schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);
    }

    // get info (from the temp)
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    rsp = new SolrQueryResponse();

    // GET /schema-designer/info
    schemaDesignerAPI.getInfo(req, rsp);
    expSettings = makeMap(
        ENABLE_DYNAMIC_FIELDS_PARAM, false,
        ENABLE_FIELD_GUESSING_PARAM, true,
        ENABLE_NESTED_DOCS_PARAM, false,
        LANGUAGES_PARAM, Collections.singletonList("en"),
        COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, rsp.getValues());

    // query to see how the schema decisions impact retrieval / ranking
    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(CommonParams.Q, "*:*");
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    rsp = new SolrQueryResponse();

    // GET /schema-designer/query
    schemaDesignerAPI.query(req, rsp);
    assertNotNull(rsp.getResponseHeader());
    SolrDocumentList results = (SolrDocumentList) rsp.getResponse();
    assertEquals(48, results.getNumFound());

    // publish schema to a config set that can be used by real collections
    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);

    String collection = "techproducts";
    reqParams.set(NEW_COLLECTION_PARAM, collection);
    reqParams.set(INDEX_TO_COLLECTION_PARAM, true);
    reqParams.set(RELOAD_COLLECTIONS_PARAM, true);
    reqParams.set(CLEANUP_TEMP_PARAM, true);
    reqParams.set(DISABLE_DESIGNER_PARAM, true);

    rsp = new SolrQueryResponse();
    schemaDesignerAPI.publish(req, rsp);
    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // listCollectionsForConfig
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    rsp = new SolrQueryResponse();
    schemaDesignerAPI.listCollectionsForConfig(req, rsp);
    List<String> collections = (List<String>) rsp.getValues().get("collections");
    assertNotNull(collections);
    assertTrue(collections.contains(collection));

    // now try to create another temp, which should fail since designer is disabled for this configSet now
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    rsp = new SolrQueryResponse();
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    try {
      schemaDesignerAPI.prepNewSchema(req, rsp);
      fail("Prep should fail for locked schema " + configSet);
    } catch (SolrException solrExc) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, solrExc.code());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSuggestFilmsXml() throws Exception {
    String configSet = "films";

    ModifiableSolrParams reqParams = new ModifiableSolrParams();

    File filmsDir = new File(ExternalPaths.SOURCE_HOME, "example/films");
    assertTrue(filmsDir.getAbsolutePath() + " not found!", filmsDir.isDirectory());
    File filmsXml = new File(filmsDir, "films.xml");
    assertTrue("example/films/films.xml not found", filmsXml.isFile());

    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, "true");

    SolrQueryRequest req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);

    // POST some sample XML docs
    ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(filmsXml);
    stream.setContentType("application/xml");
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));

    SolrQueryResponse rsp = new SolrQueryResponse();

    // POST /schema-designer/analyze
    schemaDesignerAPI.analyze(req, rsp);

    assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
    assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));
    assertNotNull(rsp.getValues().get("fields"));
    assertNotNull(rsp.getValues().get("fieldTypes"));
    List<String> docIds = (List<String>) rsp.getValues().get("docIds");
    assertNotNull(docIds);
    assertEquals(100, docIds.size()); // designer limits doc ids to top 100

    String idField = rsp.getValues()._getStr(UNIQUE_KEY_FIELD_PARAM, null);
    assertNotNull(idField);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBasicUserWorkflow() throws Exception {
    String configSet = "testJson";

    ModifiableSolrParams reqParams = new ModifiableSolrParams();

    // Use the prep endpoint to prepare the new schema
    reqParams.set(CONFIG_SET_PARAM, configSet);
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.prepNewSchema(req, rsp);
    assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
    assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));

    Map<String, Object> expSettings = makeMap(
        ENABLE_DYNAMIC_FIELDS_PARAM, true,
        ENABLE_FIELD_GUESSING_PARAM, true,
        ENABLE_NESTED_DOCS_PARAM, false,
        LANGUAGES_PARAM, Collections.emptyList(),
        COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, rsp.getValues());

    // Analyze some sample documents to refine the schema
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);

    // POST some sample JSON docs
    File booksJson = new File(ExternalPaths.SOURCE_HOME, "example/exampledocs/books.json");
    ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(booksJson);
    stream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));

    rsp = new SolrQueryResponse();

    // POST /schema-designer/analyze
    schemaDesignerAPI.analyze(req, rsp);

    assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
    assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));
    assertNotNull(rsp.getValues().get("fields"));
    assertNotNull(rsp.getValues().get("fieldTypes"));
    assertNotNull(rsp.getValues().get("docIds"));
    String idField = rsp.getValues()._getStr(UNIQUE_KEY_FIELD_PARAM, null);
    assertNotNull(idField);
    assertDesignerSettings(expSettings, rsp.getValues());

    // capture the schema version for MVCC
    SolrParams rspData = rsp.getValues().toSolrParams();
    reqParams.clear();
    int schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    // load the contents of a file
    Collection<String> files = (Collection<String>) rsp.getValues().get("files");
    assertTrue(files != null && !files.isEmpty());

    reqParams.set(CONFIG_SET_PARAM, configSet);
    String file = null;
    for (String f : files) {
      if ("solrconfig.xml".equals(f)) {
        file = f;
        break;
      }
    }
    assertNotNull("solrconfig.xml not found in files!", file);
    reqParams.set("file", file);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    rsp = new SolrQueryResponse();
    schemaDesignerAPI.getFileContents(req, rsp);
    String solrconfigXml = (String) rsp.getValues().get(file);
    assertNotNull(solrconfigXml);
    reqParams.clear();

    // Update solrconfig.xml
    rsp = new SolrQueryResponse();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set("file", file);

    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    when(req.getContentStreams()).thenReturn(
        Collections.singletonList(new ContentStreamBase.StringStream(solrconfigXml, "application/xml")));

    schemaDesignerAPI.updateFileContents(req, rsp);
    rspData = rsp.getValues().toSolrParams();
    reqParams.clear();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    // update solrconfig.xml with some invalid XML mess
    rsp = new SolrQueryResponse();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set("file", file);

    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    when(req.getContentStreams()).thenReturn(
        Collections.singletonList(new ContentStreamBase.StringStream("<config/>", "application/xml")));

    // this should fail b/c the updated solrconfig.xml is invalid
    schemaDesignerAPI.updateFileContents(req, rsp);
    rspData = rsp.getValues().toSolrParams();
    reqParams.clear();
    assertNotNull(rspData.get("updateFileError"));

    // remove dynamic fields and change the language to "en" only
    rsp = new SolrQueryResponse();
    // POST /schema-designer/analyze
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(LANGUAGES_PARAM, "en");
    reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, false);
    reqParams.set(ENABLE_FIELD_GUESSING_PARAM, false);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.analyze(req, rsp);

    expSettings = makeMap(
        ENABLE_DYNAMIC_FIELDS_PARAM, false,
        ENABLE_FIELD_GUESSING_PARAM, false,
        ENABLE_NESTED_DOCS_PARAM, false,
        LANGUAGES_PARAM, Collections.singletonList("en"),
        COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, rsp.getValues());

    List<String> filesInResp = (List<String>) rsp.getValues().get("files");
    assertEquals(5, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_en.txt"));

    rspData = rsp.getValues().toSolrParams();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    reqParams.clear();

    // add the dynamic fields back and change the languages too
    rsp = new SolrQueryResponse();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.add(LANGUAGES_PARAM, "en");
    reqParams.add(LANGUAGES_PARAM, "fr");
    reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, true);
    reqParams.set(ENABLE_FIELD_GUESSING_PARAM, false);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.analyze(req, rsp);

    expSettings = makeMap(
        ENABLE_DYNAMIC_FIELDS_PARAM, true,
        ENABLE_FIELD_GUESSING_PARAM, false,
        ENABLE_NESTED_DOCS_PARAM, false,
        LANGUAGES_PARAM, Arrays.asList("en", "fr"),
        COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, rsp.getValues());

    filesInResp = (List<String>) rsp.getValues().get("files");
    assertEquals(7, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_fr.txt"));

    rspData = rsp.getValues().toSolrParams();
    reqParams.clear();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    // add back all the default languages
    rsp = new SolrQueryResponse();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.add(LANGUAGES_PARAM, "*");
    reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, false);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.analyze(req, rsp);

    expSettings = makeMap(
        ENABLE_DYNAMIC_FIELDS_PARAM, false,
        ENABLE_FIELD_GUESSING_PARAM, false,
        ENABLE_NESTED_DOCS_PARAM, false,
        LANGUAGES_PARAM, Collections.emptyList(),
        COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, rsp.getValues());

    filesInResp = (List<String>) rsp.getValues().get("files");
    assertEquals(43, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_fr.txt"));
    assertTrue(filesInResp.contains("lang/stopwords_en.txt"));
    assertTrue(filesInResp.contains("lang/stopwords_it.txt"));

    rspData = rsp.getValues().toSolrParams();
    reqParams.clear();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    // Get the value of a sample document
    String docId = "978-0641723445";
    String fieldName = "series_t";
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(DOC_ID_PARAM, docId);
    reqParams.set(FIELD_PARAM, fieldName);
    reqParams.set(UNIQUE_KEY_FIELD_PARAM, idField);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    rsp = new SolrQueryResponse();

    // GET /schema-designer/sample
    schemaDesignerAPI.getSampleValue(req, rsp);
    rspData = rsp.getValues().toSolrParams();
    assertNotNull(rspData.get(idField));
    assertNotNull(rspData.get(fieldName));
    assertNotNull(rspData.get("analysis"));

    reqParams.clear();

    // at this point the user would refine the schema by
    // editing suggestions for fields and adding/removing fields / field types as needed

    // add a new field
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);

    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    stream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-field.json"));
    stream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));
    rsp = new SolrQueryResponse();

    // POST /schema-designer/add
    schemaDesignerAPI.addSchemaObject(req, rsp);
    assertNotNull(rsp.getValues().get("add-field"));
    rspData = rsp.getValues().toSolrParams();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);
    assertNotNull(rsp.getValues().get("fields"));

    // update an existing field
    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);

    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    // switch a single-valued field to a multi-valued field, which triggers a full rebuild of the "temp" collection
    stream = new ContentStreamBase.FileStream(getFile("schema-designer/update-author-field.json"));
    stream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));

    rsp = new SolrQueryResponse();

    // PUT /schema-designer/update
    schemaDesignerAPI.updateSchemaObject(req, rsp);
    assertNotNull(rsp.getValues().get("field"));
    rspData = rsp.getValues().toSolrParams();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    // add a new type
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);

    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    stream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-type.json"));
    stream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));
    rsp = new SolrQueryResponse();

    // POST /schema-designer/add
    schemaDesignerAPI.addSchemaObject(req, rsp);
    final String expectedTypeName = "test_txt";
    assertEquals(expectedTypeName, rsp.getValues().get("add-field-type"));
    rspData = rsp.getValues().toSolrParams();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);
    assertNotNull(rsp.getValues().get("fieldTypes"));
    List<SimpleOrderedMap<Object>> fieldTypes = (List<SimpleOrderedMap<Object>>) rsp.getValues().get("fieldTypes");
    Optional<SimpleOrderedMap<Object>> expected = fieldTypes.stream().filter(m -> expectedTypeName.equals(m.get("name"))).findFirst();
    assertTrue("New field type '" + expectedTypeName + "' not found in add type response!", expected.isPresent());

    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);

    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    stream = new ContentStreamBase.FileStream(getFile("schema-designer/update-type.json"));
    stream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));
    rsp = new SolrQueryResponse();

    // POST /schema-designer/update
    schemaDesignerAPI.updateSchemaObject(req, rsp);
    rspData = rsp.getValues().toSolrParams();
    schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    // query to see how the schema decisions impact retrieval / ranking
    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(CommonParams.Q, "*:*");
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    rsp = new SolrQueryResponse();

    // GET /schema-designer/query
    schemaDesignerAPI.query(req, rsp);
    assertNotNull(rsp.getResponseHeader());
    SolrDocumentList results = (SolrDocumentList) rsp.getResponse();
    assertEquals(4, results.size());

    // Download ZIP
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    rsp = new SolrQueryResponse();
    schemaDesignerAPI.downloadConfig(req, rsp);
    assertNotNull(rsp.getValues().get(CONTENT));

    // publish schema to a config set that can be used by real collections
    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);

    String collection = "test123";
    reqParams.set(NEW_COLLECTION_PARAM, collection);
    reqParams.set(INDEX_TO_COLLECTION_PARAM, true);
    reqParams.set(RELOAD_COLLECTIONS_PARAM, true);
    reqParams.set(CLEANUP_TEMP_PARAM, true);

    rsp = new SolrQueryResponse();
    schemaDesignerAPI.publish(req, rsp);

    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // listCollectionsForConfig
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    rsp = new SolrQueryResponse();
    schemaDesignerAPI.listCollectionsForConfig(req, rsp);
    List<String> collections = (List<String>) rsp.getValues().get("collections");
    assertNotNull(collections);
    assertTrue(collections.contains(collection));

    // verify temp designer objects were cleaned up during the publish operation ...
    String mutableId = getMutableId(configSet);
    assertFalse(cc.getZkController().getClusterState().hasCollection(mutableId));
    SolrZkClient zkClient = cc.getZkController().getZkClient();
    assertFalse(zkClient.exists("/configs/" + mutableId, true));

    SolrQuery query = new SolrQuery("*:*");
    query.setRows(0);
    QueryResponse qr = cluster.getSolrClient().query(collection, query);
    // this proves the docs were stored in the blob store too
    assertEquals(4, qr.getResults().getNumFound());
  }

  @SuppressWarnings("unchecked")
  public void testFieldUpdates() throws Exception {
    String configSet = "fieldUpdates";

    ModifiableSolrParams reqParams = new ModifiableSolrParams();

    // Use the prep endpoint to prepare the new schema
    reqParams.set(CONFIG_SET_PARAM, configSet);
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.prepNewSchema(req, rsp);
    assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
    assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));
    SolrParams rspData = rsp.getValues().toSolrParams();
    int schemaVersion = rspData.getInt(SCHEMA_VERSION_PARAM);

    // add our test field that we'll test various updates to
    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    reqParams.set(CONFIG_SET_PARAM, configSet);
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-field.json"));
    stream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stream));
    rsp = new SolrQueryResponse();

    // POST /schema-designer/add
    schemaDesignerAPI.addSchemaObject(req, rsp);
    assertNotNull(rsp.getValues().get("add-field"));

    final String fieldName = "keywords";

    Optional<SimpleOrderedMap<Object>> maybeField =
        ((List<SimpleOrderedMap<Object>>) rsp.getValues().get("fields")).stream().filter(m -> fieldName.equals(m.get("name"))).findFirst();
    assertTrue(maybeField.isPresent());
    SimpleOrderedMap<Object> field = maybeField.get();
    assertEquals(Boolean.FALSE, field.get("indexed"));
    assertEquals(Boolean.FALSE, field.get("required"));
    assertEquals(Boolean.TRUE, field.get("stored"));
    assertEquals(Boolean.TRUE, field.get("docValues"));
    assertEquals(Boolean.TRUE, field.get("useDocValuesAsStored"));
    assertEquals(Boolean.FALSE, field.get("multiValued"));
    assertEquals("string", field.get("type"));

    String mutableId = getMutableId(configSet);
    SchemaDesignerConfigSetHelper configSetHelper =
        new SchemaDesignerConfigSetHelper(cc, SchemaDesignerAPI.newSchemaSuggester(cc));
    ManagedIndexSchema schema = schemaDesignerAPI.loadLatestSchema(mutableId);

    // make it required
    Map<String, Object> updateField = makeMap("name", fieldName, "type", field.get("type"), "required", true);
    configSetHelper.updateField(configSet, updateField, schema);

    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
    SchemaField schemaField = schema.getField(fieldName);
    assertTrue(schemaField.isRequired());

    updateField = makeMap("name", fieldName, "type", field.get("type"), "required", false, "stored", false);
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertFalse(schemaField.isRequired());
    assertFalse(schemaField.stored());

    updateField = makeMap("name", fieldName, "type", field.get("type"), "required", false, "stored", false, "multiValued", true);
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertFalse(schemaField.isRequired());
    assertFalse(schemaField.stored());
    assertTrue(schemaField.multiValued());

    updateField = makeMap("name", fieldName, "type", "strings", "copyDest", "_text_");
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertTrue(schemaField.multiValued());
    assertEquals("strings", schemaField.getType().getTypeName());
    assertFalse(schemaField.isRequired());
    assertTrue(schemaField.stored());
    List<String> srcFields = schema.getCopySources("_text_");
    assertEquals(Collections.singletonList(fieldName), srcFields);
  }

  @SuppressWarnings({"unchecked"})
  public void testSchemaDiffEndpoint() throws Exception {
    String configSet = "testDiff";

    ModifiableSolrParams reqParams = new ModifiableSolrParams();

    // Use the prep endpoint to prepare the new schema
    reqParams.set(CONFIG_SET_PARAM, configSet);
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(reqParams);
    schemaDesignerAPI.prepNewSchema(req, rsp);
    assertNotNull(rsp.getValues().get(CONFIG_SET_PARAM));
    assertNotNull(rsp.getValues().get(SCHEMA_VERSION_PARAM));

    // publish schema to a config set that can be used by real collections
    reqParams.clear();
    reqParams.set(SCHEMA_VERSION_PARAM, String.valueOf(rsp.getValues().get(SCHEMA_VERSION_PARAM)));
    reqParams.set(CONFIG_SET_PARAM, configSet);

    String collection = "diff456";
    reqParams.set(NEW_COLLECTION_PARAM, collection);
    reqParams.set(INDEX_TO_COLLECTION_PARAM, true);
    reqParams.set(RELOAD_COLLECTIONS_PARAM, true);
    reqParams.set(CLEANUP_TEMP_PARAM, true);

    rsp = new SolrQueryResponse();
    schemaDesignerAPI.publish(req, rsp);

    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // Load the schema designer for the existing config set and make some changes to it
    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, "true");
    reqParams.set(ENABLE_FIELD_GUESSING_PARAM, "false");
    rsp = new SolrQueryResponse();
    schemaDesignerAPI.analyze(req, rsp);

    // Update id field to not use docValues
    List<SimpleOrderedMap<Object>> fields = (List<SimpleOrderedMap<Object>>) rsp.getValues().get("fields");
    SimpleOrderedMap<Object> idFieldMap = fields.stream().filter(field -> field.get("name").equals("id")).findFirst().get();
    idFieldMap.remove("copyDest"); // Don't include copyDest as it is not a property of SchemaField
    SimpleOrderedMap<Object> idFieldMapUpdated = idFieldMap.clone();
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("docValues", 0), Boolean.FALSE);
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("useDocValuesAsStored", 0), Boolean.FALSE);
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("omitTermFreqAndPositions", 0), Boolean.FALSE);

    SolrParams solrParams = idFieldMapUpdated.toSolrParams();
    Map<String, Object> mapParams = solrParams.toMap(new HashMap<>());
    mapParams.put("termVectors", Boolean.FALSE);
    reqParams.set(SCHEMA_VERSION_PARAM, rsp.getValues().toSolrParams().getInt(SCHEMA_VERSION_PARAM));

    ContentStreamBase.StringStream stringStream = new ContentStreamBase.StringStream(JSONUtil.toJSON(mapParams), JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(stringStream));

    rsp = new SolrQueryResponse();
    schemaDesignerAPI.updateSchemaObject(req, rsp);

    // Add a new field
    Integer schemaVersion = rsp.getValues().toSolrParams().getInt(SCHEMA_VERSION_PARAM);
    reqParams.set(SCHEMA_VERSION_PARAM, schemaVersion);
    ContentStreamBase.FileStream fileStream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-field.json"));
    fileStream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(fileStream));
    rsp = new SolrQueryResponse();
    // POST /schema-designer/add
    schemaDesignerAPI.addSchemaObject(req, rsp);
    assertNotNull(rsp.getValues().get("add-field"));

    // Add a new field type
    schemaVersion = rsp.getValues().toSolrParams().getInt(SCHEMA_VERSION_PARAM);
    reqParams.set(SCHEMA_VERSION_PARAM, schemaVersion);
    fileStream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-type.json"));
    fileStream.setContentType(JSON_MIME);
    when(req.getContentStreams()).thenReturn(Collections.singletonList(fileStream));
    rsp = new SolrQueryResponse();
    // POST /schema-designer/add
    schemaDesignerAPI.addSchemaObject(req, rsp);
    assertNotNull(rsp.getValues().get("add-field-type"));

    // Let's do a diff now
    rsp = new SolrQueryResponse();
    schemaDesignerAPI.getSchemaDiff(req, rsp);

    Map<String, Object> diff = (Map<String, Object>) rsp.getValues().get("diff");

    // field asserts
    assertNotNull(diff.get("fields"));
    Map<String, Object> fieldsDiff = (Map<String, Object>) diff.get("fields");
    assertNotNull(fieldsDiff.get("updated"));
    Map<String, Object> mapDiff = (Map<String, Object>) fieldsDiff.get("updated");
    assertEquals(
        Arrays.asList(
            ImmutableMap.of("omitTermFreqAndPositions", true, "useDocValuesAsStored", true, "docValues", true),
            ImmutableMap.of("omitTermFreqAndPositions", false, "useDocValuesAsStored", false, "docValues", false)),
        mapDiff.get("id"));
    assertNotNull(fieldsDiff.get("added"));
    Map<String, Object> fieldsAdded = (Map<String, Object>) fieldsDiff.get("added");
    assertNotNull(fieldsAdded.get("keywords"));

    // field type asserts
    assertNotNull(diff.get("fieldTypes"));
    Map<String, Object> fieldTypesDiff = (Map<String, Object>) diff.get("fieldTypes");
    assertNotNull(fieldTypesDiff.get("added"));
    Map<String, Object> fieldTypesAdded = (Map<String, Object>) fieldTypesDiff.get("added");
    assertNotNull(fieldTypesAdded.get("test_txt"));
  }

  @SuppressWarnings("rawtypes")
  protected void assertDesignerSettings(Map<String, Object> expected, NamedList actual) {
    for (String expKey : expected.keySet()) {
      Object expValue = expected.get(expKey);
      assertEquals("Value for designer setting '" + expKey + "' not match expected!", expValue, actual.get(expKey));
    }
  }
}
