/**
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
package org.apache.solr.handler.dataimport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.apache.solr.request.LocalSolrQueryRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.io.File;

/**
 * <p>
 * Test for DocBuilder using the test harness
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestDocBuilder2 extends AbstractDataImportHandlerTest {

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  public String getSchemaFile() {
    return "dataimport-schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "dataimport-solrconfig.xml";
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSingleEntity() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(loadDataConfig("single-entity-data-config.xml"));

    assertQ(req("id:1"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSingleEntity_CaseInsensitive() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desC", "one"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(dataConfigWithCaseInsensitiveFields);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertTrue("Start event listener was not called", StartEventListener.executed);
    assertTrue("End event listener was not called", EndEventListener.executed);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDynamicFields() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(dataConfigWithDynamicTransformer);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("dynamic_s:test"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRequestParamsAsVariable() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "101", "desc", "ApacheSolr"));
    MockDataSource.setIterator("select * from books where category='search'", rows.iterator());

    LocalSolrQueryRequest request = lrf.makeRequest("command", "full-import",
            "debug", "on", "clean", "true", "commit", "true",
            "category", "search",
            "dataConfig", requestParamAsVariable);
    h.query("/dataimport", request);
    assertQ(req("desc:ApacheSolr"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRequestParamsAsFieldName() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("mypk", "101", "text", "ApacheSolr"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    LocalSolrQueryRequest request = lrf.makeRequest("command", "full-import",
            "debug", "on", "clean", "true", "commit", "true",
            "mypk", "id", "text", "desc",
            "dataConfig", dataConfigWithTemplatizedFieldNames);
    h.query("/dataimport", request);
    assertQ(req("id:101"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testContext() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(loadDataConfig("data-config-with-transformer.xml"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSkipDoc() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    rows.add(createMap("id", "2", "desc", "two", "$skipDoc", "true"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(dataConfigWithDynamicTransformer);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='0']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSkipRow() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    rows.add(createMap("id", "2", "desc", "two", "$skipRow", "true"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(dataConfigWithDynamicTransformer);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='0']");

    MockDataSource.clearCache();

    rows = new ArrayList();
    rows.add(createMap("id", "3", "desc", "one"));
    rows.add(createMap("id", "4", "desc", "two"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    rows = new ArrayList();
    rows.add(createMap("name_s", "abcd"));
    MockDataSource.setIterator("3", rows.iterator());

    rows = new ArrayList();
    rows.add(createMap("name_s", "xyz", "$skipRow", "true"));
    MockDataSource.setIterator("4", rows.iterator());

    super.runFullImport(dataConfigWithTwoEntities);
    assertQ(req("id:3"), "//*[@numFound='1']");
    assertQ(req("id:4"), "//*[@numFound='1']");
    assertQ(req("name_s:abcd"), "//*[@numFound='1']");
    assertQ(req("name_s:xyz"), "//*[@numFound='0']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStopTransform() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    rows.add(createMap("id", "2", "desc", "two", "$stopTransform", "true"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(dataConfigForSkipTransform);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("name_s:xyz"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeleteDocs() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    rows.add(createMap("id", "2", "desc", "two"));
    rows.add(createMap("id", "3", "desc", "two", "$deleteDocById", "2"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(dataConfigForSkipTransform);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='0']");
    assertQ(req("id:3"), "//*[@numFound='1']");

    MockDataSource.clearCache();
    rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    rows.add(createMap("id", "2", "desc", "one"));
    rows.add(createMap("id", "3", "desc", "two", "$deleteDocByQuery", "desc:one"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(dataConfigForSkipTransform);

    assertQ(req("id:1"), "//*[@numFound='0']");
    assertQ(req("id:2"), "//*[@numFound='0']");
    assertQ(req("id:3"), "//*[@numFound='1']");
  }

  @Test
  public void testFileListEntityProcessor_lastIndexTime() throws Exception  {
    long time = System.currentTimeMillis();
    File tmpdir = new File("." + time);
    tmpdir.mkdir();
    tmpdir.deleteOnExit();

    Map<String, String> params = createMap("baseDir", tmpdir.getAbsolutePath());

    TestFileListEntityProcessor.createFile(tmpdir, "a.xml", "a.xml".getBytes(), true);
    TestFileListEntityProcessor.createFile(tmpdir, "b.xml", "b.xml".getBytes(), true);
    TestFileListEntityProcessor.createFile(tmpdir, "c.props", "c.props".getBytes(), true);
    super.runFullImport(dataConfigFileList, params);
    assertQ(req("*:*"), "//*[@numFound='3']");

    // Add a new file after a full index is done
    TestFileListEntityProcessor.createFile(tmpdir, "t.xml", "t.xml".getBytes(), false);
    super.runFullImport(dataConfigFileList, params);
    // we should find only 1 because by default clean=true is passed
    // and this particular import should find only one file t.xml
    assertQ(req("*:*"), "//*[@numFound='1']");
  }

  public static class MockTransformer extends Transformer {
    public Object transformRow(Map<String, Object> row, Context context) {
      Assert.assertTrue("Context gave incorrect data source", context.getDataSource("mockDs") instanceof MockDataSource2);
      return row;
    }
  }

  public static class AddDynamicFieldTransformer extends Transformer  {
    public Object transformRow(Map<String, Object> row, Context context) {
      // Add a dynamic field
      row.put("dynamic_s", "test");
      return row;
    }
  }

  public static class MockDataSource2 extends MockDataSource  {

  }

  public static class StartEventListener implements EventListener {
    public static boolean executed = false;

    public void onEvent(Context ctx) {
      executed = true;
    }
  }

  public static class EndEventListener implements EventListener {
    public static boolean executed = false;

    public void onEvent(Context ctx) {
      executed = true;
    }
  }

  private final String requestParamAsVariable = "<dataConfig>\n" +
          "    <dataSource type=\"MockDataSource\" />\n" +
          "    <document>\n" +
          "        <entity name=\"books\" query=\"select * from books where category='${dataimporter.request.category}'\">\n" +
          "            <field column=\"id\" />\n" +
          "            <field column=\"desc\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

   private final String dataConfigWithDynamicTransformer = "<dataConfig> <dataSource type=\"MockDataSource\"/>\n" +
          "    <document>\n" +
          "        <entity name=\"books\" query=\"select * from x\"" +
           "                transformer=\"TestDocBuilder2$AddDynamicFieldTransformer\">\n" +
          "            <field column=\"id\" />\n" +
          "            <field column=\"desc\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private final String dataConfigForSkipTransform = "<dataConfig> <dataSource  type=\"MockDataSource\"/>\n" +
          "    <document>\n" +
          "        <entity name=\"books\" query=\"select * from x\"" +
           "                transformer=\"TemplateTransformer\">\n" +
          "            <field column=\"id\" />\n" +
          "            <field column=\"desc\" />\n" +
          "            <field column=\"name_s\" template=\"xyz\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private final String dataConfigWithTwoEntities = "<dataConfig><dataSource type=\"MockDataSource\"/>\n" +
          "    <document>\n" +
          "        <entity name=\"books\" query=\"select * from x\">" +
          "            <field column=\"id\" />\n" +
          "            <field column=\"desc\" />\n" +
          "            <entity name=\"authors\" query=\"${books.id}\">" +
          "               <field column=\"name_s\" />" +
          "            </entity>" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private final String dataConfigWithCaseInsensitiveFields = "<dataConfig> <dataSource  type=\"MockDataSource\"/>\n" +
          "    <document onImportStart=\"TestDocBuilder2$StartEventListener\" onImportEnd=\"TestDocBuilder2$EndEventListener\">\n" +
          "        <entity name=\"books\" query=\"select * from x\">\n" +
          "            <field column=\"ID\" />\n" +
          "            <field column=\"Desc\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private final String dataConfigWithTemplatizedFieldNames = "<dataConfig><dataSource  type=\"MockDataSource\"/>\n" +
          "    <document>\n" +
          "        <entity name=\"books\" query=\"select * from x\">\n" +
          "            <field column=\"mypk\" name=\"${dih.request.mypk}\" />\n" +
          "            <field column=\"text\" name=\"${dih.request.text}\" />\n" +
          "        </entity>\n" +
          "    </document>\n" +
          "</dataConfig>";

  private final String dataConfigFileList = "<dataConfig>\n" +
          "\t<document>\n" +
          "\t\t<entity name=\"x\" processor=\"FileListEntityProcessor\" \n" +
          "\t\t\t\tfileName=\".*\" newerThan=\"${dih.last_index_time}\" \n" +
          "\t\t\t\tbaseDir=\"${dih.request.baseDir}\" transformer=\"TemplateTransformer\">\n" +
          "\t\t\t<field column=\"id\" template=\"${x.file}\" />\n" +
          "\t\t</entity>\n" +
          "\t</document>\n" +
          "</dataConfig>";
}
