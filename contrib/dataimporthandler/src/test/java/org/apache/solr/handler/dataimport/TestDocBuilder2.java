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
  public void testContext() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    super.runFullImport(loadDataConfig("data-config-with-transformer.xml"));
  }

  public static class MockTransformer extends Transformer {
    public Object transformRow(Map<String, Object> row, Context context) {
      Assert.assertTrue("Context gave incorrect data source", context.getDataSource("mockDs") instanceof MockDataSource2);
      return row;
    }
  }

  public static class MockDataSource2 extends MockDataSource  {

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

}
