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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.*;

/**
 * <p>
 * Test for SqlEntityProcessorDelta verifying fix for SOLR-1191
 * </p>
 * 
 *
 * @version $Id$
 * @since solr 3.1
 */
public class TestSqlEntityProcessorDeltaPrefixedPk extends AbstractDataImportHandlerTestCase {
  private static final String FULLIMPORT_QUERY = "select * from x";

  private static final String DELTA_QUERY = "select id from x where last_modified > NOW";

  private static final String DELETED_PK_QUERY = "select id from x where last_modified > NOW AND deleted='true'";

  private static final String dataConfig_delta =
    "<dataConfig>" +
    "  <dataSource  type=\"MockDataSource\"/>\n" +
    "  <document>\n" +
    "    <entity name=\"x\" transformer=\"TemplateTransformer\" pk=\"x.id\"" +
    "            query=\"" + FULLIMPORT_QUERY + "\"" +
    "            deletedPkQuery=\"" + DELETED_PK_QUERY + "\"" +
    "            deltaImportQuery=\"select * from x where id='${dih.delta.id}'\"" +
    "            deltaQuery=\"" + DELTA_QUERY + "\">\n" +
    "      <field column=\"id\" name=\"id\"/>\n" +
    "      <field column=\"desc\" name=\"desc\"/>\n" +
    "    </entity>\n" +
    "  </document>\n" +
    "</dataConfig>\n";
  
  private static final List EMPTY_LIST = Collections.EMPTY_LIST;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }

  @Before @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
    //Logger.getLogger("").setLevel(Level.ALL);
  }

  @SuppressWarnings("unchecked")
  private void add1document() throws Exception {
    List row = new ArrayList();
    row.add(createMap("id", "1", "desc", "bar"));
    MockDataSource.setIterator(FULLIMPORT_QUERY, row.iterator());

    runFullImport(dataConfig_delta);

    assertQ(req("*:* OR add1document"), "//*[@numFound='1']");
    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("desc:bar"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeltaImport_deleteResolvesUnprefixedPk() throws Exception {
    add1document();
    MockDataSource.clearCache();
    List deletedRows = new ArrayList();
    deletedRows.add(createMap("id", "1"));
    MockDataSource.setIterator(DELETED_PK_QUERY, deletedRows.iterator());
    MockDataSource.setIterator(DELTA_QUERY, EMPTY_LIST.iterator());
    runDeltaImport(dataConfig_delta);

    assertQ(req("*:* OR testDeltaImport_deleteResolvesUnprefixedPk"), "//*[@numFound='0']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeltaImport_replace_resolvesUnprefixedPk() throws Exception {
    add1document();
    MockDataSource.clearCache();
    List deltaRows = new ArrayList();
    deltaRows.add(createMap("id", "1"));
    MockDataSource.setIterator(DELTA_QUERY, deltaRows.iterator());
    MockDataSource.setIterator(DELETED_PK_QUERY, EMPTY_LIST.iterator());
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "baz"));
    MockDataSource.setIterator("select * from x where id='1'", rows.iterator());

    runDeltaImport(dataConfig_delta);

    assertQ(req("*:* OR testDeltaImport_replace_resolvesUnprefixedPk"), "//*[@numFound='1']");
    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("desc:bar"), "//*[@numFound='0']");
    assertQ(req("desc:baz"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeltaImport_addResolvesUnprefixedPk() throws Exception {
    add1document();
    MockDataSource.clearCache();

    List deltaRows = new ArrayList();
    deltaRows.add(createMap("id", "2"));
    MockDataSource.setIterator(DELTA_QUERY, deltaRows.iterator());

    List rows = new ArrayList();
    rows.add(createMap("id", "2", "desc", "xyzzy"));
    MockDataSource.setIterator("select * from x where id='2'", rows.iterator());

    runDeltaImport(dataConfig_delta);

    assertQ(req("*:* OR testDeltaImport_addResolvesUnprefixedPk"), "//*[@numFound='2']");
    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("desc:bar"), "//*[@numFound='1']");
    assertQ(req("desc:xyzzy"), "//*[@numFound='1']");
  }

}
