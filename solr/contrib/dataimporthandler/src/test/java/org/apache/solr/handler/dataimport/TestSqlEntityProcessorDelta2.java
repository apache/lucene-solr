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

/**
 * <p>
 * Test for SqlEntityProcessor which checks variations in primary key names and deleted ids
 * </p>
 * 
 *
 *
 * @since solr 1.3
 */
public class TestSqlEntityProcessorDelta2 extends AbstractDataImportHandlerTestCase {
  private static final String FULLIMPORT_QUERY = "select * from x";

  private static final String DELTA_QUERY = "select id from x where last_modified > NOW";

  private static final String DELETED_PK_QUERY = "select id from x where last_modified > NOW AND deleted='true'";

  private static final String dataConfig_delta2 =
    "<dataConfig>" +
    "  <dataSource  type=\"MockDataSource\"/>\n" +
    "  <document>\n" +
    "    <entity name=\"x\" transformer=\"TemplateTransformer\"" +
    "            query=\"" + FULLIMPORT_QUERY + "\"" +
    "            deletedPkQuery=\"" + DELETED_PK_QUERY + "\"" +
    "            deltaImportQuery=\"select * from x where id='${dih.delta.id}'\"" +
    "            deltaQuery=\"" + DELTA_QUERY + "\">\n" +
    "      <field column=\"tmpid\" template=\"prefix-${x.id}\" name=\"solr_id\"/>\n" +
    "      <entity name=\"y\" query=\"select * from y where y.A='${x.id}'\">\n" +
    "        <field column=\"desc\" />\n" +
    "      </entity>\n" +
    "    </entity>\n" +
    "  </document>\n" +
    "</dataConfig>\n";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-solr_id-schema.xml");
  }
  
  @Before @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @SuppressWarnings("unchecked")
  private void add1document() throws Exception {
    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator(FULLIMPORT_QUERY, parentRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));
    MockDataSource.setIterator("select * from y where y.A='1'", childRow
        .iterator());

    runFullImport(dataConfig_delta2);

    assertQ(req("*:* OR add1document"), "//*[@numFound='1']");
    assertQ(req("solr_id:prefix-1"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_FullImport() throws Exception {
    add1document();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_delete() throws Exception {
    add1document();
    List deletedRow = new ArrayList();
    deletedRow.add(createMap("id", "1"));
    MockDataSource.setIterator(DELETED_PK_QUERY, deletedRow.iterator());

    MockDataSource.setIterator(DELTA_QUERY, Collections
        .EMPTY_LIST.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));
    MockDataSource.setIterator("select * from y where y.A='1'", childRow
        .iterator());

    runDeltaImport(dataConfig_delta2);
    assertQ(req("*:* OR testCompositePk_DeltaImport_delete"), "//*[@numFound='0']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_empty() throws Exception {
    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "1"));
    MockDataSource.setIterator(DELTA_QUERY, deltaRow.iterator());

    MockDataSource.setIterator(DELETED_PK_QUERY, Collections
        .EMPTY_LIST.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x where id='1'", parentRow
        .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));
    MockDataSource.setIterator("select * from y where y.A='1'", childRow
        .iterator());

    runDeltaImport(dataConfig_delta2);

    assertQ(req("*:* OR testCompositePk_DeltaImport_empty"), "//*[@numFound='1']");
    assertQ(req("solr_id:prefix-1"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_replace_delete() throws Exception {
    add1document();
    MockDataSource.clearCache();

    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "1"));
    MockDataSource.setIterator(DELTA_QUERY,
        deltaRow.iterator());

    List deletedRow = new ArrayList();
    deletedRow.add(createMap("id", "1"));
    MockDataSource.setIterator(DELETED_PK_QUERY,
        deletedRow.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x where id='1'", parentRow
        .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "goodbye"));
    MockDataSource.setIterator("select * from y where y.A='1'", childRow
        .iterator());

    runDeltaImport(dataConfig_delta2);

    assertQ(req("*:* OR testCompositePk_DeltaImport_replace_delete"), "//*[@numFound='0']");
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_replace_nodelete() throws Exception {
    add1document();
    MockDataSource.clearCache();

    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "1"));
    MockDataSource.setIterator(DELTA_QUERY,
        deltaRow.iterator());

    MockDataSource.setIterator(DELETED_PK_QUERY, Collections
        .EMPTY_LIST.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x where id='1'", parentRow
        .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "goodbye"));
    MockDataSource.setIterator("select * from y where y.A='1'", childRow
        .iterator());

    runDeltaImport(dataConfig_delta2);

    assertQ(req("*:* OR XtestCompositePk_DeltaImport_replace_nodelete"), "//*[@numFound='1']");
    assertQ(req("solr_id:prefix-1"), "//*[@numFound='1']");
    assertQ(req("desc:hello OR XtestCompositePk_DeltaImport_replace_nodelete"), "//*[@numFound='0']");
    assertQ(req("desc:goodbye"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_add() throws Exception {
    add1document();
    MockDataSource.clearCache();

    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "2"));
    MockDataSource.setIterator(DELTA_QUERY,
        deltaRow.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "2"));
    MockDataSource.setIterator("select * from x where id='2'", parentRow
        .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "goodbye"));
    MockDataSource.setIterator("select * from y where y.A='2'", childRow
        .iterator());

    runDeltaImport(dataConfig_delta2);

    assertQ(req("*:* OR testCompositePk_DeltaImport_add"), "//*[@numFound='2']");
    assertQ(req("solr_id:prefix-1"), "//*[@numFound='1']");
    assertQ(req("solr_id:prefix-2"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
    assertQ(req("desc:goodbye"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_nodelta() throws Exception {
    add1document();
    MockDataSource.clearCache();

    MockDataSource.setIterator(DELTA_QUERY,
        Collections.EMPTY_LIST.iterator());

    runDeltaImport(dataConfig_delta2);

    assertQ(req("*:* OR testCompositePk_DeltaImport_nodelta"), "//*[@numFound='1']");
    assertQ(req("solr_id:prefix-1 OR testCompositePk_DeltaImport_nodelta"), "//*[@numFound='1']");
    assertQ(req("desc:hello OR testCompositePk_DeltaImport_nodelta"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_add_delete() throws Exception {
    add1document();
    MockDataSource.clearCache();

    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "2"));
    MockDataSource.setIterator(DELTA_QUERY,
        deltaRow.iterator());

    List deletedRow = new ArrayList();
    deletedRow.add(createMap("id", "1"));
    MockDataSource.setIterator(DELETED_PK_QUERY,
        deletedRow.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "2"));
    MockDataSource.setIterator("select * from x where id='2'", parentRow
        .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "goodbye"));
    MockDataSource.setIterator("select * from y where y.A='2'", childRow
        .iterator());

    runDeltaImport(dataConfig_delta2);

    assertQ(req("*:* OR XtestCompositePk_DeltaImport_add_delete"), "//*[@numFound='1']");
    assertQ(req("solr_id:prefix-2"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='0']");
    assertQ(req("desc:goodbye"), "//*[@numFound='1']");
  }
}
