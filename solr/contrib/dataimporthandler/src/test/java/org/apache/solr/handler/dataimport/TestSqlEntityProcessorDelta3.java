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

public class TestSqlEntityProcessorDelta3 extends AbstractDataImportHandlerTestCase {
  private static final String P_FULLIMPORT_QUERY = "select * from parent";
  private static final String P_DELTA_QUERY = "select parent_id from parent where last_modified > NOW";
  private static final String P_DELTAIMPORT_QUERY = "select * from parent where last_modified > NOW AND parent_id=${dih.delta.parent_id}";

  private static final String C_FULLIMPORT_QUERY = "select * from child";
  private static final String C_DELETED_PK_QUERY = "select id from child where last_modified > NOW AND deleted='true'";
  private static final String C_DELTA_QUERY = "select id from child where last_modified > NOW";
  private static final String C_PARENTDELTA_QUERY = "select parent_id from child where id=${child.id}";
  private static final String C_DELTAIMPORT_QUERY = "select * from child where last_modified > NOW AND parent_id=${dih.delta.parent_id}";
  
  private static final String dataConfig_delta =
    "<dataConfig>" +
    "  <dataSource  type=\"MockDataSource\"/>\n" +
    "  <document>" +
    "    <entity name=\"parent\" pk=\"parent_id\" rootEntity=\"false\"" +
    "            query=\"" + P_FULLIMPORT_QUERY + "\"" +
    "            deltaQuery=\"" + P_DELTA_QUERY + "\"" +
    "            deltaImportQuery=\"" + P_DELTAIMPORT_QUERY + "\">" +
    "      <field column=\"desc\" name=\"desc\"/>" +
    "      <entity name=\"child\" pk=\"id\" rootEntity=\"true\"" +
    "              query=\"" + C_FULLIMPORT_QUERY + "\"" +
    "              deletedPkQuery=\"" + C_DELETED_PK_QUERY + "\"" +
    "              deltaQuery=\"" + C_DELTA_QUERY + "\"" +
    "              parentDeltaQuery=\"" + C_PARENTDELTA_QUERY + "\"" +
    "              deltaImportQuery=\"" + C_DELTAIMPORT_QUERY + "\">" +
    "        <field column=\"id\" name=\"id\" />" +
    "      </entity>" +
    "    </entity>" +
    "  </document>" +
    "</dataConfig>\n";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
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
    parentRow.add(createMap("parent_id", "1", "desc", "d1"));
    MockDataSource.setIterator(P_FULLIMPORT_QUERY, parentRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("id", "2"));
    MockDataSource.setIterator(C_FULLIMPORT_QUERY, childRow.iterator());

    runFullImport(dataConfig_delta);

    assertQ(req("*:* OR add1document"), "//*[@numFound='1']");
    assertQ(req("id:1"), "//*[@numFound='0']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("desc:d1"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_FullImport() throws Exception {
    add1document();
  }
  
  // WORKS

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_delete() throws Exception {
    add1document();
    List deletedRow = new ArrayList();
    deletedRow.add(createMap("id", "2"));
    MockDataSource.setIterator(C_DELETED_PK_QUERY, deletedRow.iterator());
    MockDataSource.setIterator(C_DELTA_QUERY, Collections.EMPTY_LIST.iterator());

    List deletedParentRow = new ArrayList();
    deletedParentRow.add(createMap("parent_id", "1"));
    MockDataSource.setIterator("select parent_id from child where id=2", deletedParentRow.iterator());

    runDeltaImport(dataConfig_delta);
    assertQ(req("*:* OR testCompositePk_DeltaImport_delete"), "//*[@numFound='0']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_empty() throws Exception {
    List childDeltaRow = new ArrayList();
    childDeltaRow.add(createMap("id", "2"));
    MockDataSource.setIterator(C_DELTA_QUERY, childDeltaRow.iterator());
    MockDataSource.setIterator(C_DELETED_PK_QUERY, Collections.EMPTY_LIST.iterator());
    
    List childParentDeltaRow = new ArrayList();
    childParentDeltaRow.add(createMap("parent_id", "1"));
    MockDataSource.setIterator("select parent_id from child where id=2", childParentDeltaRow.iterator());
    
    MockDataSource.setIterator(P_DELTA_QUERY, Collections.EMPTY_LIST.iterator());

    List parentDeltaImportRow = new ArrayList();
    parentDeltaImportRow.add(createMap("parent_id", "1", "desc", "d1"));
    MockDataSource.setIterator("select * from parent where last_modified > NOW AND parent_id=1",
        parentDeltaImportRow.iterator());

    List childDeltaImportRow = new ArrayList();
    childDeltaImportRow.add(createMap("id", "2"));
    MockDataSource.setIterator("select * from child where last_modified > NOW AND parent_id=1",
        childDeltaImportRow.iterator());
    
    runDeltaImport(dataConfig_delta);

    assertQ(req("*:* OR testCompositePk_DeltaImport_empty"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("desc:d1"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_replace_nodelete() throws Exception {
    add1document();
    MockDataSource.clearCache();

    List deltaRow = new ArrayList();
    deltaRow.add(createMap("parent_id", "1"));
    MockDataSource.setIterator(P_DELTA_QUERY,
        deltaRow.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("parent_id", "1", "desc", "d2"));
    MockDataSource.setIterator("select * from parent where last_modified > NOW AND parent_id=1",
        parentRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("id", "2"));
    MockDataSource.setIterator("select * from child where last_modified > NOW AND parent_id=1",
        childRow.iterator());

    MockDataSource.setIterator(C_DELETED_PK_QUERY, Collections
        .EMPTY_LIST.iterator());

    runDeltaImport(dataConfig_delta);

    assertQ(req("*:* OR XtestCompositePk_DeltaImport_replace_nodelete"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("desc:s1 OR XtestCompositePk_DeltaImport_replace_nodelete"), "//*[@numFound='0']");
    assertQ(req("desc:d2"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_add() throws Exception {
    add1document();
    MockDataSource.clearCache();

    List parentDeltaRow = new ArrayList();
    parentDeltaRow.add(createMap("parent_id", "1"));
    MockDataSource.setIterator(P_DELTA_QUERY,
        parentDeltaRow.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("parent_id", "1", "desc", "d1"));
    MockDataSource.setIterator("select * from parent where last_modified > NOW AND parent_id=1",
        parentRow.iterator());

    List childDeltaRow = new ArrayList();
    childDeltaRow.add(createMap("id", "3"));
    MockDataSource.setIterator(C_DELTA_QUERY,
        childDeltaRow.iterator());

    List childParentDeltaRow = new ArrayList();
    childParentDeltaRow.add(createMap("parent_id", "1"));
    MockDataSource.setIterator("select parent_id from child where id='3'",
        childParentDeltaRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("id", "3"));
    MockDataSource.setIterator("select * from child where last_modified > NOW AND parent_id=1",
        childRow.iterator());

    runDeltaImport(dataConfig_delta);

    assertQ(req("*:* OR testCompositePk_DeltaImport_add"), "//*[@numFound='2']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("id:3"), "//*[@numFound='1']");
    assertQ(req("desc:d1"), "//*[@numFound='2']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_nodelta() throws Exception {
    add1document();
    MockDataSource.clearCache();

    MockDataSource.setIterator(P_DELTA_QUERY,
        Collections.EMPTY_LIST.iterator());

    MockDataSource.setIterator(C_DELTA_QUERY,
        Collections.EMPTY_LIST.iterator());

    runDeltaImport(dataConfig_delta);

    assertQ(req("*:* OR testCompositePk_DeltaImport_nodelta"), "//*[@numFound='1']");
    assertQ(req("id:2 OR testCompositePk_DeltaImport_nodelta"), "//*[@numFound='1']");
    assertQ(req("desc:d1 OR testCompositePk_DeltaImport_nodelta"), "//*[@numFound='1']");
  }
}
