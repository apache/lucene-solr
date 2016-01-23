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
package org.apache.solr.handler.dataimport;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
import org.apache.solr.handler.dataimport.config.DIHConfiguration;
import org.apache.solr.handler.dataimport.config.Entity;

import org.junit.After;
import org.junit.Test;

import java.util.*;

/**
 * <p>
 * Test for DocBuilder
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestDocBuilder extends AbstractDataImportHandlerTestCase {

  @Override
  @After
  public void tearDown() throws Exception {
    MockDataSource.clearCache();
    super.tearDown();
  }
  
  @Test
  public void loadClass() throws Exception {
    @SuppressWarnings("unchecked")
    Class<Transformer> clz = DocBuilder.loadClass("RegexTransformer", null);
    assertNotNull(clz);
  }

  @Test
  public void singleEntityNoRows() {
    DataImporter di = new DataImporter();
    di.loadAndInit(dc_singleEntity);
    DIHConfiguration cfg = di.getConfig();
    Entity ent = cfg.getEntities().get(0);
    MockDataSource.setIterator("select * from x", new ArrayList<Map<String, Object>>().iterator());
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals(Boolean.TRUE, swi.deleteAllCalled);
    assertEquals(Boolean.TRUE, swi.commitCalled);
    assertEquals(Boolean.TRUE, swi.finishCalled);
    assertEquals(0, swi.docs.size());
    assertEquals(1, di.getDocBuilder().importStatistics.queryCount.get());
    assertEquals(0, di.getDocBuilder().importStatistics.docCount.get());
    assertEquals(0, di.getDocBuilder().importStatistics.rowsCount.get());
  }

  @Test
  public void testDeltaImportNoRows_MustNotCommit() {
    DataImporter di = new DataImporter();
    di.loadAndInit(dc_deltaConfig);
    redirectTempProperties(di);

    DIHConfiguration cfg = di.getConfig();
    Entity ent = cfg.getEntities().get(0);
    MockDataSource.setIterator("select * from x", new ArrayList<Map<String, Object>>().iterator());
    MockDataSource.setIterator("select id from x", new ArrayList<Map<String, Object>>().iterator());
    RequestInfo rp = new RequestInfo(null, createMap("command", "delta-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals(Boolean.FALSE, swi.deleteAllCalled);
    assertEquals(Boolean.FALSE, swi.commitCalled);
    assertEquals(Boolean.TRUE, swi.finishCalled);
    assertEquals(0, swi.docs.size());
    assertEquals(1, di.getDocBuilder().importStatistics.queryCount.get());
    assertEquals(0, di.getDocBuilder().importStatistics.docCount.get());
    assertEquals(0, di.getDocBuilder().importStatistics.rowsCount.get());
  }

  @Test
  public void singleEntityOneRow() {
    DataImporter di = new DataImporter();
    di.loadAndInit(dc_singleEntity);
    DIHConfiguration cfg = di.getConfig();
    Entity ent = cfg.getEntities().get(0);
    List<Map<String, Object>> l = new ArrayList<>();
    l.add(createMap("id", 1, "desc", "one"));
    MockDataSource.setIterator("select * from x", l.iterator());
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals(Boolean.TRUE, swi.deleteAllCalled);
    assertEquals(Boolean.TRUE, swi.commitCalled);
    assertEquals(Boolean.TRUE, swi.finishCalled);
    assertEquals(1, swi.docs.size());
    assertEquals(1, di.getDocBuilder().importStatistics.queryCount.get());
    assertEquals(1, di.getDocBuilder().importStatistics.docCount.get());
    assertEquals(1, di.getDocBuilder().importStatistics.rowsCount.get());

    for (int i = 0; i < l.size(); i++) {
      Map<String, Object> map = l.get(i);
      SolrInputDocument doc = swi.docs.get(i);
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        assertEquals(entry.getValue(), doc.getFieldValue(entry.getKey()));
      }
    }
  }

  @Test
  public void testImportCommand() {
    DataImporter di = new DataImporter();
    di.loadAndInit(dc_singleEntity);
    DIHConfiguration cfg = di.getConfig();
    Entity ent = cfg.getEntities().get(0);
    List<Map<String, Object>> l = new ArrayList<>();
    l.add(createMap("id", 1, "desc", "one"));
    MockDataSource.setIterator("select * from x", l.iterator());
    RequestInfo rp = new RequestInfo(null, createMap("command", "import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals(Boolean.FALSE, swi.deleteAllCalled);
    assertEquals(Boolean.TRUE, swi.commitCalled);
    assertEquals(Boolean.TRUE, swi.finishCalled);
    assertEquals(1, swi.docs.size());
    assertEquals(1, di.getDocBuilder().importStatistics.queryCount.get());
    assertEquals(1, di.getDocBuilder().importStatistics.docCount.get());
    assertEquals(1, di.getDocBuilder().importStatistics.rowsCount.get());

    for (int i = 0; i < l.size(); i++) {
      Map<String, Object> map = (Map<String, Object>) l.get(i);
      SolrInputDocument doc = swi.docs.get(i);
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        assertEquals(entry.getValue(), doc.getFieldValue(entry.getKey()));
      }
    }
  }

  @Test
  public void singleEntityMultipleRows() {
    DataImporter di = new DataImporter();
    di.loadAndInit(dc_singleEntity);
    DIHConfiguration cfg = di.getConfig();
    Entity ent = cfg.getEntities().get(0);
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    List<Map<String, Object>> l = new ArrayList<>();
    l.add(createMap("id", 1, "desc", "one"));
    l.add(createMap("id", 2, "desc", "two"));
    l.add(createMap("id", 3, "desc", "three"));

    MockDataSource.setIterator("select * from x", l.iterator());
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals(Boolean.TRUE, swi.deleteAllCalled);
    assertEquals(Boolean.TRUE, swi.commitCalled);
    assertEquals(Boolean.TRUE, swi.finishCalled);
    assertEquals(3, swi.docs.size());
    for (int i = 0; i < l.size(); i++) {
      Map<String, Object> map = (Map<String, Object>) l.get(i);
      SolrInputDocument doc = swi.docs.get(i);
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        assertEquals(entry.getValue(), doc.getFieldValue(entry.getKey()));
      }
      assertEquals(map.get("desc"), doc.getFieldValue("desc_s"));
    }
    assertEquals(1, di.getDocBuilder().importStatistics.queryCount.get());
    assertEquals(3, di.getDocBuilder().importStatistics.docCount.get());
    assertEquals(3, di.getDocBuilder().importStatistics.rowsCount.get());
  }

  static class SolrWriterImpl extends SolrWriter {
    List<SolrInputDocument> docs = new ArrayList<>();

    Boolean deleteAllCalled = Boolean.FALSE;

    Boolean commitCalled = Boolean.FALSE;

    Boolean finishCalled = Boolean.FALSE;

    public SolrWriterImpl() {
      super(null, null);
    }

    @Override
    public boolean upload(SolrInputDocument doc) {
      return docs.add(doc);
    }

    @Override
    public void doDeleteAll() {
      deleteAllCalled = Boolean.TRUE;
    }

    @Override
    public void commit(boolean b) {
      commitCalled = Boolean.TRUE;
    }
    
    @Override
    public void close() {
      finishCalled = Boolean.TRUE;
    }
  }

  public static final String dc_singleEntity = "<dataConfig>\n"
          + "<dataSource  type=\"MockDataSource\"/>\n"
          + "    <document name=\"X\" >\n"
          + "        <entity name=\"x\" query=\"select * from x\">\n"
          + "          <field column=\"id\"/>\n"
          + "          <field column=\"desc\"/>\n"
          + "          <field column=\"desc\" name=\"desc_s\" />" + "        </entity>\n"
          + "    </document>\n" + "</dataConfig>";

  public static final String dc_deltaConfig = "<dataConfig>\n"
          + "<dataSource  type=\"MockDataSource\"/>\n"          
          + "    <document name=\"X\" >\n"
          + "        <entity name=\"x\" query=\"select * from x\" deltaQuery=\"select id from x\">\n"
          + "          <field column=\"id\"/>\n"
          + "          <field column=\"desc\"/>\n"
          + "          <field column=\"desc\" name=\"desc_s\" />" + "        </entity>\n"
          + "    </document>\n" + "</dataConfig>";

}
