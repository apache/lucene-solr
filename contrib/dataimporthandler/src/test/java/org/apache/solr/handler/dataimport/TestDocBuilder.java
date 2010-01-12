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

import org.apache.solr.common.SolrInputDocument;
import static org.apache.solr.handler.dataimport.AbstractDataImportHandlerTest.createMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * <p>
 * Test for DocBuilder
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestDocBuilder {

  @Test
  public void loadClass() throws Exception {
    Class clz = DocBuilder.loadClass("RegexTransformer", null);
    Assert.assertNotNull(clz);
  }

  @Test
  public void singleEntityNoRows() {
    try {
      DataImporter di = new DataImporter();
      di.loadAndInit(dc_singleEntity);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.document.entities.get(0);
      MockDataSource.setIterator("select * from x", new ArrayList().iterator());
      ent.dataSrc = new MockDataSource();
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams();
      rp.command = "full-import";
      SolrWriterImpl swi = new SolrWriterImpl();
      di.runCmd(rp, swi);
      Assert.assertEquals(Boolean.TRUE, swi.deleteAllCalled);
      Assert.assertEquals(Boolean.TRUE, swi.commitCalled);
      Assert.assertEquals(0, swi.docs.size());
      Assert.assertEquals(1, di.getDocBuilder().importStatistics.queryCount
              .get());
      Assert
              .assertEquals(0, di.getDocBuilder().importStatistics.docCount.get());
      Assert.assertEquals(0, di.getDocBuilder().importStatistics.rowsCount
              .get());
    } finally {
      MockDataSource.clearCache();
    }
  }

  @Test
  public void testDeltaImportNoRows_MustNotCommit() {
    try {
      DataImporter di = new DataImporter();
      di.loadAndInit(dc_deltaConfig);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.document.entities.get(0);
      MockDataSource.setIterator("select * from x", new ArrayList().iterator());
      MockDataSource.setIterator("select id from x", new ArrayList().iterator());
      ent.dataSrc = new MockDataSource();
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams(createMap("command", "delta-import"));
      SolrWriterImpl swi = new SolrWriterImpl();
      di.runCmd(rp, swi);
      Assert.assertEquals(Boolean.FALSE, swi.deleteAllCalled);
      Assert.assertEquals(Boolean.FALSE, swi.commitCalled);
      Assert.assertEquals(0, swi.docs.size());
      Assert.assertEquals(1, di.getDocBuilder().importStatistics.queryCount.get());
      Assert.assertEquals(0, di.getDocBuilder().importStatistics.docCount.get());
      Assert.assertEquals(0, di.getDocBuilder().importStatistics.rowsCount.get());
    } finally {
      MockDataSource.clearCache();
    }
  }

  @Test
  public void singleEntityOneRow() {
    try {
      DataImporter di = new DataImporter();
      di.loadAndInit(dc_singleEntity);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.document.entities.get(0);
      List l = new ArrayList();
      l.add(createMap("id", 1, "desc", "one"));
      MockDataSource.setIterator("select * from x", l.iterator());
      ent.dataSrc = new MockDataSource();
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams();
      rp.command = "full-import";
      SolrWriterImpl swi = new SolrWriterImpl();
      di.runCmd(rp, swi);
      Assert.assertEquals(Boolean.TRUE, swi.deleteAllCalled);
      Assert.assertEquals(Boolean.TRUE, swi.commitCalled);
      Assert.assertEquals(1, swi.docs.size());
      Assert.assertEquals(1, di.getDocBuilder().importStatistics.queryCount
              .get());
      Assert
              .assertEquals(1, di.getDocBuilder().importStatistics.docCount.get());
      Assert.assertEquals(1, di.getDocBuilder().importStatistics.rowsCount
              .get());

      for (int i = 0; i < l.size(); i++) {
        Map<String, Object> map = (Map<String, Object>) l.get(i);
        SolrInputDocument doc = swi.docs.get(i);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          Assert.assertEquals(entry.getValue(), doc.getFieldValue(entry
                  .getKey()));
        }
      }
    } finally {
      MockDataSource.clearCache();
    }
  }

  @Test
  public void testImportCommand() {
    try {
      DataImporter di = new DataImporter();
      di.loadAndInit(dc_singleEntity);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.document.entities.get(0);
      List l = new ArrayList();
      l.add(createMap("id", 1, "desc", "one"));
      MockDataSource.setIterator("select * from x", l.iterator());
      ent.dataSrc = new MockDataSource();
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams(createMap("command", "import"));
      SolrWriterImpl swi = new SolrWriterImpl();
      di.runCmd(rp, swi);
      Assert.assertEquals(Boolean.FALSE, swi.deleteAllCalled);
      Assert.assertEquals(Boolean.TRUE, swi.commitCalled);
      Assert.assertEquals(1, swi.docs.size());
      Assert.assertEquals(1, di.getDocBuilder().importStatistics.queryCount
              .get());
      Assert
              .assertEquals(1, di.getDocBuilder().importStatistics.docCount.get());
      Assert.assertEquals(1, di.getDocBuilder().importStatistics.rowsCount
              .get());

      for (int i = 0; i < l.size(); i++) {
        Map<String, Object> map = (Map<String, Object>) l.get(i);
        SolrInputDocument doc = swi.docs.get(i);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          Assert.assertEquals(entry.getValue(), doc.getFieldValue(entry
                  .getKey()));
        }
      }
    } finally {
      MockDataSource.clearCache();
    }
  }

  @Test
  public void singleEntityMultipleRows() {
    try {
      DataImporter di = new DataImporter();
      di.loadAndInit(dc_singleEntity);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.document.entities.get(0);
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams();
      rp.command = "full-import";
      List l = new ArrayList();
      l.add(createMap("id", 1, "desc", "one"));
      l.add(createMap("id", 2, "desc", "two"));
      l.add(createMap("id", 3, "desc", "three"));

      MockDataSource.setIterator("select * from x", l.iterator());
      ent.dataSrc = new MockDataSource();
      SolrWriterImpl swi = new SolrWriterImpl();
      di.runCmd(rp, swi);
      Assert.assertEquals(Boolean.TRUE, swi.deleteAllCalled);
      Assert.assertEquals(Boolean.TRUE, swi.commitCalled);
      Assert.assertEquals(3, swi.docs.size());
      for (int i = 0; i < l.size(); i++) {
        Map<String, Object> map = (Map<String, Object>) l.get(i);
        SolrInputDocument doc = swi.docs.get(i);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          Assert.assertEquals(entry.getValue(), doc.getFieldValue(entry.getKey()));
        }
        Assert.assertEquals(map.get("desc"), doc.getFieldValue("desc_s"));
      }
      Assert.assertEquals(1, di.getDocBuilder().importStatistics.queryCount
              .get());
      Assert
              .assertEquals(3, di.getDocBuilder().importStatistics.docCount.get());
      Assert.assertEquals(3, di.getDocBuilder().importStatistics.rowsCount
              .get());
    } finally {
      MockDataSource.clearCache();
    }
  }

  static class SolrWriterImpl extends SolrWriter {
    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

    Boolean deleteAllCalled = Boolean.FALSE;

    Boolean commitCalled = Boolean.FALSE;

    public SolrWriterImpl() {
      super(null, ".");
    }

    public boolean upload(SolrInputDocument doc) {
      return docs.add(doc);
    }

    public void log(int event, String name, Object row) {
      // Do nothing
    }

    public void doDeleteAll() {
      deleteAllCalled = Boolean.TRUE;
    }

    public void commit(boolean b) {
      commitCalled = Boolean.TRUE;
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
