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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    Class clz = DocBuilder.loadClass("RegexTransformer");
    Assert.assertNotNull(clz);
  }

  @Test
  public void singleEntityNoRows() {
    try {
      DataImporter di = new DataImporter();
      di.loadDataConfig(dc_singleEntity);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.documents.get(0).entities.get(0);
      for (DataConfig.Field field : ent.fields) {
        field.nameOrColName = field.name = field.column;
      }
      MockDataSource.setIterator("select * from x", new ArrayList().iterator());
      ent.dataSrc = new MockDataSource();
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams();
      rp.command = "full-import";
      SolrWriterImpl swi = new SolrWriterImpl();
      di.rumCmd(rp, swi, Collections.EMPTY_MAP);
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
  public void singleEntityOneRow() {
    try {
      DataImporter di = new DataImporter();
      di.loadDataConfig(dc_singleEntity);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.documents.get(0).entities.get(0);
      for (DataConfig.Field field : ent.fields) {
        field.nameOrColName = field.name = field.column;
      }
      List l = new ArrayList();
      l.add(createMap("id", 1, "desc", "one"));
      MockDataSource.setIterator("select * from x", l.iterator());
      ent.dataSrc = new MockDataSource();
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams();
      rp.command = "full-import";
      SolrWriterImpl swi = new SolrWriterImpl();
      di.rumCmd(rp, swi, Collections.EMPTY_MAP);
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
  public void singleEntityMultipleRows() {
    try {
      DataImporter di = new DataImporter();
      di.loadDataConfig(dc_singleEntity);
      DataConfig cfg = di.getConfig();
      DataConfig.Entity ent = cfg.documents.get(0).entities.get(0);
      ent.isDocRoot = true;
      DataImporter.RequestParams rp = new DataImporter.RequestParams();
      rp.command = "full-import";
      for (DataConfig.Field field : ent.fields) {
        field.nameOrColName = field.name = field.column;
      }
      List l = new ArrayList();
      l.add(createMap("id", 1, "desc", "one"));
      l.add(createMap("id", 2, "desc", "two"));
      l.add(createMap("id", 3, "desc", "three"));

      MockDataSource.setIterator("select * from x", l.iterator());
      ent.dataSrc = new MockDataSource();
      SolrWriterImpl swi = new SolrWriterImpl();
      di.rumCmd(rp, swi, Collections.EMPTY_MAP);
      Assert.assertEquals(Boolean.TRUE, swi.deleteAllCalled);
      Assert.assertEquals(Boolean.TRUE, swi.commitCalled);
      Assert.assertEquals(3, swi.docs.size());
      for (int i = 0; i < l.size(); i++) {
        Map<String, Object> map = (Map<String, Object>) l.get(i);
        SolrInputDocument doc = swi.docs.get(i);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          Assert.assertEquals(entry.getValue(), doc.getFieldValue(entry
                  .getKey()));
        }
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

    Boolean deleteAllCalled;

    Boolean commitCalled;

    public SolrWriterImpl() {
      super(null, ".");
    }

    public SolrDoc getSolrDocInstance() {
      return new DataImportHandler.SolrDocumentWrapper();
    }

    public boolean upload(SolrDoc d) {
      return docs.add(((DataImportHandler.SolrDocumentWrapper) d).doc);
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
          + "    <document name=\"X\" >\n"
          + "        <entity name=\"x\" query=\"select * from x\">\n"
          + "          <field column=\"id\"/>\n"
          + "          <field column=\"desc\"/>\n" + "        </entity>\n"
          + "    </document>\n" + "</dataConfig>";

}
