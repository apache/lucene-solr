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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.processor.UpdateRequestProcessor;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

/**
 * <p>
 * Test for writerImpl paramater (to provide own SolrWriter)
 * </p>
 * 
 * 
 * @since solr 4.0
 */
public class TestWriterImpl extends AbstractDataImportHandlerTestCase {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-nodatasource-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testDataConfigWithDataSource() throws Exception {
    @SuppressWarnings({"rawtypes"})
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    rows.add(createMap("id", "2", "desc", "two"));
    rows.add(createMap("id", "3", "desc", "break"));
    rows.add(createMap("id", "4", "desc", "four"));
    
    MockDataSource.setIterator("select * from x", rows.iterator());
    
    @SuppressWarnings({"rawtypes"})
    Map extraParams = createMap("writerImpl", TestSolrWriter.class.getName(),
        "commit", "true");
    runFullImport(loadDataConfig("data-config-with-datasource.xml"),
        extraParams);
    
    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("id:3"), "//*[@numFound='0']");
    assertQ(req("id:4"), "//*[@numFound='1']");
  }
  
  public static class TestSolrWriter extends SolrWriter {
    
    public TestSolrWriter(UpdateRequestProcessor processor, SolrQueryRequest req) {
      super(processor, req);
    }
    
    @Override
    public boolean upload(SolrInputDocument doc) {
      if (doc.getField("desc").getFirstValue().equals("break")) {
        return false;
      }
      return super.upload(doc);
    }
    
  }
  
}
