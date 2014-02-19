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
package org.apache.solr.morphlines.solr;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.kitesdk.morphline.api.Collector;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.stdlib.PipeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.typesafe.config.Config;

public class AbstractSolrMorphlineTestBase extends SolrTestCaseJ4 {

  protected Collector collector;
  protected Command morphline;
  protected SolrServer solrServer;
  protected DocumentLoader testServer;
  
  protected static final boolean TEST_WITH_EMBEDDED_SOLR_SERVER = true;
  protected static final String EXTERNAL_SOLR_SERVER_URL = System.getProperty("externalSolrServer");
//  protected static final String EXTERNAL_SOLR_SERVER_URL = "http://127.0.0.1:8983/solr";

  protected static final String RESOURCES_DIR = ExternalPaths.SOURCE_HOME + "/contrib/map-reduce/src/test-files"; 
  protected static final String DEFAULT_BASE_DIR = "solr";
  protected static final AtomicInteger SEQ_NUM = new AtomicInteger();
  protected static final AtomicInteger SEQ_NUM2 = new AtomicInteger();
  
  protected static final Object NON_EMPTY_FIELD = new Object();
  
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSolrMorphlineTestBase.class);
  
  protected String tempDir;

  @BeforeClass
  public static void beforeClass() throws Exception {
    myInitCore(DEFAULT_BASE_DIR);
  }

  protected static void myInitCore(String baseDirName) throws Exception {
    Joiner joiner = Joiner.on(File.separator);
    initCore(
        joiner.join(RESOURCES_DIR, baseDirName, "collection1", "conf", "solrconfig.xml"),
        joiner.join(RESOURCES_DIR, baseDirName, "collection1", "conf", "schema.xml"),
        joiner.join(RESOURCES_DIR, baseDirName)
        );    
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    collector = new Collector();
    
    if (EXTERNAL_SOLR_SERVER_URL != null) {
      //solrServer = new ConcurrentUpdateSolrServer(EXTERNAL_SOLR_SERVER_URL, 2, 2);
      //solrServer = new SafeConcurrentUpdateSolrServer(EXTERNAL_SOLR_SERVER_URL, 2, 2);
      solrServer = new HttpSolrServer(EXTERNAL_SOLR_SERVER_URL);
      ((HttpSolrServer)solrServer).setParser(new XMLResponseParser());
    } else {
      if (TEST_WITH_EMBEDDED_SOLR_SERVER) {
        solrServer = new EmbeddedTestSolrServer(h.getCoreContainer(), "");
      } else {
        throw new RuntimeException("Not yet implemented");
        //solrServer = new TestSolrServer(getSolrServer());
      }
    }

    int batchSize = SEQ_NUM2.incrementAndGet() % 2 == 0 ? 100 : 1; //SolrInspector.DEFAULT_SOLR_SERVER_BATCH_SIZE : 1;
    testServer = new SolrServerDocumentLoader(solrServer, batchSize);
    deleteAllDocuments();
    
    tempDir = new File(TEMP_DIR + "/test-morphlines-" + System.currentTimeMillis()).getAbsolutePath();
    new File(tempDir).mkdirs();
  }
  
  @After
  public void tearDown() throws Exception {
    collector = null;
    solrServer = null;
    super.tearDown();
  }

  protected void testDocumentTypesInternal(
      String[] files, 
      Map<String,Integer> expectedRecords, 
      Map<String, Map<String, Object>> expectedRecordContents) throws Exception {
    
    deleteAllDocuments();
    int numDocs = 0;    
    for (int i = 0; i < 1; i++) {
      
      for (String file : files) {
        File f = new File(file);
        byte[] body = Files.toByteArray(f);
        Record event = new Record();
        //event.put(Fields.ID, docId++);
        event.getFields().put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(body));
        event.getFields().put(Fields.ATTACHMENT_NAME, f.getName());
        event.getFields().put(Fields.BASE_ID, f.getName());        
        collector.reset();
        load(event);
        Integer count = expectedRecords.get(file);
        if (count != null) {
          numDocs += count;
        } else {
          numDocs++;
        }
        assertEquals("unexpected results in " + file, numDocs, queryResultSetSize("*:*"));
        Map<String, Object> expectedContents = expectedRecordContents.get(file);
        if (expectedContents != null) {
          Record actual = collector.getFirstRecord();
          for (Map.Entry<String, Object> entry : expectedContents.entrySet()) {
            if (entry.getValue() == NON_EMPTY_FIELD) {
              assertNotNull(entry.getKey());
              assertTrue(actual.getFirstValue(entry.getKey()).toString().length() > 0);
            } else if (entry.getValue() == null) {
              assertEquals("key:" + entry.getKey(), 0, actual.get(entry.getKey()).size());
            } else {
              assertEquals("key:" + entry.getKey(), Arrays.asList(entry.getValue()), actual.get(entry.getKey()));
            }
          }
        }
      }
    }
    assertEquals(numDocs, queryResultSetSize("*:*"));
  }

  private boolean load(Record record) {
    Notifications.notifyStartSession(morphline);
    return morphline.process(record);
  }
  
  protected int queryResultSetSize(String query) {
//    return collector.getRecords().size();
    try {
      testServer.commitTransaction();
      solrServer.commit(false, true, true);
      QueryResponse rsp = solrServer.query(new SolrQuery(query).setRows(Integer.MAX_VALUE));
      LOGGER.debug("rsp: {}", rsp);
      int i = 0;
      for (SolrDocument doc : rsp.getResults()) {
        LOGGER.debug("rspDoc #{}: {}", i++, doc);
      }
      int size = rsp.getResults().size();
      return size;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private void deleteAllDocuments() throws SolrServerException, IOException {
    collector.reset();
    SolrServer s = solrServer;
    s.deleteByQuery("*:*"); // delete everything!
    s.commit();
  }

  protected Command createMorphline(String file) throws IOException {
    return new PipeBuilder().build(parse(file), null, collector, createMorphlineContext());
  }

  private MorphlineContext createMorphlineContext() {
    return new SolrMorphlineContext.Builder()
      .setDocumentLoader(testServer)
//      .setDocumentLoader(new CollectingDocumentLoader(100))
      .setExceptionHandler(new FaultTolerance(false, false, SolrServerException.class.getName()))
      .setMetricRegistry(new MetricRegistry())
      .build();
  }
  
  private Config parse(String file) throws IOException {
    SolrLocator locator = new SolrLocator(createMorphlineContext());
    locator.setSolrHomeDir(testSolrHome + "/collection1");
    File morphlineFile;
    if (new File(file).isAbsolute()) {
      morphlineFile = new File(file + ".conf");
    } else {
      morphlineFile = new File(RESOURCES_DIR + "/" + file + ".conf");
    }
    Config config = new Compiler().parse(morphlineFile, locator.toConfig("SOLR_LOCATOR"));
    config = config.getConfigList("morphlines").get(0);
    return config;
  }
  
  protected void startSession() {
    Notifications.notifyStartSession(morphline);
  }

  protected void testDocumentContent(HashMap<String, ExpectedResult> expectedResultMap)
  throws Exception {
    QueryResponse rsp = solrServer.query(new SolrQuery("*:*").setRows(Integer.MAX_VALUE));
    // Check that every expected field/values shows up in the actual query
    for (Entry<String, ExpectedResult> current : expectedResultMap.entrySet()) {
      String field = current.getKey();
      for (String expectedFieldValue : current.getValue().getFieldValues()) {
        ExpectedResult.CompareType compareType = current.getValue().getCompareType();
        boolean foundField = false;

        for (SolrDocument doc : rsp.getResults()) {
          Collection<Object> actualFieldValues = doc.getFieldValues(field);
          if (compareType == ExpectedResult.CompareType.equals) {
            if (actualFieldValues != null && actualFieldValues.contains(expectedFieldValue)) {
              foundField = true;
              break;
            }
          }
          else {
            for (Iterator<Object> it = actualFieldValues.iterator(); it.hasNext(); ) {
              String actualValue = it.next().toString();  // test only supports string comparison
              if (actualFieldValues != null && actualValue.contains(expectedFieldValue)) {
                foundField = true;
                break;
              }
            }
          }
        }
        assert(foundField); // didn't find expected field/value in query
      }
    }
  }

  /**
   * Representation of the expected output of a SolrQuery.
   */
  protected static class ExpectedResult {
    private HashSet<String> fieldValues;
    public enum CompareType {
      equals,    // Compare with equals, i.e. actual.equals(expected)
      contains;  // Compare with contains, i.e. actual.contains(expected)
    }
    private CompareType compareType;

    public ExpectedResult(HashSet<String> fieldValues, CompareType compareType) {
      this.fieldValues = fieldValues;
      this.compareType = compareType;
    }
    public HashSet<String> getFieldValues() { return fieldValues; }
    public CompareType getCompareType() { return compareType; }
  }
  
  public static void setupMorphline(String tempDir, String file, boolean replaceSolrLocator) throws IOException {
    String morphlineText = FileUtils.readFileToString(new File(RESOURCES_DIR + "/" + file + ".conf"), "UTF-8");
    morphlineText = morphlineText.replaceAll("RESOURCES_DIR", new File(tempDir).getAbsolutePath());
    if (replaceSolrLocator) {
      morphlineText = morphlineText.replaceAll("\\$\\{SOLR_LOCATOR\\}",
          "{ collection : collection1 }");
    }
    new File(tempDir + "/" + file + ".conf").getParentFile().mkdirs();
    FileUtils.writeStringToFile(new File(tempDir + "/" + file + ".conf"), morphlineText, "UTF-8");
  }
}
