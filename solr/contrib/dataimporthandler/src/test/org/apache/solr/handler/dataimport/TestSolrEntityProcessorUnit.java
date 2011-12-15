/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.dataimport;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test of SolrEntityProcessor. A very basic test outside of the DIH.
 */
public class TestSolrEntityProcessorUnit extends AbstractDataImportHandlerTestCase {
  
  private static final Logger LOG = LoggerFactory.getLogger(TestSolrEntityProcessorUnit.class);
  private static final String ID = "id";
  
  public void testQuery() {
    String[][][] docs = generateDocs(2);
    
    MockSolrEntityProcessor processor = new MockSolrEntityProcessor(docs);
    
    assertExpectedDocs(docs, processor);
    assertEquals(1, processor.getQueryCount());
  }
  
  public void testNumDocsGreaterThanRows() {
    String[][][] docs = generateDocs(44);
    
    MockSolrEntityProcessor processor = new MockSolrEntityProcessor(docs, 10);
    assertExpectedDocs(docs, processor);
    assertEquals(5, processor.getQueryCount());
  }
  
  public void testMultiValuedFields() {
    String[][][] docs = new String[1][2][2];
    String[][] doc = new String[][] { {"id", "1"}, {"multi", "multi1"},
        {"multi", "multi2"}, {"multi", "multi3"}};
    docs[0] = doc;
    
    MockSolrEntityProcessor processor = new MockSolrEntityProcessor(docs);
    
    Map<String,Object> next = processor.nextRow();
    assertNotNull(next);
    assertEquals(doc[0][1], next.get(doc[0][0]));
    
    String[] multiValued = {"multi1", "multi2", "multi3"};
    assertEquals(Arrays.asList(multiValued), next.get(doc[1][0]));
    assertEquals(1, processor.getQueryCount());
    assertNull(processor.nextRow());
    
  }
  
  public void testMultiThread() {
    int numThreads = 5;
    int numDocs = 40;
    String[][][] docs = generateDocs(numDocs);
    final MockSolrEntityProcessor entityProcessor = new MockSolrEntityProcessor(docs, 25);
    
    final Map<String,Map<String,Object>> rowList = new HashMap<String,Map<String,Object>>();
    final CountDownLatch latch = new CountDownLatch(numThreads);
    for (int i = 0; i < numThreads; i++) {
      Runnable runnable = new Runnable() {
        public void run() {
          try {
            while (true) {
              Map<String,Object> row;
              synchronized (entityProcessor) {
                row = entityProcessor.nextRow();
              }
              if (row == null) {
                break;
              }
              rowList.put(row.get(ID).toString(), row);
            }
          } finally {
            latch.countDown();
          }
        }
      };
      
      new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>()).execute(runnable);
    }
    
    try {
      latch.await();
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
    
    assertEquals(numDocs, rowList.size());
    
    for (String[][] expectedDoc : docs) {
      Map<String,Object> row = rowList.get(expectedDoc[0][1]);
      assertNotNull(row);
      int i = 0;
      for (Entry<String,Object> entry : row.entrySet()) {
        assertEquals(expectedDoc[i][0], entry.getKey());
        assertEquals(expectedDoc[i][1], entry.getValue());
        i++;
      }
      rowList.remove(expectedDoc[0][1]);
    }
    
    assertEquals(0, rowList.size());
    
  }
  
  private static String[][][] generateDocs(int numDocs) {
    String[][][] docs = new String[numDocs][2][2];
    for (int i = 0; i < numDocs; i++) {
      docs[i] = new String[][] { {"id", Integer.toString(i+1)},
          {"description", "Description" + Integer.toString(i+1)}};
    }
    return docs;
  }
  
  private static void assertExpectedDocs(String[][][] expectedDocs, SolrEntityProcessor processor) {
    for (String[][] expectedDoc : expectedDocs) {
      Map<String, Object> next = processor.nextRow();
      assertNotNull(next);
      assertEquals(expectedDoc[0][1], next.get(expectedDoc[0][0]));
      assertEquals(expectedDoc[1][1], next.get(expectedDoc[1][0]));
    }
    assertNull(processor.nextRow());
  }
}
