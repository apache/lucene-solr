/*
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

import java.util.*;

/**
 * Unit test of SolrEntityProcessor. A very basic test outside of the DIH.
 */
public class TestSolrEntityProcessorUnit extends AbstractDataImportHandlerTestCase {

  private static final String ID = "id";

  public void testQuery() {
    List<Doc> docs = generateUniqueDocs(2);

    MockSolrEntityProcessor processor = createAndInit(docs);
    try {
      assertExpectedDocs(docs, processor);
      assertEquals(1, processor.getQueryCount());
    } finally {
      processor.destroy();
    }
  }

  private MockSolrEntityProcessor createAndInit(List<Doc> docs) {
    return createAndInit(docs, SolrEntityProcessor.ROWS_DEFAULT);
  }

  public void testNumDocsGreaterThanRows() {
    List<Doc> docs = generateUniqueDocs(44);

    int rowsNum = 10;
    MockSolrEntityProcessor processor = createAndInit(docs, rowsNum);
    try {
      assertExpectedDocs(docs, processor);
      assertEquals(5, processor.getQueryCount());
    } finally {
      processor.destroy();
    }
  }

  private MockSolrEntityProcessor createAndInit(List<Doc> docs, int rowsNum) {
    MockSolrEntityProcessor processor = new MockSolrEntityProcessor(docs, rowsNum);
    HashMap<String,String> entityAttrs = new HashMap<String,String>(){{put(SolrEntityProcessor.SOLR_SERVER,"http://route:66/no");}};
    processor.init(getContext(null, null, null, null, Collections.emptyList(), 
        entityAttrs));
    return processor;
  }

  public void testMultiValuedFields() {
    List<Doc> docs = new ArrayList<>();
    List<FldType> types = new ArrayList<>();
    types.add(new FldType(ID, ONE_ONE, new SVal('A', 'Z', 4, 4)));
    types.add(new FldType("description", new IRange(3, 3), new SVal('a', 'c', 1, 1)));
    Doc testDoc = createDoc(types);
    docs.add(testDoc);

    MockSolrEntityProcessor processor = createAndInit(docs);
    try {
      Map<String, Object> next = processor.nextRow();
      assertNotNull(next);
  
      @SuppressWarnings("unchecked")
      List<Comparable> multiField = (List<Comparable>) next.get("description");
      assertEquals(testDoc.getValues("description").size(), multiField.size());
      assertEquals(testDoc.getValues("description"), multiField);
      assertEquals(1, processor.getQueryCount());
      assertNull(processor.nextRow());
    } finally {
      processor.destroy();
    }
  }

  private List<Doc> generateUniqueDocs(int numDocs) {
    List<FldType> types = new ArrayList<>();
    types.add(new FldType(ID, ONE_ONE, new SVal('A', 'Z', 4, 40)));
    types.add(new FldType("description", new IRange(1, 3), new SVal('a', 'c', 1, 1)));

    Set<Comparable> previousIds = new HashSet<>();
    List<Doc> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      Doc doc = createDoc(types);
      while (previousIds.contains(doc.id)) {
        doc = createDoc(types);
      }
      previousIds.add(doc.id);
      docs.add(doc);
    }
    return docs;
  }

  private static void assertExpectedDocs(List<Doc> expectedDocs, SolrEntityProcessor processor) {
    for (Doc expectedDoc : expectedDocs) {
      Map<String, Object> next = processor.nextRow();
      assertNotNull(next);
      assertEquals(expectedDoc.id, next.get("id"));
      assertEquals(expectedDoc.getValues("description"), next.get("description"));
    }
    assertNull(processor.nextRow());
  }

}
