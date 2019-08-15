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

import java.util.*;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.handler.dataimport.SolrEntityProcessor.SolrDocumentListIterator;
import org.junit.Test;

/**
 * Unit test of SolrEntityProcessor. A very basic test outside of the DIH.
 */
@SuppressWarnings("rawtypes")
public class TestSolrEntityProcessorUnit extends AbstractDataImportHandlerTestCase {

  private static final class NoNextMockProcessor extends SolrEntityProcessor {
    @Override
    protected void nextPage() {
    }
  }

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

  @SuppressWarnings("serial")
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
  @SuppressWarnings("serial")
  @Test (expected = DataImportHandlerException.class)
  public void testNoQuery() {
    SolrEntityProcessor processor = new SolrEntityProcessor();
    
    HashMap<String,String> entityAttrs = new HashMap<String,String>(){{put(SolrEntityProcessor.SOLR_SERVER,"http://route:66/no");}};
    processor.init(getContext(null, null, null, null, Collections.emptyList(), 
        entityAttrs));
    try {
    processor.buildIterator();
    }finally {
      processor.destroy();
    }
  }
  
  @SuppressWarnings("serial")
  public void testPagingQuery() {
    SolrEntityProcessor processor = new NoNextMockProcessor() ;
    
    HashMap<String,String> entityAttrs = new HashMap<String,String>(){{
      put(SolrEntityProcessor.SOLR_SERVER,"http://route:66/no");
      if (random().nextBoolean()) {
        List<String> noCursor = Arrays.asList("","false",CursorMarkParams.CURSOR_MARK_START);//only 'true' not '*'
        Collections.shuffle(noCursor, random());
        put(CursorMarkParams.CURSOR_MARK_PARAM,  noCursor.get(0));
      }}};
    processor.init(getContext(null, null, null, null, Collections.emptyList(), 
        entityAttrs));
    try {
    processor.buildIterator();
    SolrQuery query = new SolrQuery();
    ((SolrDocumentListIterator) processor.rowIterator).passNextPage(query);
    assertEquals("0", query.get(CommonParams.START));
    assertNull( query.get(CursorMarkParams.CURSOR_MARK_PARAM));
    assertNotNull( query.get(CommonParams.TIME_ALLOWED));
    }finally {
      processor.destroy();
    }
  }
  
  @SuppressWarnings("serial")
  public void testCursorQuery() {
    SolrEntityProcessor processor = new NoNextMockProcessor() ;
    
    HashMap<String,String> entityAttrs = new HashMap<String,String>(){{
      put(SolrEntityProcessor.SOLR_SERVER,"http://route:66/no");
      put(CursorMarkParams.CURSOR_MARK_PARAM,"true");
      }};
    processor.init(getContext(null, null, null, null, Collections.emptyList(), 
        entityAttrs));
    try {
    processor.buildIterator();
    SolrQuery query = new SolrQuery();
    ((SolrDocumentListIterator) processor.rowIterator).passNextPage(query);
    assertNull(query.get(CommonParams.START));
    assertEquals(CursorMarkParams.CURSOR_MARK_START, query.get(CursorMarkParams.CURSOR_MARK_PARAM));
    assertNull( query.get(CommonParams.TIME_ALLOWED));
    }finally {
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
