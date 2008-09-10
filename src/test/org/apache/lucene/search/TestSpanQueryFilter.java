package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class TestSpanQueryFilter extends LuceneTestCase {


  public TestSpanQueryFilter(String s) {
    super(s);
  }

  public void testFilterWorks() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true, 
                                         IndexWriter.MaxFieldLength.LIMITED);
    for (int i = 0; i < 500; i++) {
      Document document = new Document();
      document.add(new Field("field", English.intToEnglish(i) + " equals " + English.intToEnglish(i),
              Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(document);
    }
    writer.close();

    IndexReader reader = IndexReader.open(dir);

    SpanTermQuery query = new SpanTermQuery(new Term("field", English.intToEnglish(10).trim()));
    SpanQueryFilter filter = new SpanQueryFilter(query);
    SpanFilterResult result = filter.bitSpans(reader);
    DocIdSet docIdSet = result.getDocIdSet();
    assertTrue("docIdSet is null and it shouldn't be", docIdSet != null);
    assertContainsDocId("docIdSet doesn't contain docId 10", docIdSet, 10);
    List spans = result.getPositions();
    assertTrue("spans is null and it shouldn't be", spans != null);
    int size = getDocIdSetSize(docIdSet);
    assertTrue("spans Size: " + spans.size() + " is not: " + size, spans.size() == size);
    for (Iterator iterator = spans.iterator(); iterator.hasNext();) {
       SpanFilterResult.PositionInfo info = (SpanFilterResult.PositionInfo) iterator.next();
      assertTrue("info is null and it shouldn't be", info != null);
      //The doc should indicate the bit is on
      assertContainsDocId("docIdSet doesn't contain docId " + info.getDoc(), docIdSet, info.getDoc());
      //There should be two positions in each
      assertTrue("info.getPositions() Size: " + info.getPositions().size() + " is not: " + 2, info.getPositions().size() == 2);
    }
    reader.close();
  }
  
  int getDocIdSetSize(DocIdSet docIdSet) throws Exception {
    int size = 0;
    DocIdSetIterator it = docIdSet.iterator();
    while (it.next()) {
      size++;
    }
    return size;
  }
  
  public void assertContainsDocId(String msg, DocIdSet docIdSet, int docId) throws Exception {
    DocIdSetIterator it = docIdSet.iterator();
    assertTrue(msg, it.skipTo(docId));
    assertTrue(msg, it.doc() == docId);
  }
}
