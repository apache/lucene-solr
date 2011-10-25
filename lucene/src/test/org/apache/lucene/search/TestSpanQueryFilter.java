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

import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;

public class TestSpanQueryFilter extends LuceneTestCase {

  public void testFilterWorks() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < 500; i++) {
      Document document = new Document();
      document.add(newField("field", English.intToEnglish(i) + " equals " + English.intToEnglish(i),
              TextField.TYPE_UNSTORED));
      writer.addDocument(document);
    }
    final int number = 10;
    IndexReader reader = writer.getReader(); 
    writer.close();
    AtomicReaderContext[] leaves = ReaderUtil.leaves(reader.getTopReaderContext());
    int subIndex = ReaderUtil.subIndex(number, leaves); // find the reader with this document in it
    SpanTermQuery query = new SpanTermQuery(new Term("field", English.intToEnglish(number).trim()));
    SpanQueryFilter filter = new SpanQueryFilter(query);
    SpanFilterResult result = filter.bitSpans(leaves[subIndex], leaves[subIndex].reader.getLiveDocs());
    DocIdSet docIdSet = result.getDocIdSet();
    assertTrue("docIdSet is null and it shouldn't be", docIdSet != null);
    assertContainsDocId("docIdSet doesn't contain docId 10", docIdSet, number - leaves[subIndex].docBase);
    List<SpanFilterResult.PositionInfo> spans = result.getPositions();
    assertTrue("spans is null and it shouldn't be", spans != null);
    int size = getDocIdSetSize(docIdSet);
    assertTrue("spans Size: " + spans.size() + " is not: " + size, spans.size() == size);
    for (final SpanFilterResult.PositionInfo info: spans) {
      assertTrue("info is null and it shouldn't be", info != null);
      //The doc should indicate the bit is on
      assertContainsDocId("docIdSet doesn't contain docId " + info.getDoc(), docIdSet, info.getDoc());
      //There should be two positions in each
      assertTrue("info.getPositions() Size: " + info.getPositions().size() + " is not: " + 2, info.getPositions().size() == 2);
    }
    
    reader.close();
    dir.close();
  }
  
  int getDocIdSetSize(DocIdSet docIdSet) throws Exception {
    int size = 0;
    DocIdSetIterator it = docIdSet.iterator();
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      size++;
    }
    return size;
  }
  
  public void assertContainsDocId(String msg, DocIdSet docIdSet, int docId) throws Exception {
    DocIdSetIterator it = docIdSet.iterator();
    assertTrue(msg, it.advance(docId) != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(msg, it.docID() == docId);
  }
}
