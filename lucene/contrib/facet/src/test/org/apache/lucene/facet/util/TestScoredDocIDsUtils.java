package org.apache.lucene.facet.util;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetDISI;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIDsIterator;
import org.apache.lucene.facet.search.ScoredDocIdCollector;

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

public class TestScoredDocIDsUtils extends LuceneTestCase {

  @Test
  public void testComplementIterator() throws Exception {
    final int n = atLeast(10000);
    final OpenBitSet bits = new OpenBitSet(n);
    for (int i = 0; i < 5 * n; i++) {
      bits.flip(random.nextInt(n));
    }
    
    OpenBitSet verify = new OpenBitSet(n);
    verify.or(bits);

    ScoredDocIDs scoredDocIDs = ScoredDocIdsUtils.createScoredDocIds(bits, n); 

    Directory dir = newDirectory();
    IndexReader reader = createReaderWithNDocs(random, n, dir);
    try { 
      assertEquals(n - verify.cardinality(), ScoredDocIdsUtils.getComplementSet(scoredDocIDs, 
        reader).size());
    } finally {
      reader.close();
      dir.close();
    }
  }

  @Test
  public void testAllDocs() throws Exception {
    int maxDoc = 3;
    Directory dir = newDirectory();
    IndexReader reader = createReaderWithNDocs(random, maxDoc, dir);
    try {
      ScoredDocIDs all = ScoredDocIdsUtils.createAllDocsScoredDocIDs(reader);
      assertEquals("invalid size", maxDoc, all.size());
      ScoredDocIDsIterator iter = all.iterator();
      int doc = 0;
      while (iter.next()) {
        assertEquals("invalid doc ID: " + iter.getDocID(), doc++, iter.getDocID());
        assertEquals("invalid score: " + iter.getScore(), ScoredDocIDsIterator.DEFAULT_SCORE, iter.getScore(), 0.0f);
      }
      assertEquals("invalid maxDoc: " + doc, maxDoc, doc);
      
      DocIdSet docIDs = all.getDocIDs();
      assertTrue("should be cacheable", docIDs.isCacheable());
      DocIdSetIterator docIDsIter = docIDs.iterator();
      assertEquals("nextDoc() hasn't been called yet", -1, docIDsIter.docID());
      assertEquals(0, docIDsIter.nextDoc());
      assertEquals(1, docIDsIter.advance(1));
      // if advance is smaller than current doc, advance to cur+1.
      assertEquals(2, docIDsIter.advance(0));
    } finally {
      reader.close();
      dir.close();
    }
  }
  
  @Test
  public void testWithDeletions() throws Exception {
    int N_DOCS = 100;

    DocumentFactory docFactory = new DocumentFactory(N_DOCS) {
      @Override
      public boolean markedDeleted(int docNum) {
        return (docNum % 3 == 0 ||        // every 3rd documents, including first 
            docNum == numDocs - 1 ||     // last document
            docNum == numDocs / 2 ||     // 3 consecutive documents in the middle
            docNum == 1 + numDocs / 2 ||
            docNum == 2 + numDocs / 2);
      }
      
      // every 6th document (starting from the 2nd) would contain 'alpha'
      @Override
      public boolean haveAlpha(int docNum) {
        return (docNum % 6 == 1);
      }
    };
    
    Directory dir = newDirectory();
    IndexReader reader = createReaderWithNDocs(random, N_DOCS, docFactory, dir);
    try {
      int numErasedDocs = reader.numDeletedDocs();

      ScoredDocIDs allDocs = ScoredDocIdsUtils.createAllDocsScoredDocIDs(reader);
      ScoredDocIDsIterator it = allDocs.iterator();
      int numIteratedDocs = 0;
      while (it.next()) {
        numIteratedDocs++;
        int docNum = it.getDocID();
        assertFalse(
            "Deleted docs must not appear in the allDocsScoredDocIds set: " + docNum,
            docFactory.markedDeleted(docNum));
      }

      assertEquals("Wrong number of (live) documents", allDocs.size(), numIteratedDocs);
      
      assertEquals("Wrong number of (live) documents", N_DOCS
          - numErasedDocs, numIteratedDocs);

      // Get all 'alpha' documents
      ScoredDocIdCollector collector = ScoredDocIdCollector.create(reader.maxDoc(), false);
      Query q = new TermQuery(new Term(DocumentFactory.field, DocumentFactory.alphaTxt));
      IndexSearcher searcher = newSearcher(reader);
      searcher.search(q, collector);
      searcher.close();

      ScoredDocIDs scoredDocIds = collector.getScoredDocIDs();
      OpenBitSet resultSet = new OpenBitSetDISI(scoredDocIds.getDocIDs().iterator(), reader.maxDoc());
      
      // Getting the complement set of the query result
      ScoredDocIDs complementSet = ScoredDocIdsUtils.getComplementSet(scoredDocIds, reader);

      assertEquals("Number of documents in complement set mismatch",
          reader.numDocs() - scoredDocIds.size(), complementSet.size());

      // now make sure the documents in the complement set are not deleted
      // and not in the original result set
      ScoredDocIDsIterator compIterator = complementSet.iterator();
      while (compIterator.next()) {
        int docNum = compIterator.getDocID();
        assertFalse(
            "Complement-Set must not contain deleted documents (doc="+docNum+")",
            reader.isDeleted(docNum));
        assertFalse(
            "Complement-Set must not contain deleted documents (doc="+docNum+")",
            docFactory.markedDeleted(docNum));
        assertFalse(
            "Complement-Set must not contain docs from the original set (doc="+docNum+")",
            resultSet.fastGet(docNum));
      }
    } finally {
      reader.close();
      dir.close();
    }
  }
  
  /**
   * Creates an index with n documents, this method is meant for testing purposes ONLY
   */
  static IndexReader createReaderWithNDocs(Random random, int nDocs, Directory directory) throws IOException {
    return createReaderWithNDocs(random, nDocs, new DocumentFactory(nDocs), directory);
  }

  private static class DocumentFactory {
    protected final static String field = "content";
    protected final static String delTxt = "delete";
    protected final static String alphaTxt = "alpha";
    
    private final static Field deletionMark = new Field(field, delTxt, Store.NO, Index.NOT_ANALYZED_NO_NORMS);
    private final static Field alphaContent = new Field(field, alphaTxt, Store.NO, Index.NOT_ANALYZED_NO_NORMS);
    
    protected final int numDocs;
    
    public DocumentFactory(int totalNumDocs) {
      this.numDocs = totalNumDocs;
    }
    
    public boolean markedDeleted(int docNum) {
      return false;
    }

    public Document getDoc(int docNum) {
      Document doc = new Document();
      if (markedDeleted(docNum)) {
        doc.add(deletionMark);
      }

      if (haveAlpha(docNum)) {
        doc.add(alphaContent);
      }
      return doc;
    }

    public boolean haveAlpha(int docNum) {
      return false;
    }
  }

  static IndexReader createReaderWithNDocs(Random random, int nDocs, DocumentFactory docFactory, Directory dir) throws IOException {
    // Create the index - force log-merge policy since we rely on docs order.
    RandomIndexWriter writer = new RandomIndexWriter(random, dir,
        newIndexWriterConfig(random, TEST_VERSION_CURRENT,
            new MockAnalyzer(random, MockTokenizer.KEYWORD, false))
            .setMergePolicy(newLogMergePolicy()));
    for (int docNum = 0; docNum < nDocs; docNum++) {
      writer.addDocument(docFactory.getDoc(docNum));
    }
    writer.close();

    // Delete documents marked for deletion
    IndexReader reader = IndexReader.open(dir, false);
    reader.deleteDocuments(new Term(DocumentFactory.field, DocumentFactory.delTxt));
    reader.close();

    // Open a fresh read-only reader with the deletions in place
    return IndexReader.open(dir, true);
  }
}
