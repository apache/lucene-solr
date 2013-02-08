package org.apache.lucene.facet.util;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIDsIterator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Test;

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

public class TestScoredDocIDsUtils extends FacetTestCase {

  @Test
  public void testComplementIterator() throws Exception {
    final int n = atLeast(10000);
    final FixedBitSet bits = new FixedBitSet(n);
    Random random = random();
    for (int i = 0; i < n; i++) {
      int idx = random.nextInt(n);
      bits.flip(idx, idx + 1);
    }
    
    FixedBitSet verify = new FixedBitSet(bits);

    ScoredDocIDs scoredDocIDs = ScoredDocIdsUtils.createScoredDocIds(bits, n); 

    Directory dir = newDirectory();
    IndexReader reader = createReaderWithNDocs(random, n, dir);
    try { 
      assertEquals(n - verify.cardinality(), ScoredDocIdsUtils.getComplementSet(scoredDocIDs, reader).size());
    } finally {
      reader.close();
      dir.close();
    }
  }

  @Test
  public void testAllDocs() throws Exception {
    int maxDoc = 3;
    Directory dir = newDirectory();
    IndexReader reader = createReaderWithNDocs(random(), maxDoc, dir);
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
    
    private final static Field deletionMark = new StringField(field, delTxt, Field.Store.NO);
    private final static Field alphaContent = new StringField(field, alphaTxt, Field.Store.NO);
    
    public DocumentFactory(int totalNumDocs) {
    }
    
    public boolean markedDeleted(int docNum) {
      return false;
    }

    public Document getDoc(int docNum) {
      Document doc = new Document();
      if (markedDeleted(docNum)) {
        doc.add(deletionMark);
        // Add a special field for docs that are marked for deletion. Later we
        // assert that those docs are not returned by all-scored-doc-IDs.
        FieldType ft = new FieldType();
        ft.setStored(true);
        doc.add(new Field("del", Integer.toString(docNum), ft));
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
    RandomIndexWriter writer = new RandomIndexWriter(random, dir,
        newIndexWriterConfig(random, TEST_VERSION_CURRENT,
            new MockAnalyzer(random, MockTokenizer.KEYWORD, false)));
    for (int docNum = 0; docNum < nDocs; docNum++) {
      writer.addDocument(docFactory.getDoc(docNum));
    }
    // Delete documents marked for deletion
    writer.deleteDocuments(new Term(DocumentFactory.field, DocumentFactory.delTxt));
    writer.close();

    // Open a fresh read-only reader with the deletions in place
    return DirectoryReader.open(dir);
  }
}
