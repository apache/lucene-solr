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
package org.apache.lucene.index;


import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.document.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.junit.Test;

public class TestRollingUpdates extends LuceneTestCase {

  // Just updates the same set of N docs over and over, to
  // stress out deletions

  @Test
  public void testRollingUpdates() throws Exception {
    Random random = new Random(random().nextLong());
    final BaseDirectoryWrapper dir = newDirectory();
    
    final LineFileDocs docs = new LineFileDocs(random);

    //provider.register(new MemoryCodec());
    if (random().nextBoolean()) {
      Codec.setDefault(TestUtil.alwaysPostingsFormat(new MemoryPostingsFormat(random().nextBoolean(), random.nextFloat())));
    }

    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));

    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(analyzer));
    final int SIZE = atLeast(20);
    int id = 0;
    IndexReader r = null;
    IndexSearcher s = null;
    final int numUpdates = (int) (SIZE * (2+(TEST_NIGHTLY ? 200*random().nextDouble() : 5*random().nextDouble())));
    if (VERBOSE) {
      System.out.println("TEST: numUpdates=" + numUpdates);
    }
    int updateCount = 0;
    // TODO: sometimes update ids not in order...
    for(int docIter=0;docIter<numUpdates;docIter++) {
      final Document doc = docs.nextDoc();
      final String myID = Integer.toString(id);
      if (id == SIZE-1) {
        id = 0;
      } else {
        id++;
      }
      if (VERBOSE) {
        System.out.println("  docIter=" + docIter + " id=" + id);
      }
      ((Field) doc.getField("docid")).setStringValue(myID);

      Term idTerm = new Term("docid", myID);

      final boolean doUpdate;
      if (s != null && updateCount < SIZE) {
        TopDocs hits = s.search(new TermQuery(idTerm), 1);
        assertEquals(1, hits.totalHits);
        doUpdate = w.tryDeleteDocument(r, hits.scoreDocs[0].doc) == -1;
        if (VERBOSE) {
          if (doUpdate) {
            System.out.println("  tryDeleteDocument failed");
          } else {
            System.out.println("  tryDeleteDocument succeeded");
          }
        }
      } else {
        doUpdate = true;
        if (VERBOSE) {
          System.out.println("  no searcher: doUpdate=true");
        }
      }

      updateCount++;

      if (doUpdate) {
        if (random().nextBoolean()) {
          w.updateDocument(idTerm, doc);
        } else {
          // It's OK to not be atomic for this test (no separate thread reopening readers):
          w.deleteDocuments(new TermQuery(idTerm));
          w.addDocument(doc);
        }
      } else {
        w.addDocument(doc);
      }

      if (docIter >= SIZE && random().nextInt(50) == 17) {
        if (r != null) {
          r.close();
        }

        final boolean applyDeletions = random().nextBoolean();

        if (VERBOSE) {
          System.out.println("TEST: reopen applyDeletions=" + applyDeletions);
        }

        r = w.getReader(applyDeletions, false);
        if (applyDeletions) {
          s = newSearcher(r);
        } else {
          s = null;
        }
        assertTrue("applyDeletions=" + applyDeletions + " r.numDocs()=" + r.numDocs() + " vs SIZE=" + SIZE, !applyDeletions || r.numDocs() == SIZE);
        updateCount = 0;
      }
    }

    if (r != null) {
      r.close();
    }

    w.commit();
    assertEquals(SIZE, w.numDocs());

    w.close();

    TestIndexWriter.assertNoUnreferencedFiles(dir, "leftover files after rolling updates");

    docs.close();
    
    // LUCENE-4455:
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    long totalBytes = 0;
    for(SegmentCommitInfo sipc : infos) {
      totalBytes += sipc.sizeInBytes();
    }
    long totalBytes2 = 0;
    
    for(String fileName : dir.listAll()) {
      if (IndexFileNames.CODEC_FILE_PATTERN.matcher(fileName).matches()) {
        totalBytes2 += dir.fileLength(fileName);
      }
    }
    assertEquals(totalBytes2, totalBytes);
    dir.close();
  }
  
  
  public void testUpdateSameDoc() throws Exception {
    final Directory dir = newDirectory();

    final LineFileDocs docs = new LineFileDocs(random());
    for (int r = 0; r < 3; r++) {
      final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                   .setMaxBufferedDocs(2));
      final int numUpdates = atLeast(20);
      int numThreads = TestUtil.nextInt(random(), 2, 6);
      IndexingThread[] threads = new IndexingThread[numThreads];
      for (int i = 0; i < numThreads; i++) {
        threads[i] = new IndexingThread(docs, w, numUpdates);
        threads[i].start();
      }

      for (int i = 0; i < numThreads; i++) {
        threads[i].join();
      }

      w.close();
    }

    IndexReader open = DirectoryReader.open(dir);
    assertEquals(1, open.numDocs());
    open.close();
    docs.close();
    dir.close();
  }
  
  static class IndexingThread extends Thread {
    final LineFileDocs docs;
    final IndexWriter writer;
    final int num;
    
    public IndexingThread(LineFileDocs docs, IndexWriter writer, int num) {
      super();
      this.docs = docs;
      this.writer = writer;
      this.num = num;
    }

    @Override
    public void run() {
      try {
        DirectoryReader open = null;
        for (int i = 0; i < num; i++) {
          Document doc = new Document();// docs.nextDoc();
          BytesRef br = new BytesRef("test");
          doc.add(newStringField("id", br, Field.Store.NO));
          writer.updateDocument(new Term("id", br), doc);
          if (random().nextInt(3) == 0) {
            if (open == null) {
              open = DirectoryReader.open(writer);
            }
            DirectoryReader reader = DirectoryReader.openIfChanged(open);
            if (reader != null) {
              open.close();
              open = reader;
            }
            assertEquals("iter: " + i + " numDocs: "+ open.numDocs() + " del: " + open.numDeletedDocs() + " max: " + open.maxDoc(), 1, open.numDocs());
          }
        }
        if (open != null) {
          open.close();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
