package org.apache.lucene.index;

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

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestConcurrentMergeScheduler extends LuceneTestCase {
  
  private static final Analyzer ANALYZER = new SimpleAnalyzer();

  private static class FailOnlyOnFlush extends MockRAMDirectory.Failure {
    boolean doFail = false;

    public void setDoFail() {
      this.doFail = true;
    }
    public void clearDoFail() {
      this.doFail = false;
    }

    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("doFlush".equals(trace[i].getMethodName())) {
            //new RuntimeException().printStackTrace(System.out);
            throw new IOException("now failing during flush");
          }
        }
      }
    }
  }

  // Make sure running BG merges still work fine even when
  // we are hitting exceptions during flushing.
  public void testFlushExceptions() throws IOException {

    MockRAMDirectory directory = new MockRAMDirectory();
    FailOnlyOnFlush failure = new FailOnlyOnFlush();
    directory.failOn(failure);

    IndexWriter writer = new IndexWriter(directory, true, ANALYZER, true);
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    writer.setMergeScheduler(cms);
    writer.setMaxBufferedDocs(2);
    Document doc = new Document();
    Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    doc.add(idField);
    for(int i=0;i<10;i++) {
      for(int j=0;j<20;j++) {
        idField.setValue(Integer.toString(i*20+j));
        writer.addDocument(doc);
      }

      writer.addDocument(doc);

      failure.setDoFail();
      try {
        writer.flush();
        fail("failed to hit IOException");
      } catch (IOException ioe) {
        failure.clearDoFail();
      }
    }

    writer.close();
    IndexReader reader = IndexReader.open(directory);
    assertEquals(200, reader.numDocs());
    reader.close();
    directory.close();
  }

  // Test that deletes committed after a merge started and
  // before it finishes, are correctly merged back:
  public void testDeleteMerging() throws IOException {

    RAMDirectory directory = new MockRAMDirectory();

    IndexWriter writer = new IndexWriter(directory, true, ANALYZER, true);
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    writer.setMergeScheduler(cms);

    LogDocMergePolicy mp = new LogDocMergePolicy();
    writer.setMergePolicy(mp);

    // Force degenerate merging so we can get a mix of
    // merging of segments with and without deletes at the
    // start:
    mp.setMinMergeDocs(1000);

    Document doc = new Document();
    Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    doc.add(idField);
    for(int i=0;i<10;i++) {
      for(int j=0;j<100;j++) {
        idField.setValue(Integer.toString(i*100+j));
        writer.addDocument(doc);
      }

      int delID = i;
      while(delID < 100*(1+i)) {
        writer.deleteDocuments(new Term("id", ""+delID));
        delID += 10;
      }

      writer.flush();
    }

    writer.close();
    IndexReader reader = IndexReader.open(directory);
    // Verify that we did not lose any deletes...
    assertEquals(450, reader.numDocs());
    reader.close();
    directory.close();
  }

  public void testNoExtraFiles() throws IOException {

    RAMDirectory directory = new MockRAMDirectory();

    for(int pass=0;pass<2;pass++) {

      boolean autoCommit = pass==0;
      IndexWriter writer = new IndexWriter(directory, autoCommit, ANALYZER, true);

      for(int iter=0;iter<7;iter++) {
        ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
        writer.setMergeScheduler(cms);
        writer.setMaxBufferedDocs(2);

        for(int j=0;j<21;j++) {
          Document doc = new Document();
          doc.add(new Field("content", "a b c", Field.Store.NO, Field.Index.ANALYZED));
          writer.addDocument(doc);
        }
        
        writer.close();
        TestIndexWriter.assertNoUnreferencedFiles(directory, "testNoExtraFiles autoCommit=" + autoCommit);

        // Reopen
        writer = new IndexWriter(directory, autoCommit, ANALYZER, false);
      }

      writer.close();
    }

    directory.close();
  }

  public void testNoWaitClose() throws IOException {
    RAMDirectory directory = new MockRAMDirectory();

    Document doc = new Document();
    Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    doc.add(idField);

    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = pass==0;
      IndexWriter writer = new IndexWriter(directory, autoCommit, ANALYZER, true);

      for(int iter=0;iter<10;iter++) {
        ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
        writer.setMergeScheduler(cms);
        writer.setMaxBufferedDocs(2);
        writer.setMergeFactor(100);

        for(int j=0;j<201;j++) {
          idField.setValue(Integer.toString(iter*201+j));
          writer.addDocument(doc);
        }

        int delID = iter*201;
        for(int j=0;j<20;j++) {
          writer.deleteDocuments(new Term("id", Integer.toString(delID)));
          delID += 5;
        }

        // Force a bunch of merge threads to kick off so we
        // stress out aborting them on close:
        writer.setMergeFactor(3);
        writer.addDocument(doc);
        writer.flush();

        writer.close(false);

        IndexReader reader = IndexReader.open(directory);
        assertEquals((1+iter)*182, reader.numDocs());
        reader.close();

        // Reopen
        writer = new IndexWriter(directory, autoCommit, ANALYZER, false);
      }
      writer.close();
    }

    directory.close();
  }
}
