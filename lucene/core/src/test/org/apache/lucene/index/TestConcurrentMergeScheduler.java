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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

public class TestConcurrentMergeScheduler extends LuceneTestCase {
  
  private class FailOnlyOnFlush extends MockDirectoryWrapper.Failure {
    boolean doFail;
    boolean hitExc;

    @Override
    public void setDoFail() {
      this.doFail = true;
      hitExc = false;
    }
    @Override
    public void clearDoFail() {
      this.doFail = false;
    }

    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (doFail && isTestThread()) {
        boolean isDoFlush = false;
        boolean isClose = false;
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("flush".equals(trace[i].getMethodName())) {
            isDoFlush = true;
          }
          if ("close".equals(trace[i].getMethodName())) {
            isClose = true;
          }
        }
        if (isDoFlush && !isClose && random().nextBoolean()) {
          hitExc = true;
          throw new IOException(Thread.currentThread().getName() + ": now failing during flush");
        }
      }
    }
  }

  // Make sure running BG merges still work fine even when
  // we are hitting exceptions during flushing.
  public void testFlushExceptions() throws IOException {
    MockDirectoryWrapper directory = newDirectory();
    FailOnlyOnFlush failure = new FailOnlyOnFlush();
    directory.failOn(failure);

    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2));
    Document doc = new Document();
    Field idField = newField("id", "", StringField.TYPE_STORED);
    doc.add(idField);
    int extraCount = 0;

    for(int i=0;i<10;i++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + i);
      }

      for(int j=0;j<20;j++) {
        idField.setStringValue(Integer.toString(i*20+j));
        writer.addDocument(doc);
      }

      // must cycle here because sometimes the merge flushes
      // the doc we just added and so there's nothing to
      // flush, and we don't hit the exception
      while(true) {
        writer.addDocument(doc);
        failure.setDoFail();
        try {
          writer.flush(true, true);
          if (failure.hitExc) {
            fail("failed to hit IOException");
          }
          extraCount++;
        } catch (IOException ioe) {
          if (VERBOSE) {
            ioe.printStackTrace(System.out);
          }
          failure.clearDoFail();
          break;
        }
      }
      assertEquals(20*(i+1)+extraCount, writer.numDocs());
    }

    writer.close();
    IndexReader reader = IndexReader.open(directory);
    assertEquals(200+extraCount, reader.numDocs());
    reader.close();
    directory.close();
  }

  // Test that deletes committed after a merge started and
  // before it finishes, are correctly merged back:
  public void testDeleteMerging() throws IOException {
    MockDirectoryWrapper directory = newDirectory();

    LogDocMergePolicy mp = new LogDocMergePolicy();
    // Force degenerate merging so we can get a mix of
    // merging of segments with and without deletes at the
    // start:
    mp.setMinMergeDocs(1000);
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMergePolicy(mp));

    Document doc = new Document();
    Field idField = newField("id", "", StringField.TYPE_STORED);
    doc.add(idField);
    for(int i=0;i<10;i++) {
      if (VERBOSE) {
        System.out.println("\nTEST: cycle");
      }
      for(int j=0;j<100;j++) {
        idField.setStringValue(Integer.toString(i*100+j));
        writer.addDocument(doc);
      }

      int delID = i;
      while(delID < 100*(1+i)) {
        if (VERBOSE) {
          System.out.println("TEST: del " + delID);
        }
        writer.deleteDocuments(new Term("id", ""+delID));
        delID += 10;
      }

      writer.commit();
    }

    writer.close();
    IndexReader reader = IndexReader.open(directory);
    // Verify that we did not lose any deletes...
    assertEquals(450, reader.numDocs());
    reader.close();
    directory.close();
  }

  public void testNoExtraFiles() throws IOException {
    MockDirectoryWrapper directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(2));

    for(int iter=0;iter<7;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }

      for(int j=0;j<21;j++) {
        Document doc = new Document();
        doc.add(newField("content", "a b c", TextField.TYPE_UNSTORED));
        writer.addDocument(doc);
      }
        
      writer.close();
      TestIndexWriter.assertNoUnreferencedFiles(directory, "testNoExtraFiles");

      // Reopen
      writer = new IndexWriter(directory, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random()))
          .setOpenMode(OpenMode.APPEND).setMaxBufferedDocs(2));
    }

    writer.close();

    directory.close();
  }

  public void testNoWaitClose() throws IOException {
    MockDirectoryWrapper directory = newDirectory();
    Document doc = new Document();
    Field idField = newField("id", "", StringField.TYPE_STORED);
    doc.add(idField);

    IndexWriter writer = new IndexWriter(
        directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setMaxBufferedDocs(2).
            setMergePolicy(newLogMergePolicy(100))
    );

    for(int iter=0;iter<10;iter++) {

      for(int j=0;j<201;j++) {
        idField.setStringValue(Integer.toString(iter*201+j));
        writer.addDocument(doc);
      }

      int delID = iter*201;
      for(int j=0;j<20;j++) {
        writer.deleteDocuments(new Term("id", Integer.toString(delID)));
        delID += 5;
      }

      // Force a bunch of merge threads to kick off so we
      // stress out aborting them on close:
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(3);
      writer.addDocument(doc);
      writer.commit();

      writer.close(false);

      IndexReader reader = IndexReader.open(directory);
      assertEquals((1+iter)*182, reader.numDocs());
      reader.close();

      // Reopen
      writer = new IndexWriter(
          directory,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setOpenMode(OpenMode.APPEND).
              setMergePolicy(newLogMergePolicy(100))
      );
    }
    writer.close();

    directory.close();
  }
}
