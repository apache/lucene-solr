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
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;

public class TestCrash extends LuceneTestCase {

  private IndexWriter initIndex(Random random, boolean initialCommit) throws IOException {
    return initIndex(random, newDirectory(random), initialCommit);
  }

  private IndexWriter initIndex(Random random, MockDirectoryWrapper dir, boolean initialCommit) throws IOException {
    dir.setLockFactory(NoLockFactory.getNoLockFactory());

    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMaxBufferedDocs(10).setMergeScheduler(new ConcurrentMergeScheduler()));
    ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();
    if (initialCommit) {
      writer.commit();
    }
    
    Document doc = new Document();
    doc.add(newField("content", "aaa", TextField.TYPE_UNSTORED));
    doc.add(newField("id", "0", TextField.TYPE_UNSTORED));
    for(int i=0;i<157;i++)
      writer.addDocument(doc);

    return writer;
  }

  private void crash(final IndexWriter writer) throws IOException {
    final MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();
    ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler();
    cms.sync();
    dir.crash();
    cms.sync();
    dir.clearCrash();
  }

  public void testCrashWhileIndexing() throws IOException {
    // This test relies on being able to open a reader before any commit
    // happened, so we must create an initial commit just to allow that, but
    // before any documents were added.
    IndexWriter writer = initIndex(random(), true);
    MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();
    crash(writer);
    IndexReader reader = IndexReader.open(dir);
    assertTrue(reader.numDocs() < 157);
    reader.close();
    dir.close();
  }

  public void testWriterAfterCrash() throws IOException {
    // This test relies on being able to open a reader before any commit
    // happened, so we must create an initial commit just to allow that, but
    // before any documents were added.
    IndexWriter writer = initIndex(random(), true);
    MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();
    dir.setPreventDoubleWrite(false);
    crash(writer);
    writer = initIndex(random(), dir, false);
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    assertTrue(reader.numDocs() < 314);
    reader.close();
    dir.close();
  }

  public void testCrashAfterReopen() throws IOException {
    IndexWriter writer = initIndex(random(), false);
    MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();
    writer.close();
    writer = initIndex(random(), dir, false);
    assertEquals(314, writer.maxDoc());
    crash(writer);

    /*
    System.out.println("\n\nTEST: open reader");
    String[] l = dir.list();
    Arrays.sort(l);
    for(int i=0;i<l.length;i++)
      System.out.println("file " + i + " = " + l[i] + " " +
    dir.fileLength(l[i]) + " bytes");
    */

    IndexReader reader = IndexReader.open(dir);
    assertTrue(reader.numDocs() >= 157);
    reader.close();
    dir.close();
  }

  public void testCrashAfterClose() throws IOException {
    
    IndexWriter writer = initIndex(random(), false);
    MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();

    writer.close();
    dir.crash();

    /*
    String[] l = dir.list();
    Arrays.sort(l);
    for(int i=0;i<l.length;i++)
      System.out.println("file " + i + " = " + l[i] + " " + dir.fileLength(l[i]) + " bytes");
    */

    IndexReader reader = IndexReader.open(dir);
    assertEquals(157, reader.numDocs());
    reader.close();
    dir.close();
  }

  public void testCrashAfterCloseNoWait() throws IOException {
    
    IndexWriter writer = initIndex(random(), false);
    MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();

    writer.close(false);

    dir.crash();

    /*
    String[] l = dir.list();
    Arrays.sort(l);
    for(int i=0;i<l.length;i++)
      System.out.println("file " + i + " = " + l[i] + " " + dir.fileLength(l[i]) + " bytes");
    */
    IndexReader reader = IndexReader.open(dir);
    assertEquals(157, reader.numDocs());
    reader.close();
    dir.close();
  }
}
