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


import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.LuceneTestCase;

public class TestCrash extends LuceneTestCase {

  private IndexWriter initIndex(Random random, boolean initialCommit) throws IOException {
    return initIndex(random, newMockDirectory(random, NoLockFactory.INSTANCE), initialCommit, true);
  }

  private IndexWriter initIndex(Random random, MockDirectoryWrapper dir, boolean initialCommit, boolean commitOnClose) throws IOException {
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random))
        .setMaxBufferedDocs(10).setMergeScheduler(new ConcurrentMergeScheduler()).setCommitOnClose(commitOnClose));
    ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();
    if (initialCommit) {
      writer.commit();
    }
    
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    doc.add(newTextField("id", "0", Field.Store.NO));
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

    // We create leftover files because merging could be
    // running when we crash:
    dir.setAssertNoUnrefencedFilesOnClose(false);

    crash(writer);

    IndexReader reader = DirectoryReader.open(dir);
    assertTrue(reader.numDocs() < 157);
    reader.close();

    // Make a new dir, copying from the crashed dir, and
    // open IW on it, to confirm IW "recovers" after a
    // crash:
    Directory dir2 = newDirectory(dir);
    dir.close();

    new RandomIndexWriter(random(), dir2).close();
    dir2.close();
  }

  public void testWriterAfterCrash() throws IOException {
    // This test relies on being able to open a reader before any commit
    // happened, so we must create an initial commit just to allow that, but
    // before any documents were added.
    if (VERBOSE) {
      System.out.println("TEST: initIndex");
    }
    IndexWriter writer = initIndex(random(), true);
    if (VERBOSE) {
      System.out.println("TEST: done initIndex");
    }
    MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();

    // We create leftover files because merging could be
    // running / store files could be open when we crash:
    dir.setAssertNoUnrefencedFilesOnClose(false);

    dir.setPreventDoubleWrite(false);
    if (VERBOSE) {
      System.out.println("TEST: now crash");
    }
    crash(writer);
    writer = initIndex(random(), dir, false, true);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    assertTrue(reader.numDocs() < 314);
    reader.close();

    // Make a new dir, copying from the crashed dir, and
    // open IW on it, to confirm IW "recovers" after a
    // crash:
    Directory dir2 = newDirectory(dir);
    dir.close();

    new RandomIndexWriter(random(), dir2).close();
    dir2.close();
  }

  public void testCrashAfterReopen() throws IOException {
    IndexWriter writer = initIndex(random(), false);
    MockDirectoryWrapper dir = (MockDirectoryWrapper) writer.getDirectory();

    // We create leftover files because merging could be
    // running when we crash:
    dir.setAssertNoUnrefencedFilesOnClose(false);

    writer.close();
    writer = initIndex(random(), dir, false, true);
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

    IndexReader reader = DirectoryReader.open(dir);
    assertTrue(reader.numDocs() >= 157);
    reader.close();

    // Make a new dir, copying from the crashed dir, and
    // open IW on it, to confirm IW "recovers" after a
    // crash:
    Directory dir2 = newDirectory(dir);
    dir.close();

    new RandomIndexWriter(random(), dir2).close();
    dir2.close();
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

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(157, reader.numDocs());
    reader.close();
    dir.close();
  }

  public void testCrashAfterCloseNoWait() throws IOException {
    Random random = random();
    MockDirectoryWrapper dir = newMockDirectory(random, NoLockFactory.INSTANCE);
    IndexWriter writer = initIndex(random, dir, false, false);

    try {
      writer.commit();
    } finally {
      writer.close();
    }

    dir.crash();

    /*
    String[] l = dir.list();
    Arrays.sort(l);
    for(int i=0;i<l.length;i++)
      System.out.println("file " + i + " = " + l[i] + " " + dir.fileLength(l[i]) + " bytes");
    */
    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(157, reader.numDocs());
    reader.close();
    dir.close();
  }
}
