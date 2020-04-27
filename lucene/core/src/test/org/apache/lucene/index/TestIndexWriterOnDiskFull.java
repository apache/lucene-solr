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
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests for IndexWriter when the disk runs out of space
 */
public class TestIndexWriterOnDiskFull extends LuceneTestCase {

  /*
   * Make sure IndexWriter cleans up on hitting a disk
   * full exception in addDocument.
   * TODO: how to do this on windows with FSDirectory?
   */
  public void testAddDocumentOnDiskFull() throws IOException {

    for(int pass=0;pass<2;pass++) {
      if (VERBOSE) {
        System.out.println("TEST: pass=" + pass);
      }
      boolean doAbort = pass == 1;
      long diskFree = TestUtil.nextInt(random(), 100, 300);
      boolean indexExists = false;
      while(true) {
        if (VERBOSE) {
          System.out.println("TEST: cycle: diskFree=" + diskFree);
        }
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new RAMDirectory());
        dir.setMaxSizeInBytes(diskFree);
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        MergeScheduler ms = writer.getConfig().getMergeScheduler();
        if (ms instanceof ConcurrentMergeScheduler) {
          // This test intentionally produces exceptions
          // in the threads that CMS launches; we don't
          // want to pollute test output with these.
          ((ConcurrentMergeScheduler) ms).setSuppressExceptions();
        }

        boolean hitError = false;
        try {
          for(int i=0;i<200;i++) {
            addDoc(writer);
          }
          if (VERBOSE) {
            System.out.println("TEST: done adding docs; now commit");
          }
          writer.commit();
          indexExists = true;
        } catch (IOException e) {
          if (VERBOSE) {
            System.out.println("TEST: exception on addDoc");
            e.printStackTrace(System.out);
          }
          hitError = true;
        }

        if (hitError) {
          if (doAbort) {
            if (VERBOSE) {
              System.out.println("TEST: now rollback");
            }
            writer.rollback();
          } else {
            try {
              if (VERBOSE) {
                System.out.println("TEST: now close");
              }
              writer.close();
            } catch (IOException e) {
              if (VERBOSE) {
                System.out.println("TEST: exception on close; retry w/ no disk space limit");
                e.printStackTrace(System.out);
              }
              dir.setMaxSizeInBytes(0);
              try {
                writer.close();
              } catch (AlreadyClosedException ace) {
                // OK
              }
            }
          }

          //_TestUtil.syncConcurrentMerges(ms);

          if (indexExists) {
            // Make sure reader can open the index:
            DirectoryReader.open(dir).close();
          }
            
          dir.close();
          // Now try again w/ more space:

          diskFree += TEST_NIGHTLY ? TestUtil.nextInt(random(), 400, 600) : TestUtil.nextInt(random(), 3000, 5000);
        } else {
          //_TestUtil.syncConcurrentMerges(writer);
          dir.setMaxSizeInBytes(0);
          writer.close();
          dir.close();
          break;
        }
      }
    }
  }

  // TODO: make @Nightly variant that provokes more disk
  // fulls

  // TODO: have test fail if on any given top
  // iter there was not a single IOE hit

  /*
  Test: make sure when we run out of disk space or hit
  random IOExceptions in any of the addIndexes(*) calls
  that 1) index is not corrupt (searcher can open/search
  it) and 2) transactional semantics are followed:
  either all or none of the incoming documents were in
  fact added.
   */
  public void testAddIndexOnDiskFull() throws IOException {
    // MemoryCodec, since it uses FST, is not necessarily
    // "additive", ie if you add up N small FSTs, then merge
    // them, the merged result can easily be larger than the
    // sum because the merged FST may use array encoding for
    // some arcs (which uses more space):

    final String idFormat = TestUtil.getPostingsFormat("id");
    final String contentFormat = TestUtil.getPostingsFormat("content");

    int START_COUNT = 57;
    int NUM_DIR = TEST_NIGHTLY ? 50 : 5;
    int END_COUNT = START_COUNT + NUM_DIR* (TEST_NIGHTLY ? 25 : 5);
    
    // Build up a bunch of dirs that have indexes which we
    // will then merge together by calling addIndexes(*):
    Directory[] dirs = new Directory[NUM_DIR];
    long inputDiskUsage = 0;
    for(int i=0;i<NUM_DIR;i++) {
      dirs[i] = newDirectory();
      IndexWriter writer = new IndexWriter(dirs[i], newIndexWriterConfig(new MockAnalyzer(random())));
      for(int j=0;j<25;j++) {
        addDocWithIndex(writer, 25*i+j);
      }
      writer.close();
      String[] files = dirs[i].listAll();
      for(int j=0;j<files.length;j++) {
        inputDiskUsage += dirs[i].fileLength(files[j]);
      }
    }
    
    // Now, build a starting index that has START_COUNT docs.  We
    // will then try to addIndexes into a copy of this:
    MockDirectoryWrapper startDir = newMockDirectory();
    IndexWriter writer = new IndexWriter(startDir, newIndexWriterConfig(new MockAnalyzer(random())));
    for(int j=0;j<START_COUNT;j++) {
      addDocWithIndex(writer, j);
    }
    writer.close();
    
    // Make sure starting index seems to be working properly:
    Term searchTerm = new Term("content", "aaa");        
    IndexReader reader = DirectoryReader.open(startDir);
    assertEquals("first docFreq", 57, reader.docFreq(searchTerm));
    
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), 1000).scoreDocs;
    assertEquals("first number of hits", 57, hits.length);
    reader.close();
    
    // Iterate with larger and larger amounts of free
    // disk space.  With little free disk space,
    // addIndexes will certainly run out of space &
    // fail.  Verify that when this happens, index is
    // not corrupt and index in fact has added no
    // documents.  Then, we increase disk space by 2000
    // bytes each iteration.  At some point there is
    // enough free disk space and addIndexes should
    // succeed and index should show all documents were
    // added.
    
    // String[] files = startDir.listAll();
    long diskUsage = startDir.sizeInBytes();
    
    long startDiskUsage = 0;
    String[] files = startDir.listAll();
    for(int i=0;i<files.length;i++) {
      startDiskUsage += startDir.fileLength(files[i]);
    }

    for(int iter=0;iter<3;iter++) {
      
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      
      // Start with 100 bytes more than we are currently using:
      long diskFree = diskUsage+ TestUtil.nextInt(random(), 50, 200);
      
      int method = iter;
      
      boolean success = false;
      boolean done = false;
      
      String methodName;
      if (0 == method) {
        methodName = "addIndexes(Directory[]) + forceMerge(1)";
      } else if (1 == method) {
        methodName = "addIndexes(IndexReader[])";
      } else {
        methodName = "addIndexes(Directory[])";
      }
      
      while(!done) {
        if (VERBOSE) {
          System.out.println("TEST: cycle...");
        }
        
        // Make a new dir that will enforce disk usage:
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(startDir));
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
          .setOpenMode(OpenMode.APPEND)
          .setMergePolicy(newLogMergePolicy(false));
        writer = new IndexWriter(dir, iwc);
        Exception err = null;

        for(int x=0;x<2;x++) {
          MergeScheduler ms = writer.getConfig().getMergeScheduler();
          if (ms instanceof ConcurrentMergeScheduler) {
            // This test intentionally produces exceptions
            // in the threads that CMS launches; we don't
            // want to pollute test output with these.
            if (0 == x) {
              ((ConcurrentMergeScheduler) ms).setSuppressExceptions();
            } else {
              ((ConcurrentMergeScheduler) ms).clearSuppressExceptions();
            }
          }
          
          // Two loops: first time, limit disk space &
          // throw random IOExceptions; second time, no
          // disk space limit:
          
          double rate = 0.05;
          double diskRatio = ((double) diskFree)/diskUsage;
          long thisDiskFree;
          
          String testName = null;
          
          if (0 == x) {
            dir.setRandomIOExceptionRateOnOpen(random().nextDouble()*0.01);
            thisDiskFree = diskFree;
            if (diskRatio >= 2.0) {
              rate /= 2;
            }
            if (diskRatio >= 4.0) {
              rate /= 2;
            }
            if (diskRatio >= 6.0) {
              rate = 0.0;
            }
            if (VERBOSE) {
              testName = "disk full test " + methodName + " with disk full at " + diskFree + " bytes";
            }
          } else {
            dir.setRandomIOExceptionRateOnOpen(0.0);
            thisDiskFree = 0;
            rate = 0.0;
            if (VERBOSE) {
              testName = "disk full test " + methodName + " with unlimited disk space";
            }
          }
          
          if (VERBOSE) {
            System.out.println("\ncycle: " + testName);
          }
          
          dir.setTrackDiskUsage(true);
          dir.setMaxSizeInBytes(thisDiskFree);
          dir.setRandomIOExceptionRate(rate);
          
          try {
            
            if (0 == method) {
              if (VERBOSE) {
                System.out.println("TEST: now addIndexes count=" + dirs.length);
              }
              writer.addIndexes(dirs);
              if (VERBOSE) {
                System.out.println("TEST: now forceMerge");
              }
              writer.forceMerge(1);
            } else if (1 == method) {
              DirectoryReader readers[] = new DirectoryReader[dirs.length];
              for(int i=0;i<dirs.length;i++) {
                readers[i] = DirectoryReader.open(dirs[i]);
              }
              try {
                TestUtil.addIndexesSlowly(writer, readers);
              } finally {
                for(int i=0;i<dirs.length;i++) {
                  readers[i].close();
                }
              }
            } else {
              writer.addIndexes(dirs);
            }
            
            success = true;
            if (VERBOSE) {
              System.out.println("  success!");
            }
            
            if (0 == x) {
              done = true;
            }
            
          } catch (IllegalStateException | IOException e) {
            success = false;
            err = e;
            if (VERBOSE) {
              System.out.println("  hit Exception: " + e);
              e.printStackTrace(System.out);
            }
            
            if (1 == x) {
              e.printStackTrace(System.out);
              fail(methodName + " hit IOException after disk space was freed up");
            }
          }
          
          if (x == 1) {
            // Make sure all threads from ConcurrentMergeScheduler are done
            TestUtil.syncConcurrentMerges(writer);
          } else {
            dir.setRandomIOExceptionRateOnOpen(0.0);
            writer.rollback();
            writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                     .setOpenMode(OpenMode.APPEND)
                                     .setMergePolicy(newLogMergePolicy(false)));
          }
          
          if (VERBOSE) {
            System.out.println("  now test readers");
          }
          
          // Finally, verify index is not corrupt, and, if
          // we succeeded, we see all docs added, and if we
          // failed, we see either all docs or no docs added
          // (transactional semantics):
          dir.setRandomIOExceptionRateOnOpen(0.0);
          try {
            reader = DirectoryReader.open(dir);
          } catch (IOException e) {
            e.printStackTrace(System.out);
            fail(testName + ": exception when creating IndexReader: " + e);
          }
          int result = reader.docFreq(searchTerm);
          if (success) {
            if (result != START_COUNT) {
              fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT);
            }
          } else {
            // On hitting exception we still may have added
            // all docs:
            if (result != START_COUNT && result != END_COUNT) {
              err.printStackTrace(System.out);
              fail(testName + ": method did throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " or " + END_COUNT);
            }
          }
          
          searcher = newSearcher(reader);
          try {
            hits = searcher.search(new TermQuery(searchTerm), END_COUNT).scoreDocs;
          } catch (IOException e) {
            e.printStackTrace(System.out);
            fail(testName + ": exception when searching: " + e);
          }
          int result2 = hits.length;
          if (success) {
            if (result2 != result) {
              fail(testName + ": method did not throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + result);
            }
          } else {
            // On hitting exception we still may have added
            // all docs:
            if (result2 != result) {
              err.printStackTrace(System.out);
              fail(testName + ": method did throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + result);
            }
          }
          
          reader.close();
          if (VERBOSE) {
            System.out.println("  count is " + result);
          }
          
          if (done || result == END_COUNT) {
            break;
          }
        }
        
        if (VERBOSE) {
          System.out.println("  start disk = " + startDiskUsage + "; input disk = " + inputDiskUsage + "; max used = " + dir.getMaxUsedSizeInBytes());
        }
        
        if (done) {
          // Javadocs state that temp free Directory space
          // required is at most 2X total input size of
          // indices so let's make sure:
          assertTrue("max free Directory space required exceeded 1X the total input index sizes during " + methodName +
                     ": max temp usage = " + (dir.getMaxUsedSizeInBytes()-startDiskUsage) + " bytes vs limit=" + (2*(startDiskUsage + inputDiskUsage)) +
                     "; starting disk usage = " + startDiskUsage + " bytes; " +
                     "input index disk usage = " + inputDiskUsage + " bytes",
                     (dir.getMaxUsedSizeInBytes()-startDiskUsage) < 2*(startDiskUsage + inputDiskUsage));
        }
        
        // Make sure we don't hit disk full during close below:
        dir.setMaxSizeInBytes(0);
        dir.setRandomIOExceptionRate(0.0);
        dir.setRandomIOExceptionRateOnOpen(0.0);
        
        writer.close();
        
        dir.close();
        
        // Try again with more free space:
        diskFree += TEST_NIGHTLY ? TestUtil.nextInt(random(), 4000, 8000) : TestUtil.nextInt(random(), 40000, 80000);
      }
    }
    
    startDir.close();
    for (Directory dir : dirs)
      dir.close();
  }
  
  private static class FailTwiceDuringMerge extends MockDirectoryWrapper.Failure {
    public boolean didFail1;
    public boolean didFail2;

    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (!doFail) {
        return;
      }
      if (callStackContains(SegmentMerger.class, "mergeTerms") && !didFail1) {
        didFail1 = true;
        throw new IOException("fake disk full during mergeTerms");
      }
      if (callStackContains(LiveDocsFormat.class, "writeLiveDocs") && !didFail2) {
        didFail2 = true;
        throw new IOException("fake disk full while writing LiveDocs");
      }
    }
  }
  
  // LUCENE-2593
  public void testCorruptionAfterDiskFullDuringMerge() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    //IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random)).setReaderPooling(true));
    IndexWriter w = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergeScheduler(new SerialMergeScheduler())
          .setReaderPooling(true)
          .setMergePolicy(new FilterMergePolicy(newLogMergePolicy(2)) {
            @Override
            public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
              // we can do this because we add/delete/add (and dont merge to "nothing")
              return true;
            }
          })
    );
    Document doc = new Document();

    doc.add(newTextField("f", "doctor who", Field.Store.NO));
    w.addDocument(doc);
    w.commit();

    w.deleteDocuments(new Term("f", "who"));
    w.addDocument(doc);
    
    // disk fills up!
    FailTwiceDuringMerge ftdm = new FailTwiceDuringMerge();
    ftdm.setDoFail();
    dir.failOn(ftdm);

    expectThrows(IOException.class, () -> {
      w.commit();
    });
    assertTrue(ftdm.didFail1 || ftdm.didFail2);

    TestUtil.checkIndex(dir);
    ftdm.clearDoFail();
    expectThrows(AlreadyClosedException.class, () -> {
      w.addDocument(doc);
    });

    dir.close();
  }
  
  // LUCENE-1130: make sure immediate disk full on creating
  // an IndexWriter (hit during DWPT#updateDocuments()) is
  // OK:
  public void testImmediateDiskFull() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(2)
                                                .setMergeScheduler(new ConcurrentMergeScheduler())
                                                .setCommitOnClose(false));
    writer.commit(); // empty commit, to not create confusing situation with first commit
    dir.setMaxSizeInBytes(Math.max(1, dir.getRecomputedActualSizeInBytes()));
    final Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    doc.add(newField("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", customType));
    expectThrows(IOException.class, () -> {
      writer.addDocument(doc);
    });
    assertTrue(writer.isDeleterClosed());
    assertTrue(writer.isClosed());

    dir.close();
  }
  
  // TODO: these are also in TestIndexWriter... add a simple doc-writing method
  // like this to LuceneTestCase?
  private void addDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    doc.add(new NumericDocValuesField("numericdv", 1));
    doc.add(new IntPoint("point", 1));
    doc.add(new IntPoint("point2d", 1, 1));
    writer.addDocument(doc);
  }
  
  private void addDocWithIndex(IndexWriter writer, int index) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa " + index, Field.Store.NO));
    doc.add(newTextField("id", "" + index, Field.Store.NO));
    doc.add(new NumericDocValuesField("numericdv", 1));
    doc.add(new IntPoint("point", 1));
    doc.add(new IntPoint("point2d", 1, 1));
    writer.addDocument(doc);
  }
}
