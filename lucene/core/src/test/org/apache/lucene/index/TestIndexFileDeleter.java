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


import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestIndexFileDeleter extends LuceneTestCase {
  
  public void testDeleteLeftoverFiles() throws IOException {
    Directory dir = newDirectory();

    MergePolicy mergePolicy = newLogMergePolicy(true, 10);
    
    // This test expects all of its segments to be in CFS
    mergePolicy.setNoCFSRatio(1.0);
    mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);

    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(10)
            .setMergePolicy(mergePolicy).setUseCompoundFile(true)
    );

    int i;
    for(i=0;i<35;i++) {
      addDoc(writer, i);
    }
    writer.getConfig().getMergePolicy().setNoCFSRatio(0.0);
    writer.getConfig().setUseCompoundFile(false);
    for(;i<45;i++) {
      addDoc(writer, i);
    }
    writer.close();

    // Delete one doc so we get a .del file:
    writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setUseCompoundFile(true)
    );
    Term searchTerm = new Term("id", "7");
    writer.deleteDocuments(searchTerm);
    writer.close();
    
    // read in index to try to not depend on codec-specific filenames so much
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    SegmentInfo si0 = sis.info(0).info;
    SegmentInfo si1 = sis.info(1).info;
    SegmentInfo si3 = sis.info(3).info;

    // Now, artificially create an extra .del file & extra
    // .s0 file:
    String[] files = dir.listAll();

    /*
    for(int j=0;j<files.length;j++) {
      System.out.println(j + ": " + files[j]);
    }
    */

    // TODO: fix this test better
    String ext = ".liv";
    
    // Create a bogus separate del file for a
    // segment that already has a separate del file: 
    copyFile(dir, "_0_1" + ext, "_0_2" + ext);

    // Create a bogus separate del file for a
    // segment that does not yet have a separate del file:
    copyFile(dir, "_0_1" + ext, "_1_1" + ext);

    // Create a bogus separate del file for a
    // non-existent segment:
    copyFile(dir, "_0_1" + ext, "_188_1" + ext);

    String cfsFiles0[] = si0.getCodec() instanceof SimpleTextCodec ? new String[] { "_0.scf" } : new String[] { "_0.cfs", "_0.cfe" };
    
    // Create a bogus segment file:
    copyFile(dir, cfsFiles0[0], "_188.cfs");

    // Create a bogus fnm file when the CFS already exists:
    copyFile(dir, cfsFiles0[0], "_0.fnm");

    // Create a bogus cfs file shadowing a non-cfs segment:
    
    // TODO: assert is bogus (relies upon codec-specific filenames)
    assertTrue(slowFileExists(dir, "_3.fdt") || slowFileExists(dir, "_3.fld"));
    
    String cfsFiles3[] = si3.getCodec() instanceof SimpleTextCodec ? new String[] { "_3.scf" } : new String[] { "_3.cfs", "_3.cfe" };
    for (String f : cfsFiles3) {
      assertTrue(!slowFileExists(dir, f));
    }
    
    String cfsFiles1[] = si1.getCodec() instanceof SimpleTextCodec ? new String[] { "_1.scf" } : new String[] { "_1.cfs", "_1.cfe" };
    copyFile(dir, cfsFiles1[0], "_3.cfs");
    
    String[] filesPre = dir.listAll();

    // Open & close a writer: it should delete the above files and nothing more:
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                    .setOpenMode(OpenMode.APPEND));
    writer.close();

    String[] files2 = dir.listAll();
    dir.close();

    Arrays.sort(files);
    Arrays.sort(files2);
    
    Set<String> dif = difFiles(files, files2);
    
    if (!Arrays.equals(files, files2)) {
      fail("IndexFileDeleter failed to delete unreferenced extra files: should have deleted " + (filesPre.length-files.length) + " files but only deleted " + (filesPre.length - files2.length) + "; expected files:\n    " + asString(files) + "\n  actual files:\n    " + asString(files2)+"\ndiff: "+dif);
    }
  }

  private static Set<String> difFiles(String[] files1, String[] files2) {
    Set<String> set1 = new HashSet<>();
    Set<String> set2 = new HashSet<>();
    Set<String> extra = new HashSet<>();
    
    for (int x=0; x < files1.length; x++) {
      set1.add(files1[x]);
    }
    for (int x=0; x < files2.length; x++) {
      set2.add(files2[x]);
    }
    Iterator<String> i1 = set1.iterator();
    while (i1.hasNext()) {
      String o = i1.next();
      if (!set2.contains(o)) {
        extra.add(o);
      }
    }
    Iterator<String> i2 = set2.iterator();
    while (i2.hasNext()) {
      String o = i2.next();
      if (!set1.contains(o)) {
        extra.add(o);
      }
    }
    return extra;
  }
  
  private String asString(String[] l) {
    String s = "";
    for(int i=0;i<l.length;i++) {
      if (i > 0) {
        s += "\n    ";
      }
      s += l[i];
    }
    return s;
  }

  public void copyFile(Directory dir, String src, String dest) throws IOException {
    IndexInput in = dir.openInput(src, newIOContext(random()));
    IndexOutput out = dir.createOutput(dest, newIOContext(random()));
    byte[] b = new byte[1024];
    long remainder = in.length();
    while(remainder > 0) {
      int len = (int) Math.min(b.length, remainder);
      in.readBytes(b, 0, len);
      out.writeBytes(b, len);
      remainder -= len;
    }
    in.close();
    out.close();
  }

  private void addDoc(IndexWriter writer, int id) throws IOException
  {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    doc.add(newStringField("id", Integer.toString(id), Field.Store.NO));
    writer.addDocument(doc);
  }
  
  public void testVirusScannerDoesntCorruptIndex() throws IOException {
    Path path = createTempDir();
    Directory dir = newFSDirectory(addVirusChecker(path));
    TestUtil.disableVirusChecker(dir);
    
    // add empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();
    // add a trash unreferenced file
    dir.createOutput("_0.si", IOContext.DEFAULT).close();

    // start virus scanner
    TestUtil.enableVirusChecker(dir);
    
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    iw.addDocument(new Document());
    // stop virus scanner
    TestUtil.disableVirusChecker(dir);
    iw.commit();
    iw.close();
    dir.close();
  }
  
  public void testNoSegmentsDotGenInflation() throws IOException {
    Directory dir = newMockDirectory();
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(1, sis.getGeneration());
    
    // no inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.getGeneration());

    dir.close();
  }
  
  public void testSegmentsInflation() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setCheckIndexOnClose(false); // TODO: allow falling back more than one commit
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(1, sis.getGeneration());
    
    // add trash commit
    dir.createOutput(IndexFileNames.SEGMENTS + "_2", IOContext.DEFAULT).close();
    
    // ensure inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(2, sis.getGeneration());
    
    // add another trash commit
    dir.createOutput(IndexFileNames.SEGMENTS + "_4", IOContext.DEFAULT).close();
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(4, sis.getGeneration());

    dir.close();
  }
  
  public void testSegmentNameInflation() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(0, sis.counter);
    
    // no inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(0, sis.counter);
    
    // add trash per-segment file
    dir.createOutput(IndexFileNames.segmentFileName("_0", "", "foo"), IOContext.DEFAULT).close();
    
    // ensure inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.counter);
    
    // add trash per-segment file
    dir.createOutput(IndexFileNames.segmentFileName("_3", "", "foo"), IOContext.DEFAULT).close();
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(4, sis.counter);
    
    // ensure we write _4 segment next
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    iw.addDocument(new Document());
    iw.commit();
    iw.close();
    sis = SegmentInfos.readLatestCommit(dir);
    assertEquals("_4", sis.info(0).info.name);
    assertEquals(5, sis.counter);
    
    dir.close();
  }
  
  public void testGenerationInflation() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    
    // initial commit
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    iw.addDocument(new Document());
    iw.commit();
    iw.close();   
    
    // no deletes: start at 1
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(1, sis.info(0).getNextDelGen());
    
    // no inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.info(0).getNextDelGen());
    
    // add trash per-segment deletes file
    dir.createOutput(IndexFileNames.fileNameFromGeneration("_0", "del", 2), IOContext.DEFAULT).close();
    
    // ensure inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(3, sis.info(0).getNextDelGen());
    
    dir.close();
  }
  
  public void testTrashyFile() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setCheckIndexOnClose(false); // TODO: maybe handle such trash better elsewhere...
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(1, sis.getGeneration());
    
    // add trash file
    dir.createOutput(IndexFileNames.SEGMENTS + "_", IOContext.DEFAULT).close();
    
    // no inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.getGeneration());

    dir.close();
  }
  
  public void testTrashyGenFile() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    
    // initial commit
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    iw.addDocument(new Document());
    iw.commit();
    iw.close();   
    
    // no deletes: start at 1
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    assertEquals(1, sis.info(0).getNextDelGen());
    
    // add trash file
    dir.createOutput("_1_A", IOContext.DEFAULT).close();
    
    // no inflation
    inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.info(0).getNextDelGen());

    dir.close();
  }
  
  // IFD's inflater is "raw" and expects to only see codec files, 
  // and rightfully so, it filters them out.
  static void inflateGens(SegmentInfos sis, Collection<String> files, InfoStream stream) {
    List<String> filtered = new ArrayList<>();
    for (String file : files) {
      if (IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches() ||
          file.startsWith(IndexFileNames.SEGMENTS) ||
          file.startsWith(IndexFileNames.PENDING_SEGMENTS)) {
        filtered.add(file);
      }
    }
    IndexFileDeleter.inflateGens(sis, filtered, stream);
  }

  // LUCENE-5919
  public void testExcInDecRef() throws Throwable {
    MockDirectoryWrapper dir = newMockDirectory();

    // disable slow things: we don't rely upon sleeps here.
    dir.setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    dir.setUseSlowOpenClosers(false);

    final AtomicBoolean doFailExc = new AtomicBoolean();

    dir.failOn(new MockDirectoryWrapper.Failure() {
        @Override
        public void eval(MockDirectoryWrapper dir) throws IOException {
          if (doFailExc.get() && random().nextInt(4) == 1) {
            Exception e = new Exception();
            StackTraceElement stack[] = e.getStackTrace();
            for (int i = 0; i < stack.length; i++) {
              if (stack[i].getClassName().equals(IndexFileDeleter.class.getName()) && stack[i].getMethodName().equals("decRef")) {
                throw new RuntimeException("fake fail");
              }
            }
          }
        }
      });

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    //iwc.setMergeScheduler(new SerialMergeScheduler());
    MergeScheduler ms = iwc.getMergeScheduler();
    if (ms instanceof ConcurrentMergeScheduler) {
      final ConcurrentMergeScheduler suppressFakeFail = new ConcurrentMergeScheduler() {
          @Override
          protected void handleMergeException(Directory dir, Throwable exc) {
            // suppress only FakeIOException:
            if (exc instanceof RuntimeException && exc.getMessage().equals("fake fail")) {
              // ok to ignore
            } else if ((exc instanceof AlreadyClosedException || exc instanceof IllegalStateException) 
                        && exc.getCause() != null && "fake fail".equals(exc.getCause().getMessage())) {
              // also ok to ignore
            } else {
              super.handleMergeException(dir, exc);
            }
          }
        };
      final ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) ms;
      suppressFakeFail.setMaxMergesAndThreads(cms.getMaxMergeCount(), cms.getMaxThreadCount());
      iwc.setMergeScheduler(suppressFakeFail);
    }

    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    // Since we hit exc during merging, a partial
    // forceMerge can easily return when there are still
    // too many segments in the index:
    w.setDoRandomForceMergeAssert(false);

    doFailExc.set(true);
    int ITERS = atLeast(1000);
    for(int iter=0;iter<ITERS;iter++) {
      try {
        if (random().nextInt(10) == 5) {
          w.commit();
        } else if (random().nextInt(10) == 7) {
          w.getReader().close();
        } else {
          Document doc = new Document();
          doc.add(newTextField("field", "some text", Field.Store.NO));
          w.addDocument(doc);
        }
      } catch (Throwable t) {
        if (t.toString().contains("fake fail") || (t.getCause() != null && t.getCause().toString().contains("fake fail"))) {
          // ok
        } else {
          throw t;
        }
      }
    }

    doFailExc.set(false);
    w.close();
    dir.close();
  }

  // LUCENE-6835: make sure best-effort to not create an "apparently but not really" corrupt index is working:
  public void testExcInDeleteFile() throws Throwable {
    int iters = atLeast(10);
    for(int iter=0;iter<iters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      MockDirectoryWrapper dir = newMockDirectory();

      final AtomicBoolean doFailExc = new AtomicBoolean();

      dir.failOn(new MockDirectoryWrapper.Failure() {
          @Override
          public void eval(MockDirectoryWrapper dir) throws IOException {
            if (doFailExc.get() && random().nextInt(4) == 1) {
              Exception e = new Exception();
              StackTraceElement stack[] = e.getStackTrace();
              for (int i = 0; i < stack.length; i++) {
                if (stack[i].getClassName().equals(MockDirectoryWrapper.class.getName()) && stack[i].getMethodName().equals("deleteFile")) {
                  throw new MockDirectoryWrapper.FakeIOException();
                }
              }
            }
          }
        });

      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setMergeScheduler(new SerialMergeScheduler());
      RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
      w.addDocument(new Document());

      // makes segments_1
      if (VERBOSE) {
        System.out.println("TEST: now commit");
      }
      w.commit();

      w.addDocument(new Document());
      doFailExc.set(true);
      if (VERBOSE) {
        System.out.println("TEST: now close");
      }
      try {
        w.close();
        if (VERBOSE) {
          System.out.println("TEST: no exception (ok)");
        }
      } catch (RuntimeException re) {
        assertTrue(re.getCause() instanceof MockDirectoryWrapper.FakeIOException);
        // good
        if (VERBOSE) {
          System.out.println("TEST: got expected exception:");
          re.printStackTrace(System.out);
        }
      } catch (MockDirectoryWrapper.FakeIOException fioe) {
        // good
        if (VERBOSE) {
          System.out.println("TEST: got expected exception:");
          fioe.printStackTrace(System.out);
        }
      }
      doFailExc.set(false);
      assertFalse(w.w.isOpen());

      for(String name : dir.listAll()) {
        if (name.startsWith(IndexFileNames.SEGMENTS)) {
          if (VERBOSE) {
            System.out.println("TEST: now read " + name);
          }
          SegmentInfos.readCommit(dir, name);
        }
      }
      dir.close();
    }
  }
}
