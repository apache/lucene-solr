package org.apache.lucene.index;

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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
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

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestIndexFileDeleter extends LuceneTestCase {
  
  public void testDeleteLeftoverFiles() throws IOException {
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setPreventDoubleWrite(false);
      // ensure we actually delete files
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }

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

    // Now, artificially create an extra .del file & extra
    // .s0 file:
    String[] files = dir.listAll();

    /*
    for(int j=0;j<files.length;j++) {
      System.out.println(j + ": " + files[j]);
    }
    */

    // TODO: fix this test better
    String ext = Codec.getDefault().getName().equals("SimpleText") ? ".liv" : ".del";
    
    // Create a bogus separate del file for a
    // segment that already has a separate del file: 
    copyFile(dir, "_0_1" + ext, "_0_2" + ext);

    // Create a bogus separate del file for a
    // segment that does not yet have a separate del file:
    copyFile(dir, "_0_1" + ext, "_1_1" + ext);

    // Create a bogus separate del file for a
    // non-existent segment:
    copyFile(dir, "_0_1" + ext, "_188_1" + ext);

    // Create a bogus segment file:
    copyFile(dir, "_0.cfs", "_188.cfs");

    // Create a bogus fnm file when the CFS already exists:
    copyFile(dir, "_0.cfs", "_0.fnm");
    
    // Create some old segments file:
    copyFile(dir, "segments_2", "segments");
    copyFile(dir, "segments_2", "segments_1");

    // Create a bogus cfs file shadowing a non-cfs segment:
    
    // TODO: assert is bogus (relies upon codec-specific filenames)
    assertTrue(slowFileExists(dir, "_3.fdt") || slowFileExists(dir, "_3.fld"));
    assertTrue(!slowFileExists(dir, "_3.cfs"));
    copyFile(dir, "_1.cfs", "_3.cfs");
    
    String[] filesPre = dir.listAll();

    // Open & close a writer: it should delete the above 4
    // files and nothing more:
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
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setPreventDoubleWrite(false); // we arent trying to test this
    dir.setEnableVirusScanner(false); // we have our own to make test reproduce always
    
    // add empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();
    // add a trash unreferenced file
    dir.createOutput("_0.si", IOContext.DEFAULT).close();

    // start virus scanner
    final AtomicBoolean stopScanning = new AtomicBoolean();
    dir.failOn(new MockDirectoryWrapper.Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        if (stopScanning.get()) {
          return;
        }
        for (StackTraceElement f : new Exception().getStackTrace()) {
          if ("deleteFile".equals(f.getMethodName())) {
            throw new IOException("temporarily cannot delete file");
          }
        }
      }
    });
    
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    iw.addDocument(new Document());
    // stop virus scanner
    stopScanning.set(true);
    iw.commit();
    iw.close();
    dir.close();
  }
  
  public void testNoSegmentsDotGenInflation() throws IOException {
    Directory dir = newMockDirectory();
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    assertEquals(1, sis.getGeneration());
    
    // no inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.getGeneration());

    dir.close();
  }
  
  public void testSegmentsInflation() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setCheckIndexOnClose(false); // TODO: allow falling back more than one commit
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    assertEquals(1, sis.getGeneration());
    
    // add trash commit
    dir.createOutput(IndexFileNames.SEGMENTS + "_2", IOContext.DEFAULT).close();
    
    // ensure inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(2, sis.getGeneration());
    
    // add another trash commit
    dir.createOutput(IndexFileNames.SEGMENTS + "_4", IOContext.DEFAULT).close();
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(4, sis.getGeneration());

    dir.close();
  }
  
  public void testSegmentNameInflation() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    assertEquals(0, sis.counter);
    
    // no inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(0, sis.counter);
    
    // add trash per-segment file
    dir.createOutput(IndexFileNames.segmentFileName("_0", "", "foo"), IOContext.DEFAULT).close();
    
    // ensure inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.counter);
    
    // add trash per-segment file
    dir.createOutput(IndexFileNames.segmentFileName("_3", "", "foo"), IOContext.DEFAULT).close();
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(4, sis.counter);
    
    // ensure we write _4 segment next
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    iw.addDocument(new Document());
    iw.commit();
    iw.close();
    sis = new SegmentInfos();
    sis.read(dir);
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
    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    assertEquals(1, sis.info(0).getNextDelGen());
    
    // no inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.info(0).getNextDelGen());
    
    // add trash per-segment deletes file
    dir.createOutput(IndexFileNames.fileNameFromGeneration("_0", "del", 2), IOContext.DEFAULT).close();
    
    // ensure inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(3, sis.info(0).getNextDelGen());
    
    dir.close();
  }
  
  public void testTrashyFile() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setCheckIndexOnClose(false); // TODO: maybe handle such trash better elsewhere...
    
    // empty commit
    new IndexWriter(dir, new IndexWriterConfig(null)).close();   
    
    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    assertEquals(1, sis.getGeneration());
    
    // add trash file
    dir.createOutput(IndexFileNames.SEGMENTS + "_", IOContext.DEFAULT).close();
    
    // no inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
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
    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    assertEquals(1, sis.info(0).getNextDelGen());
    
    // add trash file
    dir.createOutput("_1_A", IOContext.DEFAULT).close();
    
    // no inflation
    IndexFileDeleter.inflateGens(sis, Arrays.asList(dir.listAll()), InfoStream.getDefault());
    assertEquals(1, sis.info(0).getNextDelGen());

    dir.close();
  }

  // LUCENE-5919
  public void testExcInDecRef() throws Exception {
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
          protected void handleMergeException(Throwable exc) {
            // suppress only FakeIOException:
            if (exc instanceof RuntimeException && exc.getMessage().equals("fake fail")) {
              // ok to ignore
            } else if (exc instanceof AlreadyClosedException && exc.getCause() != null && "fake fail".equals(exc.getCause().getMessage())) {
              // also ok to ignore
            } else {
              super.handleMergeException(exc);
            }
          }
        };
      final ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) ms;
      suppressFakeFail.setMaxMergesAndThreads(cms.getMaxMergeCount(), cms.getMaxThreadCount());
      suppressFakeFail.setMergeThreadPriority(cms.getMergeThreadPriority());
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
      } catch (IOException ioe) {
        if (ioe.getMessage().contains("background merge hit exception")) {
          Throwable cause = ioe.getCause();
          if (cause != null && cause instanceof RuntimeException && ((RuntimeException) cause).getMessage().equals("fake fail")) {
            // ok
          } else {
            throw ioe;
          }
        } else {
          throw ioe;
        }
      } catch (RuntimeException re) {
        if (re.getMessage().equals("fake fail")) {
          // ok
        } else if (re instanceof AlreadyClosedException && re.getCause() != null && "fake fail".equals(re.getCause().getMessage())) {
          break; // our test got unlucky, triggered our strange exception after successful finishCommit, caused a disaster!
        } else {
          throw re;
        }
      }
    }

    doFailExc.set(false);
    w.close();
    dir.close();
  }
}
