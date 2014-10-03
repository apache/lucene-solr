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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

/**
 * Abstract class to do basic tests for a compound format.
 * NOTE: This test focuses on the compound impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new CompoundFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given CompoundFormat that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseCompoundFormatTestCase extends BaseIndexFileFormatTestCase {
  private Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Path file = createTempDir("testIndex");
    dir = newFSDirectory(file);
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }
  
  // test that empty CFS is empty
  public void testEmpty() throws IOException {
    Directory dir = newDirectory();
    
    final SegmentInfo si = newSegmentInfo(dir, "_123");
    si.getCodec().compoundFormat().write(dir, si, Collections.<String>emptyList(), MergeState.CheckAbort.NONE, IOContext.DEFAULT);
    Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si, IOContext.DEFAULT);
    assertEquals(0, cfs.listAll().length);
    cfs.close();
    dir.close();
  }
  
  // test that a second call to close() behaves according to Closeable
  public void testDoubleClose() throws IOException {
    final String testfile = "_123.test";

    Directory dir = newDirectory();
    IndexOutput out = dir.createOutput(testfile, IOContext.DEFAULT);
    out.writeInt(3);
    out.close();
    
    final SegmentInfo si = newSegmentInfo(dir, "_123");
    si.getCodec().compoundFormat().write(dir, si, Collections.singleton(testfile), MergeState.CheckAbort.NONE, IOContext.DEFAULT);
    Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si, IOContext.DEFAULT);
    assertEquals(1, cfs.listAll().length);
    cfs.close();
    cfs.close(); // second close should not throw exception
    dir.close();
  }
  
  // LUCENE-5724: things like NRTCachingDir rely upon IOContext being properly passed down
  public void testPassIOContext() throws IOException {
    final String testfile = "_123.test";
    final IOContext myContext = new IOContext();

    Directory dir = new FilterDirectory(newDirectory()) {
      @Override
      public IndexOutput createOutput(String name, IOContext context) throws IOException {
        assertSame(myContext, context);
        return super.createOutput(name, context);
      }
    };
    IndexOutput out = dir.createOutput(testfile, myContext);
    out.writeInt(3);
    out.close();
    
    final SegmentInfo si = newSegmentInfo(dir, "_123");
    si.getCodec().compoundFormat().write(dir, si, Collections.singleton(testfile), MergeState.CheckAbort.NONE, myContext);
    dir.close();
  }
  
  // LUCENE-5724: actually test we play nice with NRTCachingDir and massive file
  public void testLargeCFS() throws IOException {   
    final String testfile = "_123.test";
    IOContext context = new IOContext(new FlushInfo(0, 512*1024*1024));

    Directory dir = new NRTCachingDirectory(newFSDirectory(createTempDir()), 2.0, 25.0);

    IndexOutput out = dir.createOutput(testfile, context);
    byte[] bytes = new byte[512];
    for(int i=0;i<1024*1024;i++) {
      out.writeBytes(bytes, 0, bytes.length);
    }
    out.close();
    
    final SegmentInfo si = newSegmentInfo(dir, "_123");
    si.getCodec().compoundFormat().write(dir, si, Collections.singleton(testfile), MergeState.CheckAbort.NONE, context);

    dir.close();
  }
  
  // Just tests that we can open all files returned by listAll
  public void testListAll() throws Exception {
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      // test lists files manually and tries to verify every .cfs it finds,
      // but a virus scanner could leave some trash.
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }
    // riw should sometimes create docvalues fields, etc
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    // these fields should sometimes get term vectors, etc
    Field idField = newStringField("id", "", Field.Store.NO);
    Field bodyField = newTextField("body", "", Field.Store.NO);
    doc.add(idField);
    doc.add(bodyField);
    for (int i = 0; i < 100; i++) {
      idField.setStringValue(Integer.toString(i));
      bodyField.setStringValue(TestUtil.randomUnicodeString(random()));
      riw.addDocument(doc);
      if (random().nextInt(7) == 0) {
        riw.commit();
      }
    }
    riw.close();
    SegmentInfos infos = new SegmentInfos();
    infos.read(dir);
    for (SegmentCommitInfo si : infos) {
      if (si.info.getUseCompoundFile()) {
        try (Directory cfsDir = si.info.getCodec().compoundFormat().getCompoundReader(dir, si.info, newIOContext(random()))) {
          for (String cfsFile : cfsDir.listAll()) {
            try (IndexInput cfsIn = cfsDir.openInput(cfsFile, IOContext.DEFAULT)) {}
          }
        }
      }
    }
    dir.close();
  }
  
  /** Returns a new fake segment */
  static SegmentInfo newSegmentInfo(Directory dir, String name) {
    return new SegmentInfo(dir, Version.LATEST, name, 10000, false, Codec.getDefault(), null, StringHelper.randomId());
  }
  
  /** Creates a file of the specified size with random data. */
  static void createRandomFile(Directory dir, String name, int size) throws IOException {
    IndexOutput os = dir.createOutput(name, newIOContext(random()));
    for (int i=0; i<size; i++) {
      byte b = (byte) (Math.random() * 256);
      os.writeByte(b);
    }
    os.close();
  }
  
  /** Creates a file of the specified size with sequential data. The first
   *  byte is written as the start byte provided. All subsequent bytes are
   *  computed as start + offset where offset is the number of the byte.
   */
  static void createSequenceFile(Directory dir, String name, byte start, int size) throws IOException {
    IndexOutput os = dir.createOutput(name, newIOContext(random()));
    for (int i=0; i < size; i++) {
      os.writeByte(start);
      start ++;
    }
    os.close();
  }
  
  static void assertSameStreams(String msg, IndexInput expected, IndexInput test) throws IOException {
    assertNotNull(msg + " null expected", expected);
    assertNotNull(msg + " null test", test);
    assertEquals(msg + " length", expected.length(), test.length());
    assertEquals(msg + " position", expected.getFilePointer(), test.getFilePointer());
    
    byte expectedBuffer[] = new byte[512];
    byte testBuffer[] = new byte[expectedBuffer.length];
    
    long remainder = expected.length() - expected.getFilePointer();
    while (remainder > 0) {
      int readLen = (int) Math.min(remainder, expectedBuffer.length);
      expected.readBytes(expectedBuffer, 0, readLen);
      test.readBytes(testBuffer, 0, readLen);
      assertEqualArrays(msg + ", remainder " + remainder, expectedBuffer, testBuffer, 0, readLen);
      remainder -= readLen;
    }
  }
  
  static void assertSameStreams(String msg, IndexInput expected, IndexInput actual, long seekTo) throws IOException {
    if (seekTo >= 0 && seekTo < expected.length()) {
      expected.seek(seekTo);
      actual.seek(seekTo);
      assertSameStreams(msg + ", seek(mid)", expected, actual);
    }
  }
  
  static void assertSameSeekBehavior(String msg, IndexInput expected, IndexInput actual) throws IOException {
    // seek to 0
    long point = 0;
    assertSameStreams(msg + ", seek(0)", expected, actual, point);
    
    // seek to middle
    point = expected.length() / 2l;
    assertSameStreams(msg + ", seek(mid)", expected, actual, point);
    
    // seek to end - 2
    point = expected.length() - 2;
    assertSameStreams(msg + ", seek(end-2)", expected, actual, point);
    
    // seek to end - 1
    point = expected.length() - 1;
    assertSameStreams(msg + ", seek(end-1)", expected, actual, point);
    
    // seek to the end
    point = expected.length();
    assertSameStreams(msg + ", seek(end)", expected, actual, point);
    
    // seek past end
    point = expected.length() + 1;
    assertSameStreams(msg + ", seek(end+1)", expected, actual, point);
  }
  
  static void assertEqualArrays(String msg, byte[] expected, byte[] test, int start, int len) {
    assertNotNull(msg + " null expected", expected);
    assertNotNull(msg + " null test", test);
    
    for (int i=start; i<len; i++) {
      assertEquals(msg + " " + i, expected[i], test[i]);
    }
  }

  @Override
  protected void addRandomFields(Document doc) {
    doc.add(new StoredField("foobar", TestUtil.randomSimpleString(random())));
  }

  @Override
  public void testMergeStability() throws Exception {
    assumeTrue("test does not work with CFS", true);
  }
}
