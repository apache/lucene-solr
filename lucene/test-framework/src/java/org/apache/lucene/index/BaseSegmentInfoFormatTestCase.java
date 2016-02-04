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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper.Failure;
import org.apache.lucene.store.MockDirectoryWrapper.FakeIOException;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

/**
 * Abstract class to do basic tests for si format.
 * NOTE: This test focuses on the si impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new si format that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given si Format that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseSegmentInfoFormatTestCase extends BaseIndexFileFormatTestCase {
  
  /** Test files map */
  public void testFiles() throws Exception {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertEquals(info.files(), info2.files());
    dir.close();
  }
  
  /** Tests SI writer adds itself to files... */
  public void testAddsSelfToFiles() throws Exception {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
    Set<String> originalFiles = Collections.singleton("_123.a");
    info.setFiles(originalFiles);
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    
    Set<String> modifiedFiles = info.files();
    assertTrue(modifiedFiles.containsAll(originalFiles));
    assertTrue("did you forget to add yourself to files()", modifiedFiles.size() > originalFiles.size());
    
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertEquals(info.files(), info2.files());
    try {
      info2.files().add("bogus");
      fail("files set should be immutable");
    } catch (UnsupportedOperationException expected) {
      // ok
    }
    dir.close();
  }
  
  /** Test diagnostics map */
  public void testDiagnostics() throws Exception {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    Map<String,String> diagnostics = new HashMap<>();
    diagnostics.put("key1", "value1");
    diagnostics.put("key2", "value2");
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       diagnostics, id, new HashMap<String,String>());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertEquals(diagnostics, info2.getDiagnostics());
    try {
      info2.getDiagnostics().put("bogus", "bogus");
      fail("diagnostics map should be immutable");
    } catch (UnsupportedOperationException expected) {
      // ok
    }
    dir.close();
  }
  
  /** Test attributes map */
  public void testAttributes() throws Exception {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    Map<String,String> attributes = new HashMap<>();
    attributes.put("key1", "value1");
    attributes.put("key2", "value2");
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, attributes);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertAttributesEquals(attributes, info2.getAttributes());
    try {
      info2.getAttributes().put("bogus", "bogus");
      fail("attributes map should be immutable");
    } catch (UnsupportedOperationException expected) {
      // ok
    }
    dir.close();
  }
  
  /** Test unique ID */
  public void testUniqueID() throws Exception {
    Codec codec = getCodec();
    Directory dir = newDirectory();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertIDEquals(id, info2.getId());
    dir.close();
  }
  
  /** Test versions */
  public void testVersions() throws Exception {
    Codec codec = getCodec();
    for (Version v : getVersions()) {
      Directory dir = newDirectory();
      byte id[] = StringHelper.randomId();
      SegmentInfo info = new SegmentInfo(dir, v, "_123", 1, false, codec, 
                                         Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
      info.setFiles(Collections.<String>emptySet());
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
      SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
      assertEquals(info2.getVersion(), v);
      dir.close();
    }
  }
  
  /** 
   * Test segment infos write that hits exception immediately on open.
   * make sure we get our exception back, no file handle leaks, etc. 
   */
  public void testExceptionOnCreateOutput() throws Exception {
    Failure fail = new Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
          if (doFail && "createOutput".equals(e.getMethodName())) {
            throw new FakeIOException();
          }
        }
      }
    };
    
    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
    info.setFiles(Collections.<String>emptySet());
    
    fail.setDoFail();
    try {
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
      fail("didn't get expected exception");
    } catch (FakeIOException expected) {
      // ok
    } finally {
      fail.clearDoFail();
    }
    
    dir.close();
  }
  
  /** 
   * Test segment infos write that hits exception on close.
   * make sure we get our exception back, no file handle leaks, etc. 
   */
  public void testExceptionOnCloseOutput() throws Exception {
    Failure fail = new Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
          if (doFail && "close".equals(e.getMethodName())) {
            throw new FakeIOException();
          }
        }
      }
    };
    
    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
    info.setFiles(Collections.<String>emptySet());
    
    fail.setDoFail();
    try {
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
      fail("didn't get expected exception");
    } catch (FakeIOException expected) {
      // ok
    } finally {
      fail.clearDoFail();
    }
    
    dir.close();
  }
  
  /** 
   * Test segment infos read that hits exception immediately on open.
   * make sure we get our exception back, no file handle leaks, etc. 
   */
  public void testExceptionOnOpenInput() throws Exception {
    Failure fail = new Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
          if (doFail && "openInput".equals(e.getMethodName())) {
            throw new FakeIOException();
          }
        }
      }
    };
    
    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    
    fail.setDoFail();
    try {
      codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
      fail("didn't get expected exception");
    } catch (FakeIOException expected) {
      // ok
    } finally {
      fail.clearDoFail();
    }
    
    dir.close();
  }
  
  /** 
   * Test segment infos read that hits exception on close
   * make sure we get our exception back, no file handle leaks, etc. 
   */
  public void testExceptionOnCloseInput() throws Exception {
    Failure fail = new Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
          if (doFail && "close".equals(e.getMethodName())) {
            throw new FakeIOException();
          }
        }
      }
    };
    
    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<String,String>());
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    
    fail.setDoFail();
    try {
      codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
      fail("didn't get expected exception");
    } catch (FakeIOException expected) {
      // ok
    } finally {
      fail.clearDoFail();
    }
    dir.close();
  }
  
  /** 
   * Sets some otherwise hard-to-test properties: 
   * random segment names, ID values, document count, etc and round-trips
   */
  public void testRandom() throws Exception {
    Codec codec = getCodec();
    Version[] versions = getVersions();
    for (int i = 0; i < 10; i++) {
      Directory dir = newDirectory();
      Version version = versions[random().nextInt(versions.length)];
      String name = "_" + Integer.toString(random().nextInt(Integer.MAX_VALUE), Character.MAX_RADIX);
      int docCount = TestUtil.nextInt(random(), 1, IndexWriter.MAX_DOCS);
      boolean isCompoundFile = random().nextBoolean();
      Set<String> files = new HashSet<>();
      int numFiles = random().nextInt(10);
      for (int j = 0; j < numFiles; j++) {
        String file = IndexFileNames.segmentFileName(name, "", Integer.toString(j));
        files.add(file);
        dir.createOutput(file, IOContext.DEFAULT).close();
      }
      Map<String,String> diagnostics = new HashMap<>();
      int numDiags = random().nextInt(10);
      for (int j = 0; j < numDiags; j++) {
        diagnostics.put(TestUtil.randomUnicodeString(random()), 
                        TestUtil.randomUnicodeString(random()));
      }
      byte id[] = new byte[StringHelper.ID_LENGTH];
      random().nextBytes(id);
      
      Map<String,String> attributes = new HashMap<>();
      int numAttributes = random().nextInt(10);
      for (int j = 0; j < numAttributes; j++) {
        attributes.put(TestUtil.randomUnicodeString(random()), 
                       TestUtil.randomUnicodeString(random()));
      }
      
      SegmentInfo info = new SegmentInfo(dir, version, name, docCount, isCompoundFile, codec, diagnostics, id, attributes);
      info.setFiles(files);
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
      SegmentInfo info2 = codec.segmentInfoFormat().read(dir, name, id, IOContext.DEFAULT);
      assertEquals(info, info2);
 
      dir.close();
    }
  }
  
  protected final void assertEquals(SegmentInfo expected, SegmentInfo actual) {
    assertSame(expected.dir, actual.dir);
    assertEquals(expected.name, actual.name);
    assertEquals(expected.files(), actual.files());
    // we don't assert this, because SI format has nothing to do with it... set by SIS
    // assertSame(expected.getCodec(), actual.getCodec());
    assertEquals(expected.getDiagnostics(), actual.getDiagnostics());
    assertEquals(expected.maxDoc(), actual.maxDoc());
    assertIDEquals(expected.getId(), actual.getId());
    assertEquals(expected.getUseCompoundFile(), actual.getUseCompoundFile());
    assertEquals(expected.getVersion(), actual.getVersion());
    assertAttributesEquals(expected.getAttributes(), actual.getAttributes());
  }
  
  /** Returns the versions this SI should test */
  protected abstract Version[] getVersions();
  
  /** 
   * assert that unique id is equal. 
   * @deprecated only exists to be overridden by old codecs that didnt support this
   */
  @Deprecated
  protected void assertIDEquals(byte expected[], byte actual[]) {
    assertArrayEquals(expected, actual);
  }
  
  /** 
   * assert that attributes map is equal. 
   * @deprecated only exists to be overridden by old codecs that didnt support this
   */
  @Deprecated
  protected void assertAttributesEquals(Map<String,String> expected, Map<String,String> actual) {
    assertEquals(expected, actual);
  }
  
  @Override
  protected void addRandomFields(Document doc) {
    doc.add(new StoredField("foobar", TestUtil.randomSimpleString(random())));
  }

  @Override
  public void testRamBytesUsed() throws IOException {
    assumeTrue("not applicable for this format", true);
  }
}
