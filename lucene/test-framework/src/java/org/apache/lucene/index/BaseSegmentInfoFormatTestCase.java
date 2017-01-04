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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
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
                                       Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
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
                                       Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
    Set<String> originalFiles = Collections.singleton("_123.a");
    info.setFiles(originalFiles);
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    
    Set<String> modifiedFiles = info.files();
    assertTrue(modifiedFiles.containsAll(originalFiles));
    assertTrue("did you forget to add yourself to files()", modifiedFiles.size() > originalFiles.size());
    
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertEquals(info.files(), info2.files());

    // files set should be immutable
    expectThrows(UnsupportedOperationException.class, () -> {
      info2.files().add("bogus");
    });

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
                                       diagnostics, id, new HashMap<>(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertEquals(diagnostics, info2.getDiagnostics());

    // diagnostics map should be immutable
    expectThrows(UnsupportedOperationException.class, () -> {
      info2.getDiagnostics().put("bogus", "bogus");
    });

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
                                       Collections.emptyMap(), id, attributes, null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    assertEquals(attributes, info2.getAttributes());
    
    // attributes map should be immutable
    expectThrows(UnsupportedOperationException.class, () -> {
      info2.getAttributes().put("bogus", "bogus");
    });

    dir.close();
  }
  
  /** Test unique ID */
  public void testUniqueID() throws Exception {
    Codec codec = getCodec();
    Directory dir = newDirectory();
    byte id[] = StringHelper.randomId();
    SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
                                       Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
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
                                         Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
      info.setFiles(Collections.<String>emptySet());
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
      SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
      assertEquals(info2.getVersion(), v);
      dir.close();
    }
  }

  protected boolean supportsIndexSort() {
    return true;
  }

  private SortField randomIndexSortField() {
    boolean reversed = random().nextBoolean();
    SortField sortField;
    switch(random().nextInt(10)) {
      case 0:
        sortField = new SortField(TestUtil.randomSimpleString(random()), SortField.Type.INT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextInt());
        }
        break;
      case 1:
        sortField = new SortedNumericSortField(TestUtil.randomSimpleString(random()), SortField.Type.INT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextInt());
        }
        break;

      case 2:
        sortField = new SortField(TestUtil.randomSimpleString(random()), SortField.Type.LONG, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextLong());
        }
        break;
      case 3:
        sortField = new SortedNumericSortField(TestUtil.randomSimpleString(random()), SortField.Type.LONG, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextLong());
        }
        break;
      case 4:
        sortField = new SortField(TestUtil.randomSimpleString(random()), SortField.Type.FLOAT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextFloat());
        }
        break;
      case 5:
        sortField = new SortedNumericSortField(TestUtil.randomSimpleString(random()), SortField.Type.FLOAT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextFloat());
        }
        break;
      case 6:
        sortField = new SortField(TestUtil.randomSimpleString(random()), SortField.Type.DOUBLE, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextDouble());
        }
        break;
      case 7:
        sortField = new SortedNumericSortField(TestUtil.randomSimpleString(random()), SortField.Type.DOUBLE, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextDouble());
        }
        break;
      case 8:
        sortField = new SortField(TestUtil.randomSimpleString(random()), SortField.Type.STRING, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(SortField.STRING_LAST);
        }
        break;
      case 9:
        sortField = new SortedSetSortField(TestUtil.randomSimpleString(random()), reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(SortField.STRING_LAST);
        }
        break;
      default:
        sortField = null;
        fail();
    }
    return sortField;
  }

  /** Test sort */
  public void testSort() throws IOException {
    assumeTrue("test requires a codec that can read/write index sort", supportsIndexSort());

    final int iters = atLeast(5);
    for (int i = 0; i < iters; ++i) {
      Sort sort;
      if (i == 0) {
        sort = null;
      } else {
        final int numSortFields = TestUtil.nextInt(random(), 1, 3);
        SortField[] sortFields = new SortField[numSortFields];
        for (int j = 0; j < numSortFields; ++j) {
          sortFields[j] = randomIndexSortField();
        }
        sort = new Sort(sortFields);
      }

      Directory dir = newDirectory();
      Codec codec = getCodec();
      byte id[] = StringHelper.randomId();
      SegmentInfo info = new SegmentInfo(dir, getVersions()[0], "_123", 1, false, codec, 
          Collections.<String,String>emptyMap(), id, new HashMap<>(), sort);
      info.setFiles(Collections.<String>emptySet());
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
      SegmentInfo info2 = codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
      assertEquals(sort, info2.getIndexSort());
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
                                       Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
    info.setFiles(Collections.<String>emptySet());
    
    fail.setDoFail();
    expectThrows(FakeIOException.class, () -> {
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    });
    fail.clearDoFail();
    
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
                                       Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
    info.setFiles(Collections.<String>emptySet());
    
    fail.setDoFail();
    expectThrows(FakeIOException.class, () -> {
      codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    });
    fail.clearDoFail();
    
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
                                       Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    
    fail.setDoFail();
    expectThrows(FakeIOException.class, () -> {
      codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    });
    fail.clearDoFail();
    
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
                                       Collections.<String,String>emptyMap(), id, new HashMap<>(), null);
    info.setFiles(Collections.<String>emptySet());
    codec.segmentInfoFormat().write(dir, info, IOContext.DEFAULT);
    
    fail.setDoFail();
    expectThrows(FakeIOException.class, () -> {
      codec.segmentInfoFormat().read(dir, "_123", id, IOContext.DEFAULT);
    });
    fail.clearDoFail();

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
      
      SegmentInfo info = new SegmentInfo(dir, version, name, docCount, isCompoundFile, codec, diagnostics, id, attributes, null);
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
    assertEquals(expected.getAttributes(), actual.getAttributes());
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
  
  @Override
  protected void addRandomFields(Document doc) {
    doc.add(new StoredField("foobar", TestUtil.randomSimpleString(random())));
  }

  @Override
  public void testRamBytesUsed() throws IOException {
    assumeTrue("not applicable for this format", true);
  }
}
