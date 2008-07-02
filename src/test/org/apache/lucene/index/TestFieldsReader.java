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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util._TestUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TestFieldsReader extends LuceneTestCase {
  private RAMDirectory dir = new RAMDirectory();
  private Document testDoc = new Document();
  private FieldInfos fieldInfos = null;

  private final static String TEST_SEGMENT_NAME = "_0";

  public TestFieldsReader(String s) {
    super(s);
  }

  protected void setUp() throws Exception {
    super.setUp();
    fieldInfos = new FieldInfos();
    DocHelper.setupDoc(testDoc);
    fieldInfos.add(testDoc);
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setUseCompoundFile(false);
    writer.addDocument(testDoc);
    writer.close();
  }

  public void test() throws IOException {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    assertTrue(reader != null);
    assertTrue(reader.size() == 1);
    Document doc = reader.doc(0, null);
    assertTrue(doc != null);
    assertTrue(doc.getField(DocHelper.TEXT_FIELD_1_KEY) != null);

    Fieldable field = doc.getField(DocHelper.TEXT_FIELD_2_KEY);
    assertTrue(field != null);
    assertTrue(field.isTermVectorStored() == true);

    assertTrue(field.isStoreOffsetWithTermVector() == true);
    assertTrue(field.isStorePositionWithTermVector() == true);
    assertTrue(field.getOmitNorms() == false);

    field = doc.getField(DocHelper.TEXT_FIELD_3_KEY);
    assertTrue(field != null);
    assertTrue(field.isTermVectorStored() == false);
    assertTrue(field.isStoreOffsetWithTermVector() == false);
    assertTrue(field.isStorePositionWithTermVector() == false);
    assertTrue(field.getOmitNorms() == true);


    reader.close();
  }


  public void testLazyFields() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    assertTrue(reader != null);
    assertTrue(reader.size() == 1);
    Set loadFieldNames = new HashSet();
    loadFieldNames.add(DocHelper.TEXT_FIELD_1_KEY);
    loadFieldNames.add(DocHelper.TEXT_FIELD_UTF1_KEY);
    Set lazyFieldNames = new HashSet();
    //new String[]{DocHelper.LARGE_LAZY_FIELD_KEY, DocHelper.LAZY_FIELD_KEY, DocHelper.LAZY_FIELD_BINARY_KEY};
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_BINARY_KEY);
    lazyFieldNames.add(DocHelper.TEXT_FIELD_UTF2_KEY);
    lazyFieldNames.add(DocHelper.COMPRESSED_TEXT_FIELD_2_KEY);
    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
    Document doc = reader.doc(0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    Fieldable field = doc.getFieldable(DocHelper.LAZY_FIELD_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("field is not lazy and it should be", field.isLazy());
    String value = field.stringValue();
    assertTrue("value is null and it shouldn't be", value != null);
    assertTrue(value + " is not equal to " + DocHelper.LAZY_FIELD_TEXT, value.equals(DocHelper.LAZY_FIELD_TEXT) == true);
    field = doc.getFieldable(DocHelper.COMPRESSED_TEXT_FIELD_2_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("field is not lazy and it should be", field.isLazy());
    assertTrue("binaryValue isn't null for lazy string field", field.binaryValue() == null);
    value = field.stringValue();
    assertTrue("value is null and it shouldn't be", value != null);
    assertTrue(value + " is not equal to " + DocHelper.FIELD_2_COMPRESSED_TEXT, value.equals(DocHelper.FIELD_2_COMPRESSED_TEXT) == true);
    field = doc.getFieldable(DocHelper.TEXT_FIELD_1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.isLazy() == false);
    field = doc.getFieldable(DocHelper.TEXT_FIELD_UTF1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.isLazy() == false);
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF1_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF1_TEXT) == true);

    field = doc.getFieldable(DocHelper.TEXT_FIELD_UTF2_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.isLazy() == true);
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF2_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF2_TEXT) == true);

    field = doc.getFieldable(DocHelper.LAZY_FIELD_BINARY_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("stringValue isn't null for lazy binary field", field.stringValue() == null);

    byte [] bytes = field.binaryValue();
    assertTrue("bytes is null and it shouldn't be", bytes != null);
    assertTrue("", DocHelper.LAZY_FIELD_BINARY_BYTES.length == bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      assertTrue("byte[" + i + "] is mismatched", bytes[i] == DocHelper.LAZY_FIELD_BINARY_BYTES[i]);

    }
  }

  public void testLazyFieldsAfterClose() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    assertTrue(reader != null);
    assertTrue(reader.size() == 1);
    Set loadFieldNames = new HashSet();
    loadFieldNames.add(DocHelper.TEXT_FIELD_1_KEY);
    loadFieldNames.add(DocHelper.TEXT_FIELD_UTF1_KEY);
    Set lazyFieldNames = new HashSet();
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_BINARY_KEY);
    lazyFieldNames.add(DocHelper.TEXT_FIELD_UTF2_KEY);
    lazyFieldNames.add(DocHelper.COMPRESSED_TEXT_FIELD_2_KEY);
    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
    Document doc = reader.doc(0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    Fieldable field = doc.getFieldable(DocHelper.LAZY_FIELD_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("field is not lazy and it should be", field.isLazy());
    reader.close();
    try {
      field.stringValue();
      fail("did not hit AlreadyClosedException as expected");
    } catch (AlreadyClosedException e) {
      // expected
    }
  }

  public void testLoadFirst() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    assertTrue(reader != null);
    assertTrue(reader.size() == 1);
    LoadFirstFieldSelector fieldSelector = new LoadFirstFieldSelector();
    Document doc = reader.doc(0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    int count = 0;
    List l = doc.getFields();
    for (Iterator iter = l.iterator(); iter.hasNext();) {
      Field field = (Field) iter.next();
      assertTrue("field is null and it shouldn't be", field != null);
      String sv = field.stringValue();
      assertTrue("sv is null and it shouldn't be", sv != null);
      count++;
    }
    assertTrue(count + " does not equal: " + 1, count == 1);
  }

  /**
   * Not really a test per se, but we should have some way of assessing whether this is worthwhile.
   * <p/>
   * Must test using a File based directory
   *
   * @throws Exception
   */
  public void testLazyPerformance() throws Exception {
    String tmpIODir = System.getProperty("tempDir");
    String userName = System.getProperty("user.name");
    String path = tmpIODir + File.separator + "lazyDir" + userName;
    File file = new File(path);
    _TestUtil.rmDir(file);
    FSDirectory tmpDir = FSDirectory.getDirectory(file);
    assertTrue(tmpDir != null);

    IndexWriter writer = new IndexWriter(tmpDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setUseCompoundFile(false);
    writer.addDocument(testDoc);
    writer.close();

    assertTrue(fieldInfos != null);
    FieldsReader reader;
    long lazyTime = 0;
    long regularTime = 0;
    int length = 50;
    Set lazyFieldNames = new HashSet();
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(Collections.EMPTY_SET, lazyFieldNames);

    for (int i = 0; i < length; i++) {
      reader = new FieldsReader(tmpDir, TEST_SEGMENT_NAME, fieldInfos);
      assertTrue(reader != null);
      assertTrue(reader.size() == 1);

      Document doc;
      doc = reader.doc(0, null);//Load all of them
      assertTrue("doc is null and it shouldn't be", doc != null);
      Fieldable field = doc.getFieldable(DocHelper.LARGE_LAZY_FIELD_KEY);
      assertTrue("field is lazy", field.isLazy() == false);
      String value;
      long start;
      long finish;
      start = System.currentTimeMillis();
      //On my machine this was always 0ms.
      value = field.stringValue();
      finish = System.currentTimeMillis();
      assertTrue("value is null and it shouldn't be", value != null);
      assertTrue("field is null and it shouldn't be", field != null);
      regularTime += (finish - start);
      reader.close();
      reader = null;
      doc = null;
      //Hmmm, are we still in cache???
      System.gc();
      reader = new FieldsReader(tmpDir, TEST_SEGMENT_NAME, fieldInfos);
      doc = reader.doc(0, fieldSelector);
      field = doc.getFieldable(DocHelper.LARGE_LAZY_FIELD_KEY);
      assertTrue("field is not lazy", field.isLazy() == true);
      start = System.currentTimeMillis();
      //On my machine this took around 50 - 70ms
      value = field.stringValue();
      finish = System.currentTimeMillis();
      assertTrue("value is null and it shouldn't be", value != null);
      lazyTime += (finish - start);
      reader.close();

    }
    System.out.println("Average Non-lazy time (should be very close to zero): " + regularTime / length + " ms for " + length + " reads");
    System.out.println("Average Lazy Time (should be greater than zero): " + lazyTime / length + " ms for " + length + " reads");
  }
  
  public void testLoadSize() throws IOException {
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    Document doc;
    
    doc = reader.doc(0, new FieldSelector(){
      public FieldSelectorResult accept(String fieldName) {
        if (fieldName.equals(DocHelper.TEXT_FIELD_1_KEY) ||
            fieldName.equals(DocHelper.COMPRESSED_TEXT_FIELD_2_KEY) ||
            fieldName.equals(DocHelper.LAZY_FIELD_BINARY_KEY))
          return FieldSelectorResult.SIZE;
        else if (fieldName.equals(DocHelper.TEXT_FIELD_3_KEY))
          return FieldSelectorResult.LOAD;
        else
          return FieldSelectorResult.NO_LOAD;
      }
    });
    Fieldable f1 = doc.getFieldable(DocHelper.TEXT_FIELD_1_KEY);
    Fieldable f3 = doc.getFieldable(DocHelper.TEXT_FIELD_3_KEY);
    Fieldable fb = doc.getFieldable(DocHelper.LAZY_FIELD_BINARY_KEY);
    assertTrue(f1.isBinary());
    assertTrue(!f3.isBinary());
    assertTrue(fb.isBinary());
    assertSizeEquals(2*DocHelper.FIELD_1_TEXT.length(), f1.binaryValue());
    assertEquals(DocHelper.FIELD_3_TEXT, f3.stringValue());
    assertSizeEquals(DocHelper.LAZY_FIELD_BINARY_BYTES.length, fb.binaryValue());
    
    reader.close();
  }
  
  private void assertSizeEquals(int size, byte[] sizebytes) {
    assertEquals((byte) (size>>>24), sizebytes[0]);
    assertEquals((byte) (size>>>16), sizebytes[1]);
    assertEquals((byte) (size>>> 8), sizebytes[2]);
    assertEquals((byte)  size      , sizebytes[3]);
  }

  public static class FaultyFSDirectory extends Directory {

    FSDirectory fsDir;
    public FaultyFSDirectory(File dir) throws IOException {
      fsDir = FSDirectory.getDirectory(dir);
      lockFactory = fsDir.getLockFactory();
    }
    public IndexInput openInput(String name) throws IOException {
      return new FaultyIndexInput(fsDir.openInput(name));
    }
    public String[] list() throws IOException {
      return fsDir.list();
    }
    public boolean fileExists(String name) throws IOException {
      return fsDir.fileExists(name);
    }
    public long fileModified(String name) throws IOException {
      return fsDir.fileModified(name);
    }
    public void touchFile(String name) throws IOException {
      fsDir.touchFile(name);
    }
    public void deleteFile(String name) throws IOException {
      fsDir.deleteFile(name);
    }
    public void renameFile(String name, String newName) throws IOException {
      fsDir.renameFile(name, newName);
    }
    public long fileLength(String name) throws IOException {
      return fsDir.fileLength(name);
    }
    public IndexOutput createOutput(String name) throws IOException {
      return fsDir.createOutput(name);
    }
    public void close() throws IOException {
      fsDir.close();
    }
  }

  private static class FaultyIndexInput extends BufferedIndexInput {
    IndexInput delegate;
    static boolean doFail;
    int count;
    private FaultyIndexInput(IndexInput delegate) {
      this.delegate = delegate;
    }
    private void simOutage() throws IOException {
      if (doFail && count++ % 2 == 1) {
        throw new IOException("Simulated network outage");
      }
    }
    public void readInternal(byte[] b, int offset, int length) throws IOException {
      simOutage();
      delegate.readBytes(b, offset, length);
    }
    public void seekInternal(long pos) throws IOException {
      //simOutage();
      delegate.seek(pos);
    }
    public long length() {
      return delegate.length();
    }
    public void close() throws IOException {
      delegate.close();
    }
    public Object clone() {
      return new FaultyIndexInput((IndexInput) delegate.clone());
    }
  }

  // LUCENE-1262
  public void testExceptions() throws Throwable {
    String tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null)
      throw new IOException("java.io.tmpdir undefined, cannot run test");
    File indexDir = new File(tempDir, "testfieldswriterexceptions");

    try {
      Directory dir = new FaultyFSDirectory(indexDir);
      IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      for(int i=0;i<2;i++)
        writer.addDocument(testDoc);
      writer.optimize();
      writer.close();

      IndexReader reader = IndexReader.open(dir);

      FaultyIndexInput.doFail = true;

      boolean exc = false;

      for(int i=0;i<2;i++) {
        try {
          reader.document(i);
        } catch (IOException ioe) {
          // expected
          exc = true;
        }
        try {
          reader.document(i);
        } catch (IOException ioe) {
          // expected
          exc = true;
        }
      }
      assertTrue(exc);
      reader.close();
      dir.close();
    } finally {
      _TestUtil.rmDir(indexDir);
    }

  }
}
