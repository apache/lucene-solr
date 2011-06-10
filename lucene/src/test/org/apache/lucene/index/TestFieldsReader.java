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

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.LoadFirstFieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFieldsReader extends LuceneTestCase {
  private static Directory dir;
  private static Document testDoc = new Document();
  private static FieldInfos fieldInfos = null;
  private final static String TEST_SEGMENT_NAME = "_0";

  @BeforeClass
  public static void beforeClass() throws Exception {
    fieldInfos = new FieldInfos();
    DocHelper.setupDoc(testDoc);
    _TestUtil.add(testDoc, fieldInfos);
    dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy());
    ((LogMergePolicy) conf.getMergePolicy()).setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(testDoc);
    writer.close();
    FaultyIndexInput.doFail = false;
  }

  @AfterClass
  public static void afterClass() throws Exception {
    dir.close();
    dir = null;
    fieldInfos = null;
    testDoc = null;
  }
  public void test() throws IOException {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
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
    assertTrue(field.getOmitTermFreqAndPositions() == false);

    field = doc.getField(DocHelper.TEXT_FIELD_3_KEY);
    assertTrue(field != null);
    assertTrue(field.isTermVectorStored() == false);
    assertTrue(field.isStoreOffsetWithTermVector() == false);
    assertTrue(field.isStorePositionWithTermVector() == false);
    assertTrue(field.getOmitNorms() == true);
    assertTrue(field.getOmitTermFreqAndPositions() == false);

    field = doc.getField(DocHelper.NO_TF_KEY);
    assertTrue(field != null);
    assertTrue(field.isTermVectorStored() == false);
    assertTrue(field.isStoreOffsetWithTermVector() == false);
    assertTrue(field.isStorePositionWithTermVector() == false);
    assertTrue(field.getOmitNorms() == false);
    assertTrue(field.getOmitTermFreqAndPositions() == true);
    reader.close();
  }


  public void testLazyFields() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    assertTrue(reader.size() == 1);
    Set<String> loadFieldNames = new HashSet<String>();
    loadFieldNames.add(DocHelper.TEXT_FIELD_1_KEY);
    loadFieldNames.add(DocHelper.TEXT_FIELD_UTF1_KEY);
    Set<String> lazyFieldNames = new HashSet<String>();
    //new String[]{DocHelper.LARGE_LAZY_FIELD_KEY, DocHelper.LAZY_FIELD_KEY, DocHelper.LAZY_FIELD_BINARY_KEY};
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_BINARY_KEY);
    lazyFieldNames.add(DocHelper.TEXT_FIELD_UTF2_KEY);
    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
    Document doc = reader.doc(0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    Fieldable field = doc.getFieldable(DocHelper.LAZY_FIELD_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("field is not lazy and it should be", field.isLazy());
    String value = field.stringValue();
    assertTrue("value is null and it shouldn't be", value != null);
    assertTrue(value + " is not equal to " + DocHelper.LAZY_FIELD_TEXT, value.equals(DocHelper.LAZY_FIELD_TEXT) == true);
    assertTrue("calling stringValue() twice should give same reference", field.stringValue() == field.stringValue());

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

    byte [] bytes = field.getBinaryValue();
    assertTrue("bytes is null and it shouldn't be", bytes != null);
    assertTrue("", DocHelper.LAZY_FIELD_BINARY_BYTES.length == bytes.length);
    assertTrue("calling binaryValue() twice should give same reference", field.getBinaryValue() == field.getBinaryValue());
    for (int i = 0; i < bytes.length; i++) {
      assertTrue("byte[" + i + "] is mismatched", bytes[i] == DocHelper.LAZY_FIELD_BINARY_BYTES[i]);

    }
    reader.close();
  }

  public void testLatentFields() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    assertTrue(reader.size() == 1);
    Set<String> loadFieldNames = new HashSet<String>();
    loadFieldNames.add(DocHelper.TEXT_FIELD_1_KEY);
    loadFieldNames.add(DocHelper.TEXT_FIELD_UTF1_KEY);
    Set<String> lazyFieldNames = new HashSet<String>();
    //new String[]{DocHelper.LARGE_LAZY_FIELD_KEY, DocHelper.LAZY_FIELD_KEY, DocHelper.LAZY_FIELD_BINARY_KEY};
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_BINARY_KEY);
    lazyFieldNames.add(DocHelper.TEXT_FIELD_UTF2_KEY);

    // Use LATENT instead of LAZY
    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames) {
        @Override
        public FieldSelectorResult accept(String fieldName) {
          final FieldSelectorResult result = super.accept(fieldName);
          if (result == FieldSelectorResult.LAZY_LOAD) {
            return FieldSelectorResult.LATENT;
          } else {
            return result;
          }
        }
      };

    Document doc = reader.doc(0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    Fieldable field = doc.getFieldable(DocHelper.LAZY_FIELD_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("field is not lazy and it should be", field.isLazy());
    String value = field.stringValue();
    assertTrue("value is null and it shouldn't be", value != null);
    assertTrue(value + " is not equal to " + DocHelper.LAZY_FIELD_TEXT, value.equals(DocHelper.LAZY_FIELD_TEXT) == true);
    assertTrue("calling stringValue() twice should give different references", field.stringValue() != field.stringValue());

    field = doc.getFieldable(DocHelper.TEXT_FIELD_1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.isLazy() == false);
    assertTrue("calling stringValue() twice should give same reference", field.stringValue() == field.stringValue());

    field = doc.getFieldable(DocHelper.TEXT_FIELD_UTF1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.isLazy() == false);
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF1_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF1_TEXT) == true);
    assertTrue("calling stringValue() twice should give same reference", field.stringValue() == field.stringValue());

    field = doc.getFieldable(DocHelper.TEXT_FIELD_UTF2_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.isLazy() == true);
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF2_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF2_TEXT) == true);
    assertTrue("calling stringValue() twice should give different references", field.stringValue() != field.stringValue());

    field = doc.getFieldable(DocHelper.LAZY_FIELD_BINARY_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("stringValue isn't null for lazy binary field", field.stringValue() == null);
    assertTrue("calling binaryValue() twice should give different references", field.getBinaryValue() != field.getBinaryValue());

    byte [] bytes = field.getBinaryValue();
    assertTrue("bytes is null and it shouldn't be", bytes != null);
    assertTrue("", DocHelper.LAZY_FIELD_BINARY_BYTES.length == bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      assertTrue("byte[" + i + "] is mismatched", bytes[i] == DocHelper.LAZY_FIELD_BINARY_BYTES[i]);

    }
    reader.close();
  }




  public void testLazyFieldsAfterClose() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    assertTrue(reader.size() == 1);
    Set<String> loadFieldNames = new HashSet<String>();
    loadFieldNames.add(DocHelper.TEXT_FIELD_1_KEY);
    loadFieldNames.add(DocHelper.TEXT_FIELD_UTF1_KEY);
    Set<String> lazyFieldNames = new HashSet<String>();
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_KEY);
    lazyFieldNames.add(DocHelper.LAZY_FIELD_BINARY_KEY);
    lazyFieldNames.add(DocHelper.TEXT_FIELD_UTF2_KEY);
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
    assertTrue(reader.size() == 1);
    LoadFirstFieldSelector fieldSelector = new LoadFirstFieldSelector();
    Document doc = reader.doc(0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    int count = 0;
    List<Fieldable> l = doc.getFields();
    for (final Fieldable fieldable : l ) {
      Field field = (Field) fieldable;

      assertTrue("field is null and it shouldn't be", field != null);
      String sv = field.stringValue();
      assertTrue("sv is null and it shouldn't be", sv != null);
      count++;
    }
    assertTrue(count + " does not equal: " + 1, count == 1);
    reader.close();
  }

  /**
   * Not really a test per se, but we should have some way of assessing whether this is worthwhile.
   * <p/>
   * Must test using a File based directory
   *
   * @throws Exception
   */
  public void testLazyPerformance() throws Exception {
    String userName = System.getProperty("user.name");
    File file = _TestUtil.getTempDir("lazyDir" + userName);
    Directory tmpDir = newFSDirectory(file);
    assertTrue(tmpDir != null);

    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE).setMergePolicy(newLogMergePolicy());
    ((LogMergePolicy) conf.getMergePolicy()).setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(tmpDir, conf);
    writer.addDocument(testDoc);
    writer.close();

    assertTrue(fieldInfos != null);
    FieldsReader reader;
    long lazyTime = 0;
    long regularTime = 0;
    int length = 10;
    Set<String> lazyFieldNames = new HashSet<String>();
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(Collections. <String> emptySet(), lazyFieldNames);

    for (int i = 0; i < length; i++) {
      reader = new FieldsReader(tmpDir, TEST_SEGMENT_NAME, fieldInfos);
      assertTrue(reader.size() == 1);

      Document doc;
      doc = reader.doc(0, null);//Load all of them
      assertTrue("doc is null and it shouldn't be", doc != null);
      Fieldable field = doc.getFieldable(DocHelper.LARGE_LAZY_FIELD_KEY);
      assertTrue("field is null and it shouldn't be", field != null);
      assertTrue("field is lazy", field.isLazy() == false);
      String value;
      long start;
      long finish;
      start = System.currentTimeMillis();
      //On my machine this was always 0ms.
      value = field.stringValue();
      finish = System.currentTimeMillis();
      assertTrue("value is null and it shouldn't be", value != null);
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
    tmpDir.close();
    if (VERBOSE) {
      System.out.println("Average Non-lazy time (should be very close to zero): " + regularTime / length + " ms for " + length + " reads");
      System.out.println("Average Lazy Time (should be greater than zero): " + lazyTime / length + " ms for " + length + " reads");
    }
  }
  
  public void testLoadSize() throws IOException {
    FieldsReader reader = new FieldsReader(dir, TEST_SEGMENT_NAME, fieldInfos);
    Document doc;
    
    doc = reader.doc(0, new FieldSelector(){
      public FieldSelectorResult accept(String fieldName) {
        if (fieldName.equals(DocHelper.TEXT_FIELD_1_KEY) ||
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
    assertSizeEquals(2*DocHelper.FIELD_1_TEXT.length(), f1.getBinaryValue());
    assertEquals(DocHelper.FIELD_3_TEXT, f3.stringValue());
    assertSizeEquals(DocHelper.LAZY_FIELD_BINARY_BYTES.length, fb.getBinaryValue());
    
    reader.close();
  }
  
  private void assertSizeEquals(int size, byte[] sizebytes) {
    assertEquals((byte) (size>>>24), sizebytes[0]);
    assertEquals((byte) (size>>>16), sizebytes[1]);
    assertEquals((byte) (size>>> 8), sizebytes[2]);
    assertEquals((byte)  size      , sizebytes[3]);
  }

  public static class FaultyFSDirectory extends Directory {

    Directory fsDir;
    
    public FaultyFSDirectory(File dir) throws IOException {
      fsDir = newFSDirectory(dir);
      lockFactory = fsDir.getLockFactory();
    }
    @Override
    public IndexInput openInput(String name) throws IOException {
      return new FaultyIndexInput(fsDir.openInput(name));
    }
    @Override
    public String[] listAll() throws IOException {
      return fsDir.listAll();
    }
    @Override
    public boolean fileExists(String name) throws IOException {
      return fsDir.fileExists(name);
    }
    @Override
    public long fileModified(String name) throws IOException {
      return fsDir.fileModified(name);
    }
    @Override
    public void deleteFile(String name) throws IOException {
      fsDir.deleteFile(name);
    }
    @Override
    public long fileLength(String name) throws IOException {
      return fsDir.fileLength(name);
    }
    @Override
    public IndexOutput createOutput(String name) throws IOException {
      return fsDir.createOutput(name);
    }
    @Override
    public void sync(Collection<String> names) throws IOException {
      fsDir.sync(names);
    }
    @Override
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
    @Override
    public void readInternal(byte[] b, int offset, int length) throws IOException {
      simOutage();
      delegate.readBytes(b, offset, length);
    }
    @Override
    public void seekInternal(long pos) throws IOException {
      //simOutage();
      delegate.seek(pos);
    }
    @Override
    public long length() {
      return delegate.length();
    }
    @Override
    public void close() throws IOException {
      delegate.close();
    }
    @Override
    public Object clone() {
      return new FaultyIndexInput((IndexInput) delegate.clone());
    }
  }

  // LUCENE-1262
  public void testExceptions() throws Throwable {
    File indexDir = _TestUtil.getTempDir("testfieldswriterexceptions");

    try {
      Directory dir = new FaultyFSDirectory(indexDir);
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( 
          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE));
      for(int i=0;i<2;i++)
        writer.addDocument(testDoc);
      writer.optimize();
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);

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
  
  public void testNumericField() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir);
    final int numDocs = atLeast(500);
    final Number[] answers = new Number[numDocs];
    final NumericField.DataType[] typeAnswers = new NumericField.DataType[numDocs];
    for(int id=0;id<numDocs;id++) {
      Document doc = new Document();
      NumericField nf = new NumericField("nf", Field.Store.YES, false);
      doc.add(nf);
      final Number answer;
      final NumericField.DataType typeAnswer;
      if (random.nextBoolean()) {
        // float/double
        if (random.nextBoolean()) {
          final float f = random.nextFloat();
          nf.setFloatValue(f);
          answer = Float.valueOf(f);
          typeAnswer = NumericField.DataType.FLOAT;
        } else {
          final double d = random.nextDouble();
          nf.setDoubleValue(d);
          answer = Double.valueOf(d);
          typeAnswer = NumericField.DataType.DOUBLE;
        }
      } else {
        // int/long
        if (random.nextBoolean()) {
          final int i = random.nextInt();
          nf.setIntValue(i);
          answer = Integer.valueOf(i);
          typeAnswer = NumericField.DataType.INT;
        } else {
          final long l = random.nextLong();
          nf.setLongValue(l);
          answer = Long.valueOf(l);
          typeAnswer = NumericField.DataType.LONG;
        }
      }
      answers[id] = answer;
      typeAnswers[id] = typeAnswer;
      doc.add(new NumericField("id", Integer.MAX_VALUE, Field.Store.NO, true).setIntValue(id));
      w.addDocument(doc);
    }
    final IndexReader r = w.getReader();
    w.close();
    
    assertEquals(numDocs, r.numDocs());

    for(IndexReader sub : r.getSequentialSubReaders()) {
      final int[] ids = FieldCache.DEFAULT.getInts(sub, "id");
      for(int docID=0;docID<sub.numDocs();docID++) {
        final Document doc = sub.document(docID);
        final Fieldable f = doc.getFieldable("nf");
        assertTrue("got f=" + f, f instanceof NumericField);
        final NumericField nf = (NumericField) f;
        assertEquals(answers[ids[docID]], nf.getNumericValue());
        assertSame(typeAnswers[ids[docID]], nf.getDataType());
      }
    }
    r.close();
    dir.close();
  }
  
}
