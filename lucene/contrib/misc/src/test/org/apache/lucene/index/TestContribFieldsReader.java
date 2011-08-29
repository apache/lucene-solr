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
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.FieldSelectorVisitor;
import org.apache.lucene.document.LoadFirstFieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;


public class TestContribFieldsReader extends LuceneTestCase {
  private static Directory dir;
  private static org.apache.lucene.document.Document testDoc = new org.apache.lucene.document.Document();
  private static FieldInfos fieldInfos = null;

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
  }

  @AfterClass
  public static void afterClass() throws Exception {
    dir.close();
    dir = null;
    fieldInfos = null;
    testDoc = null;
  }

  private Document getDocument(IndexReader ir, int docID, FieldSelector selector) throws IOException {
    final FieldSelectorVisitor visitor = new FieldSelectorVisitor(selector);
    ir.document(docID, visitor);
    return visitor.getDocument();
  }

  public void testLazyFields() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    IndexReader reader = IndexReader.open(dir);
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
    Document doc = getDocument(reader, 0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    IndexableField field = doc.getField(DocHelper.LAZY_FIELD_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("field is not lazy and it should be", field.getClass().getSimpleName().equals("LazyField"));
    String value = field.stringValue();
    assertTrue("value is null and it shouldn't be", value != null);
    assertTrue(value + " is not equal to " + DocHelper.LAZY_FIELD_TEXT, value.equals(DocHelper.LAZY_FIELD_TEXT) == true);
    assertTrue("calling stringValue() twice should give same reference", field.stringValue() == field.stringValue());

    field = doc.getField(DocHelper.TEXT_FIELD_1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertFalse("Field is lazy and it should not be", field.getClass().getSimpleName().equals("LazyField"));
    field = doc.getField(DocHelper.TEXT_FIELD_UTF1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertFalse("Field is lazy and it should not be", field.getClass().getSimpleName().equals("LazyField"));
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF1_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF1_TEXT) == true);

    field = doc.getField(DocHelper.TEXT_FIELD_UTF2_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.getClass().getSimpleName().equals("LazyField"));
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF2_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF2_TEXT) == true);

    field = doc.getField(DocHelper.LAZY_FIELD_BINARY_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("stringValue isn't null for lazy binary field", field.stringValue() == null);

    byte [] bytes = field.binaryValue().bytes;
    assertTrue("bytes is null and it shouldn't be", bytes != null);
    assertTrue("", DocHelper.LAZY_FIELD_BINARY_BYTES.length == bytes.length);
    assertTrue("calling binaryValue() twice should give same reference", field.binaryValue().bytes == field.binaryValue().bytes);
    for (int i = 0; i < bytes.length; i++) {
      assertTrue("byte[" + i + "] is mismatched", bytes[i] == DocHelper.LAZY_FIELD_BINARY_BYTES[i]);

    }
    reader.close();
  }

  public void testLatentFields() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    IndexReader reader = IndexReader.open(dir);
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

    Document doc = getDocument(reader, 0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    IndexableField field = doc.getField(DocHelper.LAZY_FIELD_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("field is not lazy and it should be", field.getClass().getSimpleName().equals("LazyField"));
    String value = field.stringValue();
    assertTrue("value is null and it shouldn't be", value != null);
    assertTrue(value + " is not equal to " + DocHelper.LAZY_FIELD_TEXT, value.equals(DocHelper.LAZY_FIELD_TEXT) == true);
    assertTrue("calling stringValue() twice should give different references", field.stringValue() != field.stringValue());

    field = doc.getField(DocHelper.TEXT_FIELD_1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertFalse("Field is lazy and it should not be", field.getClass().getSimpleName().equals("LazyField"));
    assertTrue("calling stringValue() twice should give same reference", field.stringValue() == field.stringValue());

    field = doc.getField(DocHelper.TEXT_FIELD_UTF1_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertFalse("Field is lazy and it should not be", field.getClass().getSimpleName().equals("LazyField"));
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF1_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF1_TEXT) == true);
    assertTrue("calling stringValue() twice should give same reference", field.stringValue() == field.stringValue());

    field = doc.getField(DocHelper.TEXT_FIELD_UTF2_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("Field is lazy and it should not be", field.getClass().getSimpleName().equals("LazyField"));
    assertTrue(field.stringValue() + " is not equal to " + DocHelper.FIELD_UTF2_TEXT, field.stringValue().equals(DocHelper.FIELD_UTF2_TEXT) == true);
    assertTrue("calling stringValue() twice should give different references", field.stringValue() != field.stringValue());

    field = doc.getField(DocHelper.LAZY_FIELD_BINARY_KEY);
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue("stringValue isn't null for lazy binary field", field.stringValue() == null);
    assertTrue("calling binaryValue() twice should give different references", field.binaryValue().bytes != field.binaryValue().bytes);

    byte [] bytes = field.binaryValue().bytes;
    assertTrue("bytes is null and it shouldn't be", bytes != null);
    assertTrue("", DocHelper.LAZY_FIELD_BINARY_BYTES.length == bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      assertTrue("byte[" + i + "] is mismatched", bytes[i] == DocHelper.LAZY_FIELD_BINARY_BYTES[i]);

    }
    reader.close();
  }

  public void testLoadFirst() throws Exception {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    IndexReader reader = IndexReader.open(dir);
    LoadFirstFieldSelector fieldSelector = new LoadFirstFieldSelector();
    Document doc = getDocument(reader, 0, fieldSelector);
    assertTrue("doc is null and it shouldn't be", doc != null);
    int count = 0;
    List<IndexableField> l = doc.getFields();
    for (final IndexableField IndexableField : l ) {
      Field field = (Field) IndexableField;

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
    long lazyTime = 0;
    long regularTime = 0;
    int length = 10;
    Set<String> lazyFieldNames = new HashSet<String>();
    lazyFieldNames.add(DocHelper.LARGE_LAZY_FIELD_KEY);
    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(Collections. <String> emptySet(), lazyFieldNames);

    for (int i = 0; i < length; i++) {
      IndexReader reader = IndexReader.open(tmpDir);

      Document doc;
      doc = reader.document(0);//Load all of them
      assertTrue("doc is null and it shouldn't be", doc != null);
      IndexableField field = doc.getField(DocHelper.LARGE_LAZY_FIELD_KEY);
      assertTrue("field is null and it shouldn't be", field != null);
      assertFalse("field is lazy", field.getClass().getSimpleName().equals("LazyField"));
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
      reader = IndexReader.open(tmpDir);
      doc = getDocument(reader, 0, fieldSelector);
      field = doc.getField(DocHelper.LARGE_LAZY_FIELD_KEY);
      assertTrue("field is not lazy", field.getClass().getSimpleName().equals("LazyField"));
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
    IndexReader reader = IndexReader.open(dir);
    Document doc;
    
    doc = getDocument(reader, 0, new FieldSelector(){
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
    IndexableField f1 = doc.getField(DocHelper.TEXT_FIELD_1_KEY);
    IndexableField f3 = doc.getField(DocHelper.TEXT_FIELD_3_KEY);
    IndexableField fb = doc.getField(DocHelper.LAZY_FIELD_BINARY_KEY);
    assertTrue(f1.binaryValue()!=null);
    assertTrue(f3.binaryValue()==null);
    assertTrue(fb.binaryValue()!=null);
    assertSizeEquals(2*DocHelper.FIELD_1_TEXT.length(), f1.binaryValue().bytes);
    assertEquals(DocHelper.FIELD_3_TEXT, f3.stringValue());
    assertSizeEquals(DocHelper.LAZY_FIELD_BINARY_BYTES.length, fb.binaryValue().bytes);
    
    reader.close();
  }
  
  private void assertSizeEquals(int size, byte[] sizebytes) {
    assertEquals((byte) (size>>>24), sizebytes[0]);
    assertEquals((byte) (size>>>16), sizebytes[1]);
    assertEquals((byte) (size>>> 8), sizebytes[2]);
    assertEquals((byte)  size      , sizebytes[3]);
  }
}
