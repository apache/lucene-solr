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
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
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

  @BeforeClass
  public static void beforeClass() throws Exception {
    fieldInfos = new FieldInfos(new FieldInfos.FieldNumberBiMap());
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
    IndexReader reader = IndexReader.open(dir);
    Document doc = reader.document(0);
    assertTrue(doc != null);
    assertTrue(doc.getField(DocHelper.TEXT_FIELD_1_KEY) != null);

    Field field = (Field) doc.getField(DocHelper.TEXT_FIELD_2_KEY);
    assertTrue(field != null);
    assertTrue(field.fieldType().storeTermVectors());

    assertTrue(field.fieldType().storeTermVectorOffsets());
    assertTrue(field.fieldType().storeTermVectorPositions());
    assertFalse(field.fieldType().omitNorms());
    assertTrue(field.fieldType().indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    field = (Field) doc.getField(DocHelper.TEXT_FIELD_3_KEY);
    assertTrue(field != null);
    assertFalse(field.fieldType().storeTermVectors());
    assertFalse(field.fieldType().storeTermVectorOffsets());
    assertFalse(field.fieldType().storeTermVectorPositions());
    assertTrue(field.fieldType().omitNorms());
    assertTrue(field.fieldType().indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    field = (Field) doc.getField(DocHelper.NO_TF_KEY);
    assertTrue(field != null);
    assertFalse(field.fieldType().storeTermVectors());
    assertFalse(field.fieldType().storeTermVectorOffsets());
    assertFalse(field.fieldType().storeTermVectorPositions());
    assertFalse(field.fieldType().omitNorms());
    assertTrue(field.fieldType().indexOptions() == IndexOptions.DOCS_ONLY);

    DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(DocHelper.TEXT_FIELD_3_KEY);
    reader.document(0, visitor);
    final List<IndexableField> fields = visitor.getDocument().getFields();
    assertEquals(1, fields.size());
    assertEquals(DocHelper.TEXT_FIELD_3_KEY, fields.get(0).name());
    reader.close();
  }


  public static class FaultyFSDirectory extends Directory {

    Directory fsDir;
    
    public FaultyFSDirectory(File dir) throws IOException {
      fsDir = newFSDirectory(dir);
      lockFactory = fsDir.getLockFactory();
    }
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      return new FaultyIndexInput(fsDir.openInput(name, context));
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
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      return fsDir.createOutput(name, context);
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
      super("FaultyIndexInput(" + delegate + ")", BufferedIndexInput.BUFFER_SIZE);
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
      writer.forceMerge(1);
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
      NumericField nf = new NumericField("nf", NumericField.TYPE_STORED);
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
      doc.add(new NumericField("id", Integer.MAX_VALUE).setIntValue(id));
      w.addDocument(doc);
    }
    final IndexReader r = w.getReader();
    w.close();
    
    assertEquals(numDocs, r.numDocs());

    for(IndexReader sub : r.getSequentialSubReaders()) {
      final int[] ids = FieldCache.DEFAULT.getInts(sub, "id", false);
      for(int docID=0;docID<sub.numDocs();docID++) {
        final Document doc = sub.document(docID);
        final Field f = (Field) doc.getField("nf");
        assertTrue("got f=" + f, f instanceof NumericField);
        final NumericField nf = (NumericField) f;
        assertEquals(answers[ids[docID]], nf.numericValue());
        assertSame(typeAnswers[ids[docID]], nf.numericDataType());
      }
    }
    r.close();
    dir.close();
  }

  public void testIndexedBit() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir);
    Document doc = new Document();
    FieldType onlyStored = new FieldType();
    onlyStored.setStored(true);
    doc.add(new Field("field", "value", onlyStored));
    doc.add(new Field("field2", "value", StringField.TYPE_STORED));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();
    assertFalse(r.document(0).getField("field").fieldType().indexed());
    assertTrue(r.document(0).getField("field2").fieldType().indexed());
    r.close();
    dir.close();
  }
}
