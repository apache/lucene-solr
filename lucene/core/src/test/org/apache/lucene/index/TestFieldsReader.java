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

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFieldsReader extends LuceneTestCase {
  private static Directory dir;
  private static Document testDoc;
  private static FieldInfos.Builder fieldInfos = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testDoc = new Document();
    fieldInfos = new FieldInfos.Builder();
    DocHelper.setupDoc(testDoc);
    for (IndexableField field : testDoc) {
      fieldInfos.addOrUpdate(field.name(), field.fieldType());
    }
    dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
                               .setMergePolicy(newLogMergePolicy());
    conf.getMergePolicy().setNoCFSRatio(0.0);
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
    IndexReader reader = DirectoryReader.open(dir);
    Document doc = reader.document(0);
    assertTrue(doc != null);
    assertTrue(doc.getField(DocHelper.TEXT_FIELD_1_KEY) != null);

    Field field = (Field) doc.getField(DocHelper.TEXT_FIELD_2_KEY);
    assertTrue(field != null);
    assertTrue(field.fieldType().storeTermVectors());

    assertFalse(field.fieldType().omitNorms());
    assertTrue(field.fieldType().indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    field = (Field) doc.getField(DocHelper.TEXT_FIELD_3_KEY);
    assertTrue(field != null);
    assertFalse(field.fieldType().storeTermVectors());
    assertTrue(field.fieldType().omitNorms());
    assertTrue(field.fieldType().indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    field = (Field) doc.getField(DocHelper.NO_TF_KEY);
    assertTrue(field != null);
    assertFalse(field.fieldType().storeTermVectors());
    assertFalse(field.fieldType().omitNorms());
    assertTrue(field.fieldType().indexOptions() == IndexOptions.DOCS_ONLY);

    DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(DocHelper.TEXT_FIELD_3_KEY);
    reader.document(0, visitor);
    final List<IndexableField> fields = visitor.getDocument().getFields();
    assertEquals(1, fields.size());
    assertEquals(DocHelper.TEXT_FIELD_3_KEY, fields.get(0).name());
    reader.close();
  }


  public static class FaultyFSDirectory extends BaseDirectory {

    Directory fsDir;
    
    public FaultyFSDirectory(File dir) {
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
      delegate.seek(getFilePointer());
      delegate.readBytes(b, offset, length);
    }
    @Override
    public void seekInternal(long pos) throws IOException {
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
    public FaultyIndexInput clone() {
      FaultyIndexInput i = new FaultyIndexInput(delegate.clone());
      // seek the clone to our current position
      try {
        i.seek(getFilePointer());
      } catch (IOException e) {
        throw new RuntimeException();
      }
      return i;
    }
    
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      IndexInput slice = delegate.slice(sliceDescription, offset, length);
      return new FaultyIndexInput(slice);
    }
  }

  // LUCENE-1262
  public void testExceptions() throws Throwable {
    File indexDir = createTempDir("testfieldswriterexceptions");

    try {
      Directory dir = new FaultyFSDirectory(indexDir);
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
                                .setOpenMode(OpenMode.CREATE);
      IndexWriter writer = new IndexWriter(dir, iwc);
      for(int i=0;i<2;i++)
        writer.addDocument(testDoc);
      writer.forceMerge(1);
      writer.close();

      IndexReader reader = DirectoryReader.open(dir);

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
      TestUtil.rm(indexDir);
    }

  }
}
