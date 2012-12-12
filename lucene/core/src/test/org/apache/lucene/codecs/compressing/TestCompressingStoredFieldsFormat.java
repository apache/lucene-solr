package org.apache.lucene.codecs.compressing;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestCompressingStoredFieldsFormat extends LuceneTestCase {

  private static final Codec NON_COMPRESSING_CODEC = new SimpleTextCodec();

  private Directory dir;
  IndexWriterConfig iwConf;
  private RandomIndexWriter iw;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    iwConf.setCodec(CompressingCodec.randomInstance(random()));
    iw = new RandomIndexWriter(random(), dir, iwConf);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.close(iw, dir);
    iw = null;
    dir = null;
  }

  private byte[] randomByteArray(int length, int max) {
    final byte[] result = new byte[length];
    for (int i = 0; i < length; ++i) {
      result[i] = (byte) random().nextInt(max);
    }
    return result;
  }

  public void testWriteReadMerge() throws IOException {
    final int docCount = atLeast(200);
    final byte[][][] data = new byte [docCount][][];
    for (int i = 0; i < docCount; ++i) {
      final int fieldCount = rarely()
          ? RandomInts.randomIntBetween(random(), 1, 500)
          : RandomInts.randomIntBetween(random(), 1, 5);
      data[i] = new byte[fieldCount][];
      for (int j = 0; j < fieldCount; ++j) {
        final int length = rarely()
            ? random().nextInt(1000)
            : random().nextInt(10);
        final int max = rarely() ? 256 : 2;
        data[i][j] = randomByteArray(length, max);
      }
    }

    final FieldType type = new FieldType(StringField.TYPE_STORED);
    type.setIndexed(false);
    type.freeze();
    IntField id = new IntField("id", 0, Store.YES);
    for (int i = 0; i < data.length; ++i) {
      Document doc = new Document();
      doc.add(id);
      id.setIntValue(i);
      for (int j = 0; j < data[i].length; ++j) {
        Field f = new Field("bytes" + j, data[i][j], type);
        doc.add(f);
      }
      iw.w.addDocument(doc);
      if (random().nextBoolean() && (i % (data.length / 10) == 0)) {
        iw.w.close();
        // test merging against a non-compressing codec
        if (iwConf.getCodec() == NON_COMPRESSING_CODEC) {
          iwConf.setCodec(CompressingCodec.randomInstance(random()));
        } else {
          iwConf.setCodec(NON_COMPRESSING_CODEC);
        }
        iw = new RandomIndexWriter(random(), dir, iwConf);
      }
    }

    for (int i = 0; i < 10; ++i) {
      final int min = random().nextInt(data.length);
      final int max = min + random().nextInt(20);
      iw.deleteDocuments(NumericRangeQuery.newIntRange("id", min, max, true, false));
    }

    iw.forceMerge(2); // force merges with deletions

    iw.commit();

    final DirectoryReader ir = DirectoryReader.open(dir);
    assertTrue(ir.numDocs() > 0);
    int numDocs = 0;
    for (int i = 0; i < ir.maxDoc(); ++i) {
      final Document doc = ir.document(i);
      if (doc == null) {
        continue;
      }
      ++ numDocs;
      final int docId = doc.getField("id").numericValue().intValue();
      assertEquals(data[docId].length + 1, doc.getFields().size());
      for (int j = 0; j < data[docId].length; ++j) {
        final byte[] arr = data[docId][j];
        final BytesRef arr2Ref = doc.getBinaryValue("bytes" + j);
        final byte[] arr2 = Arrays.copyOfRange(arr2Ref.bytes, arr2Ref.offset, arr2Ref.offset + arr2Ref.length);
        assertArrayEquals(arr, arr2);
      }
    }
    assertTrue(ir.numDocs() <= numDocs);
    ir.close();

    iw.deleteAll();
    iw.commit();
    iw.forceMerge(1);
  }

  public void testReadSkip() throws IOException {
    FieldType ft = new FieldType();
    ft.setStored(true);
    ft.freeze();

    final String string = _TestUtil.randomSimpleString(random(), 50);
    final byte[] bytes = string.getBytes("UTF-8");
    final long l = random().nextBoolean() ? random().nextInt(42) : random().nextLong();
    final int i = random().nextBoolean() ? random().nextInt(42) : random().nextInt();
    final float f = random().nextFloat();
    final double d = random().nextDouble();

    List<Field> fields = Arrays.asList(
        new Field("bytes", bytes, ft),
        new Field("string", string, ft),
        new LongField("long", l, Store.YES),
        new IntField("int", i, Store.YES),
        new FloatField("float", f, Store.YES),
        new DoubleField("double", d, Store.YES)
    );

    for (int k = 0; k < 100; ++k) {
      Document doc = new Document();
      for (Field fld : fields) {
        doc.add(fld);
      }
      iw.w.addDocument(doc);
    }
    iw.commit();

    final DirectoryReader reader = DirectoryReader.open(dir);
    final int docID = random().nextInt(100);
    for (Field fld : fields) {
      String fldName = fld.name();
      final Document sDoc = reader.document(docID, Collections.singleton(fldName));
      final IndexableField sField = sDoc.getField(fldName);
      if (Field.class.equals(fld.getClass())) {
        assertEquals(fld.binaryValue(), sField.binaryValue());
        assertEquals(fld.stringValue(), sField.stringValue());
      } else {
        assertEquals(fld.numericValue(), sField.numericValue());
      }
    }
    reader.close();
  }

  public void testEmptyDocs() throws IOException {
    // make sure that the fact that documents might be empty is not a problem
    final Document emptyDoc = new Document();
    final int numDocs = random().nextBoolean() ? 1 : atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      iw.addDocument(emptyDoc);
    }
    iw.commit();
    final DirectoryReader rd = DirectoryReader.open(dir);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = rd.document(i);
      assertNotNull(doc);
      assertTrue(doc.getFields().isEmpty());
    }
    rd.close();
  }

  public void testConcurrentReads() throws Exception {
    // make sure the readers are properly cloned
    final Document doc = new Document();
    final Field field = new StringField("fld", "", Store.YES);
    doc.add(field);
    final int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      field.setStringValue("" + i);
      iw.addDocument(doc);
    }
    iw.commit();

    final DirectoryReader rd = DirectoryReader.open(dir);
    final IndexSearcher searcher = new IndexSearcher(rd);
    final int concurrentReads = atLeast(5);
    final int readsPerThread = atLeast(50);
    final List<Thread> readThreads = new ArrayList<Thread>();
    final AtomicReference<Exception> ex = new AtomicReference<Exception>();
    for (int i = 0; i < concurrentReads; ++i) {
      readThreads.add(new Thread() {

        int[] queries;

        {
          queries = new int[readsPerThread];
          for (int i = 0; i < queries.length; ++i) {
            queries[i] = random().nextInt(numDocs);
          }
        }

        @Override
        public void run() {
          for (int q : queries) {
            final Query query = new TermQuery(new Term("fld", "" + q));
            try {
              final TopDocs topDocs = searcher.search(query, 1);
              if (topDocs.totalHits != 1) {
                throw new IllegalStateException("Expected 1 hit, got " + topDocs.totalHits);
              }
              final Document sdoc = rd.document(topDocs.scoreDocs[0].doc);
              if (sdoc == null || sdoc.get("fld") == null) {
                throw new IllegalStateException("Could not find document " + q);
              }
              if (!Integer.toString(q).equals(sdoc.get("fld"))) {
                throw new IllegalStateException("Expected " + q + ", but got " + sdoc.get("fld"));
              }
            } catch (Exception e) {
              ex.compareAndSet(null, e);
            }
          }
        }
      });
    }
    for (Thread thread : readThreads) {
      thread.start();
    }
    for (Thread thread : readThreads) {
      thread.join();
    }
    rd.close();
    if (ex.get() != null) {
      throw ex.get();
    }
  }

  @Nightly
  public void testBigDocuments() throws IOException {
    // "big" as "much bigger than the chunk size"
    // for this test we force a FS dir
    iw.close();
    dir.close();
    dir = newFSDirectory(_TestUtil.getTempDir(getClass().getSimpleName()));
    iw = new RandomIndexWriter(random(), dir, iwConf);

    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper) dir).setThrottling(Throttling.NEVER);
    }

    final Document emptyDoc = new Document(); // emptyDoc
    final Document bigDoc1 = new Document(); // lot of small fields
    final Document bigDoc2 = new Document(); // 1 very big field

    final Field idField = new StringField("id", "", Store.NO);
    emptyDoc.add(idField);
    bigDoc1.add(idField);
    bigDoc2.add(idField);

    final FieldType onlyStored = new FieldType(StringField.TYPE_STORED);
    onlyStored.setIndexed(false);

    final Field smallField = new Field("fld", randomByteArray(random().nextInt(10), 256), onlyStored);
    final int numFields = RandomInts.randomIntBetween(random(), 500000, 1000000);
    for (int i = 0; i < numFields; ++i) {
      bigDoc1.add(smallField);
    }

    final Field bigField = new Field("fld", randomByteArray(RandomInts.randomIntBetween(random(), 1000000, 5000000), 2), onlyStored);
    bigDoc2.add(bigField);

    final int numDocs = atLeast(5);
    final Document[] docs = new Document[numDocs];
    for (int i = 0; i < numDocs; ++i) {
      docs[i] = RandomPicks.randomFrom(random(), Arrays.asList(emptyDoc, bigDoc1, bigDoc2));
    }
    for (int i = 0; i < numDocs; ++i) {
      idField.setStringValue("" + i);
      iw.addDocument(docs[i]);
      if (random().nextInt(numDocs) == 0) {
        iw.commit();
      }
    }
    iw.commit();
    iw.forceMerge(1); // look at what happens when big docs are merged
    final DirectoryReader rd = DirectoryReader.open(dir);
    final IndexSearcher searcher = new IndexSearcher(rd);
    for (int i = 0; i < numDocs; ++i) {
      final Query query = new TermQuery(new Term("id", "" + i));
      final TopDocs topDocs = searcher.search(query, 1);
      assertEquals("" + i, 1, topDocs.totalHits);
      final Document doc = rd.document(topDocs.scoreDocs[0].doc);
      assertNotNull(doc);
      final IndexableField[] fieldValues = doc.getFields("fld");
      assertEquals(docs[i].getFields("fld").length, fieldValues.length);
      if (fieldValues.length > 0) {
        assertEquals(docs[i].getFields("fld")[0].binaryValue(), fieldValues[0].binaryValue());
      }
    }
    rd.close();
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testDeletePartiallyWrittenFilesIfAbort() throws IOException {
    // disable CFS because this test checks file names
    iwConf.setMergePolicy(newLogMergePolicy(false));
    iw.close();
    iw = new RandomIndexWriter(random(), dir, iwConf);

    final Document validDoc = new Document();
    validDoc.add(new IntField("id", 0, Store.YES));
    iw.addDocument(validDoc);
    iw.commit();
    
    // make sure that #writeField will fail to trigger an abort
    final Document invalidDoc = new Document();
    FieldType fieldType = new FieldType();
    fieldType.setStored(true);
    invalidDoc.add(new Field("invalid", fieldType) {
      
      @Override
      public String stringValue() {
        return null;
      }
      
    });
    
    try {
      iw.addDocument(invalidDoc);
      iw.commit();
    }
    finally {
      int counter = 0;
      for (String fileName : dir.listAll()) {
        if (fileName.endsWith(".fdt") || fileName.endsWith(".fdx")) {
          counter++;
        }
      }
      // Only one .fdt and one .fdx files must have been found
      assertEquals(2, counter);
    }
  }

}
