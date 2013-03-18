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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.Nightly;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

/**
 * Base class aiming at testing {@link StoredFieldsFormat stored fields formats}.
 * To test a new format, all you need is to register a new {@link Codec} which
 * uses it and extend this class and override {@link #getCodec()}.
 * @lucene.experimental
 */
public abstract class BaseStoredFieldsFormatTestCase extends LuceneTestCase {
  private Codec savedCodec;

  /**
   * Returns the Codec to run tests against
   */
  protected abstract Codec getCodec();

  public void setUp() throws Exception {
    super.setUp();
    // set the default codec, so adding test cases to this isn't fragile
    savedCodec = Codec.getDefault();
    Codec.setDefault(getCodec());
  }

  public void tearDown() throws Exception {
    Codec.setDefault(savedCodec); // restore
    super.tearDown();
  }
  
  public void testRandomStoredFields() throws IOException {
    Directory dir = newDirectory();
    Random rand = random();
    RandomIndexWriter w = new RandomIndexWriter(rand, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(_TestUtil.nextInt(rand, 5, 20)));
    //w.w.setUseCompoundFile(false);
    final int docCount = atLeast(200);
    final int fieldCount = _TestUtil.nextInt(rand, 1, 5);

    final List<Integer> fieldIDs = new ArrayList<Integer>();

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    Field idField = newField("id", "", customType);

    for(int i=0;i<fieldCount;i++) {
      fieldIDs.add(i);
    }

    final Map<String,Document> docs = new HashMap<String,Document>();

    if (VERBOSE) {
      System.out.println("TEST: build index docCount=" + docCount);
    }

    FieldType customType2 = new FieldType();
    customType2.setStored(true);
    for(int i=0;i<docCount;i++) {
      Document doc = new Document();
      doc.add(idField);
      final String id = ""+i;
      idField.setStringValue(id);
      docs.put(id, doc);
      if (VERBOSE) {
        System.out.println("TEST: add doc id=" + id);
      }

      for(int field: fieldIDs) {
        final String s;
        if (rand.nextInt(4) != 3) {
          s = _TestUtil.randomUnicodeString(rand, 1000);
          doc.add(newField("f"+field, s, customType2));
        } else {
          s = null;
        }
      }
      w.addDocument(doc);
      if (rand.nextInt(50) == 17) {
        // mixup binding of field name -> Number every so often
        Collections.shuffle(fieldIDs);
      }
      if (rand.nextInt(5) == 3 && i > 0) {
        final String delID = ""+rand.nextInt(i);
        if (VERBOSE) {
          System.out.println("TEST: delete doc id=" + delID);
        }
        w.deleteDocuments(new Term("id", delID));
        docs.remove(delID);
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: " + docs.size() + " docs in index; now load fields");
    }
    if (docs.size() > 0) {
      String[] idsList = docs.keySet().toArray(new String[docs.size()]);

      for(int x=0;x<2;x++) {
        IndexReader r = w.getReader();
        IndexSearcher s = newSearcher(r);

        if (VERBOSE) {
          System.out.println("TEST: cycle x=" + x + " r=" + r);
        }

        int num = atLeast(1000);
        for(int iter=0;iter<num;iter++) {
          String testID = idsList[rand.nextInt(idsList.length)];
          if (VERBOSE) {
            System.out.println("TEST: test id=" + testID);
          }
          TopDocs hits = s.search(new TermQuery(new Term("id", testID)), 1);
          assertEquals(1, hits.totalHits);
          Document doc = r.document(hits.scoreDocs[0].doc);
          Document docExp = docs.get(testID);
          for(int i=0;i<fieldCount;i++) {
            assertEquals("doc " + testID + ", field f" + fieldCount + " is wrong", docExp.get("f"+i),  doc.get("f"+i));
          }
        }
        r.close();
        w.forceMerge(1);
      }
    }
    w.close();
    dir.close();
  }
  
  // LUCENE-1727: make sure doc fields are stored in order
  public void testStoredFieldsOrder() throws Throwable {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();

    FieldType customType = new FieldType();
    customType.setStored(true);
    doc.add(newField("zzz", "a b c", customType));
    doc.add(newField("aaa", "a b c", customType));
    doc.add(newField("zzz", "1 2 3", customType));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    Document doc2 = r.document(0);
    Iterator<IndexableField> it = doc2.getFields().iterator();
    assertTrue(it.hasNext());
    Field f = (Field) it.next();
    assertEquals(f.name(), "zzz");
    assertEquals(f.stringValue(), "a b c");

    assertTrue(it.hasNext());
    f = (Field) it.next();
    assertEquals(f.name(), "aaa");
    assertEquals(f.stringValue(), "a b c");

    assertTrue(it.hasNext());
    f = (Field) it.next();
    assertEquals(f.name(), "zzz");
    assertEquals(f.stringValue(), "1 2 3");
    assertFalse(it.hasNext());
    r.close();
    w.close();
    d.close();
  }
  
  // LUCENE-1219
  public void testBinaryFieldOffsetLength() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    byte[] b = new byte[50];
    for(int i=0;i<50;i++)
      b[i] = (byte) (i+77);

    Document doc = new Document();
    Field f = new StoredField("binary", b, 10, 17);
    byte[] bx = f.binaryValue().bytes;
    assertTrue(bx != null);
    assertEquals(50, bx.length);
    assertEquals(10, f.binaryValue().offset);
    assertEquals(17, f.binaryValue().length);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader ir = DirectoryReader.open(dir);
    Document doc2 = ir.document(0);
    IndexableField f2 = doc2.getField("binary");
    b = f2.binaryValue().bytes;
    assertTrue(b != null);
    assertEquals(17, b.length, 17);
    assertEquals(87, b[0]);
    ir.close();
    dir.close();
  }
  
  public void testNumericField() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(500);
    final Number[] answers = new Number[numDocs];
    final NumericType[] typeAnswers = new NumericType[numDocs];
    for(int id=0;id<numDocs;id++) {
      Document doc = new Document();
      final Field nf;
      final Field sf;
      final Number answer;
      final NumericType typeAnswer;
      if (random().nextBoolean()) {
        // float/double
        if (random().nextBoolean()) {
          final float f = random().nextFloat();
          answer = Float.valueOf(f);
          nf = new FloatField("nf", f, Field.Store.NO);
          sf = new StoredField("nf", f);
          typeAnswer = NumericType.FLOAT;
        } else {
          final double d = random().nextDouble();
          answer = Double.valueOf(d);
          nf = new DoubleField("nf", d, Field.Store.NO);
          sf = new StoredField("nf", d);
          typeAnswer = NumericType.DOUBLE;
        }
      } else {
        // int/long
        if (random().nextBoolean()) {
          final int i = random().nextInt();
          answer = Integer.valueOf(i);
          nf = new IntField("nf", i, Field.Store.NO);
          sf = new StoredField("nf", i);
          typeAnswer = NumericType.INT;
        } else {
          final long l = random().nextLong();
          answer = Long.valueOf(l);
          nf = new LongField("nf", l, Field.Store.NO);
          sf = new StoredField("nf", l);
          typeAnswer = NumericType.LONG;
        }
      }
      doc.add(nf);
      doc.add(sf);
      answers[id] = answer;
      typeAnswers[id] = typeAnswer;
      FieldType ft = new FieldType(IntField.TYPE_STORED);
      ft.setNumericPrecisionStep(Integer.MAX_VALUE);
      doc.add(new IntField("id", id, ft));
      w.addDocument(doc);
    }
    final DirectoryReader r = w.getReader();
    w.close();
    
    assertEquals(numDocs, r.numDocs());

    for(AtomicReaderContext ctx : r.leaves()) {
      final AtomicReader sub = ctx.reader();
      final FieldCache.Ints ids = FieldCache.DEFAULT.getInts(sub, "id", false);
      for(int docID=0;docID<sub.numDocs();docID++) {
        final Document doc = sub.document(docID);
        final Field f = (Field) doc.getField("nf");
        assertTrue("got f=" + f, f instanceof StoredField);
        assertEquals(answers[ids.get(docID)], f.numericValue());
      }
    }
    r.close();
    dir.close();
  }

  public void testIndexedBit() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType onlyStored = new FieldType();
    onlyStored.setStored(true);
    doc.add(new Field("field", "value", onlyStored));
    doc.add(new StringField("field2", "value", Field.Store.YES));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();
    assertFalse(r.document(0).getField("field").fieldType().indexed());
    assertTrue(r.document(0).getField("field2").fieldType().indexed());
    r.close();
    dir.close();
  }
  
  public void testReadSkip() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
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
    iw.close();
    dir.close();
  }
  
  public void testEmptyDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
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
    
    iw.close();
    dir.close();
  }
  
  public void testConcurrentReads() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
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
    
    iw.close();
    dir.close();
  }
  
  private byte[] randomByteArray(int length, int max) {
    final byte[] result = new byte[length];
    for (int i = 0; i < length; ++i) {
      result[i] = (byte) random().nextInt(max);
    }
    return result;
  }
  
  public void testWriteReadMerge() throws IOException {
    // get another codec, other than the default: so we are merging segments across different codecs
    final Codec otherCodec;
    if ("SimpleText".equals(Codec.getDefault().getName())) {
      otherCodec = new Lucene42Codec();
    } else {
      otherCodec = new SimpleTextCodec();
    }
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
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
        if (iwConf.getCodec() == otherCodec) {
          iwConf.setCodec(Codec.getDefault());
        } else {
          iwConf.setCodec(otherCodec);
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
    
    iw.close();
    dir.close();
  }
  
  @Nightly
  public void testBigDocuments() throws IOException {
    // "big" as "much bigger than the chunk size"
    // for this test we force a FS dir
    Directory dir = newFSDirectory(_TestUtil.getTempDir(getClass().getSimpleName()));
    IndexWriterConfig iwConf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);

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
    iw.close();
    dir.close();
  }
}
