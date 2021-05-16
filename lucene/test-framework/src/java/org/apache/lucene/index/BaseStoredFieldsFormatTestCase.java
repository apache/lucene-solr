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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

/**
 * Base class aiming at testing {@link StoredFieldsFormat stored fields formats}.
 * To test a new format, all you need is to register a new {@link Codec} which
 * uses it and extend this class and override {@link #getCodec()}.
 * @lucene.experimental
 */
public abstract class BaseStoredFieldsFormatTestCase extends BaseIndexFileFormatTestCase {

  @Override
  protected void addRandomFields(Document d) {
    final int numValues = random().nextInt(3);
    for (int i = 0; i < numValues; ++i) {
      d.add(new StoredField("f", TestUtil.randomSimpleString(random(), 100)));
    }
  }

  public void testRandomStoredFields() throws IOException {
    Directory dir = newDirectory();
    Random rand = random();
    RandomIndexWriter w = new RandomIndexWriter(rand, dir, newIndexWriterConfig(new MockAnalyzer(random())).setMaxBufferedDocs(TestUtil.nextInt(rand, 5, 20)));
    //w.w.setNoCFSRatio(0.0);
    final int docCount = atLeast(200);
    final int fieldCount = TestUtil.nextInt(rand, 1, 5);

    final List<Integer> fieldIDs = new ArrayList<>();

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    Field idField = newField("id", "", customType);

    for(int i=0;i<fieldCount;i++) {
      fieldIDs.add(i);
    }

    final Map<String,Document> docs = new HashMap<>();

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
          s = TestUtil.randomUnicodeString(rand, 1000);
          doc.add(newField("f"+field, s, customType2));
        } else {
          s = null;
        }
      }
      w.addDocument(doc);
      if (rand.nextInt(50) == 17) {
        // mixup binding of field name -> Number every so often
        Collections.shuffle(fieldIDs, random());
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
        DirectoryReader r = maybeWrapWithMergingReader(w.getReader());
        IndexSearcher s = newSearcher(r);

        if (VERBOSE) {
          System.out.println("TEST: cycle x=" + x + " r=" + r);
        }

        int num = atLeast(100);
        for(int iter=0;iter<num;iter++) {
          String testID = idsList[rand.nextInt(idsList.length)];
          if (VERBOSE) {
            System.out.println("TEST: test id=" + testID);
          }
          TopDocs hits = s.search(new TermQuery(new Term("id", testID)), 1);
          assertEquals(1, hits.totalHits.value);
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
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();

    FieldType customType = new FieldType();
    customType.setStored(true);
    doc.add(newField("zzz", "a b c", customType));
    doc.add(newField("aaa", "a b c", customType));
    doc.add(newField("zzz", "1 2 3", customType));
    w.addDocument(doc);
    IndexReader r = maybeWrapWithMergingReader(w.getReader());
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
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
    final Class<?>[] typeAnswers = new Class<?>[numDocs];
    for(int id=0;id<numDocs;id++) {
      Document doc = new Document();
      final Field nf;
      final Number answer;
      final Class<?> typeAnswer;
      if (random().nextBoolean()) {
        // float/double
        if (random().nextBoolean()) {
          final float f = random().nextFloat();
          answer = Float.valueOf(f);
          nf = new StoredField("nf", f);
          typeAnswer = Float.class;
        } else {
          final double d = random().nextDouble();
          answer = Double.valueOf(d);
          nf = new StoredField("nf", d);
          typeAnswer = Double.class;
        }
      } else {
        // int/long
        if (random().nextBoolean()) {
          final int i = random().nextInt();
          answer = Integer.valueOf(i);
          nf = new StoredField("nf", i);
          typeAnswer = Integer.class;
        } else {
          final long l = random().nextLong();
          answer = Long.valueOf(l);
          nf = new StoredField("nf", l);
          typeAnswer = Long.class;
        }
      }
      doc.add(nf);
      answers[id] = answer;
      typeAnswers[id] = typeAnswer;
      doc.add(new StoredField("id", id));
      doc.add(new IntPoint("id", id));
      doc.add(new NumericDocValuesField("id", id));
      w.addDocument(doc);
    }
    final DirectoryReader r = maybeWrapWithMergingReader(w.getReader());
    w.close();
    
    assertEquals(numDocs, r.numDocs());

    for(LeafReaderContext ctx : r.leaves()) {
      final LeafReader sub = ctx.reader();
      final NumericDocValues ids = DocValues.getNumeric(sub, "id");
      for(int docID=0;docID<sub.numDocs();docID++) {
        final Document doc = sub.document(docID);
        final Field f = (Field) doc.getField("nf");
        assertTrue("got f=" + f, f instanceof StoredField);
        assertEquals(docID, ids.nextDoc());
        assertEquals(answers[(int) ids.longValue()], f.numericValue());
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
    IndexReader r = maybeWrapWithMergingReader(w.getReader());
    w.close();
    assertEquals(IndexOptions.NONE, r.document(0).getField("field").fieldType().indexOptions());
    assertNotNull(r.document(0).getField("field2").fieldType().indexOptions());
    r.close();
    dir.close();
  }
  
  public void testReadSkip() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomNumbers.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
    FieldType ft = new FieldType();
    ft.setStored(true);
    ft.freeze();

    final String string = TestUtil.randomSimpleString(random(), 50);
    final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    final long l = random().nextBoolean() ? random().nextInt(42) : random().nextLong();
    final int i = random().nextBoolean() ? random().nextInt(42) : random().nextInt();
    final float f = random().nextFloat();
    final double d = random().nextDouble();

    List<Field> fields = Arrays.asList(
        new Field("bytes", bytes, ft),
        new Field("string", string, ft),
        new StoredField("long", l),
        new StoredField("int", i),
        new StoredField("float", f),
        new StoredField("double", d)
    );

    for (int k = 0; k < 100; ++k) {
      Document doc = new Document();
      for (Field fld : fields) {
        doc.add(fld);
      }
      iw.w.addDocument(doc);
    }
    iw.commit();

    final DirectoryReader reader = maybeWrapWithMergingReader(DirectoryReader.open(dir));
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
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomNumbers.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
    // make sure that the fact that documents might be empty is not a problem
    final Document emptyDoc = new Document();
    final int numDocs = random().nextBoolean() ? 1 : atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      iw.addDocument(emptyDoc);
    }
    iw.commit();
    final DirectoryReader rd = maybeWrapWithMergingReader(DirectoryReader.open(dir));
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
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomNumbers.randomIntBetween(random(), 2, 30));
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

    final DirectoryReader rd = maybeWrapWithMergingReader(DirectoryReader.open(dir));
    final IndexSearcher searcher = new IndexSearcher(rd);
    final int concurrentReads = atLeast(5);
    final int readsPerThread = atLeast(50);
    final List<Thread> readThreads = new ArrayList<>();
    final AtomicReference<Exception> ex = new AtomicReference<>();
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
              if (topDocs.totalHits.value != 1) {
                throw new IllegalStateException("Expected 1 hit, got " + topDocs.totalHits.value);
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
      otherCodec = TestUtil.getDefaultCodec();
    } else {
      otherCodec = new SimpleTextCodec();
    }
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomNumbers.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);

    final int docCount = atLeast(200);
    final byte[][][] data = new byte [docCount][][];
    for (int i = 0; i < docCount; ++i) {
      final int fieldCount = rarely()
          ? RandomNumbers.randomIntBetween(random(), 1, 500)
          : RandomNumbers.randomIntBetween(random(), 1, 5);
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
    type.setIndexOptions(IndexOptions.NONE);
    type.freeze();
    IntPoint id = new IntPoint("id", 0);
    StoredField idStored = new StoredField("id", 0);
    for (int i = 0; i < data.length; ++i) {
      Document doc = new Document();
      doc.add(id);
      doc.add(idStored);
      id.setIntValue(i);
      idStored.setIntValue(i);
      for (int j = 0; j < data[i].length; ++j) {
        Field f = new Field("bytes" + j, data[i][j], type);
        doc.add(f);
      }
      iw.w.addDocument(doc);
      if (random().nextBoolean() && (i % (data.length / 10) == 0)) {
        iw.w.close();
        IndexWriterConfig iwConfNew = newIndexWriterConfig(new MockAnalyzer(random()));
        // test merging against a non-compressing codec
        if (iwConf.getCodec() == otherCodec) {
          iwConfNew.setCodec(Codec.getDefault());
        } else {
          iwConfNew.setCodec(otherCodec);
        }
        iwConf = iwConfNew;
        iw = new RandomIndexWriter(random(), dir, iwConf);
      }
    }

    for (int i = 0; i < 10; ++i) {
      final int min = random().nextInt(data.length);
      final int max = min + random().nextInt(20);
      iw.deleteDocuments(IntPoint.newRangeQuery("id", min, max-1));
    }

    iw.forceMerge(2); // force merges with deletions

    iw.commit();

    final DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(dir));
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
        final byte[] arr2 = BytesRef.deepCopyOf(arr2Ref).bytes;
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

  /** A dummy filter reader that reverse the order of documents in stored fields. */
  private static class DummyFilterLeafReader extends FilterLeafReader {

    public DummyFilterLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
      super.document(maxDoc() - 1 - docID, visitor);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }

  }

  private static class DummyFilterDirectoryReader extends FilterDirectoryReader {

    public DummyFilterDirectoryReader(DirectoryReader in) throws IOException {
      super(in, new SubReaderWrapper() {
        @Override
        public LeafReader wrap(LeafReader reader) {
          return new DummyFilterLeafReader(reader);
        }
      });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new DummyFilterDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
    
  }

  public void testMergeFilterReader() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(200);
    final String[] stringValues = new String[10];
    for (int i = 0; i < stringValues.length; ++i) {
      stringValues[i] = RandomStrings.randomRealisticUnicodeOfLength(random(), 10);
    }
    Document[] docs = new Document[numDocs];
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("to_delete", random().nextBoolean() ? "yes" : "no", Store.NO));
      doc.add(new StoredField("id", i));
      doc.add(new StoredField("i", random().nextInt(50)));
      doc.add(new StoredField("l", random().nextLong()));
      doc.add(new StoredField("d", random().nextDouble()));
      doc.add(new StoredField("f", random().nextFloat()));
      doc.add(new StoredField("s", RandomPicks.randomFrom(random(), stringValues)));
      doc.add(new StoredField("b", new BytesRef(RandomPicks.randomFrom(random(), stringValues))));
      docs[i] = doc;
      w.addDocument(doc);
    }
    if (random().nextBoolean()) {
      w.deleteDocuments(new Term("to_delete", "yes"));
    }
    w.commit();
    w.close();
    
    DirectoryReader reader = new DummyFilterDirectoryReader(maybeWrapWithMergingReader(DirectoryReader.open(dir)));
    
    Directory dir2 = newDirectory();
    w = new RandomIndexWriter(random(), dir2);
    TestUtil.addIndexesSlowly(w.w, reader);
    reader.close();
    dir.close();

    reader = maybeWrapWithMergingReader(w.getReader());
    for (int i = 0; i < reader.maxDoc(); ++i) {
      final Document doc = reader.document(i);
      final int id = doc.getField("id").numericValue().intValue();
      final Document expected = docs[id];
      assertEquals(expected.get("s"), doc.get("s"));
      assertEquals(expected.getField("i").numericValue(), doc.getField("i").numericValue());
      assertEquals(expected.getField("l").numericValue(), doc.getField("l").numericValue());
      assertEquals(expected.getField("d").numericValue(), doc.getField("d").numericValue());
      assertEquals(expected.getField("f").numericValue(), doc.getField("f").numericValue());
      assertEquals(expected.getField("b").binaryValue(), doc.getField("b").binaryValue());
    }

    reader.close();
    w.close();
    TestUtil.checkIndex(dir2);
    dir2.close();
  }

  @Nightly
  public void testBigDocuments() throws IOException {
    assumeWorkingMMapOnWindows();
    
    // "big" as "much bigger than the chunk size"
    // for this test we force a FS dir
    // we can't just use newFSDirectory, because this test doesn't really index anything.
    // so if we get NRTCachingDir+SimpleText, we make massive stored fields and OOM (LUCENE-4484)
    Directory dir = new MockDirectoryWrapper(random(), new MMapDirectory(createTempDir("testBigDocuments")));
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomNumbers.randomIntBetween(random(), 2, 30));
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
    onlyStored.setIndexOptions(IndexOptions.NONE);

    final Field smallField = new Field("fld", randomByteArray(random().nextInt(10), 256), onlyStored);
    final int numFields = RandomNumbers.randomIntBetween(random(), 500000, 1000000);
    for (int i = 0; i < numFields; ++i) {
      bigDoc1.add(smallField);
    }

    final Field bigField = new Field("fld", randomByteArray(RandomNumbers.randomIntBetween(random(), 1000000, 5000000), 2), onlyStored);
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
    final DirectoryReader rd = maybeWrapWithMergingReader(DirectoryReader.open(dir));
    final IndexSearcher searcher = new IndexSearcher(rd);
    for (int i = 0; i < numDocs; ++i) {
      final Query query = new TermQuery(new Term("id", "" + i));
      final TopDocs topDocs = searcher.search(query, 1);
      assertEquals("" + i, 1, topDocs.totalHits.value);
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

  public void testBulkMergeWithDeletes() throws IOException {
    final int numDocs = atLeast(200);
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE));
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new StoredField("f", TestUtil.randomSimpleString(random())));
      w.addDocument(doc);
    }
    final int deleteCount = TestUtil.nextInt(random(), 5, numDocs);
    for (int i = 0; i < deleteCount; ++i) {
      final int id = random().nextInt(numDocs);
      w.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    w.commit();
    w.close();
    w = new RandomIndexWriter(random(), dir);
    w.forceMerge(TestUtil.nextInt(random(), 1, 3));
    w.commit();
    w.close();
    TestUtil.checkIndex(dir);
    dir.close();
  }

  /** mix up field numbers, merge, and check that data is correct */
  public void testMismatchedFields() throws Exception {
    Directory dirs[] = new Directory[10];
    for (int i = 0; i < dirs.length; i++) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(null);
      IndexWriter iw = new IndexWriter(dir, iwc);
      Document doc = new Document();
      for (int j = 0; j < 10; j++) {
        // add fields where name=value (e.g. 3=3) so we can detect if stuff gets screwed up.
        doc.add(new StringField(Integer.toString(j), Integer.toString(j), Field.Store.YES));
      }
      for (int j = 0; j < 10; j++) {
        iw.addDocument(doc);
      }
      
      DirectoryReader reader = maybeWrapWithMergingReader(DirectoryReader.open(iw));
      // mix up fields explicitly
      if (random().nextBoolean()) {
        reader = new MismatchedDirectoryReader(reader, random());
      }
      dirs[i] = newDirectory();
      IndexWriter adder = new IndexWriter(dirs[i], new IndexWriterConfig(null));
      TestUtil.addIndexesSlowly(adder, reader);
      adder.commit();
      adder.close();
      
      IOUtils.close(reader, iw, dir);
    }
    
    Directory everything = newDirectory();
    IndexWriter iw = new IndexWriter(everything, new IndexWriterConfig(null));
    iw.addIndexes(dirs);
    iw.forceMerge(1);
    
    LeafReader ir = getOnlyLeafReader(DirectoryReader.open(iw));
    for (int i = 0; i < ir.maxDoc(); i++) {
      Document doc = ir.document(i);
      assertEquals(10, doc.getFields().size());
      for (int j = 0; j < 10; j++) {
        assertEquals(Integer.toString(j), doc.get(Integer.toString(j)));
      }
    }

    IOUtils.close(iw, ir, everything);
    IOUtils.close(dirs);
  }

  public void testRandomStoredFieldsWithIndexSort() throws Exception {
    final SortField[] sortFields;
    if (random().nextBoolean()) {
      sortFields =
          new SortField[]{
              new SortField("sort-1", SortField.Type.LONG),
              new SortField("sort-2", SortField.Type.INT)
          };
    } else {
      sortFields = new SortField[]{new SortField("sort-1", SortField.Type.LONG)};
    }
    List<String> storedFields = new ArrayList<>();
    int numFields = TestUtil.nextInt(random(), 1, 10);
    for (int i = 0; i < numFields; i++) {
      storedFields.add("f-" + i);
    }
    FieldType storeType = new FieldType(TextField.TYPE_STORED);
    storeType.setStored(true);
    Function<String, Document> documentFactory =
        id -> {
          Document doc = new Document();
          doc.add(new StringField("id", id, random().nextBoolean() ? Store.YES : Store.NO));
          if (random().nextInt(100) <= 5) {
            Collections.shuffle(storedFields, random());
          }
          for (String fieldName : storedFields) {
            if (random().nextBoolean()) {
              String s = TestUtil.randomUnicodeString(random(), 100);
              doc.add(newField(fieldName, s, storeType));
            }
          }
          for (SortField sortField : sortFields) {
            doc.add(
                new NumericDocValuesField(
                    sortField.getField(), TestUtil.nextInt(random(), 0, 10000)));
          }
          return doc;
        };

    Map<String, Document> docs = new HashMap<>();
    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      String id = Integer.toString(i);
      docs.put(id, documentFactory.apply(id));
    }

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 5, 20));
    iwc.setIndexSort(new Sort(sortFields));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    List<String> addedIds = new ArrayList<>();
    Runnable verifyStoreFields =
        () -> {
          if (addedIds.isEmpty()) {
            return;
          }
          try (DirectoryReader reader = maybeWrapWithMergingReader(iw.getReader())) {
            IndexSearcher searcher = new IndexSearcher(reader);
            int iters = TestUtil.nextInt(random(), 1, 10);
            for (int i = 0; i < iters; i++) {
              String testID = addedIds.get(random().nextInt(addedIds.size()));
              if (VERBOSE) {
                System.out.println("TEST: test id=" + testID);
              }
              TopDocs hits = searcher.search(new TermQuery(new Term("id", testID)), 1);
              assertEquals(1, hits.totalHits.value);
              List<IndexableField> expectedFields =
                  docs.get(testID).getFields().stream()
                      .filter(f -> f.fieldType().stored())
                      .collect(Collectors.toList());
              Document actualDoc = reader.document(hits.scoreDocs[0].doc);
              assertEquals(expectedFields.size(), actualDoc.getFields().size());
              for (IndexableField expectedField : expectedFields) {
                IndexableField[] actualFields = actualDoc.getFields(expectedField.name());
                assertEquals(1, actualFields.length);
                assertEquals(expectedField.stringValue(), actualFields[0].stringValue());
              }
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
    final List<String> ids = new ArrayList<>(docs.keySet());
    Collections.shuffle(ids, random());
    for (String id : ids) {
      if (random().nextInt(100) < 5) {
        // add via foreign reader
        IndexWriterConfig otherIwc = newIndexWriterConfig();
        otherIwc.setIndexSort(new Sort(sortFields));
        try (Directory otherDir = newDirectory();
             RandomIndexWriter otherIw = new RandomIndexWriter(random(), otherDir, otherIwc)) {
          otherIw.addDocument(docs.get(id));
          try (DirectoryReader otherReader = otherIw.getReader()) {
            TestUtil.addIndexesSlowly(iw.w, otherReader);
          }
        }
      } else {
        // add normally
        iw.addDocument(docs.get(id));
      }
      addedIds.add(id);
      if (random().nextInt(100) < 5) {
        String deletingId = addedIds.remove(random().nextInt(addedIds.size()));
        if (random().nextBoolean()) {
          iw.deleteDocuments(new TermQuery(new Term("id", deletingId)));
          addedIds.remove(deletingId);
        } else {
          final Document newDoc = documentFactory.apply(deletingId);
          docs.put(deletingId, newDoc);
          iw.updateDocument(new Term("id", deletingId), newDoc);
        }
      }
      if (random().nextInt(100) < 5) {
        verifyStoreFields.run();
      }
      if (random().nextInt(100) < 2) {
        iw.forceMerge(TestUtil.nextInt(random(), 1, 3));
      }
    }
    verifyStoreFields.run();
    iw.forceMerge(TestUtil.nextInt(random(), 1, 3));
    verifyStoreFields.run();
    IOUtils.close(iw, dir);
  }

}
