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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
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
    FieldTypes fieldTypes = d.getFieldTypes();
    fieldTypes.setMultiValued("f");
    final int numValues = random().nextInt(3);
    for (int i = 0; i < numValues; ++i) {
      d.addStoredString("f", TestUtil.randomSimpleString(random(), 100));
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

    for(int i=0;i<fieldCount;i++) {
      fieldIDs.add(i);
    }

    final Map<String,Document> docs = new HashMap<>();

    if (VERBOSE) {
      System.out.println("TEST: build index docCount=" + docCount);
    }

    for(int i=0;i<docCount;i++) {
      Document doc = w.newDocument();
      final String id = ""+i;
      doc.addAtom("id", id);
      docs.put(id, doc);
      if (VERBOSE) {
        System.out.println("TEST: add doc id=" + id);
      }

      for(int field: fieldIDs) {
        final String s;
        if (rand.nextInt(4) != 3) {
          s = TestUtil.randomUnicodeString(rand, 1000);
          doc.addStoredString("f"+field, s);
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
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = w.newDocument();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("zzz");

    doc.addStoredString("zzz", "a b c");
    doc.addStoredString("aaa", "a b c");
    doc.addStoredString("zzz", "1 2 3");
    w.addDocument(doc);
    IndexReader r = w.getReader();
    Document doc2 = r.document(0);
    Iterator<IndexableField> it = doc2.iterator();
    assertTrue(it.hasNext());
    IndexableField f = it.next();
    assertEquals(f.name(), "zzz");
    assertEquals(f.stringValue(), "a b c");

    assertTrue(it.hasNext());
    f = it.next();
    assertEquals(f.name(), "aaa");
    assertEquals(f.stringValue(), "a b c");

    assertTrue(it.hasNext());
    f = it.next();
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

    Document doc = w.newDocument();
    doc.addBinary("binary", new BytesRef(b, 10, 17));
    BytesRef binaryValue = doc.getBinary("binary");
    assertEquals(10, binaryValue.offset);
    assertEquals(17, binaryValue.length);
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
  
  public void testIndexedBit() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = w.newDocument();
    doc.addStoredString("field", "value");
    doc.addAtom("field2", "value");
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();
    assertEquals(IndexOptions.NONE, r.document(0).getField("field").fieldType().indexOptions());
    assertNotNull(r.document(0).getField("field2").fieldType().indexOptions());
    r.close();
    dir.close();
  }
  
  public void testReadSkip() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
    final String string = TestUtil.randomSimpleString(random(), 50);
    final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    final long l = random().nextBoolean() ? random().nextInt(42) : random().nextLong();
    final int i = random().nextBoolean() ? random().nextInt(42) : random().nextInt();
    final float f = random().nextFloat();
    final double d = random().nextDouble();

    for (int k = 0; k < 100; ++k) {
      Document doc = iw.newDocument();
      doc.addStoredBinary("bytes", bytes);
      doc.addStoredString("string", string);
      doc.addInt("int", i);
      doc.addLong("long", l);
      doc.addFloat("float", f);
      doc.addDouble("double", d);
      iw.w.addDocument(doc);
    }
    iw.commit();

    final DirectoryReader reader = DirectoryReader.open(dir);
    final int docID = random().nextInt(100);
    for (String fldName : new String[] {"bytes", "string", "int", "long", "float", "double"}) {
      final Document sDoc = reader.document(docID, Collections.singleton(fldName));

      final IndexableField sField = sDoc.getField(fldName);
      switch (fldName) {
      case "bytes":
        assertEquals(new BytesRef(bytes), sField.binaryValue());
        break;
      case "string":
        assertEquals(string, sField.stringValue());
        break;
      case "int":
        assertEquals(i, sField.numericValue().intValue());
        break;
      case "long":
        assertEquals(l, sField.numericValue().longValue());
        break;
      case "float":
        assertEquals(f, sField.numericValue().floatValue(), 0.0f);
        break;
      case "double":
        assertEquals(d, sField.numericValue().doubleValue(), 0.0);
        break;
      default:
        assert false;
      }
    }
    reader.close();
    iw.close();
    dir.close();
  }
  
  public void testEmptyDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
    // make sure that the fact that documents might be empty is not a problem
    final Document emptyDoc = iw.newDocument();
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
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    
    // make sure the readers are properly cloned
    final int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = iw.newDocument();
      doc.addAtom("fld", "" + i);
      iw.addDocument(doc);
    }
    iw.commit();

    final DirectoryReader rd = DirectoryReader.open(dir);
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
      otherCodec = TestUtil.getDefaultCodec();
    } else {
      otherCodec = new SimpleTextCodec();
    }
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    FieldTypes fieldTypes = iw.getFieldTypes();
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

    for (int i = 0; i < data.length; ++i) {
      Document doc = iw.newDocument();
      doc.addInt("id", i);
      for (int j = 0; j < data[i].length; ++j) {
        doc.addStoredBinary("bytes" + j, new BytesRef(data[i][j]));
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
      iw.deleteDocuments(new ConstantScoreQuery(fieldTypes.newIntRangeFilter("id", min, true, max, false)));
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
        final BytesRef arr2Ref = doc.getBinary("bytes" + j);
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

  /** A dummy filter reader that reverse the order of documents in stored fields. */
  private static class DummyFilterLeafReader extends FilterLeafReader {

    public DummyFilterLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
      super.document(maxDoc() - 1 - docID, visitor);
    }

  }

  private static class DummyFilterDirectoryReader extends FilterDirectoryReader {

    public DummyFilterDirectoryReader(DirectoryReader in) {
      super(in, new SubReaderWrapper() {
        @Override
        public LeafReader wrap(LeafReader reader) {
          return new DummyFilterLeafReader(reader);
        }
      });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
      return new DummyFilterDirectoryReader(in);
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
      Document doc = w.newDocument();
      doc.addAtom("to_delete", random().nextBoolean() ? "yes" : "no");
      doc.addStoredInt("id", i);
      doc.addStoredInt("i", random().nextInt(50));
      doc.addStoredLong("l", random().nextLong());
      doc.addStoredDouble("d", random().nextDouble());
      doc.addStoredFloat("f", random().nextFloat());
      doc.addStoredString("s", RandomPicks.randomFrom(random(), stringValues));
      doc.addStoredBinary("b", new BytesRef(RandomPicks.randomFrom(random(), stringValues)));
      docs[i] = doc;
      w.addDocument(doc);
    }
    if (random().nextBoolean()) {
      w.deleteDocuments(new Term("to_delete", "yes"));
    }
    w.commit();
    w.close();
    
    DirectoryReader reader = new DummyFilterDirectoryReader(DirectoryReader.open(dir));
    
    Directory dir2 = newDirectory();
    w = new RandomIndexWriter(random(), dir2);
    w.addIndexes(reader);
    reader.close();
    dir.close();

    reader = w.getReader();
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
    // "big" as "much bigger than the chunk size"
    // for this test we force a FS dir
    // we can't just use newFSDirectory, because this test doesn't really index anything.
    // so if we get NRTCachingDir+SimpleText, we make massive stored fields and OOM (LUCENE-4484)
    Directory dir = new MockDirectoryWrapper(random(), new MMapDirectory(createTempDir("testBigDocuments")));
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomInts.randomIntBetween(random(), 2, 30));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.setMultiValued("fld");

    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper) dir).setThrottling(Throttling.NEVER);
    }
    final int numFields = RandomInts.randomIntBetween(random(), 500000, 1000000);
    final int numDocs = atLeast(5);
    Document[] docs = new Document[numDocs];
    for (int i = 0; i < numDocs; ++i) {
      Document doc = iw.newDocument();
      int x = random().nextInt(3);
      doc.addAtom("id", "" + i);
      if (x == 1) {
        for (int j = 0; j < numFields; ++j) {
          doc.addStoredBinary("fld", randomByteArray(random().nextInt(10), 256));
        }
      } else {
        doc.addStoredBinary("fld", randomByteArray(RandomInts.randomIntBetween(random(), 1000000, 5000000), 2));
      }
      iw.addDocument(doc);
      if (random().nextInt(numDocs) == 0) {
        iw.commit();
      }
      docs[i] = doc;
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
      final List<IndexableField> fieldValues = doc.getFields("fld");
      assertEquals(docs[i].getFields("fld").size(), fieldValues.size());
      if (fieldValues.size() > 0) {
        assertEquals(docs[i].getFields("fld").get(0).binaryValue(), fieldValues.get(0).binaryValue());
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
      Document doc = w.newDocument();
      doc.addAtom("id", Integer.toString(i));
      doc.addAtom("f", TestUtil.randomSimpleString(random()));
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
      Document doc = iw.newDocument();
      for (int j = 0; j < 10; j++) {
        // add fields where name=value (e.g. 3=3) so we can detect if stuff gets screwed up.
        doc.addAtom(Integer.toString(j), Integer.toString(j));
      }
      for (int j = 0; j < 10; j++) {
        iw.addDocument(doc);
      }
      
      DirectoryReader reader = DirectoryReader.open(iw, true);
      // mix up fields explicitly
      if (random().nextBoolean()) {
        reader = new MismatchedDirectoryReader(reader, random());
      }
      dirs[i] = newDirectory();
      IndexWriter adder = new IndexWriter(dirs[i], new IndexWriterConfig(null));
      adder.addIndexes(reader);
      adder.commit();
      adder.close();
      
      IOUtils.close(reader, iw, dir);
    }
    
    Directory everything = newDirectory();
    IndexWriter iw = new IndexWriter(everything, new IndexWriterConfig(null));
    iw.addIndexes(dirs);
    iw.forceMerge(1);
    
    LeafReader ir = getOnlySegmentReader(DirectoryReader.open(iw, true));
    for (int i = 0; i < ir.maxDoc(); i++) {
      Document doc = ir.document(i);
      assertEquals(10, doc.getFields().size());
      for (int j = 0; j < 10; j++) {
        assertEquals(Integer.toString(j), doc.getString(Integer.toString(j)));
      }
    }

    IOUtils.close(iw, ir, everything);
    IOUtils.close(dirs);
  }
}
