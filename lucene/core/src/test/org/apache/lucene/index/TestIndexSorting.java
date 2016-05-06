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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

// nocommit test tie break
// nocommit test multiple sorts
// nocommit test update dvs

// nocommit test EarlyTerminatingCollector

// nocommit must test all supported SortField.Type

public class TestIndexSorting extends LuceneTestCase {

  public void testSortOnMerge(boolean withDeletes) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    final int numDocs = atLeast(1000);
    final FixedBitSet deleted = new FixedBitSet(numDocs);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("foo", random().nextInt(20)));
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new NumericDocValuesField("id", i));
      w.addDocument(doc);
      if (random().nextInt(5) == 0) {
        w.getReader().close();
      } else if (random().nextInt(30) == 0) {
        w.forceMerge(2);
      } else if (random().nextInt(4) == 0) {
        final int id = TestUtil.nextInt(random(), 0, i);
        deleted.set(id);
        w.deleteDocuments(new Term("id", Integer.toString(id)));
      }
    }

    // Check that segments are sorted
    DirectoryReader reader = w.getReader();
    for (LeafReaderContext ctx : reader.leaves()) {
      final SegmentReader leaf = (SegmentReader) ctx.reader();
      SegmentInfo info = leaf.getSegmentInfo().info;
      switch (info.getDiagnostics().get(IndexWriter.SOURCE)) {
        case IndexWriter.SOURCE_FLUSH:
          assertNull(info.getIndexSort());
          break;
        case IndexWriter.SOURCE_MERGE:
          assertEquals(indexSort, info.getIndexSort());
          final NumericDocValues values = leaf.getNumericDocValues("foo");
          long previous = Long.MIN_VALUE;
          for (int i = 0; i < leaf.maxDoc(); ++i) {
            final long value = values.get(i);
            assertTrue(value >= previous);
            previous = value;
          }
          break;
        default:
          fail();
      }
    }

    // Now check that the index is consistent
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < numDocs; ++i) {
      TermQuery termQuery = new TermQuery(new Term("id", Integer.toString(i)));
      final TopDocs topDocs = searcher.search(termQuery, 1);
      if (deleted.get(i)) {
        assertEquals(0, topDocs.totalHits);
      } else {
        assertEquals(1, topDocs.totalHits);
        assertEquals(i, MultiDocValues.getNumericValues(reader, "id").get(topDocs.scoreDocs[0].doc));
        Document document = reader.document(topDocs.scoreDocs[0].doc);
        assertEquals(Integer.toString(i), document.get("id"));
      }
    }

    reader.close();
    w.close();
    dir.close();
  }

  public void testSortOnMerge() throws IOException {
    testSortOnMerge(false);
  }

  public void testSortOnMergeWithDeletes() throws IOException {
    testSortOnMerge(true);
  }

  static class UpdateRunnable implements Runnable {

    private final int numDocs;
    private final Random random;
    private final AtomicInteger updateCount;
    private final IndexWriter w;
    private final Map<Integer, Long> values;
    private final CountDownLatch latch;

    UpdateRunnable(int numDocs, Random random, CountDownLatch latch, AtomicInteger updateCount, IndexWriter w, Map<Integer, Long> values) {
      this.numDocs = numDocs;
      this.random = random;
      this.latch = latch;
      this.updateCount = updateCount;
      this.w = w;
      this.values = values;
    }

    @Override
    public void run() {
      try {
        latch.await();
        while (updateCount.decrementAndGet() >= 0) {
          final int id = random.nextInt(numDocs);
          final long value = random.nextInt(20);
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(id), Store.NO));
          doc.add(new NumericDocValuesField("foo", value));

          synchronized (values) {
            w.updateDocument(new Term("id", Integer.toString(id)), doc);
            values.put(id, value);
          }

          switch (random.nextInt(10)) {
            case 0:
            case 1:
              // reopen
              DirectoryReader.open(w).close();
              break;
            case 2:
              w.forceMerge(3);
              break;
          }
        }
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

  }

  // There is tricky logic to resolve deletes that happened while merging
  public void testConcurrentUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Map<Integer, Long> values = new HashMap<>();

    final int numDocs = atLeast(100);
    Thread[] threads = new Thread[2];
    final AtomicInteger updateCount = new AtomicInteger(atLeast(1000));
    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < threads.length; ++i) {
      Random r = new Random(random().nextLong());
      threads[i] = new Thread(new UpdateRunnable(numDocs, r, latch, updateCount, w, values));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    latch.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < numDocs; ++i) {
      final TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(i))), 1);
      if (values.containsKey(i) == false) {
        assertEquals(0, topDocs.totalHits);
      } else {
        assertEquals(1, topDocs.totalHits);
        assertEquals(values.get(i).longValue(), MultiDocValues.getNumericValues(reader, "foo").get(topDocs.scoreDocs[0].doc));
      }
    }
    reader.close();
    w.close();
    dir.close();
  }

  static class DVUpdateRunnable implements Runnable {

    private final int numDocs;
    private final Random random;
    private final AtomicInteger updateCount;
    private final IndexWriter w;
    private final Map<Integer, Long> values;
    private final CountDownLatch latch;

    DVUpdateRunnable(int numDocs, Random random, CountDownLatch latch, AtomicInteger updateCount, IndexWriter w, Map<Integer, Long> values) {
      this.numDocs = numDocs;
      this.random = random;
      this.latch = latch;
      this.updateCount = updateCount;
      this.w = w;
      this.values = values;
    }

    @Override
    public void run() {
      try {
        latch.await();
        while (updateCount.decrementAndGet() >= 0) {
          final int id = random.nextInt(numDocs);
          final long value = random.nextInt(20);

          synchronized (values) {
            w.updateDocValues(new Term("id", Integer.toString(id)), new NumericDocValuesField("foo", value));
            values.put(id, value);
          }

          switch (random.nextInt(10)) {
            case 0:
            case 1:
              // reopen
              DirectoryReader.open(w).close();
              break;
            case 2:
              w.forceMerge(3);
              break;
          }
        }
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

  }

  // There is tricky logic to resolve dv updates that happened while merging
  public void testConcurrentDVUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Map<Integer, Long> values = new HashMap<>();

    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.NO));
      doc.add(new NumericDocValuesField("foo", -1));
      w.addDocument(doc);
      values.put(i, -1L);
    }
    Thread[] threads = new Thread[2];
    final AtomicInteger updateCount = new AtomicInteger(atLeast(1000));
    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < threads.length; ++i) {
      Random r = new Random(random().nextLong());
      threads[i] = new Thread(new DVUpdateRunnable(numDocs, r, latch, updateCount, w, values));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    latch.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < numDocs; ++i) {
      final TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(i))), 1);
      assertEquals(1, topDocs.totalHits);
      assertEquals(values.get(i).longValue(), MultiDocValues.getNumericValues(reader, "foo").get(topDocs.scoreDocs[0].doc));
    }
    reader.close();
    w.close();
    dir.close();
  }

  public void testAddIndexes(boolean withDeletes) throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.NO));
      doc.add(new NumericDocValuesField("foo", random().nextInt(20)));
      w.addDocument(doc);
    }
    if (withDeletes) {
      for (int i = random().nextInt(5); i < numDocs; i += TestUtil.nextInt(random(), 1, 5)) {
        w.deleteDocuments(new Term("id", Integer.toString(i)));
      }
    }
    final IndexReader reader = w.getReader();

    Directory dir2 = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w2 = new IndexWriter(dir2, iwc);

    CodecReader[] codecReaders = new CodecReader[reader.leaves().size()];
    for (int i = 0; i < codecReaders.length; ++i) {
      codecReaders[i] = (CodecReader) reader.leaves().get(i).reader();
    }
    w2.addIndexes(codecReaders);
    final IndexReader reader2 = w2.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    final IndexSearcher searcher2 = newSearcher(reader2);
    for (int i = 0; i < numDocs; ++i) {
      Query query = new TermQuery(new Term("id", Integer.toString(i)));
      final TopDocs topDocs = searcher.search(query, 1);
      final TopDocs topDocs2 = searcher2.search(query, 1);
      assertEquals(topDocs.totalHits, topDocs2.totalHits);
      if (topDocs.totalHits == 1) {
        assertEquals(
            MultiDocValues.getNumericValues(reader, "foo").get(topDocs.scoreDocs[0].doc),
            MultiDocValues.getNumericValues(reader2, "foo").get(topDocs2.scoreDocs[0].doc));
      }
    }

    IOUtils.close(reader, reader2, w, w2, dir, dir2);
  }

  public void testAddIndexes() throws Exception {
    testAddIndexes(false);
  }

  public void testAddIndexesWithDeletions() throws Exception {
    testAddIndexes(true);
  }

  public void testBadSort() throws Exception {
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      iwc.setIndexSort(Sort.RELEVANCE);
    });
    assertEquals("invalid SortField type: must be one of [STRING, INT, FLOAT, LONG, DOUBLE, BYTES] but got: <score>", expected.getMessage());
  }

  // you can't change the index sort on an existing index:
  public void testIllegalChangeSort() throws Exception {
    final Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setIndexSort(new Sort(new SortField("foo", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.addDocument(new Document());
    DirectoryReader.open(w).close();
    w.addDocument(new Document());
    w.forceMerge(1);
    w.close();

    final IndexWriterConfig iwc2 = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc2.setIndexSort(new Sort(new SortField("bar", SortField.Type.LONG)));
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
        new IndexWriter(dir, iwc2);
    });
    String message = e.getMessage();
    assertTrue(message.contains("cannot change previous indexSort=<long: \"foo\">"));
    assertTrue(message.contains("to new indexSort=<long: \"bar\">"));
    dir.close();
  }

  static final class NormsSimilarity extends Similarity {
    
    private final Similarity in;
    
    public NormsSimilarity(Similarity in) {
      this.in = in;
    }
    
    @Override
    public long computeNorm(FieldInvertState state) {
      if (state.getName().equals(NORMS_FIELD)) {
        return Float.floatToIntBits(state.getBoost());
      } else {
        return in.computeNorm(state);
      }
    }
    
    @Override
    public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
      return in.computeWeight(collectionStats, termStats);
    }
    
    @Override
    public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
      return in.simScorer(weight, context);
    }
    
  }
  
  static final class PositionsTokenStream extends TokenStream {
    
    private final CharTermAttribute term;
    private final PayloadAttribute payload;
    private final OffsetAttribute offset;
    
    private int pos, off;
    
    public PositionsTokenStream() {
      term = addAttribute(CharTermAttribute.class);
      payload = addAttribute(PayloadAttribute.class);
      offset = addAttribute(OffsetAttribute.class);
    }
    
    @Override
    public boolean incrementToken() throws IOException {
      if (pos == 0) {
        return false;
      }
      
      clearAttributes();
      term.append(DOC_POSITIONS_TERM);
      payload.setPayload(new BytesRef(Integer.toString(pos)));
      offset.setOffset(off, off);
      --pos;
      ++off;
      return true;
    }
    
    void setId(int id) {
      pos = id / 10 + 1;
      off = 0;
    }
  }

  private static Directory dir;
  private static IndexReader sortedReader;

  private static final FieldType TERM_VECTORS_TYPE = new FieldType(TextField.TYPE_NOT_STORED);
  static {
    TERM_VECTORS_TYPE.setStoreTermVectors(true);
    TERM_VECTORS_TYPE.freeze();
  }
  
  private static final FieldType POSITIONS_TYPE = new FieldType(TextField.TYPE_NOT_STORED);
  static {
    POSITIONS_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    POSITIONS_TYPE.freeze();
  }

  private static final String ID_FIELD = "id";
  private static final String DOCS_ENUM_FIELD = "docs";
  private static final String DOCS_ENUM_TERM = "$all$";
  private static final String DOC_POSITIONS_FIELD = "positions";
  private static final String DOC_POSITIONS_TERM = "$all$";
  private static final String NUMERIC_DV_FIELD = "numeric";
  private static final String SORTED_NUMERIC_DV_FIELD = "sorted_numeric";
  private static final String NORMS_FIELD = "norm";
  private static final String BINARY_DV_FIELD = "binary";
  private static final String SORTED_DV_FIELD = "sorted";
  private static final String SORTED_SET_DV_FIELD = "sorted_set";
  private static final String TERM_VECTORS_FIELD = "term_vectors";
  private static final String DIMENSIONAL_FIELD = "numeric1d";

  private static Document doc(final int id, PositionsTokenStream positions) {
    final Document doc = new Document();
    doc.add(new StringField(ID_FIELD, Integer.toString(id), Store.YES));
    doc.add(new StringField(DOCS_ENUM_FIELD, DOCS_ENUM_TERM, Store.NO));
    positions.setId(id);
    doc.add(new Field(DOC_POSITIONS_FIELD, positions, POSITIONS_TYPE));
    doc.add(new NumericDocValuesField(NUMERIC_DV_FIELD, id));
    TextField norms = new TextField(NORMS_FIELD, Integer.toString(id), Store.NO);
    norms.setBoost(Float.intBitsToFloat(id));
    doc.add(norms);
    doc.add(new BinaryDocValuesField(BINARY_DV_FIELD, new BytesRef(Integer.toString(id))));
    doc.add(new SortedDocValuesField(SORTED_DV_FIELD, new BytesRef(Integer.toString(id))));
    doc.add(new SortedSetDocValuesField(SORTED_SET_DV_FIELD, new BytesRef(Integer.toString(id))));
    doc.add(new SortedSetDocValuesField(SORTED_SET_DV_FIELD, new BytesRef(Integer.toString(id + 1))));
    doc.add(new SortedNumericDocValuesField(SORTED_NUMERIC_DV_FIELD, id));
    doc.add(new SortedNumericDocValuesField(SORTED_NUMERIC_DV_FIELD, id + 1));
    doc.add(new Field(TERM_VECTORS_FIELD, Integer.toString(id), TERM_VECTORS_TYPE));
    byte[] bytes = new byte[4];
    NumericUtils.intToSortableBytes(id, bytes, 0);
    doc.add(new BinaryPoint(DIMENSIONAL_FIELD, bytes));
    return doc;
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sortedReader != null) {
      sortedReader.close();
      sortedReader = null;
    }
    if (dir != null) {
      dir.close();
      dir = null;
    }
  }

  @BeforeClass
  public static void createIndex() throws Exception {
    dir = newFSDirectory(createTempDir());
    int numDocs = atLeast(100);

    List<Integer> ids = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      ids.add(Integer.valueOf(i * 10));
    }
    // shuffle them for indexing
    Collections.shuffle(ids, random());
    if (VERBOSE) {
      System.out.println("Shuffled IDs for indexing: " + Arrays.toString(ids.toArray()));
    }
    
    PositionsTokenStream positions = new PositionsTokenStream();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(4); // create some segments
    conf.setSimilarity(new NormsSimilarity(conf.getSimilarity())); // for testing norms field
    // nocommit
    conf.setMergeScheduler(new SerialMergeScheduler());
    // sort the index by id (as integer, in NUMERIC_DV_FIELD)
    conf.setIndexSort(new Sort(new SortField(NUMERIC_DV_FIELD, SortField.Type.INT)));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    writer.setDoRandomForceMerge(false);
    for (int id : ids) {
      writer.addDocument(doc(id, positions));
    }
    // delete some documents
    writer.commit();
    // nocommit need thread safety test too
    for (Integer id : ids) {
      if (random().nextDouble() < 0.2) {
        if (VERBOSE) {
          System.out.println("delete doc_id " + id);
        }
        writer.deleteDocuments(new Term(ID_FIELD, id.toString()));
      }
    }
    
    sortedReader = writer.getReader();
    writer.close();
    
    TestUtil.checkReader(sortedReader);
  }

  // nocommit just do assertReaderEquals, don't use @BeforeClass, etc.?

  public void testBinaryDocValuesField() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      BinaryDocValues dv = reader.getBinaryDocValues(BINARY_DV_FIELD);
      boolean isSorted = reader.getIndexSort() != null;
      int lastID = Integer.MIN_VALUE;
      for (int docID = 0; docID < reader.maxDoc(); docID++) {
        BytesRef bytes = dv.get(docID);
        String idString = reader.document(docID).get(ID_FIELD);
        assertEquals("incorrect binary DocValues for doc " + docID, idString, bytes.utf8ToString());
        if (isSorted) {
          int id = Integer.parseInt(idString);
          assertTrue("lastID=" + lastID + " vs id=" + id, lastID < id);
          lastID = id;
        }
      }
    }
  }

  public void testPostings() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      TermsEnum termsEnum = reader.terms(DOC_POSITIONS_FIELD).iterator();
      assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef(DOC_POSITIONS_TERM)));
      PostingsEnum sortedPositions = termsEnum.postings(null, PostingsEnum.ALL);
      int doc;
    
      // test nextDoc()
      while ((doc = sortedPositions.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        int freq = sortedPositions.freq();
        int id = Integer.parseInt(reader.document(doc).get(ID_FIELD));
        assertEquals("incorrect freq for doc=" + doc, id / 10 + 1, freq);
        for (int i = 0; i < freq; i++) {
          assertEquals("incorrect position for doc=" + doc, i, sortedPositions.nextPosition());
          assertEquals("incorrect startOffset for doc=" + doc, i, sortedPositions.startOffset());
          assertEquals("incorrect endOffset for doc=" + doc, i, sortedPositions.endOffset());
          assertEquals("incorrect payload for doc=" + doc, freq - i, Integer.parseInt(sortedPositions.getPayload().utf8ToString()));
        }
      }
    
      // test advance()
      final PostingsEnum reuse = sortedPositions;
      sortedPositions = termsEnum.postings(reuse, PostingsEnum.ALL);

      doc = 0;
      while ((doc = sortedPositions.advance(doc + TestUtil.nextInt(random(), 1, 5))) != DocIdSetIterator.NO_MORE_DOCS) {
        int freq = sortedPositions.freq();
        int id = Integer.parseInt(reader.document(doc).get(ID_FIELD));
        assertEquals("incorrect freq for doc=" + doc, id / 10 + 1, freq);
        for (int i = 0; i < freq; i++) {
          assertEquals("incorrect position for doc=" + doc, i, sortedPositions.nextPosition());
          assertEquals("incorrect startOffset for doc=" + doc, i, sortedPositions.startOffset());
          assertEquals("incorrect endOffset for doc=" + doc, i, sortedPositions.endOffset());
          assertEquals("incorrect payload for doc=" + doc, freq - i, Integer.parseInt(sortedPositions.getPayload().utf8ToString()));
        }
      }
    }
  }

  public void testDocsAreSortedByID() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      if (reader.getIndexSort() != null) {
        int maxDoc = reader.maxDoc();
        int lastID = Integer.MIN_VALUE;
        for(int doc=0;doc<maxDoc;doc++) {
          int id = Integer.parseInt(reader.document(doc).get(ID_FIELD));
          assertTrue(id > lastID);
          lastID = id;
        }
      }
    }
  }

  public void testNormValues() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      NumericDocValues dv = reader.getNormValues(NORMS_FIELD);
      int maxDoc = reader.maxDoc();
      for (int doc = 0; doc < maxDoc; doc++) {
        int id = Integer.parseInt(reader.document(doc).get(ID_FIELD));
        assertEquals("incorrect norm value for doc " + doc, id, dv.get(doc));
      }
    }
  }
  
  public void testNumericDocValuesField() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      NumericDocValues dv = reader.getNumericDocValues(NUMERIC_DV_FIELD);
      int maxDoc = reader.maxDoc();
      for (int doc = 0; doc < maxDoc; doc++) {
        int id = Integer.parseInt(reader.document(doc).get(ID_FIELD));
        assertEquals("incorrect numeric DocValues for doc " + doc, id, dv.get(doc));
      }
    }
  }
  
  public void testSortedDocValuesField() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      SortedDocValues dv = reader.getSortedDocValues(SORTED_DV_FIELD);
      int maxDoc = reader.maxDoc();
      for (int doc = 0; doc < maxDoc; doc++) {
        final BytesRef bytes = dv.get(doc);
        String id = reader.document(doc).get(ID_FIELD);
        assertEquals("incorrect sorted DocValues for doc " + doc, id, bytes.utf8ToString());
      }
    }
  }
  
  public void testSortedSetDocValuesField() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      SortedSetDocValues dv = reader.getSortedSetDocValues(SORTED_SET_DV_FIELD);
      int maxDoc = reader.maxDoc();
      for (int doc = 0; doc < maxDoc; doc++) {
        dv.setDocument(doc);
        BytesRef bytes = dv.lookupOrd(dv.nextOrd());
        String id = reader.document(doc).get(ID_FIELD);
        assertEquals("incorrect sorted-set DocValues for doc " + doc, id, bytes.utf8ToString());
        bytes = dv.lookupOrd(dv.nextOrd());
        assertEquals("incorrect sorted-set DocValues for doc " + doc, Integer.valueOf(Integer.parseInt(id) + 1).toString(), bytes.utf8ToString());
        assertEquals(SortedSetDocValues.NO_MORE_ORDS, dv.nextOrd());
      }
    }
  }

  public void testSortedNumericDocValuesField() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      SortedNumericDocValues dv = reader.getSortedNumericDocValues(SORTED_NUMERIC_DV_FIELD);
      int maxDoc = reader.maxDoc();
      for (int doc = 0; doc < maxDoc; doc++) {
        dv.setDocument(doc);
        assertEquals(2, dv.count());
        int id = Integer.parseInt(reader.document(doc).get(ID_FIELD));
        assertEquals("incorrect sorted-numeric DocValues for doc " + doc, id, dv.valueAt(0));
        assertEquals("incorrect sorted-numeric DocValues for doc " + doc, id + 1, dv.valueAt(1));
      }
    }
  }
  
  public void testTermVectors() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      LeafReader reader = ctx.reader();
      int maxDoc = reader.maxDoc();
      for (int doc = 0; doc < maxDoc; doc++) {
        Terms terms = reader.getTermVector(doc, TERM_VECTORS_FIELD);
        assertNotNull("term vectors not found for doc " + doc + " field [" + TERM_VECTORS_FIELD + "]", terms);
        String id = reader.document(doc).get(ID_FIELD);
        assertEquals("incorrect term vector for doc " + doc, id, terms.iterator().next().utf8ToString());
      }
    }
  }

  public void testPoints() throws Exception {
    for(LeafReaderContext ctx : sortedReader.leaves()) {
      final LeafReader reader = ctx.reader();
      PointValues values = reader.getPointValues();
      values.intersect(DIMENSIONAL_FIELD,
                       new IntersectVisitor() {
                         @Override
                         public void visit(int docID) {
                           throw new IllegalStateException();
                         }

                         @Override
                         public void visit(int docID, byte[] packedValues) throws IOException {
                           int id = Integer.parseInt(reader.document(docID).get(ID_FIELD));
                           assertEquals(id, NumericUtils.sortableBytesToInt(packedValues, 0));
                         }

                         @Override
                         public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                           return Relation.CELL_CROSSES_QUERY;
                         }
                       });
    }
  }
}
