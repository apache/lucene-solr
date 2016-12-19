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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.LongSummaryStatistics;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesStats.DoubleDocValuesStats;
import org.apache.lucene.search.DocValuesStats.LongDocValuesStats;
import org.apache.lucene.search.DocValuesStats.SortedDocValuesStats;
import org.apache.lucene.search.DocValuesStats.SortedDoubleDocValuesStats;
import org.apache.lucene.search.DocValuesStats.SortedLongDocValuesStats;
import org.apache.lucene.search.DocValuesStats.SortedSetDocValuesStats;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Unit tests for {@link DocValuesStatsCollector}. */
public class TestDocValuesStatsCollector extends LuceneTestCase {

  public void testNoDocsWithField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      int numDocs = TestUtil.nextInt(random(), 1, 100);
      for (int i = 0; i < numDocs; i++) {
        indexWriter.addDocument(new Document());
      }

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        LongDocValuesStats stats = new LongDocValuesStats("foo");
        searcher.search(new MatchAllDocsQuery(), new DocValuesStatsCollector(stats));

        assertEquals(0, stats.count());
        assertEquals(numDocs, stats.missing());
      }
    }
  }

  public void testOneDoc() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      String field = "numeric";
      Document doc = new Document();
      doc.add(new NumericDocValuesField(field, 1));
      doc.add(new StringField("id", "doc1", Store.NO));
      indexWriter.addDocument(doc);

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        LongDocValuesStats stats = new LongDocValuesStats(field);
        searcher.search(new MatchAllDocsQuery(), new DocValuesStatsCollector(stats));

        assertEquals(1, stats.count());
        assertEquals(0, stats.missing());
        assertEquals(1, stats.max().longValue());
        assertEquals(1, stats.min().longValue());
        assertEquals(1, stats.sum().longValue());
        assertEquals(1, stats.mean(), 0.0001);
        assertEquals(0, stats.variance(), 0.0001);
        assertEquals(0, stats.stdev(), 0.0001);
      }
    }
  }

  public void testDocsWithLongValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      String field = "numeric";
      int numDocs = TestUtil.nextInt(random(), 1, 100);
      long[] docValues = new long[numDocs];
      int nextVal = 1;
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) { // not all documents have a value
          doc.add(new NumericDocValuesField(field, nextVal));
          doc.add(new StringField("id", "doc" + i, Store.NO));
          docValues[i] = nextVal;
          ++nextVal;
        }
        indexWriter.addDocument(doc);
      }

      // 20% of cases delete some docs
      if (random().nextDouble() < 0.2) {
        for (int i = 0; i < numDocs; i++) {
          if (random().nextBoolean()) {
            indexWriter.deleteDocuments(new Term("id", "doc" + i));
            docValues[i] = 0;
          }
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        LongDocValuesStats stats = new LongDocValuesStats(field);
        searcher.search(new MatchAllDocsQuery(), new DocValuesStatsCollector(stats));

        int expCount = (int) Arrays.stream(docValues).filter(v -> v > 0).count();
        assertEquals(expCount, stats.count());
        int numDocsWithoutField = (int) getZeroValues(docValues).count();
        assertEquals(computeExpMissing(numDocsWithoutField, numDocs, reader), stats.missing());
        if (stats.count() > 0) {
          LongSummaryStatistics sumStats = getPositiveValues(docValues).summaryStatistics();
          assertEquals(sumStats.getMax(), stats.max().longValue());
          assertEquals(sumStats.getMin(), stats.min().longValue());
          assertEquals(sumStats.getAverage(), stats.mean(), 0.00001);
          assertEquals(sumStats.getSum(), stats.sum().longValue());
          double variance = computeVariance(docValues, stats.mean, stats.count());
          assertEquals(variance, stats.variance(), 0.00001);
          assertEquals(Math.sqrt(variance), stats.stdev(), 0.00001);
        }
      }
    }
  }

  public void testDocsWithDoubleValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      String field = "numeric";
      int numDocs = TestUtil.nextInt(random(), 1, 100);
      double[] docValues = new double[numDocs];
      double nextVal = 1.0;
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) { // not all documents have a value
          doc.add(new DoubleDocValuesField(field, nextVal));
          doc.add(new StringField("id", "doc" + i, Store.NO));
          docValues[i] = nextVal;
          ++nextVal;
        }
        indexWriter.addDocument(doc);
      }

      // 20% of cases delete some docs
      if (random().nextDouble() < 0.2) {
        for (int i = 0; i < numDocs; i++) {
          if (random().nextBoolean()) {
            indexWriter.deleteDocuments(new Term("id", "doc" + i));
            docValues[i] = 0;
          }
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        DoubleDocValuesStats stats = new DoubleDocValuesStats(field);
        searcher.search(new MatchAllDocsQuery(), new DocValuesStatsCollector(stats));

        int expCount = (int) Arrays.stream(docValues).filter(v -> v > 0).count();
        assertEquals(expCount, stats.count());
        int numDocsWithoutField = (int) getZeroValues(docValues).count();
        assertEquals(computeExpMissing(numDocsWithoutField, numDocs, reader), stats.missing());
        if (stats.count() > 0) {
          DoubleSummaryStatistics sumStats = getPositiveValues(docValues).summaryStatistics();
          assertEquals(sumStats.getMax(), stats.max().doubleValue(), 0.00001);
          assertEquals(sumStats.getMin(), stats.min().doubleValue(), 0.00001);
          assertEquals(sumStats.getAverage(), stats.mean(), 0.00001);
          assertEquals(sumStats.getSum(), stats.sum(), 0.00001);
          double variance = computeVariance(docValues, stats.mean, stats.count());
          assertEquals(variance, stats.variance(), 0.00001);
          assertEquals(Math.sqrt(variance), stats.stdev(), 0.00001);
        }
      }
    }
  }

  public void testDocsWithMultipleLongValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      String field = "numeric";
      int numDocs = TestUtil.nextInt(random(), 1, 100);
      long[][] docValues = new long[numDocs][];
      long nextVal = 1;
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) { // not all documents have a value
          int numValues = TestUtil.nextInt(random(), 1, 5);
          docValues[i] = new long[numValues];
          for (int j = 0; j < numValues; j++) {
            doc.add(new SortedNumericDocValuesField(field, nextVal));
            docValues[i][j] = nextVal;
            ++nextVal;
          }
          doc.add(new StringField("id", "doc" + i, Store.NO));
        }
        indexWriter.addDocument(doc);
      }

      // 20% of cases delete some docs
      if (random().nextDouble() < 0.2) {
        for (int i = 0; i < numDocs; i++) {
          if (random().nextBoolean()) {
            indexWriter.deleteDocuments(new Term("id", "doc" + i));
            docValues[i] = null;
          }
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        SortedLongDocValuesStats stats = new SortedLongDocValuesStats(field);
        searcher.search(new MatchAllDocsQuery(), new DocValuesStatsCollector(stats));

        assertEquals(nonNull(docValues).count(), stats.count());
        int numDocsWithoutField = (int) isNull(docValues).count();
        assertEquals(computeExpMissing(numDocsWithoutField, numDocs, reader), stats.missing());
        if (stats.count() > 0) {
          LongSummaryStatistics sumStats = filterAndFlatValues(docValues, (v) -> v != null).summaryStatistics();
          assertEquals(sumStats.getMax(), stats.max().longValue());
          assertEquals(sumStats.getMin(), stats.min().longValue());
          assertEquals(sumStats.getAverage(), stats.mean(), 0.00001);
          assertEquals(sumStats.getSum(), stats.sum().longValue());
          assertEquals(sumStats.getCount(), stats.valuesCount());
          double variance = computeVariance(filterAndFlatValues(docValues, (v) -> v != null), stats.mean, stats.count());
          assertEquals(variance, stats.variance(), 0.00001);
          assertEquals(Math.sqrt(variance), stats.stdev(), 0.00001);
        }
      }
    }
  }

  public void testDocsWithMultipleDoubleValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      String field = "numeric";
      int numDocs = TestUtil.nextInt(random(), 1, 100);
      double[][] docValues = new double[numDocs][];
      double nextVal = 1;
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) { // not all documents have a value
          int numValues = TestUtil.nextInt(random(), 1, 5);
          docValues[i] = new double[numValues];
          for (int j = 0; j < numValues; j++) {
            doc.add(new SortedNumericDocValuesField(field, Double.doubleToRawLongBits(nextVal)));
            docValues[i][j] = nextVal;
            ++nextVal;
          }
          doc.add(new StringField("id", "doc" + i, Store.NO));
        }
        indexWriter.addDocument(doc);
      }

      // 20% of cases delete some docs
      if (random().nextDouble() < 0.2) {
        for (int i = 0; i < numDocs; i++) {
          if (random().nextBoolean()) {
            indexWriter.deleteDocuments(new Term("id", "doc" + i));
            docValues[i] = null;
          }
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        SortedDoubleDocValuesStats stats = new SortedDoubleDocValuesStats(field);
        searcher.search(new MatchAllDocsQuery(), new DocValuesStatsCollector(stats));

        assertEquals(nonNull(docValues).count(), stats.count());
        int numDocsWithoutField = (int) isNull(docValues).count();
        assertEquals(computeExpMissing(numDocsWithoutField, numDocs, reader), stats.missing());
        if (stats.count() > 0) {
          DoubleSummaryStatistics sumStats = filterAndFlatValues(docValues, (v) -> v != null).summaryStatistics();
          assertEquals(sumStats.getMax(), stats.max().longValue(), 0.00001);
          assertEquals(sumStats.getMin(), stats.min().longValue(), 0.00001);
          assertEquals(sumStats.getAverage(), stats.mean(), 0.00001);
          assertEquals(sumStats.getSum(), stats.sum().doubleValue(), 0.00001);
          assertEquals(sumStats.getCount(), stats.valuesCount());
          double variance = computeVariance(filterAndFlatValues(docValues, (v) -> v != null), stats.mean, stats.count());
          assertEquals(variance, stats.variance(), 0.00001);
          assertEquals(Math.sqrt(variance), stats.stdev(), 0.00001);
        }
      }
    }
  }

  public void testDocsWithSortedValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      String field = "sorted";
      int numDocs = TestUtil.nextInt(random(), 1, 100);
      BytesRef[] docValues = new BytesRef[numDocs];
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) { // not all documents have a value
          BytesRef val = TestUtil.randomBinaryTerm(random());
          doc.add(new SortedDocValuesField(field, val));
          doc.add(new StringField("id", "doc" + i, Store.NO));
          docValues[i] = val;
        }
        indexWriter.addDocument(doc);
      }

      // 20% of cases delete some docs
      if (random().nextDouble() < 0.2) {
        for (int i = 0; i < numDocs; i++) {
          if (random().nextBoolean()) {
            indexWriter.deleteDocuments(new Term("id", "doc" + i));
            docValues[i] = null;
          }
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        SortedDocValuesStats stats = new SortedDocValuesStats(field);
        searcher.search(new MatchAllDocsQuery(), new DocValuesStatsCollector(stats));

        int expCount = (int) nonNull(docValues).count();
        assertEquals(expCount, stats.count());
        int numDocsWithoutField = (int) isNull(docValues).count();
        assertEquals(computeExpMissing(numDocsWithoutField, numDocs, reader), stats.missing());
        if (stats.count() > 0) {
          assertEquals(nonNull(docValues).min(BytesRef::compareTo).get(), stats.min());
          assertEquals(nonNull(docValues).max(BytesRef::compareTo).get(), stats.max());
        }
      }
    }
  }

  public void testDocsWithSortedSetValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig())) {
      String field = "sorted";
      int numDocs = TestUtil.nextInt(random(), 1, 100);
      BytesRef[][] docValues = new BytesRef[numDocs][];
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) { // not all documents have a value
          int numValues = TestUtil.nextInt(random(), 1, 5);
          docValues[i] = new BytesRef[numValues];
          for (int j = 0; j < numValues; j++) {
            BytesRef val = TestUtil.randomBinaryTerm(random());
            doc.add(new SortedSetDocValuesField(field, val));
            docValues[i][j] = val;
          }
          doc.add(new StringField("id", "doc" + i, Store.NO));
        }
        indexWriter.addDocument(doc);
      }

      // 20% of cases delete some docs
      if (random().nextDouble() < 0.2) {
        for (int i = 0; i < numDocs; i++) {
          if (random().nextBoolean()) {
            indexWriter.deleteDocuments(new Term("id", "doc" + i));
            docValues[i] = null;
          }
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        SortedSetDocValuesStats stats = new SortedSetDocValuesStats(field);
        TotalHitCountCollector totalHitCount = new TotalHitCountCollector();
        searcher.search(new MatchAllDocsQuery(), MultiCollector.wrap(totalHitCount, new DocValuesStatsCollector(stats)));

        int expCount = (int) nonNull(docValues).count();
        assertEquals(expCount, stats.count());
        int numDocsWithoutField = (int) isNull(docValues).count();
        assertEquals(computeExpMissing(numDocsWithoutField, numDocs, reader), stats.missing());
        if (stats.count() > 0) {
          assertEquals(nonNull(docValues).flatMap(Arrays::stream).min(BytesRef::compareTo).get(), stats.min());
          assertEquals(nonNull(docValues).flatMap(Arrays::stream).max(BytesRef::compareTo).get(), stats.max());
        }
      }
    }
  }

  private static LongStream getPositiveValues(long[] values) {
    return Arrays.stream(values).filter(v -> v > 0);
  }

  private static DoubleStream getPositiveValues(double[] values) {
    return Arrays.stream(values).filter(v -> v > 0);
  }

  private static LongStream getZeroValues(long[] values) {
    return Arrays.stream(values).filter(v -> v == 0);
  }

  private static DoubleStream getZeroValues(double[] values) {
    return Arrays.stream(values).filter(v -> v == 0);
  }

  private static double computeVariance(long[] values, double mean, int count) {
    return getPositiveValues(values).mapToDouble(v -> (v - mean) * (v-mean)).sum() / count;
  }

  private static double computeVariance(double[] values, double mean, int count) {
    return getPositiveValues(values).map(v -> (v - mean) * (v-mean)).sum() / count;
  }

  private static LongStream filterAndFlatValues(long[][] values, Predicate<? super long[]> p) {
    return nonNull(values).flatMapToLong(Arrays::stream);
  }

  private static DoubleStream filterAndFlatValues(double[][] values, Predicate<? super double[]> p) {
    return nonNull(values).flatMapToDouble(Arrays::stream);
  }

  private static double computeVariance(LongStream values, double mean, int count) {
    return values.mapToDouble(v -> (v - mean) * (v-mean)).sum() / count;
  }

  private static double computeVariance(DoubleStream values, double mean, int count) {
    return values.map(v -> (v - mean) * (v-mean)).sum() / count;
  }

  private static <T> Stream<T> nonNull(T[] values) {
    return filterValues(values, Objects::nonNull);
  }

  private static <T> Stream<T> isNull(T[] values) {
    return filterValues(values, Objects::isNull);
  }

  private static <T> Stream<T> filterValues(T[] values, Predicate<? super T> p) {
    return Arrays.stream(values).filter(p);
  }

  private static int computeExpMissing(int numDocsWithoutField, int numIndexedDocs, IndexReader reader) {
    // The number of missing documents equals the number of docs without the field (not indexed with it, or were
    // deleted). However, in case we deleted all documents in a segment before the reader was opened, there will be
    // a mismatch between numDocs (how many we indexed) to reader.maxDoc(), so compensate for that.
    return numDocsWithoutField - reader.numDeletedDocs() - (numIndexedDocs - reader.maxDoc());
  }
}
