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
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesStats.DoubleDocValuesStats;
import org.apache.lucene.search.DocValuesStats.LongDocValuesStats;
import org.apache.lucene.store.Directory;
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
        assertEquals(getZeroValues(docValues).count() - reader.numDeletedDocs(), stats.missing());
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
        assertEquals(getZeroValues(docValues).count() - reader.numDeletedDocs(), stats.missing());
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

  private static LongStream getPositiveValues(long[] docValues) {
    return Arrays.stream(docValues).filter(v -> v > 0);
  }

  private static DoubleStream getPositiveValues(double[] docValues) {
    return Arrays.stream(docValues).filter(v -> v > 0);
  }

  private static LongStream getZeroValues(long[] docValues) {
    return Arrays.stream(docValues).filter(v -> v == 0);
  }

  private static DoubleStream getZeroValues(double[] docValues) {
    return Arrays.stream(docValues).filter(v -> v == 0);
  }

  private static double computeVariance(long[] values, double mean, int count) {
    return getPositiveValues(values).mapToDouble(v -> (v - mean) * (v-mean)).sum() / count;
  }

  private static double computeVariance(double[] values, double mean, int count) {
    return getPositiveValues(values).map(v -> (v - mean) * (v-mean)).sum() / count;
  }

}
