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
package org.apache.lucene.codecs.lucene80;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDocValuesCompression extends LuceneTestCase {
  private final Codec bestSpeed = new Lucene87Codec(Lucene87Codec.Mode.BEST_SPEED);
  private final Codec bestCompression = new Lucene87Codec(Lucene87Codec.Mode.BEST_COMPRESSION);

  public void testTermsDictCompressionForLowCardinalityFields() throws IOException {
    final int CARDINALITY = Lucene80DocValuesFormat.TERMS_DICT_BLOCK_COMPRESSION_THRESHOLD - 1;
    Set<String> valuesSet = new HashSet<>();
    for (int i = 0; i < CARDINALITY; ++i) {
      final int length = TestUtil.nextInt(random(), 10, 30);
      String value = TestUtil.randomSimpleString(random(), length);
      valuesSet.add(value);
    }

    List<String> values = new ArrayList<>(valuesSet);
    long sizeForBestSpeed = writeAndGetDocValueFileSize(bestSpeed, values);
    long sizeForBestCompression = writeAndGetDocValueFileSize(bestCompression, values);

    // Ensure terms dict data was not compressed for low-cardinality fields.
    assertEquals(sizeForBestSpeed, sizeForBestCompression);
  }

  public void testTermsDictCompressionForHighCardinalityFields() throws IOException {
    final int CARDINALITY = Lucene80DocValuesFormat.TERMS_DICT_BLOCK_COMPRESSION_THRESHOLD << 1;
    Set<String> valuesSet = new HashSet<>();
    for (int i = 0; i < CARDINALITY; ++i) {
      final int length = TestUtil.nextInt(random(), 10, 30);
      String value = TestUtil.randomSimpleString(random(), length);
      // Add common suffix for better compression ratio.
      valuesSet.add(value + "_CommonPartBetterForCompression");
    }

    List<String> values = new ArrayList<>(valuesSet);
    long sizeForBestSpeed = writeAndGetDocValueFileSize(bestSpeed, values);
    long sizeForBestCompression = writeAndGetDocValueFileSize(bestCompression, values);

    // Compression happened.
    assertTrue(sizeForBestCompression < sizeForBestSpeed);
  }

  public void testReseekAfterSkipDecompression() throws IOException {
    final int CARDINALITY = (Lucene80DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SIZE << 1) + 11;
    Set<String> valueSet = new HashSet<>(CARDINALITY);
    for (int i = 0; i < CARDINALITY; i++) {
      valueSet.add(TestUtil.randomSimpleString(random(), 64));
    }
    List<String> values = new ArrayList<>(valueSet);
    Collections.sort(values);
    // Create one non-existent value just between block-1 and block-2.
    String nonexistentValue =
        values.get(Lucene80DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SIZE - 1)
            + TestUtil.randomSimpleString(random(), 64, 128);
    int docValues = values.size();

    try (Directory directory = newDirectory()) {
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setCodec(bestCompression);
      config.setUseCompoundFile(false);
      IndexWriter writer = new IndexWriter(directory, config);
      for (int i = 0; i < 280; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", "Doc" + i, Field.Store.NO));
        doc.add(new SortedDocValuesField("sdv", new BytesRef(values.get(i % docValues))));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
      DirectoryReader dReader = DirectoryReader.open(writer);
      writer.close();

      LeafReader reader = getOnlyLeafReader(dReader);
      // Check values count.
      SortedDocValues ssdvMulti = reader.getSortedDocValues("sdv");
      assertEquals(docValues, ssdvMulti.getValueCount());

      // Seek to first block.
      int ord1 = ssdvMulti.lookupTerm(new BytesRef(values.get(0)));
      assertTrue(ord1 >= 0);
      int ord2 = ssdvMulti.lookupTerm(new BytesRef(values.get(1)));
      assertTrue(ord2 >= ord1);
      // Ensure re-seek logic is correct after skip-decompression.
      int nonexistentOrd2 = ssdvMulti.lookupTerm(new BytesRef(nonexistentValue));
      assertTrue(nonexistentOrd2 < 0);
      dReader.close();
    }
  }

  public void testLargeTermsCompression() throws IOException {
    final int CARDINALITY = Lucene80DocValuesFormat.TERMS_DICT_BLOCK_COMPRESSION_THRESHOLD << 1;
    Set<String> valuesSet = new HashSet<>();
    for (int i = 0; i < CARDINALITY; ++i) {
      final int length = TestUtil.nextInt(random(), 512, 1024);
      valuesSet.add(TestUtil.randomSimpleString(random(), length));
    }
    int valuesCount = valuesSet.size();
    List<String> values = new ArrayList<>(valuesSet);

    try (Directory directory = newDirectory()) {
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setCodec(bestCompression);
      config.setUseCompoundFile(false);
      IndexWriter writer = new IndexWriter(directory, config);
      for (int i = 0; i < 256; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", "Doc" + i, Field.Store.NO));
        doc.add(new SortedDocValuesField("sdv", new BytesRef(values.get(i % valuesCount))));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
      DirectoryReader ireader = DirectoryReader.open(writer);
      writer.close();

      LeafReader reader = getOnlyLeafReader(ireader);
      // Check values count.
      SortedDocValues ssdvMulti = reader.getSortedDocValues("sdv");
      assertEquals(valuesCount, ssdvMulti.getValueCount());
      ireader.close();
    }
  }

  // Ensure the old segment can be merged together with the new compressed segment.
  public void testMergeWithUncompressedSegment() throws IOException {
    final int CARDINALITY = Lucene80DocValuesFormat.TERMS_DICT_BLOCK_COMPRESSION_THRESHOLD << 1;
    Set<String> valuesSet = new HashSet<>();
    for (int i = 0; i < CARDINALITY; ++i) {
      final int length = TestUtil.nextInt(random(), 10, 30);
      // Add common suffix for better compression ratio.
      valuesSet.add(TestUtil.randomSimpleString(random(), length));
    }
    List<String> values = new ArrayList<>(valuesSet);
    int valuesCount = values.size();

    try (Directory directory = newDirectory()) {
      // 1. Write 256 documents without terms dict compression.
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setCodec(bestSpeed);
      config.setUseCompoundFile(false);
      IndexWriter writer = new IndexWriter(directory, config);
      for (int i = 0; i < 256; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", "Doc" + i, Field.Store.NO));
        doc.add(new SortedSetDocValuesField("ssdv", new BytesRef(values.get(i % valuesCount))));
        doc.add(
            new SortedSetDocValuesField("ssdv", new BytesRef(values.get((i + 1) % valuesCount))));
        doc.add(
            new SortedSetDocValuesField("ssdv", new BytesRef(values.get((i + 2) % valuesCount))));
        doc.add(new SortedDocValuesField("sdv", new BytesRef(values.get(i % valuesCount))));
        writer.addDocument(doc);
      }
      writer.commit();
      DirectoryReader ireader = DirectoryReader.open(writer);
      assertEquals(256, ireader.numDocs());
      LeafReader reader = getOnlyLeafReader(ireader);
      SortedSetDocValues ssdv = reader.getSortedSetDocValues("ssdv");
      assertEquals(valuesCount, ssdv.getValueCount());
      SortedDocValues sdv = reader.getSortedDocValues("sdv");
      assertEquals(valuesCount, sdv.getValueCount());
      ireader.close();
      writer.close();

      // 2. Add another 100 documents, and enabling terms dict compression.
      config = new IndexWriterConfig(analyzer);
      config.setCodec(bestCompression);
      config.setUseCompoundFile(false);
      writer = new IndexWriter(directory, config);
      // Add 2 new values.
      valuesSet.add(TestUtil.randomSimpleString(random(), 10));
      valuesSet.add(TestUtil.randomSimpleString(random(), 10));
      values = new ArrayList<>(valuesSet);
      valuesCount = valuesSet.size();

      for (int i = 256; i < 356; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", "Doc" + i, Field.Store.NO));
        doc.add(new SortedSetDocValuesField("ssdv", new BytesRef(values.get(i % valuesCount))));
        doc.add(new SortedDocValuesField("sdv", new BytesRef(values.get(i % valuesCount))));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
      ireader = DirectoryReader.open(writer);
      assertEquals(356, ireader.numDocs());
      reader = getOnlyLeafReader(ireader);
      ssdv = reader.getSortedSetDocValues("ssdv");
      assertEquals(valuesCount, ssdv.getValueCount());
      ireader.close();
      writer.close();
    }
  }

  private static long writeAndGetDocValueFileSize(Codec codec, List<String> values)
      throws IOException {
    int valuesCount = values.size();
    long dvdFileSize = -1;
    try (Directory directory = newDirectory()) {
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setCodec(codec);
      config.setUseCompoundFile(false);
      IndexWriter writer = new IndexWriter(directory, config);
      for (int i = 0; i < 256; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", "Doc" + i, Field.Store.NO));
        // Multi value sorted-set field.
        doc.add(
            new SortedSetDocValuesField("ssdv_multi_", new BytesRef(values.get(i % valuesCount))));
        doc.add(
            new SortedSetDocValuesField(
                "ssdv_multi_", new BytesRef(values.get((i + 1) % valuesCount))));
        doc.add(
            new SortedSetDocValuesField(
                "ssdv_multi_", new BytesRef(values.get((i + 2) % valuesCount))));
        // Single value sorted-set field.
        doc.add(
            new SortedSetDocValuesField("ssdv_single_", new BytesRef(values.get(i % valuesCount))));
        // Sorted field.
        doc.add(new SortedDocValuesField("sdv", new BytesRef(values.get(i % valuesCount))));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
      DirectoryReader ireader = DirectoryReader.open(writer);
      writer.close();

      LeafReader reader = getOnlyLeafReader(ireader);
      // Check values count.
      SortedSetDocValues ssdvMulti = reader.getSortedSetDocValues("ssdv_multi_");
      assertEquals(valuesCount, ssdvMulti.getValueCount());
      for (int i = 0; i < valuesCount; i++) {
        BytesRef term = ssdvMulti.lookupOrd(i);
        assertTrue(term.bytes.length > 0);
      }

      SortedSetDocValues ssdvSingle = reader.getSortedSetDocValues("ssdv_single_");
      assertEquals(valuesCount, ssdvSingle.getValueCount());
      SortedDocValues sdv = reader.getSortedDocValues("sdv");
      assertEquals(valuesCount, sdv.getValueCount());
      dvdFileSize = docValueFileSize(directory);
      assertTrue(dvdFileSize > 0);
      ireader.close();
    }

    return dvdFileSize;
  }

  static long docValueFileSize(Directory d) throws IOException {
    for (String file : d.listAll()) {
      if (file.endsWith(Lucene80DocValuesFormat.DATA_EXTENSION)) {
        return d.fileLength(file);
      }
    }
    return -1;
  }
}
