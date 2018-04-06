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
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestSoftDeletesRetentionMergePolicy extends LuceneTestCase {

  public void testKeepFullyDeletedSegments() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, indexWriterConfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new NumericDocValuesField("soft_delete", 1));
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    assertEquals(1, reader.leaves().size());
    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
    MergePolicy policy = new SoftDeletesRetentionMergePolicy("soft_delete",
        () -> new DocValuesFieldExistsQuery("keep_around"), NoMergePolicy.INSTANCE);
    assertFalse(policy.keepFullyDeletedSegment(segmentReader));
    reader.close();

    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new NumericDocValuesField("keep_around", 1));
    doc.add(new NumericDocValuesField("soft_delete", 1));
    writer.addDocument(doc);

    reader = writer.getReader();
    assertEquals(2, reader.leaves().size());
    segmentReader = (SegmentReader) reader.leaves().get(0).reader();
    assertFalse(policy.keepFullyDeletedSegment(segmentReader));

    segmentReader = (SegmentReader) reader.leaves().get(1).reader();
    assertTrue(policy.keepFullyDeletedSegment(segmentReader));

    IOUtils.close(reader, writer, dir);
  }

  public void testFieldBasedRetention() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
    Instant now = Instant.now();
    Instant time24HoursAgo = now.minus(Duration.ofDays(1));
    String softDeletesField = "soft_delete";
    Supplier<Query> docsOfLast24Hours = () -> LongPoint.newRangeQuery("creation_date", time24HoursAgo.toEpochMilli(), now.toEpochMilli());
    indexWriterConfig.setMergePolicy(new SoftDeletesRetentionMergePolicy(softDeletesField, docsOfLast24Hours,
        new LogDocMergePolicy()));
    indexWriterConfig.setSoftDeletesField(softDeletesField);
    IndexWriter writer = new IndexWriter(dir, indexWriterConfig);

    long time28HoursAgo = now.minus(Duration.ofHours(28)).toEpochMilli();
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "1", Field.Store.YES));
    doc.add(new LongPoint("creation_date", time28HoursAgo));
    writer.addDocument(doc);

    writer.flush();
    long time26HoursAgo = now.minus(Duration.ofHours(26)).toEpochMilli();
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "2", Field.Store.YES));
    doc.add(new LongPoint("creation_date", time26HoursAgo));
    writer.softUpdateDocument(new Term("id", "1"), doc, new NumericDocValuesField("soft_delete", 1));

    if (random().nextBoolean()) {
      writer.flush();
    }
    long time23HoursAgo = now.minus(Duration.ofHours(23)).toEpochMilli();
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "3", Field.Store.YES));
    doc.add(new LongPoint("creation_date", time23HoursAgo));
    writer.softUpdateDocument(new Term("id", "1"), doc, new NumericDocValuesField("soft_delete", 1));

    if (random().nextBoolean()) {
      writer.flush();
    }
    long time12HoursAgo = now.minus(Duration.ofHours(12)).toEpochMilli();
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "4", Field.Store.YES));
    doc.add(new LongPoint("creation_date", time12HoursAgo));
    writer.softUpdateDocument(new Term("id", "1"), doc, new NumericDocValuesField("soft_delete", 1));

    if (random().nextBoolean()) {
      writer.flush();
    }
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "5", Field.Store.YES));
    doc.add(new LongPoint("creation_date", now.toEpochMilli()));
    writer.softUpdateDocument(new Term("id", "1"), doc, new NumericDocValuesField("soft_delete", 1));

    if (random().nextBoolean()) {
      writer.flush();
    }
    writer.forceMerge(1);
    DirectoryReader reader = writer.getReader();
    assertEquals(1, reader.numDocs());
    assertEquals(3, reader.maxDoc());
    Set<String> versions = new HashSet<>();
    versions.add(reader.document(0, Collections.singleton("version")).get("version"));
    versions.add(reader.document(1, Collections.singleton("version")).get("version"));
    versions.add(reader.document(2, Collections.singleton("version")).get("version"));
    assertTrue(versions.contains("5"));
    assertTrue(versions.contains("4"));
    assertTrue(versions.contains("3"));
    IOUtils.close(reader, writer, dir);
  }

  public void testKeepAllDocsAcrossMerges() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
    indexWriterConfig.setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete",
        () -> new MatchAllDocsQuery(),
        new LogDocMergePolicy()));
    indexWriterConfig.setSoftDeletesField("soft_delete");
    IndexWriter writer = new IndexWriter(dir, indexWriterConfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "1"), doc,
        new NumericDocValuesField("soft_delete", 1));

    writer.commit();
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "1"), doc,
        new NumericDocValuesField("soft_delete", 1));

    writer.commit();
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new NumericDocValuesField("soft_delete", 1)); // already deleted
    writer.softUpdateDocument(new Term("id", "1"), doc,
        new NumericDocValuesField("soft_delete", 1));
    writer.commit();
    DirectoryReader reader = writer.getReader();
    assertEquals(0, reader.numDocs());
    assertEquals(3, reader.maxDoc());
    assertEquals(0, writer.numDocs());
    assertEquals(3, writer.maxDoc());
    assertEquals(3, reader.leaves().size());
    reader.close();
    writer.forceMerge(1);
    reader = writer.getReader();
    assertEquals(0, reader.numDocs());
    assertEquals(3, reader.maxDoc());
    assertEquals(0, writer.numDocs());
    assertEquals(3, writer.maxDoc());
    assertEquals(1, reader.leaves().size());
    IOUtils.close(reader, writer, dir);
  }

  /**
   * tests soft deletes that carry over deleted documents on merge for history rentention.
   */
  public void testSoftDeleteWithRetention() throws IOException, InterruptedException {
    AtomicInteger seqIds = new AtomicInteger(0);
    Directory dir = newDirectory();
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
    indexWriterConfig.setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete",
        () -> IntPoint.newRangeQuery("seq_id", seqIds.intValue() - 50, Integer.MAX_VALUE),
        indexWriterConfig.getMergePolicy()));
    indexWriterConfig.setSoftDeletesField("soft_delete");
    IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
    Thread[] threads = new Thread[2 + random().nextInt(3)];
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(threads.length);
    boolean updateSeveralDocs = random().nextBoolean();
    Set<String> ids = Collections.synchronizedSet(new HashSet<>());
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        try {
          started.countDown();
          startLatch.await();
          for (int d = 0;  d < 100; d++) {
            String id = String.valueOf(random().nextInt(10));
            int seqId = seqIds.incrementAndGet();
            if (updateSeveralDocs) {
              Document doc = new Document();
              doc.add(new StringField("id", id, Field.Store.YES));
              doc.add(new IntPoint("seq_id", seqId));
              writer.softUpdateDocuments(new Term("id", id), Arrays.asList(doc, doc),
                  new NumericDocValuesField("soft_delete", 1));
            } else {
              Document doc = new Document();
              doc.add(new StringField("id", id, Field.Store.YES));
              doc.add(new IntPoint("seq_id", seqId));
              writer.softUpdateDocument(new Term("id", id), doc,
                  new NumericDocValuesField("soft_delete", 1));
            }
            ids.add(id);
          }
        } catch (IOException | InterruptedException e) {
          throw new AssertionError(e);
        }
      });
      threads[i].start();
    }
    started.await();
    startLatch.countDown();

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = new IndexSearcher(reader);
    for (String id : ids) {
      TopDocs topDocs = searcher.search(new TermQuery(new Term("id", id)), 10);
      if (updateSeveralDocs) {
        assertEquals(2, topDocs.totalHits);
        assertEquals(Math.abs(topDocs.scoreDocs[0].doc - topDocs.scoreDocs[1].doc), 1);
      } else {
        assertEquals(1, topDocs.totalHits);
      }
    }
    writer.addDocument(new Document()); // add a dummy doc to trigger a segment here
    writer.flush();
    writer.forceMerge(1);
    DirectoryReader oldReader = reader;
    reader = DirectoryReader.openIfChanged(reader, writer);
    if (reader != null) {
      oldReader.close();
      assertNotSame(oldReader, reader);
    } else {
      reader = oldReader;
    }
    assertEquals(1, reader.leaves().size());
    LeafReaderContext leafReaderContext = reader.leaves().get(0);
    LeafReader leafReader = leafReaderContext.reader();
    searcher = new IndexSearcher(new FilterLeafReader(leafReader) {
      @Override
      public CacheHelper getCoreCacheHelper() {
        return leafReader.getCoreCacheHelper();
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return leafReader.getReaderCacheHelper();
      }

      @Override
      public Bits getLiveDocs() {
        return null;
      }

      @Override
      public int numDocs() {
        return maxDoc();
      }
    });
    TopDocs seq_id = searcher.search(IntPoint.newRangeQuery("seq_id", seqIds.intValue() - 50, Integer.MAX_VALUE), 10);
    assertTrue(seq_id.totalHits + " hits", seq_id.totalHits >= 50);
    searcher = new IndexSearcher(reader);
    for (String id : ids) {
      if (updateSeveralDocs) {
        assertEquals(2, searcher.search(new TermQuery(new Term("id", id)), 10).totalHits);
      } else {
        assertEquals(1, searcher.search(new TermQuery(new Term("id", id)), 10).totalHits);
      }
    }
    IOUtils.close(reader, writer, dir);
  }

}
