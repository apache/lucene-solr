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
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestSoftDeletesRetentionMergePolicy extends LuceneTestCase {

  public void testForceMergeFullyDeleted() throws IOException {
    Directory dir = newDirectory();
    AtomicBoolean letItGo = new AtomicBoolean(false);
    MergePolicy policy = new SoftDeletesRetentionMergePolicy("soft_delete",
        () -> letItGo.get() ? new MatchNoDocsQuery() : new MatchAllDocsQuery(), new LogDocMergePolicy());
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig().setMergePolicy(policy)
        .setSoftDeletesField("soft_delete");
    IndexWriter writer = new IndexWriter(dir, indexWriterConfig);

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new NumericDocValuesField("soft_delete", 1));
    writer.addDocument(doc);
    writer.commit();
    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.YES));
    doc.add(new NumericDocValuesField("soft_delete", 1));
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    {
      assertEquals(2, reader.leaves().size());
      final SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
      assertTrue(policy.keepFullyDeletedSegment(() -> segmentReader));
      assertEquals(0, policy.numDeletesToMerge(segmentReader.getSegmentInfo(), 0, () -> segmentReader));
    }
    {
      SegmentReader segmentReader = (SegmentReader) reader.leaves().get(1).reader();
      assertTrue(policy.keepFullyDeletedSegment(() -> segmentReader));
      assertEquals(0, policy.numDeletesToMerge(segmentReader.getSegmentInfo(), 0, () -> segmentReader));
      writer.forceMerge(1);
      reader.close();
    }
    reader = writer.getReader();
    {
      assertEquals(1, reader.leaves().size());
      SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
      assertEquals(2, reader.maxDoc());
      assertTrue(policy.keepFullyDeletedSegment(() -> segmentReader));
      assertEquals(0, policy.numDeletesToMerge(segmentReader.getSegmentInfo(), 0, () -> segmentReader));
    }
    writer.forceMerge(1); // make sure we don't merge this
    assertNull(DirectoryReader.openIfChanged(reader));

    writer.forceMergeDeletes(); // make sure we don't merge this
    assertNull(DirectoryReader.openIfChanged(reader));
    letItGo.set(true);
    writer.forceMergeDeletes(); // make sure we don't merge this
    DirectoryReader directoryReader = DirectoryReader.openIfChanged(reader);
    assertNotNull(directoryReader);
    assertEquals(0, directoryReader.numDeletedDocs());
    assertEquals(0, directoryReader.maxDoc());
    IOUtils.close(directoryReader, reader, writer, dir);
  }

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
    MergePolicy policy = new SoftDeletesRetentionMergePolicy("soft_delete",
        () -> new DocValuesFieldExistsQuery("keep_around"), NoMergePolicy.INSTANCE);
    assertFalse(policy.keepFullyDeletedSegment(() -> (SegmentReader) reader.leaves().get(0).reader()));
    reader.close();

    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new NumericDocValuesField("keep_around", 1));
    doc.add(new NumericDocValuesField("soft_delete", 1));
    writer.addDocument(doc);

    DirectoryReader reader1 = writer.getReader();
    assertEquals(2, reader1.leaves().size());
    assertFalse(policy.keepFullyDeletedSegment(() -> (SegmentReader) reader1.leaves().get(0).reader()));

    assertTrue(policy.keepFullyDeletedSegment(() -> (SegmentReader) reader1.leaves().get(1).reader()));

    IOUtils.close(reader1, writer, dir);
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
            if (rarely()) {
              writer.flush();
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

  public void testForceMergeDeletes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField("soft_delete");
    config.setMergePolicy(newMergePolicy(random(), false)); // no mock MP it might not select segments for force merge
    if (random().nextBoolean()) {
      config.setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete",
          () -> new MatchNoDocsQuery(), config.getMergePolicy()));
    }
    IndexWriter writer = new IndexWriter(dir, config);
    // The first segment includes d1 and d2
    for (int i = 0; i < 2; i++) {
      Document d = new Document();
      d.add(new StringField("id", Integer.toString(i), Field.Store.YES));
      writer.addDocument(d);
    }
    writer.flush();
    // The second segment includes only the tombstone
    Document tombstone = new Document();
    tombstone.add(new NumericDocValuesField("soft_delete", 1));
    writer.softUpdateDocument(new Term("id", "1"), tombstone, new NumericDocValuesField("soft_delete", 1));
    writer.forceMergeDeletes(true); // Internally, forceMergeDeletes will call flush to flush pending updates
    // Thus, we will have two segments - both having soft-deleted documents.
    // We expect any MP to merge these segments into one segment
    // when calling forceMergeDeletes.
    assertEquals(1, writer.segmentInfos.asList().size());
    assertEquals(1, writer.numDocs());
    assertEquals(1, writer.maxDoc());
    writer.close();
    dir.close();
  }

  public void testDropFullySoftDeletedSegment() throws Exception {
    Directory dir = newDirectory();
    String softDelete = random().nextBoolean() ? null : "soft_delete";
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(softDelete);
    config.setMergePolicy(newMergePolicy(random(), true));
    if (softDelete != null && random().nextBoolean()) {
      config.setMergePolicy(new SoftDeletesRetentionMergePolicy(softDelete,
          () -> new MatchNoDocsQuery(), config.getMergePolicy()));
    }
    IndexWriter writer = new IndexWriter(dir, config);
    for (int i = 0; i < 2; i++) {
      Document d = new Document();
      d.add(new StringField("id", Integer.toString(i), Field.Store.YES));
      writer.addDocument(d);
    }
    writer.flush();
    assertEquals(1, writer.segmentInfos.asList().size());

    if (softDelete != null) {
      // the newly created segment should be dropped as it is fully deleted (i.e. only contains deleted docs).
      if (random().nextBoolean()) {
        Document tombstone = new Document();
        tombstone.add(new NumericDocValuesField(softDelete, 1));
        writer.softUpdateDocument(new Term("id", "1"), tombstone, new NumericDocValuesField(softDelete, 1));
      } else {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(1), Field.Store.YES));
        if (random().nextBoolean()) {
          writer.softUpdateDocument(new Term("id", "1"), doc, new NumericDocValuesField(softDelete, 1));
        } else {
          writer.addDocument(doc);
        }
        writer.updateDocValues(new Term("id", "1"), new NumericDocValuesField(softDelete, 1));
      }
    } else {
      Document d = new Document();
      d.add(new StringField("id", "1", Field.Store.YES));
      writer.addDocument(d);
      writer.deleteDocuments(new Term("id", "1"));
    }
    writer.commit();
    IndexReader reader = writer.getReader();
    assertEquals(reader.numDocs(), 1);
    reader.close();
    assertEquals(1, writer.segmentInfos.asList().size());

    writer.close();
    dir.close();
  }
}
