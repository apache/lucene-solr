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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
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
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
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
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
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
    assertEquals(0, writer.getDocStats().numDocs);
    assertEquals(3, writer.getDocStats().maxDoc);
    assertEquals(3, reader.leaves().size());
    reader.close();
    writer.forceMerge(1);
    reader = writer.getReader();
    assertEquals(0, reader.numDocs());
    assertEquals(3, reader.maxDoc());
    assertEquals(0, writer.getDocStats().numDocs);
    assertEquals(3, writer.getDocStats().maxDoc);
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
        assertEquals(2, topDocs.totalHits.value);
        assertEquals(Math.abs(topDocs.scoreDocs[0].doc - topDocs.scoreDocs[1].doc), 1);
      } else {
        assertEquals(1, topDocs.totalHits.value);
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
    assertTrue(seq_id.totalHits.value + " hits", seq_id.totalHits.value >= 50);
    searcher = new IndexSearcher(reader);
    for (String id : ids) {
      if (updateSeveralDocs) {
        assertEquals(2, searcher.search(new TermQuery(new Term("id", id)), 10).totalHits.value);
      } else {
        assertEquals(1, searcher.search(new TermQuery(new Term("id", id)), 10).totalHits.value);
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
    writer.flush(false, true); // flush pending updates but don't trigger a merge, we run forceMergeDeletes below
    // Now we have have two segments - both having soft-deleted documents.
    // We expect any MP to merge these segments into one segment
    // when calling forceMergeDeletes.
    writer.forceMergeDeletes(true);
    assertEquals(1, writer.cloneSegmentInfos().size());
    assertEquals(1, writer.getDocStats().numDocs);
    assertEquals(1, writer.getDocStats().maxDoc);
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
    assertEquals(1, writer.cloneSegmentInfos().size());

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
    assertEquals(1, writer.cloneSegmentInfos().size());

    writer.close();
    dir.close();
  }

  public void testSoftDeleteWhileMergeSurvives() throws IOException {
    Directory dir = newDirectory();
    String softDelete = "soft_delete";
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(softDelete);
    AtomicBoolean update = new AtomicBoolean(true);
    config.setReaderPooling(true);
    config.setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete", () -> new DocValuesFieldExistsQuery("keep"),
        new LogDocMergePolicy()));
    IndexWriter writer = new IndexWriter(dir, config);
    writer.getConfig().setMergedSegmentWarmer(sr -> {
      if (update.compareAndSet(true, false)) {
        try {
          writer.softUpdateDocument(new Term("id", "0"), new Document(),
              new NumericDocValuesField(softDelete, 1), new NumericDocValuesField("keep", 1));
          writer.commit();
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }
    });

    boolean preExistingDeletes = random().nextBoolean();
    for (int i = 0; i < 2; i++) {
      Document d = new Document();
      d.add(new StringField("id", Integer.toString(i), Field.Store.YES));
      if (preExistingDeletes && random().nextBoolean()) {
        writer.addDocument(d); // randomly add a preexisting hard-delete that we don't carry over
        writer.deleteDocuments(new Term("id", Integer.toString(i)));
        d.add(new NumericDocValuesField("keep", 1));
        writer.addDocument(d);
      } else {
        d.add(new NumericDocValuesField("keep", 1));
        writer.addDocument(d);
      }
      writer.flush();
    }
    writer.forceMerge(1);
    writer.commit();
    assertFalse(update.get());
    DirectoryReader open = DirectoryReader.open(dir);
    assertEquals(0, open.numDeletedDocs());
    assertEquals(3, open.maxDoc());
    IOUtils.close(open, writer, dir);
  }

  /*
   * This test is trying to hard-delete a particular document while the segment is merged which is already soft-deleted
   * This requires special logic inside IndexWriter#carryOverHardDeletes since docMaps are not created for this document.
   */
  public void testDeleteDocWhileMergeThatIsSoftDeleted() throws IOException {
    Directory dir = newDirectory();
    String softDelete = "soft_delete";
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(softDelete);
    AtomicBoolean delete = new AtomicBoolean(true);
    config.setReaderPooling(true);
    config.setMergePolicy(new LogDocMergePolicy());
    IndexWriter writer = new IndexWriter(dir, config);
    Document d = new Document();
    d.add(new StringField("id", "0", Field.Store.YES));
    writer.addDocument(d);
    d = new Document();
    d.add(new StringField("id", "1", Field.Store.YES));
    writer.addDocument(d);
    if (random().nextBoolean()) {
      // randomly run with a preexisting hard delete
      d = new Document();
      d.add(new StringField("id", "2", Field.Store.YES));
      writer.addDocument(d);
      writer.deleteDocuments(new Term("id", "2"));
    }

    writer.flush();
    DirectoryReader reader = writer.getReader();
    writer.softUpdateDocument(new Term("id", "0"), new Document(),
        new NumericDocValuesField(softDelete, 1));
    writer.flush();
    writer.getConfig().setMergedSegmentWarmer(sr -> {
      if (delete.compareAndSet(true, false)) {
        try {
          long seqNo = writer.tryDeleteDocument(reader, 0);
          assertTrue("seqId was -1", seqNo !=  -1);
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }
    });
    writer.forceMerge(1);
    assertEquals(2, writer.getDocStats().numDocs);
    assertEquals(2, writer.getDocStats().maxDoc);
    assertFalse(delete.get());
    IOUtils.close(reader, writer, dir);
  }

  public void testUndeleteDocument() throws IOException {
    Directory dir = newDirectory();
    String softDelete = "soft_delete";
    IndexWriterConfig config = newIndexWriterConfig()
        .setSoftDeletesField(softDelete)
        .setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete",
        MatchAllDocsQuery::new, new LogDocMergePolicy()));
    config.setReaderPooling(true);
    config.setMergePolicy(new LogDocMergePolicy());
    IndexWriter writer = new IndexWriter(dir, config);
    Document d = new Document();
    d.add(new StringField("id", "0", Field.Store.YES));
    d.add(new StringField("seq_id", "0", Field.Store.YES));
    writer.addDocument(d);
    d = new Document();
    d.add(new StringField("id", "1", Field.Store.YES));
    writer.addDocument(d);
    writer.updateDocValues(new Term("id", "0"), new NumericDocValuesField("soft_delete", 1));
    try (IndexReader reader = writer.getReader()) {
      assertEquals(2, reader.maxDoc());
      assertEquals(1, reader.numDocs());
    }
    doUpdate(new Term("id", "0"), writer, new NumericDocValuesField("soft_delete", null));
    try (IndexReader reader = writer.getReader()) {
      assertEquals(2, reader.maxDoc());
      assertEquals(2, reader.numDocs());
    }
    IOUtils.close(writer, dir);
  }

  public void testMergeSoftDeleteAndHardDelete() throws Exception {
    Directory dir = newDirectory();
    String softDelete = "soft_delete";
    IndexWriterConfig config = newIndexWriterConfig()
        .setSoftDeletesField(softDelete)
        .setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete",
            MatchAllDocsQuery::new, new LogDocMergePolicy()));
    config.setReaderPooling(true);
    IndexWriter writer = new IndexWriter(dir, config);
    Document d = new Document();
    d.add(new StringField("id", "0", Field.Store.YES));
    writer.addDocument(d);
    d = new Document();
    d.add(new StringField("id", "1", Field.Store.YES));
    d.add(new NumericDocValuesField("soft_delete", 1));
    writer.addDocument(d);
    try (DirectoryReader reader = writer.getReader()) {
      assertEquals(2, reader.maxDoc());
      assertEquals(1, reader.numDocs());
    }
    while (true) {
      try (DirectoryReader reader = writer.getReader()) {
        TopDocs topDocs = new IndexSearcher(new IncludeSoftDeletesWrapper(reader)).search(new TermQuery(new Term("id", "1")), 1);
        assertEquals(1, topDocs.totalHits.value);
        if (writer.tryDeleteDocument(reader, topDocs.scoreDocs[0].doc) > 0) {
          break;
        }
      }
    }
    writer.forceMergeDeletes(true);
    assertEquals(1, writer.cloneSegmentInfos().size());
    SegmentCommitInfo si = writer.cloneSegmentInfos().info(0);
    assertEquals(0, si.getSoftDelCount()); // hard-delete should supersede the soft-delete
    assertEquals(0, si.getDelCount());
    assertEquals(1, si.info.maxDoc());
    IOUtils.close(writer, dir);
  }

  public void testSoftDeleteWithTryUpdateDocValue() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField("soft_delete")
        .setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete", MatchAllDocsQuery::new, newLogMergePolicy()));
    IndexWriter writer = new IndexWriter(dir, config);
    SearcherManager sm = new SearcherManager(writer, new SearcherFactory());
    Document d = new Document();
    d.add(new StringField("id", "0", Field.Store.YES));
    writer.addDocument(d);
    sm.maybeRefreshBlocking();
    doUpdate(new Term("id", "0"), writer,
        new NumericDocValuesField("soft_delete", 1), new NumericDocValuesField("other-field", 1));
    sm.maybeRefreshBlocking();
    assertEquals(1, writer.cloneSegmentInfos().size());
    SegmentCommitInfo si = writer.cloneSegmentInfos().info(0);
    assertEquals(1, si.getSoftDelCount());
    assertEquals(1, si.info.maxDoc());
    IOUtils.close(sm, writer, dir);
  }

  public void testMixedSoftDeletesAndHardDeletes() throws Exception {
    Directory dir = newDirectory();
    String softDeletesField = "soft-deletes";
    IndexWriterConfig config = newIndexWriterConfig()
        .setMaxBufferedDocs(2 + random().nextInt(50)).setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setSoftDeletesField(softDeletesField)
        .setMergePolicy(new SoftDeletesRetentionMergePolicy(softDeletesField, MatchAllDocsQuery::new, newMergePolicy()));
    IndexWriter writer = new IndexWriter(dir, config);
    int numDocs = 10 + random().nextInt(100);
    Set<String> liveDocs = new HashSet<>();
    for (int i = 0; i < numDocs; i++) {
      String id = Integer.toString(i);
      Document doc = new Document();
      doc.add(new StringField("id", id, Field.Store.YES));
      writer.addDocument(doc);
      liveDocs.add(id);
    }
    for (int i = 0; i < numDocs; i++) {
      if (random().nextBoolean()) {
        String id = Integer.toString(i);
        if (random().nextBoolean() && liveDocs.contains(id)) {
          doUpdate(new Term("id", id), writer, new NumericDocValuesField(softDeletesField, 1));
        } else {
          Document doc = new Document();
          doc.add(new StringField("id", "v" + id, Field.Store.YES));
          writer.softUpdateDocument(new Term("id", id), doc, new NumericDocValuesField(softDeletesField, 1));
          liveDocs.add("v" + id);
        }
      }
      if (random().nextBoolean() && liveDocs.isEmpty() == false) {
        String delId = RandomPicks.randomFrom(random(), liveDocs);
        if (random().nextBoolean()) {
          doDelete(new Term("id", delId), writer);
        } else {
          writer.deleteDocuments(new Term("id", delId));
        }
        liveDocs.remove(delId);
      }
    }
    try (DirectoryReader unwrapped = writer.getReader()) {
      DirectoryReader reader = new IncludeSoftDeletesWrapper(unwrapped);
      assertEquals(liveDocs.size(), reader.numDocs());
    }
    writer.commit();
    IOUtils.close(writer, dir);
  }

  public void testRewriteRetentionQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField("soft_deletes")
        .setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_deletes",
            () -> new PrefixQuery(new Term("id", "foo")), newMergePolicy()));
    IndexWriter writer = new IndexWriter(dir, config);

    Document d = new Document();
    d.add(new StringField("id", "foo-1", Field.Store.YES));
    writer.addDocument(d);
    d = new Document();
    d.add(new StringField("id", "foo-2", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "foo-1"), d, new NumericDocValuesField("soft_deletes", 1));

    d = new Document();
    d.add(new StringField("id", "bar-1", Field.Store.YES));
    writer.addDocument(d);
    d.add(new StringField("id", "bar-2", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "bar-1"), d, new NumericDocValuesField("soft_deletes", 1));

    writer.forceMerge(1);
    assertEquals(2, writer.getDocStats().numDocs); // foo-2, bar-2
    assertEquals(3, writer.getDocStats().maxDoc);  // foo-1, foo-2, bar-2
    IOUtils.close(writer, dir);
  }

  static void doUpdate(Term doc, IndexWriter writer, Field... fields) throws IOException {
    long seqId = -1;
    do { // retry if we just committing a merge
      try (DirectoryReader reader = writer.getReader()) {
        TopDocs topDocs = new IndexSearcher(new IncludeSoftDeletesWrapper(reader)).search(new TermQuery(doc), 10);
        assertEquals(1, topDocs.totalHits.value);
        int theDoc = topDocs.scoreDocs[0].doc;
        seqId = writer.tryUpdateDocValue(reader, theDoc, fields);
      }
    } while (seqId == -1);
  }

  static void doDelete(Term doc, IndexWriter writer) throws IOException {
    long seqId;
    do { // retry if we just committing a merge
      try (DirectoryReader reader = writer.getReader()) {
        TopDocs topDocs = new IndexSearcher(new IncludeSoftDeletesWrapper(reader)).search(new TermQuery(doc), 10);
        assertEquals(1, topDocs.totalHits.value);
        int theDoc = topDocs.scoreDocs[0].doc;
        seqId = writer.tryDeleteDocument(reader, theDoc);
      }
    } while (seqId == -1);
  }

  private static final class IncludeSoftDeletesSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
    @Override
    public LeafReader wrap(LeafReader reader) {
      while (reader instanceof FilterLeafReader) {
        reader = ((FilterLeafReader) reader).getDelegate();
      }
      Bits hardLiveDocs = ((SegmentReader) reader).getHardLiveDocs();
      final int numDocs;
      if (hardLiveDocs == null) {
        numDocs = reader.maxDoc();
      } else {
        int bits = 0;
        for (int i = 0; i < hardLiveDocs.length(); i++) {
          if (hardLiveDocs.get(i)) {
            bits++;
          }
        }
        numDocs = bits;
      }
      return new FilterLeafReader(reader) {
        @Override
        public int numDocs() {
          return numDocs;
        }

        @Override
        public Bits getLiveDocs() {
          return hardLiveDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
          return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
          return null;
        }
      };
    }
  }

  private static final class IncludeSoftDeletesWrapper extends FilterDirectoryReader {

    IncludeSoftDeletesWrapper(DirectoryReader in) throws IOException {
      super(in, new IncludeSoftDeletesSubReaderWrapper());
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new IncludeSoftDeletesWrapper(in);
    }


    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
}
