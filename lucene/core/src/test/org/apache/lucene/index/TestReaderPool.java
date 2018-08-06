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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NullInfoStream;

public class TestReaderPool extends LuceneTestCase {

  public void testDrop() throws IOException {
    Directory directory = newDirectory();
    FieldInfos.FieldNumbers fieldNumbers = buildIndex(directory);
    StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(directory);
    SegmentInfos segmentInfos = reader.segmentInfos.clone();

    ReaderPool pool = new ReaderPool(directory, directory, segmentInfos, fieldNumbers, () -> 0l, null, null, null);
    SegmentCommitInfo commitInfo = RandomPicks.randomFrom(random(), segmentInfos.asList());
    ReadersAndUpdates readersAndUpdates = pool.get(commitInfo, true);
    assertSame(readersAndUpdates, pool.get(commitInfo, false));
    assertTrue(pool.drop(commitInfo));
    if (random().nextBoolean()) {
      assertFalse(pool.drop(commitInfo));
    }
    assertNull(pool.get(commitInfo, false));
    pool.release(readersAndUpdates, random().nextBoolean());
    IOUtils.close(pool, reader, directory);
  }

  public void testPoolReaders() throws IOException {
    Directory directory = newDirectory();
    FieldInfos.FieldNumbers fieldNumbers = buildIndex(directory);
    StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(directory);
    SegmentInfos segmentInfos = reader.segmentInfos.clone();

    ReaderPool pool = new ReaderPool(directory, directory, segmentInfos, fieldNumbers, () -> 0l, null, null, null);
    SegmentCommitInfo commitInfo = RandomPicks.randomFrom(random(), segmentInfos.asList());
    assertFalse(pool.isReaderPoolingEnabled());
    pool.release(pool.get(commitInfo, true), random().nextBoolean());
    assertNull(pool.get(commitInfo, false));
    // now start pooling
    pool.enableReaderPooling();
    assertTrue(pool.isReaderPoolingEnabled());
    pool.release(pool.get(commitInfo, true), random().nextBoolean());
    assertNotNull(pool.get(commitInfo, false));
    assertSame(pool.get(commitInfo, false), pool.get(commitInfo, false));
    pool.drop(commitInfo);
    long ramBytesUsed = 0;
    assertEquals(0, pool.ramBytesUsed());
    for (SegmentCommitInfo info : segmentInfos) {
      pool.release(pool.get(info, true), random().nextBoolean());
      assertEquals(" used: " + ramBytesUsed + " actual: " + pool.ramBytesUsed(), 0, pool.ramBytesUsed());
      ramBytesUsed = pool.ramBytesUsed();
      assertSame(pool.get(info, false), pool.get(info, false));
    }
    assertNotSame(0, pool.ramBytesUsed());
    pool.dropAll();
    for (SegmentCommitInfo info : segmentInfos) {
      assertNull(pool.get(info, false));
    }
    assertEquals(0, pool.ramBytesUsed());
    IOUtils.close(pool, reader, directory);
  }


  public void testUpdate() throws IOException {
    Directory directory = newDirectory();
    FieldInfos.FieldNumbers fieldNumbers = buildIndex(directory);
    StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(directory);
    SegmentInfos segmentInfos = reader.segmentInfos.clone();
    ReaderPool pool = new ReaderPool(directory, directory, segmentInfos, fieldNumbers, () -> 0l,
        new NullInfoStream(), null, null);
    int id = random().nextInt(10);
    if (random().nextBoolean()) {
      pool.enableReaderPooling();
    }
    for (SegmentCommitInfo commitInfo : segmentInfos) {
      ReadersAndUpdates readersAndUpdates = pool.get(commitInfo, true);
      SegmentReader readOnlyClone = readersAndUpdates.getReadOnlyClone(IOContext.READ);
      PostingsEnum postings = readOnlyClone.postings(new Term("id", "" + id));
      boolean expectUpdate = false;
      int doc = -1;
      if (postings != null && postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        NumericDocValuesFieldUpdates number = new NumericDocValuesFieldUpdates(0, "number", commitInfo.info.maxDoc());
        number.add(doc = postings.docID(), 1000l);
        number.finish();
        readersAndUpdates.addDVUpdate(number);
        expectUpdate = true;
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
        assertTrue(pool.anyDocValuesChanges());
      } else {
        assertFalse(pool.anyDocValuesChanges());
      }
      readOnlyClone.close();
      boolean writtenToDisk;
      if (pool.isReaderPoolingEnabled()) {
        if (random().nextBoolean()) {
          writtenToDisk = pool.writeAllDocValuesUpdates();
          assertFalse(readersAndUpdates.isMerging());
        } else if (random().nextBoolean()) {
          writtenToDisk = pool.commit(segmentInfos);
          assertFalse(readersAndUpdates.isMerging());
        } else {
          writtenToDisk = pool.writeDocValuesUpdatesForMerge(Collections.singletonList(commitInfo));
          assertTrue(readersAndUpdates.isMerging());
        }
        assertFalse(pool.release(readersAndUpdates, random().nextBoolean()));
      } else {
        if (random().nextBoolean()) {
          writtenToDisk = pool.release(readersAndUpdates, random().nextBoolean());
          assertFalse(readersAndUpdates.isMerging());
        } else {
          writtenToDisk = pool.writeDocValuesUpdatesForMerge(Collections.singletonList(commitInfo));
          assertTrue(readersAndUpdates.isMerging());
          assertFalse(pool.release(readersAndUpdates, random().nextBoolean()));
        }
      }
      assertFalse(pool.anyDocValuesChanges());
      assertEquals(expectUpdate, writtenToDisk);
      if (expectUpdate) {
        readersAndUpdates = pool.get(commitInfo, true);
        SegmentReader updatedReader = readersAndUpdates.getReadOnlyClone(IOContext.READ);
        assertNotSame(-1, doc);
        NumericDocValues number = updatedReader.getNumericDocValues("number");
        assertEquals(doc, number.advance(doc));
        assertEquals(1000l, number.longValue());
       readersAndUpdates.release(updatedReader);
       assertFalse(pool.release(readersAndUpdates, random().nextBoolean()));
      }
    }
    IOUtils.close(pool, reader, directory);
  }

  public void testDeletes() throws IOException {
    Directory directory = newDirectory();
    FieldInfos.FieldNumbers fieldNumbers = buildIndex(directory);
    StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(directory);
    SegmentInfos segmentInfos = reader.segmentInfos.clone();
    ReaderPool pool = new ReaderPool(directory, directory, segmentInfos, fieldNumbers, () -> 0l,
        new NullInfoStream(), null, null);
    int id = random().nextInt(10);
    if (random().nextBoolean()) {
      pool.enableReaderPooling();
    }
    for (SegmentCommitInfo commitInfo : segmentInfos) {
      ReadersAndUpdates readersAndUpdates = pool.get(commitInfo, true);
      SegmentReader readOnlyClone = readersAndUpdates.getReadOnlyClone(IOContext.READ);
      PostingsEnum postings = readOnlyClone.postings(new Term("id", "" + id));
      boolean expectUpdate = false;
      int doc = -1;
      if (postings != null && postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        assertTrue(readersAndUpdates.delete(doc = postings.docID()));
        expectUpdate = true;
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
      }
      assertFalse(pool.anyDocValuesChanges()); // deletes are not accounted here
      readOnlyClone.close();
      boolean writtenToDisk;
      if (pool.isReaderPoolingEnabled()) {
        writtenToDisk = pool.commit(segmentInfos);
        assertFalse(pool.release(readersAndUpdates, random().nextBoolean()));
      } else {
        writtenToDisk = pool.release(readersAndUpdates, random().nextBoolean());
      }
      assertFalse(pool.anyDocValuesChanges());
      assertEquals(expectUpdate, writtenToDisk);
      if (expectUpdate) {
        readersAndUpdates = pool.get(commitInfo, true);
        SegmentReader updatedReader = readersAndUpdates.getReadOnlyClone(IOContext.READ);
        assertNotSame(-1, doc);
        assertFalse(updatedReader.getLiveDocs().get(doc));
        readersAndUpdates.release(updatedReader);
        assertFalse(pool.release(readersAndUpdates, random().nextBoolean()));
      }
    }
    IOUtils.close(pool, reader, directory);
  }

  public void testPassReaderToMergePolicyConcurrently() throws Exception {
    Directory directory = newDirectory();
    FieldInfos.FieldNumbers fieldNumbers = buildIndex(directory);
    StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(directory);
    SegmentInfos segmentInfos = reader.segmentInfos.clone();
    ReaderPool pool = new ReaderPool(directory, directory, segmentInfos, fieldNumbers, () -> 0L,
        new NullInfoStream(), null, null);
    if (random().nextBoolean()) {
      pool.enableReaderPooling();
    }
    AtomicBoolean isDone = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(1);
    Thread refresher = new Thread(() -> {
      try {
        latch.countDown();
        while (isDone.get() == false) {
          for (SegmentCommitInfo commitInfo : segmentInfos) {
            ReadersAndUpdates readersAndUpdates = pool.get(commitInfo, true);
            SegmentReader segmentReader = readersAndUpdates.getReader(IOContext.READ);
            readersAndUpdates.release(segmentReader);
            pool.release(readersAndUpdates, random().nextBoolean());
          }
        }
      } catch (Exception ex) {
        throw new AssertionError(ex);
      }
    });
    refresher.start();
    MergePolicy mergePolicy = new FilterMergePolicy(newMergePolicy()) {
      @Override
      public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
        CodecReader reader = readerIOSupplier.get();
        assert reader.maxDoc() > 0; // just try to access the reader
        return true;
      }
    };
    latch.await();
    for (int i = 0; i < reader.maxDoc(); i++) {
      for (SegmentCommitInfo commitInfo : segmentInfos) {
        ReadersAndUpdates readersAndUpdates = pool.get(commitInfo, true);
        SegmentReader sr = readersAndUpdates.getReadOnlyClone(IOContext.READ);
        PostingsEnum postings = sr.postings(new Term("id", "" + i));
        sr.decRef();
        if (postings != null) {
          for (int docId = postings.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = postings.nextDoc()) {
            readersAndUpdates.delete(docId);
            assertTrue(readersAndUpdates.keepFullyDeletedSegment(mergePolicy));
          }
        }
        assertTrue(readersAndUpdates.keepFullyDeletedSegment(mergePolicy));
        pool.release(readersAndUpdates, random().nextBoolean());
      }
    }
    isDone.set(true);
    refresher.join();
    IOUtils.close(pool, reader, directory);
  }

  private FieldInfos.FieldNumbers buildIndex(Directory directory) throws IOException {
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig());
    for (int i = 0; i < 10; i++) {
      Document document = new Document();
      document.add(new StringField("id", "" + i, Field.Store.YES));
      document.add(new NumericDocValuesField("number", i));
      writer.addDocument(document);
      if (random().nextBoolean()) {
        writer.flush();
      }
    }
    writer.commit();
    writer.close();
    return writer.globalFieldNumberMap;
  }

  public void testGetReaderByRam() throws IOException {
    Directory directory = newDirectory();
    FieldInfos.FieldNumbers fieldNumbers = buildIndex(directory);
    StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(directory);
    SegmentInfos segmentInfos = reader.segmentInfos.clone();
    ReaderPool pool = new ReaderPool(directory, directory, segmentInfos, fieldNumbers, () -> 0l,
        new NullInfoStream(), null, null);
    assertEquals(0, pool.getReadersByRam().size());

    int ord = 0;
    for (SegmentCommitInfo commitInfo : segmentInfos) {
      ReadersAndUpdates readersAndUpdates = pool.get(commitInfo, true);
      BinaryDocValuesFieldUpdates test = new BinaryDocValuesFieldUpdates(0, "test", commitInfo.info.maxDoc());
      test.add(0, new BytesRef(new byte[ord++]));
      test.finish();
      readersAndUpdates.addDVUpdate(test);
    }

    List<ReadersAndUpdates> readersByRam = pool.getReadersByRam();
    assertEquals(segmentInfos.size(), readersByRam.size());
    long previousRam = Long.MAX_VALUE;
    for (ReadersAndUpdates rld : readersByRam) {
      assertTrue("previous: " + previousRam + " now: " + rld.ramBytesUsed.get(), previousRam >= rld.ramBytesUsed.get());
      previousRam = rld.ramBytesUsed.get();
      rld.dropChanges();
      pool.drop(rld.info);
    }
    IOUtils.close(pool, reader, directory);
  }
}
