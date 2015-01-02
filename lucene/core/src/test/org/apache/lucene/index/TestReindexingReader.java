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
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

// TODO:
//   - old parallel indices are only pruned on commit/close; can we do it on refresh?

/** Simple example showing how to use ParallelLeafReader to index new
 *  stuff (postings, DVs, etc.) from previously stored fields, on the
 *  fly (during NRT reader reopen), after the  initial indexing.  The
 *  test indexes just a single stored field with text "content X" (X is
 *  a number embedded in the text).
 *
 *  Then, on reopen, for any newly created segments (flush or merge), it
 *  builds a new parallel segment by loading all stored docs, parsing
 *  out that X, and adding it as DV and numeric indexed (trie) field.
 *
 *  Finally, for searching, it builds a top-level MultiReader, with
 *  ParallelLeafReader for each segment, and then tests that random
 *  numeric range queries, and sorting by the new DV field, work
 *  correctly.
 *
 *  Each per-segment index lives in a private directory next to the main
 *  index, and they are deleted once their segments are removed from the
 *  index.  They are "volatile", meaning if e.g. the index is replicated to
 *  another machine, it's OK to not copy parallel segments indices,
 *  since they will just be regnerated (at a cost though). */

public class TestReindexingReader extends LuceneTestCase {

  static final boolean DEBUG = false;

  private ReindexingReader getReindexer(Path root) throws IOException {
    return new ReindexingReader(newFSDirectory(root), root.resolve("segs")) {
      @Override
      protected IndexWriterConfig getIndexWriterConfig() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TieredMergePolicy tmp = new TieredMergePolicy();
        // We write tiny docs, so we need tiny floor to avoid O(N^2) merging:
        tmp.setFloorSegmentMB(.01);
        iwc.setMergePolicy(tmp);
        return iwc;
      }

      @Override
      protected Directory openDirectory(Path path) throws IOException {
        MockDirectoryWrapper dir = newMockFSDirectory(path);
        dir.setUseSlowOpenClosers(false);
        dir.setThrottling(Throttling.NEVER);
        return dir;
      }

      @Override
      protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();

        // The order of our docIDs must precisely matching incoming reader:
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        IndexWriter w = new IndexWriter(parallelDir, iwc);
        int maxDoc = reader.maxDoc();

        // Slowly parse the stored field into a new doc values field:
        for(int i=0;i<maxDoc;i++) {
          // TODO: is this still O(blockSize^2)?
          Document oldDoc = reader.document(i);
          Document newDoc = w.newDocument();
          long value = Long.parseLong(oldDoc.getString("text").split(" ")[1]);
          newDoc.addLong("number", value);
          w.addDocument(newDoc);
        }

        if (random().nextBoolean()) {
          w.forceMerge(1);
        }

        w.close();
      }

      @Override
      protected long getCurrentSchemaGen() {
        return 0;
      }
    };
  }

  /** Schema change by adding a new number_<schemaGen> DV field each time. */
  private ReindexingReader getReindexerNewDVFields(Path root, final AtomicLong currentSchemaGen) throws IOException {
    return new ReindexingReader(newFSDirectory(root), root.resolve("segs")) {
      @Override
      protected IndexWriterConfig getIndexWriterConfig() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TieredMergePolicy tmp = new TieredMergePolicy();
        // We write tiny docs, so we need tiny floor to avoid O(N^2) merging:
        tmp.setFloorSegmentMB(.01);
        iwc.setMergePolicy(tmp);
        return iwc;
      }

      @Override
      protected Directory openDirectory(Path path) throws IOException {
        MockDirectoryWrapper dir = newMockFSDirectory(path);
        dir.setUseSlowOpenClosers(false);
        dir.setThrottling(Throttling.NEVER);
        return dir;
      }

      @Override
      protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();

        // The order of our docIDs must precisely matching incoming reader:
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        IndexWriter w = new IndexWriter(parallelDir, iwc);
        int maxDoc = reader.maxDoc();

        if (oldSchemaGen <= 0) {
          // Must slowly parse the stored field into a new doc values field:
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = w.newDocument();
            long value = Long.parseLong(oldDoc.getString("text").split(" ")[1]);
            newDoc.addLong("number_" + newSchemaGen, value);
            newDoc.addLong("number", value);
            w.addDocument(newDoc);
          }
        } else {
          // Just carry over doc values from previous field:
          NumericDocValues oldValues = reader.getNumericDocValues("number_" + oldSchemaGen);
          assertNotNull("oldSchemaGen=" + oldSchemaGen, oldValues);
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = w.newDocument();
            newDoc.addLong("number_" + newSchemaGen, oldValues.get(i));
            w.addDocument(newDoc);
          }
        }

        if (random().nextBoolean()) {
          w.forceMerge(1);
        }

        w.close();
      }

      @Override
      protected long getCurrentSchemaGen() {
        return currentSchemaGen.get();
      }

      @Override
      protected void checkParallelReader(LeafReader r, LeafReader parR, long schemaGen) throws IOException {
        String fieldName = "number_" + schemaGen;
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: now check parallel number DVs field=" + fieldName + " r=" + r + " parR=" + parR);
        NumericDocValues numbers = parR.getNumericDocValues(fieldName);
        if (numbers == null) {
          return;
        }
        int maxDoc = r.maxDoc();
        boolean failed = false;
        for(int i=0;i<maxDoc;i++) {
          Document oldDoc = r.document(i);
          long value = Long.parseLong(oldDoc.getString("text").split(" ")[1]);
          if (value != numbers.get(i)) {
            if (DEBUG) System.out.println("FAIL: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i) + " numbers=" + numbers);
            failed = true;
          } else if (failed) {
            if (DEBUG) System.out.println("OK: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i));
          }
        }
        assertFalse("FAILED field=" + fieldName + " r=" + r, failed);
      }
    };
  }

  /** Schema change by adding changing how the same "number" DV field is indexed. */
  private ReindexingReader getReindexerSameDVField(Path root, final AtomicLong currentSchemaGen, final AtomicLong mergingSchemaGen) throws IOException {
    return new ReindexingReader(newFSDirectory(root), root.resolve("segs")) {
      @Override
      protected IndexWriterConfig getIndexWriterConfig() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TieredMergePolicy tmp = new TieredMergePolicy();
        // We write tiny docs, so we need tiny floor to avoid O(N^2) merging:
        tmp.setFloorSegmentMB(.01);
        iwc.setMergePolicy(tmp);
        return iwc;
      }

      @Override
      protected Directory openDirectory(Path path) throws IOException {
        MockDirectoryWrapper dir = newMockFSDirectory(path);
        dir.setUseSlowOpenClosers(false);
        dir.setThrottling(Throttling.NEVER);
        return dir;
      }

      @Override
      protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();

        // The order of our docIDs must precisely matching incoming reader:
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        IndexWriter w = new IndexWriter(parallelDir, iwc);
        int maxDoc = reader.maxDoc();

        if (oldSchemaGen <= 0) {
          // Must slowly parse the stored field into a new doc values field:
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = w.newDocument();
            long value = Long.parseLong(oldDoc.getString("text").split(" ")[1]);
            newDoc.addLong("number", newSchemaGen*value);
            w.addDocument(newDoc);
          }
        } else {
          // Just carry over doc values from previous field:
          NumericDocValues oldValues = reader.getNumericDocValues("number");
          assertNotNull("oldSchemaGen=" + oldSchemaGen, oldValues);
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = w.newDocument();
            newDoc.addLong("number", newSchemaGen*(oldValues.get(i)/oldSchemaGen));
            w.addDocument(newDoc);
          }
        }

        if (random().nextBoolean()) {
          w.forceMerge(1);
        }

        w.close();
      }

      @Override
      protected long getCurrentSchemaGen() {
        return currentSchemaGen.get();
      }

      @Override
      protected long getMergingSchemaGen() {
        return mergingSchemaGen.get();
      }

      @Override
      protected void checkParallelReader(LeafReader r, LeafReader parR, long schemaGen) throws IOException {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: now check parallel number DVs r=" + r + " parR=" + parR);
        NumericDocValues numbers = parR.getNumericDocValues("numbers");
        if (numbers == null) {
          return;
        }
        int maxDoc = r.maxDoc();
        boolean failed = false;
        for(int i=0;i<maxDoc;i++) {
          Document oldDoc = r.document(i);
          long value = Long.parseLong(oldDoc.getString("text").split(" ")[1]);
          value *= schemaGen;
          if (value != numbers.get(i)) {
            System.out.println("FAIL: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i) + " numbers=" + numbers);
            failed = true;
          } else if (failed) {
            System.out.println("OK: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i));
          }
        }
        assertFalse("FAILED r=" + r, failed);
      }
    };
  }

  public void testBasicMultipleSchemaGens() throws Exception {

    AtomicLong currentSchemaGen = new AtomicLong();

    // TODO: separate refresh thread, search threads, indexing threads
    ReindexingReader reindexer = getReindexerNewDVFields(createTempDir(), currentSchemaGen);
    reindexer.commit();

    Document doc = reindexer.w.newDocument();
    doc.addLargeText("text", "number " + random().nextLong());
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: refresh @ 1 doc");
    reindexer.mgr.maybeRefresh();
    DirectoryReader r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
    } finally {
      reindexer.mgr.release(r);
    }
    //reindexer.printRefCounts();

    currentSchemaGen.incrementAndGet();

    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: increment schemaGen");
    if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST: commit");
    reindexer.commit();

    doc = reindexer.w.newDocument();
    doc.addLargeText("text", "number " + random().nextLong());
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println("TEST: refresh @ 2 docs");
    reindexer.mgr.maybeRefresh();
    //reindexer.printRefCounts();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: forceMerge");
    reindexer.w.forceMerge(1);

    currentSchemaGen.incrementAndGet();

    if (DEBUG) System.out.println("TEST: commit");
    reindexer.commit();

    if (DEBUG) System.out.println("TEST: refresh after forceMerge");
    reindexer.mgr.maybeRefresh();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: close writer");
    reindexer.close();
    reindexer.indexDir.close();
  }

  public void testRandomMultipleSchemaGens() throws Exception {

    AtomicLong currentSchemaGen = new AtomicLong();
    ReindexingReader reindexer = null;

    // TODO: separate refresh thread, search threads, indexing threads
    int numDocs = atLeast(TEST_NIGHTLY ? 20000 : 1000);
    int maxID = 0;
    Path root = createTempDir();
    int refreshEveryNumDocs = 100;
    int commitCloseNumDocs = 1000;
    for(int i=0;i<numDocs;i++) {
      if (reindexer == null) {
        reindexer = getReindexerNewDVFields(root, currentSchemaGen);
      }

      Document doc = reindexer.w.newDocument();
      String id;
      String updateID;
      if (maxID > 0 && random().nextInt(10) == 7) {
        // Replace a doc
        id = "" + random().nextInt(maxID);
        updateID = id;
      } else {
        id = "" + (maxID++);
        updateID = null;
      }
        
      doc.addAtom("id", id);
      doc.addLargeText("text", "number " + random().nextLong());
      if (updateID == null) {
        reindexer.w.addDocument(doc);
      } else {
        reindexer.w.updateDocument(new Term("id", updateID), doc);
      }
      if (random().nextInt(refreshEveryNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: refresh @ " + (i+1) + " docs");
        reindexer.mgr.maybeRefresh();

        DirectoryReader r = reindexer.mgr.acquire();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: got reader=" + r);
        try {
          checkAllNumberDVs(r, "number_" + currentSchemaGen.get(), true, 1);
        } finally {
          reindexer.mgr.release(r);
        }
        if (DEBUG) reindexer.printRefCounts();
        refreshEveryNumDocs = (int) (1.25 * refreshEveryNumDocs);
      }

      if (random().nextInt(500) == 17) {
        currentSchemaGen.incrementAndGet();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: advance schemaGen to " + currentSchemaGen);
      }

      if (i > 0 && random().nextInt(10) == 7) {
        // Random delete:
        reindexer.w.deleteDocuments(new Term("id", ""+random().nextInt(i)));
      }

      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: commit @ " + (i+1) + " docs");
        reindexer.commit();
        //reindexer.printRefCounts();
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }

      // Sometimes close & reopen writer/manager, to confirm the parallel segments persist:
      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: close writer @ " + (i+1) + " docs");
        reindexer.close();
        reindexer.indexDir.close();
        reindexer = null;
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }
    }

    if (reindexer != null) {
      reindexer.close();
      reindexer.indexDir.close();
    }
  }

  /** First schema change creates a new "number" DV field off the stored field; subsequent changes just change the value of that number
   *  field for all docs. */
  public void testRandomMultipleSchemaGensSameField() throws Exception {

    AtomicLong currentSchemaGen = new AtomicLong();
    AtomicLong mergingSchemaGen = new AtomicLong();

    ReindexingReader reindexer = null;

    // TODO: separate refresh thread, search threads, indexing threads
    int numDocs = atLeast(TEST_NIGHTLY ? 20000 : 1000);
    int maxID = 0;
    Path root = createTempDir();
    int refreshEveryNumDocs = 100;
    int commitCloseNumDocs = 1000;

    for(int i=0;i<numDocs;i++) {
      if (reindexer == null) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: open new reader/writer");
        reindexer = getReindexerSameDVField(root, currentSchemaGen, mergingSchemaGen);
      }

      Document doc = reindexer.w.newDocument();
      String id;
      String updateID;
      if (maxID > 0 && random().nextInt(10) == 7) {
        // Replace a doc
        id = "" + random().nextInt(maxID);
        updateID = id;
      } else {
        id = "" + (maxID++);
        updateID = null;
      }
        
      doc.addAtom("id", id);
      doc.addLargeText("text", "number " + TestUtil.nextInt(random(), -10000, 10000));
      if (updateID == null) {
        reindexer.w.addDocument(doc);
      } else {
        reindexer.w.updateDocument(new Term("id", updateID), doc);
      }
      if (random().nextInt(refreshEveryNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: refresh @ " + (i+1) + " docs");
        reindexer.mgr.maybeRefresh();
        DirectoryReader r = reindexer.mgr.acquire();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: got reader=" + r);
        try {
          checkAllNumberDVs(r, "number", true, (int) currentSchemaGen.get());
        } finally {
          reindexer.mgr.release(r);
        }
        if (DEBUG) reindexer.printRefCounts();
        refreshEveryNumDocs = (int) (1.25 * refreshEveryNumDocs);
      }

      if (random().nextInt(500) == 17) {
        currentSchemaGen.incrementAndGet();
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: advance schemaGen to " + currentSchemaGen);
        if (random().nextBoolean()) {
          mergingSchemaGen.incrementAndGet();
          if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: advance mergingSchemaGen to " + mergingSchemaGen);
        }
      }

      if (i > 0 && random().nextInt(10) == 7) {
        // Random delete:
        reindexer.w.deleteDocuments(new Term("id", ""+random().nextInt(i)));
      }

      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: commit @ " + (i+1) + " docs");
        reindexer.commit();
        //reindexer.printRefCounts();
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }

      // Sometimes close & reopen writer/manager, to confirm the parallel segments persist:
      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println(Thread.currentThread().getName() + ": TEST TOP: close writer @ " + (i+1) + " docs");
        reindexer.close();
        reindexer.indexDir.close();
        reindexer = null;
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }
    }

    if (reindexer != null) {
      reindexer.close();
      reindexer.indexDir.close();
    }
    Directory dir = newFSDirectory(root.resolve("index"));

    if (DirectoryReader.indexExists(dir)) {
      // Verify main index never reflects schema changes beyond mergingSchemaGen:
      try (IndexReader r = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : r.leaves()) {
          LeafReader leaf = ctx.reader();
          NumericDocValues numbers = leaf.getNumericDocValues("number");
          if (numbers != null) {
            int maxDoc = leaf.maxDoc();
            for(int i=0;i<maxDoc;i++) {
              Document doc = leaf.document(i);
              long value = Long.parseLong(doc.getString("text").split(" ")[1]);
              long dvValue = numbers.get(i);
              if (value == 0) {
                assertEquals(0, dvValue);
              } else {
                assertTrue(dvValue % value == 0);
                assertTrue(dvValue / value <= mergingSchemaGen.get());
              }
            }
          }
        }
      }
    }
    dir.close();
  }

  public void testBasic() throws Exception {
    ReindexingReader reindexer = getReindexer(createTempDir());

    // Start with initial empty commit:
    reindexer.commit();

    Document doc = reindexer.w.newDocument();
    doc.addLargeText("text", "number " + random().nextLong());
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println("TEST: refresh @ 1 doc");
    reindexer.mgr.maybeRefresh();
    DirectoryReader r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r);
      IndexSearcher s = newSearcher(r);
      testNumericDVSort(s);
      testRanges(s);
    } finally {
      reindexer.mgr.release(r);
    }
    //reindexer.printRefCounts();

    if (DEBUG) System.out.println("TEST: commit");
    reindexer.commit();

    doc = reindexer.w.newDocument();
    doc.addLargeText("text", "number " + random().nextLong());
    reindexer.w.addDocument(doc);

    if (DEBUG) System.out.println("TEST: refresh @ 2 docs");
    reindexer.mgr.maybeRefresh();
    //reindexer.printRefCounts();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r);
      IndexSearcher s = newSearcher(r);
      testNumericDVSort(s);
      testRanges(s);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: forceMerge");
    reindexer.w.forceMerge(1);

    if (DEBUG) System.out.println("TEST: commit");
    reindexer.commit();

    if (DEBUG) System.out.println("TEST: refresh after forceMerge");
    reindexer.mgr.maybeRefresh();
    r = reindexer.mgr.acquire();
    if (DEBUG) System.out.println("TEST: got reader=" + r);
    try {
      checkAllNumberDVs(r);
      IndexSearcher s = newSearcher(r);
      testNumericDVSort(s);
      testRanges(s);
    } finally {
      reindexer.mgr.release(r);
    }

    if (DEBUG) System.out.println("TEST: close writer");
    reindexer.close();
    reindexer.indexDir.close();
  }

  public void testRandom() throws Exception {
    Path root = createTempDir();
    ReindexingReader reindexer = null;

    // TODO: separate refresh thread, search threads, indexing threads
    int numDocs = atLeast(TEST_NIGHTLY ? 20000 : 1000);
    int maxID = 0;
    int refreshEveryNumDocs = 100;
    int commitCloseNumDocs = 1000;
    for(int i=0;i<numDocs;i++) {
      if (reindexer == null) {
        if (DEBUG) System.out.println("TEST: open writer @ " + (i+1) + " docs");
        reindexer = getReindexer(root);
      }

      Document doc = reindexer.w.newDocument();
      String id;
      String updateID;
      if (maxID > 0 && random().nextInt(10) == 7) {
        // Replace a doc
        id = "" + random().nextInt(maxID);
        updateID = id;
      } else {
        id = "" + (maxID++);
        updateID = null;
      }
        
      doc.addAtom("id", id);
      doc.addLargeText("text", "number " + random().nextLong());
      if (updateID == null) {
        reindexer.w.addDocument(doc);
      } else {
        reindexer.w.updateDocument(new Term("id", updateID), doc);
      }

      if (random().nextInt(refreshEveryNumDocs) == 17) {
        if (DEBUG) System.out.println("TEST: refresh @ " + (i+1) + " docs");
        reindexer.mgr.maybeRefresh();
        DirectoryReader r = reindexer.mgr.acquire();
        if (DEBUG) System.out.println("TEST: got reader=" + r);
        try {
          checkAllNumberDVs(r);
          IndexSearcher s = newSearcher(r);
          testNumericDVSort(s);
          testRanges(s);
        } finally {
          reindexer.mgr.release(r);
        }
        refreshEveryNumDocs = (int) (1.25 * refreshEveryNumDocs);
      }

      if (i > 0 && random().nextInt(10) == 7) {
        // Random delete:
        reindexer.w.deleteDocuments(new Term("id", ""+random().nextInt(i)));
      }

      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println("TEST: commit @ " + (i+1) + " docs");
        reindexer.commit();
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }

      // Sometimes close & reopen writer/manager, to confirm the parallel segments persist:
      if (random().nextInt(commitCloseNumDocs) == 17) {
        if (DEBUG) System.out.println("TEST: close writer @ " + (i+1) + " docs");
        reindexer.close();
        reindexer.indexDir.close();
        reindexer = null;
        commitCloseNumDocs = (int) (1.25 * commitCloseNumDocs);
      }
    }
    if (reindexer != null) {
      reindexer.close();
      reindexer.indexDir.close();
    }
  }

  private static void checkAllNumberDVs(IndexReader r) throws IOException {
    checkAllNumberDVs(r, "number", true, 1);
  }

  private static void checkAllNumberDVs(IndexReader r, String fieldName, boolean doThrow, int multiplier) throws IOException {
    NumericDocValues numbers = MultiDocValues.getNumericValues(r, fieldName);
    int maxDoc = r.maxDoc();
    boolean failed = false;
    long t0 = System.currentTimeMillis();
    for(int i=0;i<maxDoc;i++) {
      Document oldDoc = r.document(i);
      long value = multiplier * Long.parseLong(oldDoc.getString("text").split(" ")[1]);
      if (value != numbers.get(i)) {
        System.out.println("FAIL: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i) + " numbers=" + numbers);
        failed = true;
      } else if (failed) {
        System.out.println("OK: docID=" + i + " " + oldDoc+ " value=" + value + " number=" + numbers.get(i));
      }
    }
    if (failed) {
      if (r instanceof LeafReader == false) {
        System.out.println("TEST FAILED; check leaves");
        for(LeafReaderContext ctx : r.leaves()) {
          System.out.println("CHECK LEAF=" + ctx.reader());
          checkAllNumberDVs(ctx.reader(), fieldName, false, 1);
        }
      }
      if (doThrow) {
        assertFalse("FAILED field=" + fieldName + " r=" + r, failed);
      } else {
        System.out.println("FAILED field=" + fieldName + " r=" + r);
      }
    }
  }

  private static void testNumericDVSort(IndexSearcher s) throws IOException {
    // Confirm we can sort by the new DV field:
    TopDocs hits = s.search(new MatchAllDocsQuery(), 100, new Sort(new SortField("number", SortField.Type.LONG)));
    NumericDocValues numbers = MultiDocValues.getNumericValues(s.getIndexReader(), "number");
    long last = Long.MIN_VALUE;
    for(ScoreDoc scoreDoc : hits.scoreDocs) {
      long value = Long.parseLong(s.doc(scoreDoc.doc).getString("text").split(" ")[1]);
      assertTrue(value >= last);
      assertEquals(value, numbers.get(scoreDoc.doc));
      last = value;
    }
  }

  private static void testRanges(IndexSearcher s) throws IOException {
    NumericDocValues numbers = MultiDocValues.getNumericValues(s.getIndexReader(), "number");
    FieldTypes fieldTypes = s.getFieldTypes();
    for(int i=0;i<100;i++) {
      // Confirm we can range search by the new indexed (numeric) field:
      long min = random().nextLong();
      long max = random().nextLong();
      if (min > max) {
        long x = min;
        min = max;
        max = x;
      }

      TopDocs hits = s.search(new ConstantScoreQuery(fieldTypes.newLongRangeFilter("number", min, true, max, true)), 100);
      for(ScoreDoc scoreDoc : hits.scoreDocs) {
        long value = Long.parseLong(s.doc(scoreDoc.doc).getString("text").split(" ")[1]);
        assertTrue(value >= min);
        assertTrue(value <= max);
        assertEquals(value, numbers.get(scoreDoc.doc));
      }
    }
  }

  // TODO: test exceptions

  public void testSwitchToDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Test relies on doc order:
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    int numDocs = atLeast(1000);
    long[] numbers = new long[numDocs];
    if (VERBOSE) {
      System.out.println("numDocs=" + numDocs);
    }
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      long number = random().nextLong();
      numbers[i] = number;
      doc.addStoredLong("number", number);
      w.addDocument(doc);
      // Make sure we have at least 2 segments, else forceMerge won't do anything:
      if (i == numDocs/2) {
        w.commit();
      }
    }
    w.close();

    ReindexingReader r = new ReindexingReader(dir, createTempDir()) {
        @Override
        protected long getCurrentSchemaGen() {
          return 0;
        }

        @Override
        protected IndexWriterConfig getIndexWriterConfig() {
          // Test relies on doc order:
          return newIndexWriterConfig().setMergePolicy(new AlwaysForceMergePolicy(newLogMergePolicy()));
        }

        @Override
        protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
          assertEquals(0L, newSchemaGen);
          assertEquals(-1L, oldSchemaGen);

          IndexWriterConfig iwc = newIndexWriterConfig();
          if (VERBOSE) {
            System.out.println("TEST: build parallel " + reader);
          }

          // The order of our docIDs must precisely matching incoming reader:
          iwc.setMergePolicy(new LogByteSizeMergePolicy());
          IndexWriter w = new IndexWriter(parallelDir, iwc);
          int maxDoc = reader.maxDoc();

          // Slowly parse the stored field into a new doc values field:
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = w.newDocument();
            newDoc.addLong("numberDV", oldDoc.getLong("number").longValue());
            w.addDocument(newDoc);
          }

          if (random().nextBoolean()) {
            w.forceMerge(1);
          }

          w.close();
        }
      };
    
    if (VERBOSE) {
      System.out.println("TEST: now force merge");
    }

    r.w.forceMerge(1);
    // Make sure main + parallel index was fully updated:
    DirectoryReader dr = r.mgr.acquire();
    if (VERBOSE) {
      System.out.println("TEST: dr=" + dr);
    }
    try {
      NumericDocValues dv = MultiDocValues.getNumericValues(dr, "numberDV");
      for(int i=0;i<numDocs;i++) {
        assertEquals(numbers[i], dv.get(i));
      }
    } finally {
      r.mgr.release(dr);
    }
    r.close();

    // Make sure main index was fully updated:
    dr = DirectoryReader.open(dir);
    if (VERBOSE) {
      System.out.println("TEST: final reader " + dr);
    }
    NumericDocValues dv = MultiDocValues.getNumericValues(dr, "numberDV");
    assertNotNull(dv);
    for(int i=0;i<numDocs;i++) {
      assertEquals("docID=" + i, numbers[i], dv.get(i));
    }

    dr.close();
    dir.close();
  }

  public void testSwitchToSortedSetDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Test relies on doc order:
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("field");
    int numDocs = atLeast(1000);
    String[] strings = new String[2*numDocs];
    if (VERBOSE) {
      System.out.println("numDocs=" + numDocs);
    }
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      String s = TestUtil.randomRealisticUnicodeString(random());
      String s2 = TestUtil.randomRealisticUnicodeString(random());
      strings[2*i] = s;
      strings[2*i+1] = s2;
      doc.addStoredString("field", s);
      doc.addStoredString("field", s2);
      w.addDocument(doc);
      // Make sure we have at least 2 segments, else forceMerge won't do anything:
      if (i == numDocs/2) {
        w.commit();
      }
    }
    w.close();

    ReindexingReader r = new ReindexingReader(dir, createTempDir()) {
        @Override
        protected long getCurrentSchemaGen() {
          return 0;
        }

        @Override
        protected IndexWriterConfig getIndexWriterConfig() {
          // Test relies on doc order:
          return newIndexWriterConfig().setMergePolicy(new AlwaysForceMergePolicy(newLogMergePolicy()));
        }

        @Override
        protected void reindex(long oldSchemaGen, long newSchemaGen, LeafReader reader, Directory parallelDir) throws IOException {
          assertEquals(0L, newSchemaGen);
          assertEquals(-1L, oldSchemaGen);

          IndexWriterConfig iwc = newIndexWriterConfig();
          if (VERBOSE) {
            System.out.println("TEST: build parallel " + reader);
          }

          // The order of our docIDs must precisely matching incoming reader:
          iwc.setMergePolicy(new LogByteSizeMergePolicy());
          IndexWriter w = new IndexWriter(parallelDir, iwc);
          int maxDoc = reader.maxDoc();
          FieldTypes fieldTypes = w.getFieldTypes();
          fieldTypes.setMultiValued("fieldDV");

          // Slowly parse the stored field into a new doc values field:
          for(int i=0;i<maxDoc;i++) {
            // TODO: is this still O(blockSize^2)?
            Document oldDoc = reader.document(i);
            Document newDoc = w.newDocument();
            for(String s : oldDoc.getStrings("field")) {
              newDoc.addAtom("fieldDV", s);
            }
            w.addDocument(newDoc);
          }

          if (random().nextBoolean()) {
            w.forceMerge(1);
          }

          w.close();
        }
      };
    
    if (VERBOSE) {
      System.out.println("TEST: now force merge");
    }

    r.w.forceMerge(1);
    // Make sure main + parallel index was fully updated:
    DirectoryReader dr = r.mgr.acquire();
    if (VERBOSE) {
      System.out.println("TEST: dr=" + dr);
    }
    try {
      SortedSetDocValues dv = MultiDocValues.getSortedSetValues(dr, "fieldDV");
      assertNotNull(dv);
      for(int i=0;i<numDocs;i++) {
        dv.setDocument(i);
        long ord = dv.nextOrd();
        assert ord != SortedSetDocValues.NO_MORE_ORDS;
        String v = dv.lookupOrd(ord).utf8ToString();

        long ord2 = dv.nextOrd();
        if (ord2 == SortedSetDocValues.NO_MORE_ORDS) {
          assertTrue(v.equals(strings[2*i]));
          assertTrue(v.equals(strings[2*i+1]));
        } else {
          assert dv.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
          String v2 = dv.lookupOrd(ord2).utf8ToString();
          assertTrue((v.equals(strings[2*i]) && v2.equals(strings[2*i+1])) ||
                     (v.equals(strings[2*i+1]) && v2.equals(strings[2*i])));
        }
      }
    } finally {
      r.mgr.release(dr);
    }
    r.close();

    // Make sure main index was fully updated:
    dr = DirectoryReader.open(dir);
    if (VERBOSE) {
      System.out.println("TEST: final reader " + dr);
    }
    SortedSetDocValues dv = MultiDocValues.getSortedSetValues(dr, "fieldDV");
    assertNotNull(dv);
    for(int i=0;i<numDocs;i++) {
      dv.setDocument(i);
      long ord = dv.nextOrd();
      assert ord != SortedSetDocValues.NO_MORE_ORDS;
      String v = dv.lookupOrd(ord).utf8ToString();

      long ord2 = dv.nextOrd();
      if (ord2 == SortedSetDocValues.NO_MORE_ORDS) {
        assertTrue(v.equals(strings[2*i]));
        assertTrue(v.equals(strings[2*i+1]));
      } else {
        assert dv.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
        String v2 = dv.lookupOrd(ord2).utf8ToString();
        assertTrue((v.equals(strings[2*i]) && v2.equals(strings[2*i+1])) ||
                   (v.equals(strings[2*i+1]) && v2.equals(strings[2*i])));
      }
    }

    dr.close();
    dir.close();
  }
}
