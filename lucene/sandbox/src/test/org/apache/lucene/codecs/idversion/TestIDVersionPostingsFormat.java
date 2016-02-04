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
package org.apache.lucene.codecs.idversion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.codecs.idversion.StringAndPayloadField.SingleTokenWithPayloadTokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.PerThreadPKLookup;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LiveFieldValues;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Basic tests for IDVersionPostingsFormat
 */
// Cannot extend BasePostingsFormatTestCase because this PF is not
// general (it requires payloads, only allows 1 doc per term, etc.)
public class TestIDVersionPostingsFormat extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id0", 100));
    w.addDocument(doc);
    doc = new Document();
    doc.add(makeIDField("id1", 110));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IDVersionSegmentTermsEnum termsEnum = (IDVersionSegmentTermsEnum) r.leaves().get(0).reader().fields().terms("id").iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("id0"), 50));
    assertTrue(termsEnum.seekExact(new BytesRef("id0"), 100));
    assertFalse(termsEnum.seekExact(new BytesRef("id0"), 101));
    assertTrue(termsEnum.seekExact(new BytesRef("id1"), 50));
    assertTrue(termsEnum.seekExact(new BytesRef("id1"), 110));
    assertFalse(termsEnum.seekExact(new BytesRef("id1"), 111));
    r.close();

    w.close();
    dir.close();
  }

  private interface IDSource {
    String next();
  }

  private IDSource getRandomIDs() {
    IDSource ids;
    switch (random().nextInt(6)) {
    case 0:
      // random simple
      if (VERBOSE) {
        System.out.println("TEST: use random simple ids");
      }
      ids = new IDSource() {
          @Override
          public String next() {
            return TestUtil.randomSimpleString(random());
          }
        };
      break;
    case 1:
      // random realistic unicode
      if (VERBOSE) {
        System.out.println("TEST: use random realistic unicode ids");
      }
      ids = new IDSource() {
          @Override
          public String next() {
            return TestUtil.randomRealisticUnicodeString(random());
          }
        };
      break;
    case 2:
      // sequential
      if (VERBOSE) {
        System.out.println("TEST: use seuquential ids");
      }
      ids = new IDSource() {
          int upto;
          @Override
          public String next() {
            return Integer.toString(upto++);
          }
        };
      break;
    case 3:
      // zero-pad sequential
      if (VERBOSE) {
        System.out.println("TEST: use zero-pad seuquential ids");
      }
      ids = new IDSource() {
          final int radix = TestUtil.nextInt(random(), Character.MIN_RADIX, Character.MAX_RADIX);
          final String zeroPad = String.format(Locale.ROOT, "%0" + TestUtil.nextInt(random(), 5, 20) + "d", 0);
          int upto;
          @Override
          public String next() {
            String s = Integer.toString(upto++);
            return zeroPad.substring(zeroPad.length() - s.length()) + s;
          }
        };
      break;
    case 4:
      // random long
      if (VERBOSE) {
        System.out.println("TEST: use random long ids");
      }
      ids = new IDSource() {
          final int radix = TestUtil.nextInt(random(), Character.MIN_RADIX, Character.MAX_RADIX);
          int upto;
          @Override
          public String next() {
            return Long.toString(random().nextLong() & 0x3ffffffffffffffL, radix);
          }
        };
      break;
    case 5:
      // zero-pad random long
      if (VERBOSE) {
        System.out.println("TEST: use zero-pad random long ids");
      }
      ids = new IDSource() {
          final int radix = TestUtil.nextInt(random(), Character.MIN_RADIX, Character.MAX_RADIX);
          final String zeroPad = String.format(Locale.ROOT, "%015d", 0);
          int upto;
          @Override
          public String next() {
            return Long.toString(random().nextLong() & 0x3ffffffffffffffL, radix);
          }
        };
      break;
    default:
      throw new AssertionError();
    }

    return ids;
  }

  // TODO make a similar test for BT, w/ varied IDs:

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    int minItemsInBlock = TestUtil.nextInt(random(), 2, 50);
    int maxItemsInBlock = 2*(minItemsInBlock-1) + random().nextInt(50);
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat(minItemsInBlock, maxItemsInBlock)));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    //IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = atLeast(1000);
    Map<String,Long> idValues = new HashMap<String,Long>();
    int docUpto = 0;
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }

    IDSource ids = getRandomIDs();
    String idPrefix;
    if (random().nextBoolean()) {
      idPrefix = "";
    } else {
      idPrefix = TestUtil.randomSimpleString(random());
      if (VERBOSE) {
        System.out.println("TEST: use id prefix: " + idPrefix);
      }
    }

    boolean useMonotonicVersion = random().nextBoolean();
    if (VERBOSE) {
      System.out.println("TEST: useMonotonicVersion=" + useMonotonicVersion);
    }

    List<String> idsList = new ArrayList<>();

    long version = 0;
    while (docUpto < numDocs) {
      String idValue = idPrefix + ids.next();
      if (idValues.containsKey(idValue)) {
        continue;
      }
      if (useMonotonicVersion) {
        version += TestUtil.nextInt(random(), 1, 10);
      } else {
        version = random().nextLong() & 0x3fffffffffffffffL;
      }
      idValues.put(idValue, version);
      if (VERBOSE) {
        System.out.println("  " + idValue + " -> " + version);
      }
      Document doc = new Document();
      doc.add(makeIDField(idValue, version));
      w.addDocument(doc);
      idsList.add(idValue);

      if (idsList.size() > 0 && random().nextInt(7) == 5) {
        // Randomly delete or update a previous ID
        idValue = idsList.get(random().nextInt(idsList.size()));
        if (random().nextBoolean()) {
          if (useMonotonicVersion) {
            version += TestUtil.nextInt(random(), 1, 10);
          } else {
            version = random().nextLong() & 0x3fffffffffffffffL;
          }
          doc = new Document();
          doc.add(makeIDField(idValue, version));
          if (VERBOSE) {
            System.out.println("  update " + idValue + " -> " + version);
          }
          w.updateDocument(new Term("id", idValue), doc);
          idValues.put(idValue, version);
        } else {
          if (VERBOSE) {
            System.out.println("  delete " + idValue);
          }
          w.deleteDocuments(new Term("id", idValue));
          idValues.remove(idValue);
        }        
      }

      docUpto++;
    }

    IndexReader r = w.getReader();
    //IndexReader r = DirectoryReader.open(w);
    PerThreadVersionPKLookup lookup = new PerThreadVersionPKLookup(r, "id");

    List<Map.Entry<String,Long>> idValuesList = new ArrayList<>(idValues.entrySet());
    int iters = numDocs * 5;
    for(int iter=0;iter<iters;iter++) {
      String idValue;

      if (random().nextBoolean()) {
        idValue = idValuesList.get(random().nextInt(idValuesList.size())).getKey();
      } else if (random().nextBoolean()) {
        idValue = ids.next();
      } else {
        idValue = idPrefix + TestUtil.randomSimpleString(random());
      }

      BytesRef idValueBytes = new BytesRef(idValue);

      Long expectedVersion = idValues.get(idValue);

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " id=" + idValue + " expectedVersion=" + expectedVersion);
      }
      
      if (expectedVersion == null) {
        assertEquals("term should not have been found (doesn't exist)", -1, lookup.lookup(idValueBytes));
      } else {
        if (random().nextBoolean()) {
          if (VERBOSE) {
            System.out.println("  lookup exact version (should be found)");
          }
          assertTrue("term should have been found (version too old)", lookup.lookup(idValueBytes, expectedVersion.longValue()) != -1);
          assertEquals(expectedVersion.longValue(), lookup.getVersion());
        } else {
          if (VERBOSE) {
            System.out.println("  lookup version+1 (should not be found)");
          }
          assertEquals("term should not have been found (version newer)", -1, lookup.lookup(idValueBytes, expectedVersion.longValue()+1));
        }
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  private static class PerThreadVersionPKLookup extends PerThreadPKLookup {
    public PerThreadVersionPKLookup(IndexReader r, String field) throws IOException {
      super(r, field);
    }

    long lastVersion;

    /** Returns docID if found, else -1. */
    public int lookup(BytesRef id, long version) throws IOException {
      for(int seg=0;seg<numSegs;seg++) {
        if (((IDVersionSegmentTermsEnum) termsEnums[seg]).seekExact(id, version)) {
          if (VERBOSE) {
            System.out.println("  found in seg=" + termsEnums[seg]);
          }
          postingsEnums[seg] = termsEnums[seg].postings(postingsEnums[seg], 0);
          int docID = postingsEnums[seg].nextDoc();
          if (docID != PostingsEnum.NO_MORE_DOCS && (liveDocs[seg] == null || liveDocs[seg].get(docID))) {
            lastVersion = ((IDVersionSegmentTermsEnum) termsEnums[seg]).getVersion();
            return docBases[seg] + docID;
          }
          assert hasDeletions;
        }
      }

      return -1;
    }

    /** Only valid if lookup returned a valid docID. */
    public long getVersion() {
      return lastVersion;
    }
  }

  private static Field makeIDField(String id, long version) {
    BytesRef payload = new BytesRef(8);
    payload.length = 8;
    IDVersionPostingsFormat.longToBytes(version, payload);
    return new StringAndPayloadField("id", id, payload);
  }

  public void testMoreThanOneDocPerIDOneSegment() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    doc = new Document();
    doc.add(makeIDField("id", 17));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  public void testMoreThanOneDocPerIDTwoSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    iwc.setMergePolicy(new TieredMergePolicy());
    MergeScheduler ms = iwc.getMergeScheduler();
    if (ms instanceof ConcurrentMergeScheduler) {
      iwc.setMergeScheduler(new ConcurrentMergeScheduler() {
          @Override
          protected void handleMergeException(Directory dir, Throwable exc) {
            assertTrue(exc instanceof IllegalArgumentException);
          }
        });
    }
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(makeIDField("id", 17));
    try {
      w.addDocument(doc);
      w.commit();
      w.forceMerge(1);
      fail("didn't hit exception");
    } catch (IllegalArgumentException iae) {
      // expected: SMS will hit this
    } catch (IOException | IllegalStateException exc) {
      // expected
      assertTrue(exc.getCause() instanceof IllegalArgumentException);
    }
    w.rollback();
    dir.close();
  }

  public void testMoreThanOneDocPerIDWithUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    doc = new Document();
    doc.add(makeIDField("id", 17));
    // Replaces the doc we just indexed:
    w.updateDocument(new Term("id", "id"), doc);
    w.commit();
    w.close();
    dir.close();
  }

  public void testMoreThanOneDocPerIDWithDeletes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "id"));
    doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    w.commit();
    w.close();
    dir.close();
  }

  public void testMissingPayload() throws Exception {
    Directory dir = newDirectory();

    // MockAnalyzer minus maybePayload else it sometimes stuffs in an 8-byte payload!
    Analyzer a = new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String fieldName) {
          MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true, 100);
          tokenizer.setEnableChecks(true);
          MockTokenFilter filt = new MockTokenFilter(tokenizer, MockTokenFilter.EMPTY_STOPSET);
          return new TokenStreamComponents(tokenizer, filt);
        }
      };
    IndexWriterConfig iwc = newIndexWriterConfig(a);
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newTextField("id", "id", Field.Store.NO));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
             
    w.close();
    dir.close();
  }

  public void testMissingPositions() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("id", "id", Field.Store.NO));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
             
    w.close();
    dir.close();
  }

  public void testInvalidPayload() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new StringAndPayloadField("id", "id", new BytesRef("foo")));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
             
    w.close();
    dir.close();
  }

  public void testMoreThanOneDocPerIDWithDeletesAcrossSegments() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(makeIDField("id", 17));
    // Replaces the doc we just indexed:
    w.updateDocument(new Term("id", "id"), doc);
    w.forceMerge(1);
    w.close();
    dir.close();
  }

  // LUCENE-5693: because CheckIndex cross-checks term vectors with postings even for deleted docs, and because our PF only indexes the
  // non-deleted documents on flush, CheckIndex will see this as corruption:
  public void testCannotIndexTermVectors() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();

    FieldType ft = new FieldType(StringAndPayloadField.TYPE);
    ft.setStoreTermVectors(true);
    SingleTokenWithPayloadTokenStream ts = new SingleTokenWithPayloadTokenStream();
    BytesRef payload = new BytesRef(8);
    payload.length = 8;
    IDVersionPostingsFormat.longToBytes(17, payload);
    ts.setValue("foo", payload);
    Field field = new Field("id", ts, ft);
    doc.add(new Field("id", ts, ft));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
      // iae.printStackTrace(System.out);
    }
    w.close();
    dir.close();
  }

  public void testMoreThanOnceInSingleDoc() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id", 17));
    doc.add(makeIDField("id", 17));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  public void testInvalidVersions() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    // -1
    doc.add(new StringAndPayloadField("id", "id", new BytesRef(new byte[] {(byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff})));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    try {
      w.addDocument(doc);
      fail("should have hit exc");
    } catch (AlreadyClosedException ace) {
      // expected
    }
    dir.close();
  }

  public void testInvalidVersions2() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    // Long.MAX_VALUE:
    doc.add(new StringAndPayloadField("id", "id", new BytesRef(new byte[] {(byte)0x7f, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff})));
    try {
      w.addDocument(doc);
      w.commit();
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    try {
      w.addDocument(doc);
      fail("should have hit exc");
    } catch (AlreadyClosedException ace) {
      // expected
    }
    dir.close();
  }

  // Simulates optimistic concurrency in a distributed indexing app and confirms the latest version always wins:
  public void testGlobalVersions() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    IDSource idsSource = getRandomIDs();
    int numIDs = atLeast(100);
    if (VERBOSE) {
      System.out.println("TEST: " + numIDs + " ids");
    }
    Set<String> idsSeen = new HashSet<String>();
    while (idsSeen.size() < numIDs) {
      idsSeen.add(idsSource.next());
    }
    final String[] ids = idsSeen.toArray(new String[numIDs]);

    final Object[] locks = new Object[ids.length];
    for(int i=0;i<locks.length;i++) {
      locks[i] = new Object();
    }

    final AtomicLong nextVersion = new AtomicLong();

    final SearcherManager mgr = new SearcherManager(w.w, new SearcherFactory());

    final Long missingValue = -1L;

    final LiveFieldValues<IndexSearcher,Long> versionValues = new LiveFieldValues<IndexSearcher,Long>(mgr, missingValue) {
      @Override
      protected Long lookupFromSearcher(IndexSearcher s, String id) {
        // TODO: would be cleaner if we could do our PerThreadLookup here instead of "up above":
        // We always return missing: the caller then does a lookup against the current reader
        return missingValue;
      }
    };

    // Maps to the version the id was lasted indexed with:
    final Map<String,Long> truth = new ConcurrentHashMap<>();

    final CountDownLatch startingGun = new CountDownLatch(1);

    Thread[] threads = new Thread[TestUtil.nextInt(random(), 2, 7)];

    final int versionType = random().nextInt(3);

    if (VERBOSE) {
      if (versionType == 0) {
        System.out.println("TEST: use random versions");
      } else if (versionType == 1) {
        System.out.println("TEST: use monotonic versions");
      } else {
        System.out.println("TEST: use nanotime versions");
      }
    }

    // Run for 3 sec in normal tests, else 60 seconds for nightly:
    final long stopTime = System.currentTimeMillis() + (TEST_NIGHTLY ? 60000 : 3000);

    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              runForReal();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          private void runForReal() throws IOException, InterruptedException {
            startingGun.await();
            PerThreadVersionPKLookup lookup = null;
            IndexReader lookupReader = null;
            while (System.currentTimeMillis() < stopTime) {

              // Intentionally pull version first, and then sleep/yield, to provoke version conflicts:
              long newVersion;
              if (versionType == 0) {
                // Random:
                newVersion = random().nextLong() & 0x3fffffffffffffffL;
              } else if (versionType == 1) {
                // Monotonic
                newVersion = nextVersion.getAndIncrement();
              } else {
                newVersion = System.nanoTime();
              }

              if (versionType != 0) {
                if (random().nextBoolean()) {
                  Thread.yield();
                } else {
                  Thread.sleep(TestUtil.nextInt(random(), 1, 4));
                }
              }

              int x = random().nextInt(ids.length);

              // TODO: we could relax this, if e.g. we assign indexer thread based on ID.  This would ensure a given ID cannot be indexed at
              // the same time in multiple threads:

              // Only one thread can update an ID at once:
              synchronized (locks[x]) {

                String id = ids[x];

                // We will attempt to index id with newVersion, but only do so if id wasn't yet indexed, or it was indexed with an older
                // version (< newVersion):

                // Must lookup the RT value before pulling from the index, in case a reopen happens just after we lookup:
                Long currentVersion = versionValues.get(id);

                IndexSearcher s = mgr.acquire();
                try {
                  if (VERBOSE) System.out.println("\n" + Thread.currentThread().getName() + ": update id=" + id + " newVersion=" + newVersion);

                  if (lookup == null || lookupReader != s.getIndexReader()) {
                    // TODO: sort of messy; we could add reopen to PerThreadVersionPKLookup?
                    // TODO: this is thin ice .... that we don't incRef/decRef this reader we are implicitly holding onto:
                    lookupReader = s.getIndexReader();
                    if (VERBOSE) System.out.println(Thread.currentThread().getName() + ": open new PK lookup reader=" + lookupReader);
                    lookup = new PerThreadVersionPKLookup(lookupReader, "id");
                  }

                  Long truthVersion = truth.get(id);
                  if (VERBOSE) System.out.println(Thread.currentThread().getName() + ":   truthVersion=" + truthVersion);

                  boolean doIndex;
                  if (currentVersion == missingValue) {
                    if (VERBOSE) System.out.println(Thread.currentThread().getName() + ":   id not in RT cache");
                    int otherDocID = lookup.lookup(new BytesRef(id), newVersion+1);
                    if (otherDocID == -1) {
                      if (VERBOSE) System.out.println(Thread.currentThread().getName() + ":   id not in index, or version is <= newVersion; will index");
                      doIndex = true;
                    } else {
                      if (VERBOSE) System.out.println(Thread.currentThread().getName() + ":   id is in index with version=" + lookup.getVersion() + "; will not index");
                      doIndex = false;
                      if (truthVersion.longValue() !=lookup.getVersion()) {
                        System.out.println(Thread.currentThread() + ": now fail0!");
                      }
                      assertEquals(truthVersion.longValue(), lookup.getVersion());
                    }
                  } else {
                    if (VERBOSE) System.out.println(Thread.currentThread().getName() + ":   id is in RT cache: currentVersion=" + currentVersion);
                    doIndex = newVersion > currentVersion;
                  }

                  if (doIndex) {
                    if (VERBOSE) System.out.println(Thread.currentThread().getName() + ":   now index");
                    boolean passes = truthVersion == null || truthVersion.longValue() <= newVersion;
                    if (passes == false) {
                      System.out.println(Thread.currentThread() + ": now fail!");
                    }
                    assertTrue(passes);
                    Document doc = new Document();
                    doc.add(makeIDField(id, newVersion));
                    w.updateDocument(new Term("id", id), doc);
                    truth.put(id, newVersion);
                    versionValues.add(id, newVersion);
                  } else {
                    if (VERBOSE) System.out.println(Thread.currentThread().getName() + ":   skip index");
                    assertNotNull(truthVersion);
                    assertTrue(truthVersion.longValue() >= newVersion);
                  }
                } finally {
                  mgr.release(s);
                }
              }
            }
          }
        };
      threads[i].start();
    }

    startingGun.countDown();

    // Keep reopening the NRT reader until all indexing threads are done:
    refreshLoop:
    while (true) {
      Thread.sleep(TestUtil.nextInt(random(), 1, 10));
      mgr.maybeRefresh();
      for (Thread thread : threads) {
        if (thread.isAlive()) {
          continue refreshLoop;
        }
      }

      break;
    }

    // Verify final index against truth:
    for(int i=0;i<2;i++) {
      mgr.maybeRefresh();
      IndexSearcher s = mgr.acquire();
      try {
        IndexReader r = s.getIndexReader();
        // cannot assert this: maybe not all IDs were indexed
        /*
        assertEquals(numIDs, r.numDocs());
        if (i == 1) {
          // After forceMerge no deleted docs:
          assertEquals(numIDs, r.maxDoc());
        }
        */
        PerThreadVersionPKLookup lookup = new PerThreadVersionPKLookup(r, "id");
        for(Map.Entry<String,Long> ent : truth.entrySet()) {
          assertTrue(lookup.lookup(new BytesRef(ent.getKey()), -1L) != -1);
          assertEquals(ent.getValue().longValue(), lookup.getVersion());
        }
      } finally {
        mgr.release(s);
      }

      if (i == 1) {
        break;
      }

      // forceMerge and verify again
      w.forceMerge(1);
    }

    mgr.close();
    w.close();
    dir.close();
  }
}
