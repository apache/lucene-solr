package org.apache.lucene.codecs.idversion;

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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PerThreadPKLookup;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id0", 100));
    w.addDocument(doc);
    doc = new Document();
    doc.add(makeIDField("id1", 110));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    IDVersionSegmentTermsEnum termsEnum = (IDVersionSegmentTermsEnum) r.leaves().get(0).reader().fields().terms("id").iterator(null);
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

  // TODO make a similar test for BT, w/ varied IDs:

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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

    IDSource ids;
    switch (random().nextInt(6)) {
    case 0:
      // random simple
      if (VERBOSE) {
        System.out.println("  use random simple ids");
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
        System.out.println("  use random realistic unicode ids");
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
        System.out.println("  use seuquential ids");
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
        System.out.println("  use zero-pad seuquential ids");
      }
      ids = new IDSource() {
          final int radix = TestUtil.nextInt(random(), Character.MIN_RADIX, Character.MAX_RADIX);
          final String zeroPad = String.format(Locale.ROOT, "%0" + TestUtil.nextInt(random(), 4, 20) + "d", 0);
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
        System.out.println("  use random long ids");
      }
      ids = new IDSource() {
          final int radix = TestUtil.nextInt(random(), Character.MIN_RADIX, Character.MAX_RADIX);
          int upto;
          @Override
          public String next() {
            return Long.toString(random().nextLong() & 0x7ffffffffffffffL, radix);
          }
        };
      break;
    case 5:
      // zero-pad random long
      if (VERBOSE) {
        System.out.println("  use zero-pad random long ids");
      }
      ids = new IDSource() {
          final int radix = TestUtil.nextInt(random(), Character.MIN_RADIX, Character.MAX_RADIX);
          final String zeroPad = String.format(Locale.ROOT, "%015d", 0);
          int upto;
          @Override
          public String next() {
            return Long.toString(random().nextLong() & 0x7ffffffffffffffL, radix);
          }
        };
      break;
    default:
      throw new AssertionError();
    }

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
        version = random().nextLong() & 0x7fffffffffffffffL;
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
            version = random().nextLong() & 0x7fffffffffffffffL;
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
    //IndexReader r = DirectoryReader.open(w, true);
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
          docsEnums[seg] = termsEnums[seg].docs(liveDocs[seg], docsEnums[seg], 0);
          int docID = docsEnums[seg].nextDoc();
          if (docID != DocsEnum.NO_MORE_DOCS) {
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    doc = new Document();
    doc.add(makeIDField("id", 17));
    w.addDocument(doc);
    try {
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setCodec(TestUtil.alwaysPostingsFormat(new IDVersionPostingsFormat()));
    iwc.setMergePolicy(new TieredMergePolicy());
    MergeScheduler ms = iwc.getMergeScheduler();
    if (ms instanceof ConcurrentMergeScheduler) {
      iwc.setMergeScheduler(new ConcurrentMergeScheduler() {
          @Override
          protected void handleMergeException(Throwable exc) {
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
    } catch (IOException ioe) {
      // expected
      assertTrue(ioe.getCause() instanceof IllegalArgumentException);
    }
    w.close();
    dir.close();
  }

  public void testMoreThanOneDocPerIDWithUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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

  public void testMoreThanOnceInSingleDoc() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
}
