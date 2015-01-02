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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.document.LowSchemaField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestUniqueFields extends LuceneTestCase {

  public void testBasic1() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueAtom("field", new BytesRef("one"));
    w.addDocument(doc);

    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (NotUniqueException nue) {
      // expected
      assertEquals("field \"field\" must be unique, but value=[6f 6e 65] appears more than once", nue.getMessage());
    }
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    FieldTypes fieldTypes = s.getFieldTypes();
    assertEquals(1, hitCount(s, fieldTypes.newBinaryTermQuery("field", new BytesRef("one"))));
    assertEquals(1, r.numDocs());
    r.close();
    w.close();
    dir.close();
  }

  public void testBasic1Int() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueInt("field", 17);
    w.addDocument(doc);

    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (NotUniqueException nue) {
      // expected
      assertEquals("field \"field\" must be unique, but value=[80 0 0 11] appears more than once", nue.getMessage());
    }
    DirectoryReader r = DirectoryReader.open(w, true);
    assertEquals(1, r.numDocs());
    IndexSearcher s = newSearcher(r);
    FieldTypes fieldTypes = s.getFieldTypes();
    assertEquals(1, hitCount(s, fieldTypes.newIntTermQuery("field", 17)));
    r.close();
    w.close();
    dir.close();
  }

  public void testBasic2() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueAtom("field", new BytesRef("one"));
    w.addDocument(doc);
    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();
    mgr.maybeRefresh();

    DirectoryReader r = mgr.acquire();
    try {
      assertEquals(1, r.numDocs());
    } finally {
      mgr.release(r);
    }

    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (NotUniqueException nue) {
      // expected
      assertEquals("field \"field\" must be unique, but value=[6f 6e 65] appears more than once", nue.getMessage());
    }

    doc = w.newDocument();
    doc.addUniqueAtom("field", new BytesRef("two"));
    w.addDocument(doc);

    mgr.maybeRefresh();

    r = mgr.acquire();
    try {
      assertEquals(2, r.numDocs());
    } finally {
      mgr.release(r);
    }

    w.close();
    dir.close();
  }

  public void testBasic2Int() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueInt("field", 17);
    w.addDocument(doc);
    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();
    mgr.maybeRefresh();

    DirectoryReader r = mgr.acquire();
    try {
      assertEquals(1, r.numDocs());
    } finally {
      mgr.release(r);
    }

    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (NotUniqueException nue) {
      // expected
      assertEquals("field \"field\" must be unique, but value=[80 0 0 11] appears more than once", nue.getMessage());
    }

    doc = w.newDocument();
    doc.addUniqueInt("field", 22);
    w.addDocument(doc);

    mgr.maybeRefresh();

    r = mgr.acquire();
    try {
      assertEquals(2, r.numDocs());
    } finally {
      mgr.release(r);
    }

    w.close();
    dir.close();
  }

  public void testExcInvalidChange1() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addAtom("field", new BytesRef("one"));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addUniqueAtom("field", new BytesRef("two")),
               "field \"field\": cannot change isUnique from false to true");
    w.close();
    dir.close();
  }

  public void testExcInvalidChange1Int() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addInt("field", 17);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addUniqueInt("field", 22),
               "field \"field\": cannot change isUnique from false to true");
    w.close();
    dir.close();
  }

  public void testExcInvalidChange2() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueAtom("field", new BytesRef("one"));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addAtom("field", new BytesRef("two")),
               "field \"field\": cannot change isUnique from true to false");
    w.close();
    dir.close();
  }

  public void testExcInvalidChange2Int() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueInt("field", 17);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addInt("field", 22),
               "field \"field\": cannot change isUnique from true to false");
    w.close();
    dir.close();
  }

  public void testExcInvalidChange3() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addAtom("field", "one");
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addUniqueAtom("field", "two"),
               "field \"field\": cannot change isUnique from false to true");
    w.close();
    dir.close();
  }

  public void testExcInvalidChange3Int() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addInt("field", 17);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addUniqueInt("field", 22),
               "field \"field\": cannot change isUnique from false to true");
    w.close();
    dir.close();
  }

  public void testExcInvalidChange4() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueAtom("field", "one");
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addAtom("field", "two"),
               "field \"field\": cannot change isUnique from true to false");
    w.close();
    dir.close();
  }

  public void testExcInvalidChange4Int() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addUniqueInt("field", 17);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addInt("field", 22),
               "field \"field\": cannot change isUnique from true to false");
    w.close();
    dir.close();
  }

  public void testDeletes() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();

    Document doc = w.newDocument();
    doc.addUniqueAtom("field", new BytesRef("one"));
    w.addDocument(doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.deleteDocuments(new Term("field", new BytesRef("one")));
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.addDocument(doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.forceMerge(1);
    mgr.maybeRefresh();

    DirectoryReader r = mgr.acquire();
    try {
      assertEquals(1, r.numDocs());
    } finally {
      mgr.release(r);
    }

    w.close();
    dir.close();
  }

  public void testDeletesInt() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();

    Document doc = w.newDocument();
    doc.addUniqueInt("field", 17);
    w.addDocument(doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.deleteDocuments(fieldTypes.newIntTerm("field", 17));
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.addDocument(doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.forceMerge(1);
    mgr.maybeRefresh();

    DirectoryReader r = mgr.acquire();
    try {
      assertEquals(1, r.numDocs());
    } finally {
      mgr.release(r);
    }

    w.close();
    dir.close();
  }

  public void testUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();

    Document doc = w.newDocument();
    doc.addUniqueAtom("field", new BytesRef("one"));
    w.addDocument(doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.updateDocument(new Term("field", new BytesRef("one")), doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.updateDocument(new Term("field", new BytesRef("one")), doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.forceMerge(1);
    mgr.maybeRefresh();

    DirectoryReader r = mgr.acquire();
    try {
      assertEquals(1, r.numDocs());
    } finally {
      mgr.release(r);
    }

    w.close();
    dir.close();
  }

  public void testUpdatesInt() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();

    Document doc = w.newDocument();
    doc.addUniqueInt("field", 17);
    w.addDocument(doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.updateDocument(fieldTypes.newIntTerm("field", 17), doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.updateDocument(fieldTypes.newIntTerm("field", 17), doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }
    w.forceMerge(1);
    mgr.maybeRefresh();

    DirectoryReader r = mgr.acquire();
    try {
      assertEquals(1, r.numDocs());
    } finally {
      mgr.release(r);
    }

    w.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = w.getFieldTypes();

    Set<BytesRef> terms = new HashSet<>();
    final int numTerms = atLeast(1000);
    while (terms.size() < numTerms) {
      terms.add(new BytesRef(TestUtil.randomRealisticUnicodeString(random())));
    }
    final List<BytesRef> termsList = new ArrayList<>(terms);
    final CountDownLatch startingGun = new CountDownLatch(1);
    Thread[] threads = new Thread[TestUtil.nextInt(random(), 2, 5)];
    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();

              // First add randomly for a while:
              for(int iter=0;iter<3*numTerms;iter++) {
                BytesRef term = termsList.get(random().nextInt(termsList.size()));
                if (random().nextInt(4) == 1) {
                  w.deleteDocuments(new Term("field", term));
                } else {
                  Document doc = w.newDocument();
                  doc.addUniqueAtom("field", term);
                  if (random().nextBoolean()) {
                    w.updateDocument(new Term("field", term), doc);
                  } else {
                    try {
                      w.addDocument(doc);
                    } catch (NotUniqueException nue) {
                      // OK
                    }
                  }
                }
              }

              // Then add every single term, so we know all will be added:
              for(BytesRef term : termsList) {
                Document doc = w.newDocument();
                doc.addUniqueAtom("field", term);
                if (random().nextBoolean()) {
                  w.updateDocument(new Term("field", term), doc);
                } else {
                  try {
                    w.addDocument(doc);
                  } catch (NotUniqueException nue) {
                    // OK
                  }
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[i].start();
    }
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }
    w.forceMerge(1);
    IndexReader r = w.getReader();
    assertEquals(terms.size(), r.maxDoc());
    assertEquals(terms.size(), MultiFields.getTerms(r, "field").size());
    r.close();
    w.close();
    dir.close();
  }

  public void testRandomInt() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final FieldTypes fieldTypes = w.getFieldTypes();

    Set<Integer> terms = new HashSet<>();
    final int numTerms = atLeast(1000);
    while (terms.size() < numTerms) {
      terms.add(random().nextInt());
    }

    final List<Integer> termsList = new ArrayList<>(terms);
    final CountDownLatch startingGun = new CountDownLatch(1);
    Thread[] threads = new Thread[TestUtil.nextInt(random(), 2, 5)];
    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();

              // First add randomly for a while:
              for(int iter=0;iter<3*numTerms;iter++) {
                Integer term = termsList.get(random().nextInt(termsList.size()));
                if (iter > 0 && random().nextInt(4) == 1) {
                  w.deleteDocuments(fieldTypes.newIntTerm("field", term.intValue()));
                } else {
                  Document doc = w.newDocument();
                  doc.addUniqueInt("field", term.intValue());
                  if (random().nextBoolean()) {
                    w.updateDocument(fieldTypes.newIntTerm("field", term.intValue()), doc);
                  } else {
                    try {
                      w.addDocument(doc);
                    } catch (NotUniqueException nue) {
                      // OK
                    }
                  }
                }
              }

              // Then add every single term, so we know all will be added:
              for(Integer term : termsList) {
                Document doc = w.newDocument();
                doc.addUniqueInt("field", term.intValue());
                if (random().nextBoolean()) {
                  w.updateDocument(fieldTypes.newIntTerm("field", term.intValue()), doc);
                } else {
                  try {
                    w.addDocument(doc);
                  } catch (NotUniqueException nue) {
                    // OK
                  }
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[i].start();
    }
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }
    w.forceMerge(1);
    IndexReader r = w.getReader();
    assertEquals(terms.size(), r.maxDoc());
    assertEquals(terms.size(), MultiFields.getTerms(r, "field").size());
    r.close();
    w.close();
    dir.close();
  }

  /** Make sure CheckIndex detects violation of unique constraint, and -exorcise properly repairs it. */
  public void testExcCheckIndex() throws Exception {
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));
    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();
    Document doc2 = w.newDocument();
    doc2.addUniqueAtom("field", "one");

    w.addDocument(doc2);
    mgr.maybeRefresh();

    try {
      w.addDocument(doc2);
      fail("did not hit exception");
    } catch (NotUniqueException nue) {
      // expected
    }
    IndexReader r = mgr.acquire();
    w.addIndexes(new IndexReader[] {r});
    r.close();
    w.close();

    try (CheckIndex checker = new CheckIndex(dir)) {
        checker.setCrossCheckTermVectors(true);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8), false);
        CheckIndex.Status status = checker.checkIndex(null);
        assertFalse(status.clean);
        assertEquals(1, status.nonUniqueCount);
        checker.exorciseIndex(status);
        assertTrue(bos.toString(IOUtils.UTF_8).contains("field=\"field\" is supposed to be unique, but isn't: e.g. term=[6f 6e 65] matches both docID=0 and docID=2; total 1 non-unique documents would be deleted"));
      }
    
    r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    IndexSearcher s = newSearcher(r);
    assertEquals(1, hitCount(s, new TermQuery(new Term("field", "one"))));
    r.close();
  }

  public void testMultiValuedUnique() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addUniqueAtom("field", "foo");
    doc.addUniqueAtom("field", "bar");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);

    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "foo"), 1).totalHits);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "bar"), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }
}
