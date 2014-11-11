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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.document.Document2;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestUniqueAtom extends LuceneTestCase {

  public void testBasic1() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document2 doc = w.newDocument();
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
    assertEquals(1, r.numDocs());
    r.close();
    w.close();
    dir.close();
  }

  public void testBasic2() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document2 doc = w.newDocument();
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

  public void testDeletes() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();

    Document2 doc = w.newDocument();
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

  public void testUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();

    Document2 doc = w.newDocument();
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

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = w.getFieldTypes();

    // nocommit add in deletes/updateDocument here:

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
                Document2 doc = w.newDocument();
                BytesRef term = termsList.get(random().nextInt(termsList.size()));
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

              // Then add every single term, so we know all will be added:
              for(BytesRef term : termsList) {
                Document2 doc = w.newDocument();
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

  /** Make sure CheckIndex detects violation of unique constraint. */
  public void testExcCheckIndex() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    // we intentionally create a corrupt index:
    dir.setCheckIndexOnClose(false);

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document2 doc = w.newDocument();
    doc.addUniqueAtom("field", new BytesRef("one"));
    w.close();
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    String fieldTypesBytes = infos.getUserData().get(FieldTypes.FIELD_TYPES_KEY);
    assertNotNull(fieldTypesBytes);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    ReferenceManager<DirectoryReader> mgr = w.getReaderManager();
    doc = w.newDocument();
    doc.addAtom("field", new BytesRef("one"));

    w.addDocument(doc);
    if (random().nextBoolean()) {
      mgr.maybeRefresh();
    }

    w.addDocument(doc);
    w.close();
    infos = SegmentInfos.readLatestCommit(dir);
    // nocommit this is evil, we need to close this workaround and find a different way to test:
    infos.getUserData().put(FieldTypes.FIELD_TYPES_KEY, fieldTypesBytes);
    infos.prepareCommit(dir);
    infos.finishCommit(dir);

    try {
      TestUtil.checkIndex(dir, true, true);
      fail("did not hit exception");
    } catch (RuntimeException re) {
      // expected
      assertEquals("UNIQUE_ATOM field=\"field\" is not unique: e.g. term=[6f 6e 65] matches both docID=0 and docID=1", re.getMessage());
    }
    
    dir.close();
  }
}
