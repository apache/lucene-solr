package org.apache.lucene.index;

/**
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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.index.TestIndexReader.addDoc;
import static org.apache.lucene.index.TestIndexReader.addDocumentWithFields;
import static org.apache.lucene.index.TestIndexReader.assertTermDocsCount;
import static org.apache.lucene.index.TestIndexReader.createDocument;

public class TestIndexReaderDelete extends LuceneTestCase {
  private void deleteReaderReaderConflict(boolean optimize) throws IOException {
    Directory dir = newDirectory();

    Term searchTerm1 = new Term("content", "aaa");
    Term searchTerm2 = new Term("content", "bbb");
    Term searchTerm3 = new Term("content", "ccc");

    //  add 100 documents with term : aaa
    //  add 100 documents with term : bbb
    //  add 100 documents with term : ccc
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE));
    for (int i = 0; i < 100; i++) {
        addDoc(writer, searchTerm1.text());
        addDoc(writer, searchTerm2.text());
        addDoc(writer, searchTerm3.text());
    }
    if(optimize)
      writer.optimize();
    writer.close();

    // OPEN TWO READERS
    // Both readers get segment info as exists at this time
    IndexReader reader1 = IndexReader.open(dir, false);
    assertEquals("first opened", 100, reader1.docFreq(searchTerm1));
    assertEquals("first opened", 100, reader1.docFreq(searchTerm2));
    assertEquals("first opened", 100, reader1.docFreq(searchTerm3));
    assertTermDocsCount("first opened", reader1, searchTerm1, 100);
    assertTermDocsCount("first opened", reader1, searchTerm2, 100);
    assertTermDocsCount("first opened", reader1, searchTerm3, 100);

    IndexReader reader2 = IndexReader.open(dir, false);
    assertEquals("first opened", 100, reader2.docFreq(searchTerm1));
    assertEquals("first opened", 100, reader2.docFreq(searchTerm2));
    assertEquals("first opened", 100, reader2.docFreq(searchTerm3));
    assertTermDocsCount("first opened", reader2, searchTerm1, 100);
    assertTermDocsCount("first opened", reader2, searchTerm2, 100);
    assertTermDocsCount("first opened", reader2, searchTerm3, 100);

    // DELETE DOCS FROM READER 2 and CLOSE IT
    // delete documents containing term: aaa
    // when the reader is closed, the segment info is updated and
    // the first reader is now stale
    reader2.deleteDocuments(searchTerm1);
    assertEquals("after delete 1", 100, reader2.docFreq(searchTerm1));
    assertEquals("after delete 1", 100, reader2.docFreq(searchTerm2));
    assertEquals("after delete 1", 100, reader2.docFreq(searchTerm3));
    assertTermDocsCount("after delete 1", reader2, searchTerm1, 0);
    assertTermDocsCount("after delete 1", reader2, searchTerm2, 100);
    assertTermDocsCount("after delete 1", reader2, searchTerm3, 100);
    reader2.close();

    // Make sure reader 1 is unchanged since it was open earlier
    assertEquals("after delete 1", 100, reader1.docFreq(searchTerm1));
    assertEquals("after delete 1", 100, reader1.docFreq(searchTerm2));
    assertEquals("after delete 1", 100, reader1.docFreq(searchTerm3));
    assertTermDocsCount("after delete 1", reader1, searchTerm1, 100);
    assertTermDocsCount("after delete 1", reader1, searchTerm2, 100);
    assertTermDocsCount("after delete 1", reader1, searchTerm3, 100);


    // ATTEMPT TO DELETE FROM STALE READER
    // delete documents containing term: bbb
    try {
        reader1.deleteDocuments(searchTerm2);
        fail("Delete allowed from a stale index reader");
    } catch (IOException e) {
        /* success */
    }

    // RECREATE READER AND TRY AGAIN
    reader1.close();
    reader1 = IndexReader.open(dir, false);
    assertEquals("reopened", 100, reader1.docFreq(searchTerm1));
    assertEquals("reopened", 100, reader1.docFreq(searchTerm2));
    assertEquals("reopened", 100, reader1.docFreq(searchTerm3));
    assertTermDocsCount("reopened", reader1, searchTerm1, 0);
    assertTermDocsCount("reopened", reader1, searchTerm2, 100);
    assertTermDocsCount("reopened", reader1, searchTerm3, 100);

    reader1.deleteDocuments(searchTerm2);
    assertEquals("deleted 2", 100, reader1.docFreq(searchTerm1));
    assertEquals("deleted 2", 100, reader1.docFreq(searchTerm2));
    assertEquals("deleted 2", 100, reader1.docFreq(searchTerm3));
    assertTermDocsCount("deleted 2", reader1, searchTerm1, 0);
    assertTermDocsCount("deleted 2", reader1, searchTerm2, 0);
    assertTermDocsCount("deleted 2", reader1, searchTerm3, 100);
    reader1.close();

    // Open another reader to confirm that everything is deleted
    reader2 = IndexReader.open(dir, false);
    assertTermDocsCount("reopened 2", reader2, searchTerm1, 0);
    assertTermDocsCount("reopened 2", reader2, searchTerm2, 0);
    assertTermDocsCount("reopened 2", reader2, searchTerm3, 100);
    reader2.close();

    dir.close();
  }

  private void deleteReaderWriterConflict(boolean optimize) throws IOException {
    //Directory dir = new RAMDirectory();
    Directory dir = newDirectory();

    Term searchTerm = new Term("content", "aaa");
    Term searchTerm2 = new Term("content", "bbb");

    //  add 100 documents with term : aaa
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE));
    for (int i = 0; i < 100; i++) {
        addDoc(writer, searchTerm.text());
    }
    writer.close();

    // OPEN READER AT THIS POINT - this should fix the view of the
    // index at the point of having 100 "aaa" documents and 0 "bbb"
    IndexReader reader = IndexReader.open(dir, false);
    assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
    assertEquals("first docFreq", 0, reader.docFreq(searchTerm2));
    assertTermDocsCount("first reader", reader, searchTerm, 100);
    assertTermDocsCount("first reader", reader, searchTerm2, 0);

    // add 100 documents with term : bbb
    writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    for (int i = 0; i < 100; i++) {
        addDoc(writer, searchTerm2.text());
    }

    // REQUEST OPTIMIZATION
    // This causes a new segment to become current for all subsequent
    // searchers. Because of this, deletions made via a previously open
    // reader, which would be applied to that reader's segment, are lost
    // for subsequent searchers/readers
    if(optimize)
      writer.optimize();
    writer.close();

    // The reader should not see the new data
    assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
    assertEquals("first docFreq", 0, reader.docFreq(searchTerm2));
    assertTermDocsCount("first reader", reader, searchTerm, 100);
    assertTermDocsCount("first reader", reader, searchTerm2, 0);


    // DELETE DOCUMENTS CONTAINING TERM: aaa
    // NOTE: the reader was created when only "aaa" documents were in
    int deleted = 0;
    try {
        deleted = reader.deleteDocuments(searchTerm);
        fail("Delete allowed on an index reader with stale segment information");
    } catch (StaleReaderException e) {
        /* success */
    }

    // Re-open index reader and try again. This time it should see
    // the new data.
    reader.close();
    reader = IndexReader.open(dir, false);
    assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
    assertEquals("first docFreq", 100, reader.docFreq(searchTerm2));
    assertTermDocsCount("first reader", reader, searchTerm, 100);
    assertTermDocsCount("first reader", reader, searchTerm2, 100);

    deleted = reader.deleteDocuments(searchTerm);
    assertEquals("deleted count", 100, deleted);
    assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm));
    assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm2));
    assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
    assertTermDocsCount("deleted termDocs", reader, searchTerm2, 100);
    reader.close();

    // CREATE A NEW READER and re-test
    reader = IndexReader.open(dir, false);
    assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm2));
    assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
    assertTermDocsCount("deleted termDocs", reader, searchTerm2, 100);
    reader.close();
    dir.close();
  }

  public void testBasicDelete() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = null;
    IndexReader reader = null;
    Term searchTerm = new Term("content", "aaa");

    //  add 100 documents with term : aaa
    writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    for (int i = 0; i < 100; i++) {
        addDoc(writer, searchTerm.text());
    }
    writer.close();

    // OPEN READER AT THIS POINT - this should fix the view of the
    // index at the point of having 100 "aaa" documents and 0 "bbb"
    reader = IndexReader.open(dir, false);
    assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
    assertTermDocsCount("first reader", reader, searchTerm, 100);
    reader.close();

    // DELETE DOCUMENTS CONTAINING TERM: aaa
    int deleted = 0;
    reader = IndexReader.open(dir, false);
    deleted = reader.deleteDocuments(searchTerm);
    assertEquals("deleted count", 100, deleted);
    assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm));
    assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);

    // open a 2nd reader to make sure first reader can
    // commit its changes (.del) while second reader
    // is open:
    IndexReader reader2 = IndexReader.open(dir, false);
    reader.close();

    // CREATE A NEW READER and re-test
    reader = IndexReader.open(dir, false);
    assertEquals("deleted docFreq", 0, reader.docFreq(searchTerm));
    assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
    reader.close();
    reader2.close();
    dir.close();
  }

  public void testDeleteReaderReaderConflictUnoptimized() throws IOException {
    deleteReaderReaderConflict(false);
  }
  
  public void testDeleteReaderReaderConflictOptimized() throws IOException {
    deleteReaderReaderConflict(true);
  }
  
  public void testDeleteReaderWriterConflictUnoptimized() throws IOException {
    deleteReaderWriterConflict(false);
  }
  
  public void testDeleteReaderWriterConflictOptimized() throws IOException {
    deleteReaderWriterConflict(true);
  }
  
  public void testMultiReaderDeletes() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w= new RandomIndexWriter(random, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    doc.add(newField("f", "doctor", StringField.TYPE_UNSTORED));
    w.addDocument(doc);
    doc = new Document();
    w.commit();
    doc.add(newField("f", "who", StringField.TYPE_UNSTORED));
    w.addDocument(doc);
    IndexReader r = new SlowMultiReaderWrapper(w.getReader());
    w.close();

    assertNull(r.getLiveDocs());
    r.close();

    r = new SlowMultiReaderWrapper(IndexReader.open(dir, false));

    assertNull(r.getLiveDocs());
    assertEquals(1, r.deleteDocuments(new Term("f", "doctor")));
    assertNotNull(r.getLiveDocs());
    assertFalse(r.getLiveDocs().get(0));
    assertEquals(1, r.deleteDocuments(new Term("f", "who")));
    assertFalse(r.getLiveDocs().get(1));
    r.close();
    dir.close();
  }
  
  public void testUndeleteAll() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocumentWithFields(writer);
    addDocumentWithFields(writer);
    writer.close();
    IndexReader reader = IndexReader.open(dir, false);
    reader.deleteDocument(0);
    reader.deleteDocument(1);
    reader.undeleteAll();
    reader.close();
    reader = IndexReader.open(dir, false);
    assertEquals(2, reader.numDocs());  // nothing has really been deleted thanks to undeleteAll()
    reader.close();
    dir.close();
  }

  public void testUndeleteAllAfterClose() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocumentWithFields(writer);
    addDocumentWithFields(writer);
    writer.close();
    IndexReader reader = IndexReader.open(dir, false);
    reader.deleteDocument(0);
    reader.close();
    reader = IndexReader.open(dir, false);
    reader.undeleteAll();
    assertEquals(2, reader.numDocs());  // nothing has really been deleted thanks to undeleteAll()
    reader.close();
    dir.close();
  }

  public void testUndeleteAllAfterCloseThenReopen() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocumentWithFields(writer);
    addDocumentWithFields(writer);
    writer.close();
    IndexReader reader = IndexReader.open(dir, false);
    reader.deleteDocument(0);
    reader.close();
    reader = IndexReader.open(dir, false);
    reader.undeleteAll();
    reader.close();
    reader = IndexReader.open(dir, false);
    assertEquals(2, reader.numDocs());  // nothing has really been deleted thanks to undeleteAll()
    reader.close();
    dir.close();
  }
  
  // LUCENE-1647
  public void testIndexReaderUnDeleteAll() throws Exception {
    MockDirectoryWrapper dir = newDirectory();
    dir.setPreventDoubleWrite(false);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer.addDocument(createDocument("a"));
    writer.addDocument(createDocument("b"));
    writer.addDocument(createDocument("c"));
    writer.close();
    IndexReader reader = IndexReader.open(dir, false);
    reader.deleteDocuments(new Term("id", "a"));
    reader.flush();
    reader.deleteDocuments(new Term("id", "b"));
    reader.undeleteAll();
    reader.deleteDocuments(new Term("id", "b"));
    reader.close();
    IndexReader.open(dir,true).close();
    dir.close();
  }
}
