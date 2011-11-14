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

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Bits;

/**
 * Tests cloning multiple types of readers, modifying the liveDocs and norms
 * and verifies copy on write semantics of the liveDocs and norms is
 * implemented properly
 */
public class TestIndexReaderClone extends LuceneTestCase {
  
  public void testCloneReadOnlySegmentReader() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, false);
    IndexReader reader = IndexReader.open(dir1, false);
    IndexReader readOnlyReader = reader.clone(true);
    if (!isReadOnly(readOnlyReader)) {
      fail("reader isn't read only");
    }
    if (deleteWorked(1, readOnlyReader)) {
      fail("deleting from the original should not have worked");
    }
    reader.close();
    readOnlyReader.close();
    dir1.close();
  }

  // open non-readOnly reader1, clone to non-readOnly
  // reader2, make sure we can change reader2
  public void testCloneNoChangesStillReadOnly() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader r1 = IndexReader.open(dir1, false);
    IndexReader r2 = r1.clone(false);
    if (!deleteWorked(1, r2)) {
      fail("deleting from the cloned should have worked");
    }
    r1.close();
    r2.close();
    dir1.close();
  }
  
  // open non-readOnly reader1, clone to non-readOnly
  // reader2, make sure we can change reader1
  public void testCloneWriteToOrig() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader r1 = IndexReader.open(dir1, false);
    IndexReader r2 = r1.clone(false);
    if (!deleteWorked(1, r1)) {
      fail("deleting from the original should have worked");
    }
    r1.close();
    r2.close();
    dir1.close();
  }
  
  // open non-readOnly reader1, clone to non-readOnly
  // reader2, make sure we can change reader2
  public void testCloneWriteToClone() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader r1 = IndexReader.open(dir1, false);
    IndexReader r2 = r1.clone(false);
    if (!deleteWorked(1, r2)) {
      fail("deleting from the original should have worked");
    }
    // should fail because reader1 holds the write lock
    assertTrue("first reader should not be able to delete", !deleteWorked(1, r1));
    r2.close();
    // should fail because we are now stale (reader1
    // committed changes)
    assertTrue("first reader should not be able to delete", !deleteWorked(1, r1));
    r1.close();

    dir1.close();
  }
  
  // create single-segment index, open non-readOnly
  // SegmentReader, add docs, reopen to multireader, then do
  // delete
  public void testReopenSegmentReaderToMultiReader() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, false);
    IndexReader reader1 = IndexReader.open(dir1, false);

    TestIndexReaderReopen.modifyIndex(5, dir1);
    
    IndexReader reader2 = IndexReader.openIfChanged(reader1);
    assertNotNull(reader2);
    assertTrue(reader1 != reader2);

    assertTrue(deleteWorked(1, reader2));
    reader1.close();
    reader2.close();
    dir1.close();
  }

  // open non-readOnly reader1, clone to readOnly reader2
  public void testCloneWriteableToReadOnly() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader reader = IndexReader.open(dir1, false);
    IndexReader readOnlyReader = reader.clone(true);
    if (!isReadOnly(readOnlyReader)) {
      fail("reader isn't read only");
    }
    if (deleteWorked(1, readOnlyReader)) {
      fail("deleting from the original should not have worked");
    }
    // this readonly reader shouldn't have a write lock
    if (readOnlyReader.hasChanges) {
      fail("readOnlyReader has a write lock");
    }
    reader.close();
    readOnlyReader.close();
    dir1.close();
  }

  // open non-readOnly reader1, reopen to readOnly reader2
  public void testReopenWriteableToReadOnly() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader reader = IndexReader.open(dir1, false);
    final int docCount = reader.numDocs();
    assertTrue(deleteWorked(1, reader));
    assertEquals(docCount-1, reader.numDocs());

    IndexReader readOnlyReader = IndexReader.openIfChanged(reader, true);
    assertNotNull(readOnlyReader);
    if (!isReadOnly(readOnlyReader)) {
      fail("reader isn't read only");
    }
    assertFalse(deleteWorked(1, readOnlyReader));
    assertEquals(docCount-1, readOnlyReader.numDocs());
    reader.close();
    readOnlyReader.close();
    dir1.close();
  }

  // open readOnly reader1, clone to non-readOnly reader2
  public void testCloneReadOnlyToWriteable() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader reader1 = IndexReader.open(dir1, true);

    IndexReader reader2 = reader1.clone(false);
    if (isReadOnly(reader2)) {
      fail("reader should not be read only");
    }
    assertFalse("deleting from the original reader should not have worked", deleteWorked(1, reader1));
    // this readonly reader shouldn't yet have a write lock
    if (reader2.hasChanges) {
      fail("cloned reader should not have write lock");
    }
    assertTrue("deleting from the cloned reader should have worked", deleteWorked(1, reader2));
    reader1.close();
    reader2.close();
    dir1.close();
  }

  // open non-readOnly reader1 on multi-segment index, then
  // fully merge the index, then clone to readOnly reader2
  public void testReadOnlyCloneAfterFullMerge() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader reader1 = IndexReader.open(dir1, false);
    IndexWriter w = new IndexWriter(dir1, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    w.forceMerge(1);
    w.close();
    IndexReader reader2 = reader1.clone(true);
    assertTrue(isReadOnly(reader2));
    reader1.close();
    reader2.close();
    dir1.close();
  }
  
  private static boolean deleteWorked(int doc, IndexReader r) {
    boolean exception = false;
    try {
      // trying to delete from the original reader should throw an exception
      r.deleteDocument(doc);
    } catch (Exception ex) {
      exception = true;
    }
    return !exception;
  }
  
  public void testCloneReadOnlyDirectoryReader() throws Exception {
    final Directory dir1 = newDirectory();

    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader reader = IndexReader.open(dir1, false);
    IndexReader readOnlyReader = reader.clone(true);
    if (!isReadOnly(readOnlyReader)) {
      fail("reader isn't read only");
    }
    reader.close();
    readOnlyReader.close();
    dir1.close();
  }

  public static boolean isReadOnly(IndexReader r) {
    if (r instanceof SegmentReader) {
      return ((SegmentReader) r).readOnly;
    } else if (r instanceof DirectoryReader) {
      return ((DirectoryReader) r).readOnly;
    } else {
      return false;
    }
  }

  public void testParallelReader() throws Exception {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, true);
    final Directory dir2 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir2, true);
    IndexReader r1 = IndexReader.open(dir1, false);
    IndexReader r2 = IndexReader.open(dir2, false);

    ParallelReader pr1 = new ParallelReader();
    pr1.add(r1);
    pr1.add(r2);

    performDefaultTests(pr1);
    pr1.close();
    dir1.close();
    dir2.close();
  }

  /**
   * 1. Get a norm from the original reader 2. Clone the original reader 3.
   * Delete a document and set the norm of the cloned reader 4. Verify the norms
   * are not the same on each reader 5. Verify the doc deleted is only in the
   * cloned reader 6. Try to delete a document in the original reader, an
   * exception should be thrown
   * 
   * @param r1 IndexReader to perform tests on
   * @throws Exception
   */
  private void performDefaultTests(IndexReader r1) throws Exception {
    DefaultSimilarity sim = new DefaultSimilarity();
    float norm1 = sim.decodeNormValue(MultiNorms.norms(r1, "field1")[4]);

    IndexReader pr1Clone = (IndexReader) r1.clone();
    pr1Clone.deleteDocument(10);
    pr1Clone.setNorm(4, "field1", sim.encodeNormValue(0.5f));
    assertTrue(sim.decodeNormValue(MultiNorms.norms(r1, "field1")[4]) == norm1);
    assertTrue(sim.decodeNormValue(MultiNorms.norms(pr1Clone, "field1")[4]) != norm1);

    final Bits liveDocs = MultiFields.getLiveDocs(r1);
    assertTrue(liveDocs == null || liveDocs.get(10));
    assertFalse(MultiFields.getLiveDocs(pr1Clone).get(10));

    // try to update the original reader, which should throw an exception
    try {
      r1.deleteDocument(11);
      fail("Tried to delete doc 11 and an exception should have been thrown");
    } catch (Exception exception) {
      // expectted
    }
    pr1Clone.close();
  }

  public void testMixedReaders() throws Exception {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, true);
    final Directory dir2 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir2, true);
    IndexReader r1 = IndexReader.open(dir1, false);
    IndexReader r2 = IndexReader.open(dir2, false);

    MultiReader multiReader = new MultiReader(r1, r2);
    performDefaultTests(multiReader);
    multiReader.close();
    dir1.close();
    dir2.close();
  }

  public void testSegmentReaderUndeleteall() throws Exception {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, false);
    SegmentReader origSegmentReader = getOnlySegmentReader(IndexReader.open(dir1, false));
    origSegmentReader.deleteDocument(10);
    assertDelDocsRefCountEquals(1, origSegmentReader);
    origSegmentReader.undeleteAll();
    assertNull(origSegmentReader.liveDocsRef);
    origSegmentReader.close();
    // need to test norms?
    dir1.close();
  }
  
  public void testSegmentReaderCloseReferencing() throws Exception {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, false);
    SegmentReader origSegmentReader = getOnlySegmentReader(IndexReader.open(dir1, false));
    origSegmentReader.deleteDocument(1);
    DefaultSimilarity sim = new DefaultSimilarity();
    origSegmentReader.setNorm(4, "field1", sim.encodeNormValue(0.5f));

    SegmentReader clonedSegmentReader = (SegmentReader) origSegmentReader
        .clone();
    assertDelDocsRefCountEquals(2, origSegmentReader);
    origSegmentReader.close();
    assertDelDocsRefCountEquals(1, origSegmentReader);
    // check the norm refs
    SegmentNorms norm = clonedSegmentReader.norms.get("field1");
    assertEquals(1, norm.bytesRef().get());
    clonedSegmentReader.close();
    dir1.close();
  }
  
  public void testSegmentReaderDelDocsReferenceCounting() throws Exception {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, false);

    IndexReader origReader = IndexReader.open(dir1, false);
    SegmentReader origSegmentReader = getOnlySegmentReader(origReader);
    // liveDocsRef should be null because nothing has updated yet
    assertNull(origSegmentReader.liveDocsRef);

    // we deleted a document, so there is now a liveDocs bitvector and a
    // reference to it
    origReader.deleteDocument(1);
    assertDelDocsRefCountEquals(1, origSegmentReader);

    // the cloned segmentreader should have 2 references, 1 to itself, and 1 to
    // the original segmentreader
    IndexReader clonedReader = (IndexReader) origReader.clone();
    SegmentReader clonedSegmentReader = getOnlySegmentReader(clonedReader);
    assertDelDocsRefCountEquals(2, origSegmentReader);
    // deleting a document creates a new liveDocs bitvector, the refs goes to
    // 1
    clonedReader.deleteDocument(2);
    assertDelDocsRefCountEquals(1, origSegmentReader);
    assertDelDocsRefCountEquals(1, clonedSegmentReader);

    // make sure the deletedocs objects are different (copy
    // on write)
    assertTrue(origSegmentReader.liveDocs != clonedSegmentReader.liveDocs);

    assertDocDeleted(origSegmentReader, clonedSegmentReader, 1);
    final Bits liveDocs = origSegmentReader.getLiveDocs();
    assertTrue(liveDocs == null || liveDocs.get(2)); // doc 2 should not be deleted
                                                  // in original segmentreader
    assertFalse(clonedSegmentReader.getLiveDocs().get(2)); // doc 2 should be deleted in
                                                  // cloned segmentreader

    // deleting a doc from the original segmentreader should throw an exception
    try {
      origReader.deleteDocument(4);
      fail("expected exception");
    } catch (LockObtainFailedException lbfe) {
      // expected
    }

    origReader.close();
    // try closing the original segment reader to see if it affects the
    // clonedSegmentReader
    clonedReader.deleteDocument(3);
    clonedReader.flush();
    assertDelDocsRefCountEquals(1, clonedSegmentReader);

    // test a reopened reader
    IndexReader reopenedReader = IndexReader.openIfChanged(clonedReader);
    if (reopenedReader == null) {
      reopenedReader = clonedReader;
    }
    IndexReader cloneReader2 = (IndexReader) reopenedReader.clone();
    SegmentReader cloneSegmentReader2 = getOnlySegmentReader(cloneReader2);
    assertDelDocsRefCountEquals(2, cloneSegmentReader2);
    clonedReader.close();
    reopenedReader.close();
    cloneReader2.close();

    dir1.close();
  }

  // LUCENE-1648
  public void testCloneWithDeletes() throws Throwable {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, false);
    IndexReader origReader = IndexReader.open(dir1, false);
    origReader.deleteDocument(1);

    IndexReader clonedReader = (IndexReader) origReader.clone();
    origReader.close();
    clonedReader.close();

    IndexReader r = IndexReader.open(dir1, false);
    assertFalse(MultiFields.getLiveDocs(r).get(1));
    r.close();
    dir1.close();
  }

  // LUCENE-1648
  public void testCloneWithSetNorm() throws Throwable {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, false);
    IndexReader orig = IndexReader.open(dir1, false);
    DefaultSimilarity sim = new DefaultSimilarity();
    orig.setNorm(1, "field1", sim.encodeNormValue(17.0f));
    final byte encoded = sim.encodeNormValue(17.0f);
    assertEquals(encoded, MultiNorms.norms(orig, "field1")[1]);

    // the cloned segmentreader should have 2 references, 1 to itself, and 1 to
    // the original segmentreader
    IndexReader clonedReader = (IndexReader) orig.clone();
    orig.close();
    clonedReader.close();

    IndexReader r = IndexReader.open(dir1, false);
    assertEquals(encoded, MultiNorms.norms(r, "field1")[1]);
    r.close();
    dir1.close();
  }

  private void assertDocDeleted(SegmentReader reader, SegmentReader reader2,
      int doc) {
    assertEquals(reader.getLiveDocs().get(doc), reader2.getLiveDocs().get(doc));
  }

  private void assertDelDocsRefCountEquals(int refCount, SegmentReader reader) {
    assertEquals(refCount, reader.liveDocsRef.get());
  }
  
  public void testCloneSubreaders() throws Exception {
    final Directory dir1 = newDirectory();
 
    TestIndexReaderReopen.createIndex(random, dir1, true);
    IndexReader reader = IndexReader.open(dir1, false);
    reader.deleteDocument(1); // acquire write lock
    IndexReader[] subs = reader.getSequentialSubReaders();
    assert subs.length > 1;
    
    IndexReader[] clones = new IndexReader[subs.length];
    for (int x=0; x < subs.length; x++) {
      clones[x] = (IndexReader) subs[x].clone();
    }
    reader.close();
    for (int x=0; x < subs.length; x++) {
      clones[x].close();
    }
    dir1.close();
  }

  public void testLucene1516Bug() throws Exception {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, false);
    IndexReader r1 = IndexReader.open(dir1, false);
    r1.incRef();
    IndexReader r2 = r1.clone(false);
    r1.deleteDocument(5);
    r1.decRef();
    
    r1.incRef();
    
    r2.close();
    r1.decRef();
    r1.close();
    dir1.close();
  }

  public void testCloseStoredFields() throws Exception {
    final Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMergePolicy(newLogMergePolicy(false))
    );
    Document doc = new Document();
    doc.add(newField("field", "yes it's stored", TextField.TYPE_STORED));
    w.addDocument(doc);
    w.close();
    IndexReader r1 = IndexReader.open(dir, false);
    IndexReader r2 = r1.clone(false);
    r1.close();
    r2.close();
    dir.close();
  }
}
