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

  public void testSegmentReaderCloseReferencing() throws Exception {
    final Directory dir1 = newDirectory();
    TestIndexReaderReopen.createIndex(random, dir1, false);
    SegmentReader origSegmentReader = getOnlySegmentReader(IndexReader.open(dir1, false));
    origSegmentReader.deleteDocument(1);

    SegmentReader clonedSegmentReader = (SegmentReader) origSegmentReader
        .clone();
    assertDelDocsRefCountEquals(2, origSegmentReader);
    origSegmentReader.close();
    assertDelDocsRefCountEquals(1, origSegmentReader);
    clonedSegmentReader.close();
    dir1.close();
  }

  private void assertDelDocsRefCountEquals(int refCount, SegmentReader reader) {
    assertEquals(refCount, reader.liveDocsRef.get());
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
