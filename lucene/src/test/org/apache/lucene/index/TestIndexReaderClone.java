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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests cloning multiple types of readers, modifying the liveDocs and norms
 * and verifies copy on write semantics of the liveDocs and norms is
 * implemented properly
 */
public class TestIndexReaderClone extends LuceneTestCase {

  public void testDirectoryReader() throws Exception {
    final Directory dir = createIndex(0);
    performDefaultTests(IndexReader.open(dir));
    dir.close();
  }
  
  public void testMultiReader() throws Exception {
    final Directory dir1 = createIndex(0);
    final IndexReader r1 = IndexReader.open(dir1);
    final Directory dir2 = createIndex(0);
    final IndexReader r2 = IndexReader.open(dir2);
    final MultiReader mr = new MultiReader(r1, r2);
    performDefaultTests(mr);
    dir1.close();
    dir2.close();
  }
  
  public void testParallelReader() throws Exception {
    final Directory dir1 = createIndex(0);
    final IndexReader r1 = IndexReader.open(dir1);
    final Directory dir2 = createIndex(1);
    final IndexReader r2 = IndexReader.open(dir2);
    final ParallelReader pr = new ParallelReader();
    pr.add(r1);
    pr.add(r2);
    performDefaultTests(pr);
    dir1.close();
    dir2.close();
  }
  
  private Directory createIndex(int no) throws Exception {
    final Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMergePolicy(newLogMergePolicy(false))
    );
    Document doc = new Document();
    doc.add(newField("field"+no, "yes it's stored", TextField.TYPE_STORED));
    w.addDocument(doc);
    w.close();
    return dir;
  }

  private void performDefaultTests(IndexReader r1) throws Exception {
    IndexReader r2 = (IndexReader) r1.clone();
    assertTrue(r1 != r2);
    TestIndexReader.assertIndexEquals(r1, r2);
    r1.close();
    r2.close();
    TestIndexReaderReopen.assertReaderClosed(r1, true, true);
    TestIndexReaderReopen.assertReaderClosed(r2, true, true);
  }
}
