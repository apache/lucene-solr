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
    IndexReader r1 = IndexReader.open(dir);
    IndexReader r2 = (IndexReader) r1.clone();
    r1.close();
    r2.close();
    dir.close();
  }
}
