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
package org.apache.lucene.index;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestRollback extends LuceneTestCase {

  // LUCENE-2536
  public void testRollbackIntegrityWithBufferFlush() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter rw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(newStringField("pk", Integer.toString(i), Field.Store.YES));
      rw.addDocument(doc);
    }
    rw.close();

    // If buffer size is small enough to cause a flush, errors ensue...
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                           .setMaxBufferedDocs(2)
                                           .setOpenMode(IndexWriterConfig.OpenMode.APPEND));

    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      String value = Integer.toString(i);
      doc.add(newStringField("pk", value, Field.Store.YES));
      doc.add(newStringField("text", "foo", Field.Store.YES));
      w.updateDocument(new Term("pk", value), doc);
    }
    w.rollback();

    IndexReader r = DirectoryReader.open(dir);
    assertEquals("index should contain same number of docs post rollback", 5, r.numDocs());
    r.close();
    dir.close();
  }
}
