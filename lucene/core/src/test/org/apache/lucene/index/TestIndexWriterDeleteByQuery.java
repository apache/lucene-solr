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


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexWriterDeleteByQuery extends LuceneTestCase {

  // LUCENE-6379
  public void testDeleteMatchAllDocsQuery() throws Exception {
    Directory dir = newMaybeVirusCheckingDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    // Norms are disabled:
    doc.add(newStringField("field", "foo", Field.Store.NO));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w);
    FieldInfo fi = FieldInfos.getMergedFieldInfos(r).fieldInfo("field");
    assertNotNull(fi);
    assertFalse(fi.hasNorms());
    assertEquals(1, r.numDocs());
    assertEquals(1, r.maxDoc());

    w.deleteDocuments(new MatchAllDocsQuery());
    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    r.close();

    assertNotNull(r2);
    assertEquals(0, r2.numDocs());
    assertEquals(0, r2.maxDoc());

    // Confirm the omitNorms bit is in fact no longer set:
    doc = new Document();
    // Norms are disabled:
    doc.add(newTextField("field", "foo", Field.Store.NO));
    w.addDocument(doc);

    DirectoryReader r3 = DirectoryReader.openIfChanged(r2);
    r2.close();
    assertNotNull(r3);
    assertEquals(1, r3.numDocs());
    assertEquals(1, r3.maxDoc());

    // Make sure norms can come back to life for a field after deleting by MatchAllDocsQuery:
    fi = FieldInfos.getMergedFieldInfos(r3).fieldInfo("field");
    assertNotNull(fi);
    assertTrue(fi.hasNorms());
    r3.close();
    w.close();
    dir.close();
  }
}
