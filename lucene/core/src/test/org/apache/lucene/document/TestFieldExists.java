package org.apache.lucene.document;

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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestFieldExists extends LuceneTestCase {
  public void testFieldExistsFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addAtom("field1", "field");
    doc.addAtom("field2", "field");
    doc.addAtom("field3", "field");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("field1", "field");
    doc.addAtom("field3", "field");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("field2", "field");
    doc.addAtom("field3", "field");
    doc.addAtom("id", "2");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(),
                            fieldTypes.newFieldExistsFilter("field1"), 2);
    assertEquals(2, hits.totalHits);
    Set<String> ids = getIDs(r, hits);
    assertTrue(ids.contains("0"));
    assertTrue(ids.contains("1"));

    hits = s.search(new MatchAllDocsQuery(),
                    fieldTypes.newFieldExistsFilter("field2"), 2);
    assertEquals(2, hits.totalHits);
    ids = getIDs(r, hits);
    assertTrue(ids.contains("0"));
    assertTrue(ids.contains("2"));

    hits = s.search(new MatchAllDocsQuery(),
                    fieldTypes.newFieldExistsFilter("field3"), 3);
    assertEquals(3, hits.totalHits);
    ids = getIDs(r, hits);
    assertTrue(ids.contains("0"));
    assertTrue(ids.contains("1"));
    assertTrue(ids.contains("2"));

    assertEquals(0, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newFieldExistsFilter("field4"), 1).totalHits);

    r.close();
    w.close();
    dir.close();
  }

  private Set<String> getIDs(IndexReader r, TopDocs hits) throws IOException {
    Set<String> ids = new HashSet<>();
    for(ScoreDoc scoreDoc : hits.scoreDocs) {
      ids.add(r.document(scoreDoc.doc).getString("id"));
    }
    return ids;
  }
}
