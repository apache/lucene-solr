package org.apache.lucene.search.grouping;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License")); you may not use this file except in compliance with
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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TermAllGroupsCollectorTest extends LuceneTestCase {

  public void testTotalGroupCount() throws Exception {

    final String groupField = "author";
    FieldType customType = new FieldType();
    customType.setStored(true);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
                               random,
                               dir,
                               newIndexWriterConfig(TEST_VERSION_CURRENT,
                                                    new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    // 0
    Document doc = new Document();
    doc.add(new Field(groupField, "author1", TextField.TYPE_STORED));
    doc.add(new Field("content", "random text", TextField.TYPE_STORED));
    doc.add(new Field("id", "1", customType));
    w.addDocument(doc);

    // 1
    doc = new Document();
    doc.add(new Field(groupField, "author1", TextField.TYPE_STORED));
    doc.add(new Field("content", "some more random text blob", TextField.TYPE_STORED));
    doc.add(new Field("id", "2", customType));
    w.addDocument(doc);

    // 2
    doc = new Document();
    doc.add(new Field(groupField, "author1", TextField.TYPE_STORED));
    doc.add(new Field("content", "some more random textual data", TextField.TYPE_STORED));
    doc.add(new Field("id", "3", customType));
    w.addDocument(doc);
    w.commit(); // To ensure a second segment

    // 3
    doc = new Document();
    doc.add(new Field(groupField, "author2", TextField.TYPE_STORED));
    doc.add(new Field("content", "some random text", TextField.TYPE_STORED));
    doc.add(new Field("id", "4", customType));
    w.addDocument(doc);

    // 4
    doc = new Document();
    doc.add(new Field(groupField, "author3", TextField.TYPE_STORED));
    doc.add(new Field("content", "some more random text", TextField.TYPE_STORED));
    doc.add(new Field("id", "5", customType));
    w.addDocument(doc);

    // 5
    doc = new Document();
    doc.add(new Field(groupField, "author3", TextField.TYPE_STORED));
    doc.add(new Field("content", "random blob", TextField.TYPE_STORED));
    doc.add(new Field("id", "6", customType));
    w.addDocument(doc);

    // 6 -- no author field
    doc = new Document();
    doc.add(new Field("content", "random word stuck in alot of other text", TextField.TYPE_STORED));
    doc.add(new Field("id", "6", customType));
    w.addDocument(doc);

    IndexSearcher indexSearcher = new IndexSearcher(w.getReader());
    w.close();

    TermAllGroupsCollector c1 = new TermAllGroupsCollector(groupField);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c1);
    assertEquals(4, c1.getGroupCount());

    TermAllGroupsCollector c2 = new TermAllGroupsCollector(groupField);
    indexSearcher.search(new TermQuery(new Term("content", "some")), c2);
    assertEquals(3, c2.getGroupCount());

    TermAllGroupsCollector c3 = new TermAllGroupsCollector(groupField);
    indexSearcher.search(new TermQuery(new Term("content", "blob")), c3);
    assertEquals(2, c3.getGroupCount());

    indexSearcher.getIndexReader().close();
    dir.close();
  }
}
