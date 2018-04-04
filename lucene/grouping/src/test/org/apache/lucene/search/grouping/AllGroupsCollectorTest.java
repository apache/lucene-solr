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
package org.apache.lucene.search.grouping;

import java.util.HashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class AllGroupsCollectorTest extends LuceneTestCase {

  public void testTotalGroupCount() throws Exception {

    final String groupField = "author";
    FieldType customType = new FieldType();
    customType.setStored(true);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    // 0
    Document doc = new Document();
    addGroupField(doc, groupField, "author1");
    doc.add(new TextField("content", "random text", Field.Store.YES));
    doc.add(new Field("id", "1", customType));
    w.addDocument(doc);

    // 1
    doc = new Document();
    addGroupField(doc, groupField, "author1");
    doc.add(new TextField("content", "some more random text blob", Field.Store.YES));
    doc.add(new Field("id", "2", customType));
    w.addDocument(doc);

    // 2
    doc = new Document();
    addGroupField(doc, groupField, "author1");
    doc.add(new TextField("content", "some more random textual data", Field.Store.YES));
    doc.add(new Field("id", "3", customType));
    w.addDocument(doc);
    w.commit(); // To ensure a second segment

    // 3
    doc = new Document();
    addGroupField(doc, groupField, "author2");
    doc.add(new TextField("content", "some random text", Field.Store.YES));
    doc.add(new Field("id", "4", customType));
    w.addDocument(doc);

    // 4
    doc = new Document();
    addGroupField(doc, groupField, "author3");
    doc.add(new TextField("content", "some more random text", Field.Store.YES));
    doc.add(new Field("id", "5", customType));
    w.addDocument(doc);

    // 5
    doc = new Document();
    addGroupField(doc, groupField, "author3");
    doc.add(new TextField("content", "random blob", Field.Store.YES));
    doc.add(new Field("id", "6", customType));
    w.addDocument(doc);

    // 6 -- no author field
    doc = new Document();
    doc.add(new TextField("content", "random word stuck in alot of other text", Field.Store.YES));
    doc.add(new Field("id", "6", customType));
    w.addDocument(doc);

    IndexSearcher indexSearcher = newSearcher(w.getReader());
    w.close();

    AllGroupsCollector<?> allGroupsCollector = createRandomCollector(groupField);
    indexSearcher.search(new TermQuery(new Term("content", "random")), allGroupsCollector);
    assertEquals(4, allGroupsCollector.getGroupCount());

    allGroupsCollector = createRandomCollector(groupField);
    indexSearcher.search(new TermQuery(new Term("content", "some")), allGroupsCollector);
    assertEquals(3, allGroupsCollector.getGroupCount());

    allGroupsCollector = createRandomCollector(groupField);
    indexSearcher.search(new TermQuery(new Term("content", "blob")), allGroupsCollector);
    assertEquals(2, allGroupsCollector.getGroupCount());

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  private void addGroupField(Document doc, String groupField, String value) {
    doc.add(new TextField(groupField, value, Field.Store.YES));
    doc.add(new SortedDocValuesField(groupField, new BytesRef(value)));
  }

  private AllGroupsCollector<?> createRandomCollector(String groupField) {
    if (random().nextBoolean()) {
      return new AllGroupsCollector<>(new TermGroupSelector(groupField));
    }
    else {
      ValueSource vs = new BytesRefFieldSource(groupField);
      return new AllGroupsCollector<>(new ValueSourceGroupSelector(vs, new HashMap<>()));
    }
  }

}
