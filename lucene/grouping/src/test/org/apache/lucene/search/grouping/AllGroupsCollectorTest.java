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

import java.util.HashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class AllGroupsCollectorTest extends LuceneTestCase {

  public void testTotalGroupCount() throws Exception {

    final String groupField = "author";

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    // 0
    Document doc = w.newDocument();
    addGroupField(doc, groupField, "author1");
    doc.addLargeText("content", "random text");
    doc.addStored("id", "1");
    w.addDocument(doc);

    // 1
    doc = w.newDocument();
    addGroupField(doc, groupField, "author1");
    doc.addLargeText("content", "some more random text blob");
    doc.addStored("id", "2");
    w.addDocument(doc);

    // 2
    doc = w.newDocument();
    addGroupField(doc, groupField, "author1");
    doc.addLargeText("content", "some more random textual data");
    doc.addStored("id", "3");
    w.addDocument(doc);
    w.commit(); // To ensure a second segment

    // 3
    doc = w.newDocument();
    addGroupField(doc, groupField, "author2");
    doc.addLargeText("content", "some random text");
    doc.addStored("id", "4");
    w.addDocument(doc);

    // 4
    doc = w.newDocument();
    addGroupField(doc, groupField, "author3");
    doc.addLargeText("content", "some more random text");
    doc.addStored("id", "5");
    w.addDocument(doc);

    // 5
    doc = w.newDocument();
    addGroupField(doc, groupField, "author3");
    doc.addLargeText("content", "random blob");
    doc.addStored("id", "6");
    w.addDocument(doc);

    // 6 -- no author field
    doc = w.newDocument();
    doc.addLargeText("content", "random word stuck in alot of other text");
    doc.addStored("id", "6");
    w.addDocument(doc);

    IndexSearcher indexSearcher = newSearcher(w.getReader());
    w.close();

    AbstractAllGroupsCollector<?> allGroupsCollector = createRandomCollector(groupField);
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
    doc.addAtom(groupField, value);
  }

  private AbstractAllGroupsCollector<?> createRandomCollector(String groupField) {
    AbstractAllGroupsCollector<?> selected;
    if (random().nextBoolean()) {
      selected = new TermAllGroupsCollector(groupField);
    } else {
      ValueSource vs = new BytesRefFieldSource(groupField);
      selected = new FunctionAllGroupsCollector(vs, new HashMap<>());
    }

    if (VERBOSE) {
      System.out.println("Selected implementation: " + selected.getClass().getName());
    }

    return selected;
  }

}
