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
import org.apache.lucene.document.*;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.dv.DVAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.HashMap;

public class AllGroupsCollectorTest extends LuceneTestCase {

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
    boolean canUseIDV = !"Lucene3x".equals(w.w.getConfig().getCodec().getName());

    // 0
    Document doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV);
    doc.add(new Field("content", "random text", TextField.TYPE_STORED));
    doc.add(new Field("id", "1", customType));
    w.addDocument(doc);

    // 1
    doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV);
    doc.add(new Field("content", "some more random text blob", TextField.TYPE_STORED));
    doc.add(new Field("id", "2", customType));
    w.addDocument(doc);

    // 2
    doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV);
    doc.add(new Field("content", "some more random textual data", TextField.TYPE_STORED));
    doc.add(new Field("id", "3", customType));
    w.addDocument(doc);
    w.commit(); // To ensure a second segment

    // 3
    doc = new Document();
    addGroupField(doc, groupField, "author2", canUseIDV);
    doc.add(new Field("content", "some random text", TextField.TYPE_STORED));
    doc.add(new Field("id", "4", customType));
    w.addDocument(doc);

    // 4
    doc = new Document();
    addGroupField(doc, groupField, "author3", canUseIDV);
    doc.add(new Field("content", "some more random text", TextField.TYPE_STORED));
    doc.add(new Field("id", "5", customType));
    w.addDocument(doc);

    // 5
    doc = new Document();
    addGroupField(doc, groupField, "author3", canUseIDV);
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

    AbstractAllGroupsCollector c1 = createRandomCollector(groupField, canUseIDV);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c1);
    assertEquals(4, c1.getGroupCount());

    AbstractAllGroupsCollector c2 = createRandomCollector(groupField, canUseIDV);
    indexSearcher.search(new TermQuery(new Term("content", "some")), c2);
    assertEquals(3, c2.getGroupCount());

    AbstractAllGroupsCollector c3 = createRandomCollector(groupField, canUseIDV);
    indexSearcher.search(new TermQuery(new Term("content", "blob")), c3);
    assertEquals(2, c3.getGroupCount());

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  private void addGroupField(Document doc, String groupField, String value, boolean canUseIDV) {
    doc.add(new Field(groupField, value, TextField.TYPE_STORED));
    if (canUseIDV) {
      DocValuesField valuesField = new DocValuesField(groupField);
      valuesField.setBytes(new BytesRef(value), Type.BYTES_VAR_SORTED);
      doc.add(valuesField);
    }
  }

  private AbstractAllGroupsCollector createRandomCollector(String groupField, boolean canUseIDV) throws IOException {
    AbstractAllGroupsCollector selected;
    if (random.nextBoolean() && canUseIDV) {
      boolean diskResident = random.nextBoolean();
      selected = DVAllGroupsCollector.create(groupField, Type.BYTES_VAR_SORTED, diskResident);
    } else if (random.nextBoolean()) {
      selected = new TermAllGroupsCollector(groupField);
    } else {
      ValueSource vs = new BytesRefFieldSource(groupField);
      selected = new FunctionAllGroupsCollector(vs, new HashMap());
    }

    if (VERBOSE) {
      System.out.println("Selected implementation: " + selected.getClass().getName());
    }

    return selected;
  }

}
