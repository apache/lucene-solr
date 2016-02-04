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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.mutable.MutableValueStr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class GroupingSearchTest extends LuceneTestCase {

  // Tests some very basic usages...
  public void testBasic() throws Exception {

    final String groupField = "author";

    FieldType customType = new FieldType();
    customType.setStored(true);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    boolean canUseIDV = true;
    List<Document> documents = new ArrayList<>();
    // 0
    Document doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV);
    doc.add(new TextField("content", "random text", Field.Store.YES));
    doc.add(new Field("id", "1", customType));
    documents.add(doc);

    // 1
    doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV);
    doc.add(new TextField("content", "some more random text", Field.Store.YES));
    doc.add(new Field("id", "2", customType));
    documents.add(doc);

    // 2
    doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV);
    doc.add(new TextField("content", "some more random textual data", Field.Store.YES));
    doc.add(new Field("id", "3", customType));
    doc.add(new StringField("groupend", "x", Field.Store.NO));
    documents.add(doc);
    w.addDocuments(documents);
    documents.clear();

    // 3
    doc = new Document();
    addGroupField(doc, groupField, "author2", canUseIDV);
    doc.add(new TextField("content", "some random text", Field.Store.YES));
    doc.add(new Field("id", "4", customType));
    doc.add(new StringField("groupend", "x", Field.Store.NO));
    w.addDocument(doc);

    // 4
    doc = new Document();
    addGroupField(doc, groupField, "author3", canUseIDV);
    doc.add(new TextField("content", "some more random text", Field.Store.YES));
    doc.add(new Field("id", "5", customType));
    documents.add(doc);

    // 5
    doc = new Document();
    addGroupField(doc, groupField, "author3", canUseIDV);
    doc.add(new TextField("content", "random", Field.Store.YES));
    doc.add(new Field("id", "6", customType));
    doc.add(new StringField("groupend", "x", Field.Store.NO));
    documents.add(doc);
    w.addDocuments(documents);
    documents.clear();

    // 6 -- no author field
    doc = new Document();
    doc.add(new TextField("content", "random word stuck in alot of other text", Field.Store.YES));
    doc.add(new Field("id", "6", customType));
    doc.add(new StringField("groupend", "x", Field.Store.NO));

    w.addDocument(doc);

    IndexSearcher indexSearcher = newSearcher(w.getReader());
    w.close();

    Sort groupSort = Sort.RELEVANCE;
    GroupingSearch groupingSearch = createRandomGroupingSearch(groupField, groupSort, 5, canUseIDV);

    TopGroups<?> groups = groupingSearch.search(indexSearcher, new TermQuery(new Term("content", "random")), 0, 10);

    assertEquals(7, groups.totalHitCount);
    assertEquals(7, groups.totalGroupedHitCount);
    assertEquals(4, groups.groups.length);

    // relevance order: 5, 0, 3, 4, 1, 2, 6

    // the later a document is added the higher this docId
    // value
    GroupDocs<?> group = groups.groups[0];
    compareGroupValue("author3", group);
    assertEquals(2, group.scoreDocs.length);
    assertEquals(5, group.scoreDocs[0].doc);
    assertEquals(4, group.scoreDocs[1].doc);
    assertTrue(group.scoreDocs[0].score >= group.scoreDocs[1].score);

    group = groups.groups[1];
    compareGroupValue("author1", group);
    assertEquals(3, group.scoreDocs.length);
    assertEquals(0, group.scoreDocs[0].doc);
    assertEquals(1, group.scoreDocs[1].doc);
    assertEquals(2, group.scoreDocs[2].doc);
    assertTrue(group.scoreDocs[0].score >= group.scoreDocs[1].score);
    assertTrue(group.scoreDocs[1].score >= group.scoreDocs[2].score);

    group = groups.groups[2];
    compareGroupValue("author2", group);
    assertEquals(1, group.scoreDocs.length);
    assertEquals(3, group.scoreDocs[0].doc);

    group = groups.groups[3];
    compareGroupValue(null, group);
    assertEquals(1, group.scoreDocs.length);
    assertEquals(6, group.scoreDocs[0].doc);

    Query lastDocInBlock = new TermQuery(new Term("groupend", "x"));
    groupingSearch = new GroupingSearch(lastDocInBlock);
    groups = groupingSearch.search(indexSearcher, new TermQuery(new Term("content", "random")), 0, 10);

    assertEquals(7, groups.totalHitCount);
    assertEquals(7, groups.totalGroupedHitCount);
    assertEquals(4, groups.totalGroupCount.longValue());
    assertEquals(4, groups.groups.length);
    
    indexSearcher.getIndexReader().close();
    dir.close();
  }

  private void addGroupField(Document doc, String groupField, String value, boolean canUseIDV) {
    doc.add(new TextField(groupField, value, Field.Store.YES));
    if (canUseIDV) {
      doc.add(new SortedDocValuesField(groupField, new BytesRef(value)));
    }
  }

  private void compareGroupValue(String expected, GroupDocs<?> group) {
    if (expected == null) {
      if (group.groupValue == null) {
        return;
      } else if (group.groupValue.getClass().isAssignableFrom(MutableValueStr.class)) {
        return;
      } else if (((BytesRef) group.groupValue).length == 0) {
        return;
      }
      fail();
    }

    if (group.groupValue.getClass().isAssignableFrom(BytesRef.class)) {
      assertEquals(new BytesRef(expected), group.groupValue);
    } else if (group.groupValue.getClass().isAssignableFrom(MutableValueStr.class)) {
      MutableValueStr v = new MutableValueStr();
      v.value.copyChars(expected);
      assertEquals(v, group.groupValue);
    } else {
      fail();
    }
  }

  private GroupingSearch createRandomGroupingSearch(String groupField, Sort groupSort, int docsInGroup, boolean canUseIDV) {
    GroupingSearch groupingSearch;
    if (random().nextBoolean()) {
      ValueSource vs = new BytesRefFieldSource(groupField);
      groupingSearch = new GroupingSearch(vs, new HashMap<>());
    } else {
      groupingSearch = new GroupingSearch(groupField);
    }

    groupingSearch.setGroupSort(groupSort);
    groupingSearch.setGroupDocsLimit(docsInGroup);

    if (random().nextBoolean()) {
      groupingSearch.setCachingInMB(4.0, true);
    }

    return groupingSearch;
  }

  public void testSetAllGroups() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    doc.add(newField("group", "foo", StringField.TYPE_NOT_STORED));
    doc.add(new SortedDocValuesField("group", new BytesRef("foo")));
    w.addDocument(doc);

    IndexSearcher indexSearcher = newSearcher(w.getReader());
    w.close();

    GroupingSearch gs = new GroupingSearch("group");
    gs.setAllGroups(true);
    TopGroups<?> groups = gs.search(indexSearcher, new TermQuery(new Term("group", "foo")), 0, 10);
    assertEquals(1, groups.totalHitCount);
    //assertEquals(1, groups.totalGroupCount.intValue());
    assertEquals(1, groups.totalGroupedHitCount);
    assertEquals(1, gs.getAllMatchingGroups().size());
    indexSearcher.getIndexReader().close();
    dir.close();
  }
}
