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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public abstract class BaseGroupSelectorTestCase<T> extends LuceneTestCase {

  protected abstract void addGroupField(Document document, int id);

  protected abstract GroupSelector<T> getGroupSelector();

  protected abstract Query filterQuery(T groupValue);

  public void testSortByRelevance() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    indexRandomDocs(w);
    IndexSearcher searcher = new IndexSearcher(w.getReader());
    w.close();

    String[] query = new String[]{ "foo", "bar", "baz" };
    Query topLevel = new TermQuery(new Term("text", query[random().nextInt(query.length)]));

    GroupingSearch grouper = new GroupingSearch(getGroupSelector());
    grouper.setGroupDocsLimit(10);
    TopGroups<T> topGroups = grouper.search(searcher, topLevel, 0, 5);
    TopDocs topDoc = searcher.search(topLevel, 1);
    for (int i = 0; i < topGroups.groups.length; i++) {
      // Each group should have a result set equal to that returned by the top-level query,
      // filtered by the group value.
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(filterQuery(topGroups.groups[i].groupValue), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 10);
      assertScoreDocsEquals(topGroups.groups[i].scoreDocs, td.scoreDocs);
      if (i == 0) {
        assertEquals(td.scoreDocs[0].doc, topDoc.scoreDocs[0].doc);
        assertEquals(td.scoreDocs[0].score, topDoc.scoreDocs[0].score, 0);
      }
    }

    searcher.getIndexReader().close();
    dir.close();
  }

  public void testSortGroups() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    indexRandomDocs(w);
    IndexSearcher searcher = new IndexSearcher(w.getReader());
    w.close();

    String[] query = new String[]{ "foo", "bar", "baz" };
    Query topLevel = new TermQuery(new Term("text", query[random().nextInt(query.length)]));

    GroupingSearch grouper = new GroupingSearch(getGroupSelector());
    grouper.setGroupDocsLimit(10);
    Sort sort = new Sort(new SortField("sort1", SortField.Type.STRING), new SortField("sort2", SortField.Type.LONG));
    grouper.setGroupSort(sort);
    TopGroups<T> topGroups = grouper.search(searcher, topLevel, 0, 5);
    TopDocs topDoc = searcher.search(topLevel, 1, sort);
    for (int i = 0; i < topGroups.groups.length; i++) {
      // We're sorting the groups by a defined Sort, but each group itself should be ordered
      // by doc relevance, and should be equal to the results of a top-level query filtered
      // by the group value
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(filterQuery(topGroups.groups[i].groupValue), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 10);
      assertScoreDocsEquals(topGroups.groups[i].scoreDocs, td.scoreDocs);
      // The top group should have sort values equal to the sort values of the top doc of
      // a top-level search sorted by the same Sort; subsequent groups should have sort values
      // that compare lower than their predecessor.
      if (i > 0) {
        assertSortsBefore(topGroups.groups[i - 1], topGroups.groups[i]);
      } else {
        assertArrayEquals(((FieldDoc)topDoc.scoreDocs[0]).fields, topGroups.groups[0].groupSortValues);
      }
    }

    searcher.getIndexReader().close();
    dir.close();
  }

  public void testSortWithinGroups() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    indexRandomDocs(w);
    IndexSearcher searcher = new IndexSearcher(w.getReader());
    w.close();

    String[] query = new String[]{ "foo", "bar", "baz" };
    Query topLevel = new TermQuery(new Term("text", query[random().nextInt(query.length)]));

    GroupingSearch grouper = new GroupingSearch(getGroupSelector());
    grouper.setGroupDocsLimit(10);
    Sort sort = new Sort(new SortField("sort1", SortField.Type.STRING), new SortField("sort2", SortField.Type.LONG));
    grouper.setSortWithinGroup(sort);

    TopGroups<T> topGroups = grouper.search(searcher, topLevel, 0, 5);
    TopDocs topDoc = searcher.search(topLevel, 1);

    for (int i = 0; i < topGroups.groups.length; i++) {
      // Check top-level ordering by score: first group's maxScore should be equal to the
      // top score returned by a simple search with no grouping; subsequent groups should
      // all have equal or lower maxScores
      if (i == 0) {
        assertEquals(topDoc.scoreDocs[0].score, topGroups.groups[0].maxScore, 0);
      } else {
        assertTrue(topGroups.groups[i].maxScore <= topGroups.groups[i - 1].maxScore);
      }
      // Groups themselves are ordered by a defined Sort, and each should give the same result as
      // the top-level query, filtered by the group value, with the same Sort
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(filterQuery(topGroups.groups[i].groupValue), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 10, sort);
      assertScoreDocsEquals(td.scoreDocs, topGroups.groups[i].scoreDocs);
    }

    searcher.getIndexReader().close();
    dir.close();

  }

  public void testGroupHeads() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    indexRandomDocs(w);
    IndexSearcher searcher = new IndexSearcher(w.getReader());
    w.close();

    String[] query = new String[]{ "foo", "bar", "baz" };
    Query topLevel = new TermQuery(new Term("text", query[random().nextInt(query.length)]));

    GroupSelector<T> groupSelector = getGroupSelector();
    GroupingSearch grouping = new GroupingSearch(groupSelector);
    grouping.setAllGroups(true);
    grouping.setAllGroupHeads(true);

    grouping.search(searcher, topLevel, 0, 1);
    Collection<T> matchingGroups = grouping.getAllMatchingGroups();

    // The number of hits from the top-level query should equal the sum of
    // the number of hits from the query filtered by each group value in turn
    int totalHits = searcher.count(topLevel);
    int groupHits = 0;
    for (T groupValue : matchingGroups) {
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(filterQuery(groupValue), BooleanClause.Occur.FILTER)
          .build();
      groupHits += searcher.count(filtered);
    }
    assertEquals(totalHits, groupHits);

    Bits groupHeads = grouping.getAllGroupHeads();
    int cardinality = 0;
    for (int i = 0; i < groupHeads.length(); i++) {
      if (groupHeads.get(i)) {
        cardinality++;
      }
    }
    assertEquals(matchingGroups.size(), cardinality);   // We should have one set bit per matching group

    // Each group head should correspond to the topdoc of a search filtered by
    // that group
    for (T groupValue : matchingGroups) {
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(filterQuery(groupValue), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 1);
      assertTrue(groupHeads.get(td.scoreDocs[0].doc));
    }

    searcher.getIndexReader().close();
    dir.close();
  }

  public void testGroupHeadsWithSort() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    indexRandomDocs(w);
    IndexSearcher searcher = new IndexSearcher(w.getReader());
    w.close();

    String[] query = new String[]{ "foo", "bar", "baz" };
    Query topLevel = new TermQuery(new Term("text", query[random().nextInt(query.length)]));

    Sort sort = new Sort(new SortField("sort1", SortField.Type.STRING), new SortField("sort2", SortField.Type.LONG));
    GroupSelector<T> groupSelector = getGroupSelector();
    GroupingSearch grouping = new GroupingSearch(groupSelector);
    grouping.setAllGroups(true);
    grouping.setAllGroupHeads(true);
    grouping.setSortWithinGroup(sort);

    grouping.search(searcher, topLevel, 0, 1);
    Collection<T> matchingGroups = grouping.getAllMatchingGroups();

    Bits groupHeads = grouping.getAllGroupHeads();
    int cardinality = 0;
    for (int i = 0; i < groupHeads.length(); i++) {
      if (groupHeads.get(i)) {
        cardinality++;
      }
    }
    assertEquals(matchingGroups.size(), cardinality);   // We should have one set bit per matching group

    // Each group head should correspond to the topdoc of a search filtered by
    // that group using the same within-group sort
    for (T groupValue : matchingGroups) {
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(filterQuery(groupValue), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 1, sort);
      assertTrue(groupHeads.get(td.scoreDocs[0].doc));
    }

    searcher.getIndexReader().close();
    dir.close();
  }

  private void indexRandomDocs(RandomIndexWriter w) throws IOException {
    String[] texts = new String[]{ "foo", "bar", "bar baz", "foo foo bar" };

    int numDocs = random().nextInt(200);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", i));
      doc.add(new TextField("name", Integer.toString(i), Field.Store.YES));
      doc.add(new TextField("text", texts[random().nextInt(texts.length)], Field.Store.NO));
      doc.add(new SortedDocValuesField("sort1", new BytesRef("sort" + random().nextInt(4))));
      doc.add(new NumericDocValuesField("sort2", random().nextLong()));
      addGroupField(doc, i);
      w.addDocument(doc);
    }
  }

  private void assertSortsBefore(GroupDocs<T> first, GroupDocs<T> second) {
    Object[] groupSortValues = second.groupSortValues;
    Object[] prevSortValues = first.groupSortValues;
    assertTrue(((BytesRef)prevSortValues[0]).compareTo((BytesRef)groupSortValues[0]) <= 0);
    if (prevSortValues[0].equals(groupSortValues[0])) {
      assertTrue((long)prevSortValues[1] <= (long)groupSortValues[1]);
    }
  }

  private static void assertScoreDocsEquals(ScoreDoc[] expected, ScoreDoc[] actual) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i].doc, actual[i].doc);
      assertEquals(expected[i].score, actual[i].score, 0);
    }
  }

}
