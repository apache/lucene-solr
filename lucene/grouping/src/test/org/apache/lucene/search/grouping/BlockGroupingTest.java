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
import java.util.ArrayList;
import java.util.List;

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
import org.apache.lucene.util.BytesRef;

public class BlockGroupingTest extends AbstractGroupingTestCase {

  public void testSimple() throws IOException {

    Shard shard = new Shard();
    indexRandomDocs(shard.writer);
    IndexSearcher searcher = shard.getIndexSearcher();

    Query blockEndQuery = new TermQuery(new Term("blockEnd", "true"));
    GroupingSearch grouper = new GroupingSearch(blockEndQuery);
    grouper.setGroupDocsLimit(10);

    Query topLevel = new TermQuery(new Term("text", "grandmother"));
    TopGroups<?> tg = grouper.search(searcher, topLevel, 0, 5);

    // We're sorting by score, so the score of the top group should be the same as the
    // score of the top document from the same query with no grouping
    TopDocs topDoc = searcher.search(topLevel, 1);
    assertEquals(topDoc.scoreDocs[0].score, tg.groups[0].scoreDocs[0].score, 0);
    assertEquals(topDoc.scoreDocs[0].doc, tg.groups[0].scoreDocs[0].doc);

    for (int i = 0; i < tg.groups.length; i++) {
      String bookName = searcher.doc(tg.groups[i].scoreDocs[0].doc).get("book");
      // The contents of each group should be equal to the results of a search for
      // that group alone
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(new TermQuery(new Term("book", bookName)), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 10);
      assertScoreDocsEquals(td.scoreDocs, tg.groups[i].scoreDocs);
    }

    shard.close();

  }

  public void testTopLevelSort() throws IOException {

    Shard shard = new Shard();
    indexRandomDocs(shard.writer);
    IndexSearcher searcher = shard.getIndexSearcher();

    Sort sort = new Sort(new SortField("length", SortField.Type.LONG));

    Query blockEndQuery = new TermQuery(new Term("blockEnd", "true"));
    GroupingSearch grouper = new GroupingSearch(blockEndQuery);
    grouper.setGroupDocsLimit(10);
    grouper.setGroupSort(sort);     // groups returned sorted by length, chapters within group sorted by relevancy

    Query topLevel = new TermQuery(new Term("text", "grandmother"));
    TopGroups<?> tg = grouper.search(searcher, topLevel, 0, 5);

    // The sort value of the top doc in the top group should be the same as the sort value
    // of the top result from the same search done with no grouping
    TopDocs topDoc = searcher.search(topLevel, 1, sort);
    assertEquals(((FieldDoc)topDoc.scoreDocs[0]).fields[0], tg.groups[0].groupSortValues[0]);

    for (int i = 0; i < tg.groups.length; i++) {
      String bookName = searcher.doc(tg.groups[i].scoreDocs[0].doc).get("book");
      // The contents of each group should be equal to the results of a search for
      // that group alone, sorted by score
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(new TermQuery(new Term("book", bookName)), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 10);
      assertScoreDocsEquals(td.scoreDocs, tg.groups[i].scoreDocs);
      if (i > 1) {
        assertSortsBefore(tg.groups[i - 1], tg.groups[i]);
      }
    }

    shard.close();

  }

  public void testWithinGroupSort() throws IOException {

    Shard shard = new Shard();
    indexRandomDocs(shard.writer);
    IndexSearcher searcher = shard.getIndexSearcher();

    Sort sort = new Sort(new SortField("length", SortField.Type.LONG));

    Query blockEndQuery = new TermQuery(new Term("blockEnd", "true"));
    GroupingSearch grouper = new GroupingSearch(blockEndQuery);
    grouper.setGroupDocsLimit(10);
    grouper.setSortWithinGroup(sort);     // groups returned sorted by relevancy, chapters within group sorted by length

    Query topLevel = new TermQuery(new Term("text", "grandmother"));
    TopGroups<?> tg = grouper.search(searcher, topLevel, 0, 5);

    // We're sorting by score, so the score of the top group should be the same as the
    // score of the top document from the same query with no grouping
    TopDocs topDoc = searcher.search(topLevel, 1);
    assertEquals(topDoc.scoreDocs[0].score, (float)tg.groups[0].groupSortValues[0], 0);

    for (int i = 0; i < tg.groups.length; i++) {
      String bookName = searcher.doc(tg.groups[i].scoreDocs[0].doc).get("book");
      // The contents of each group should be equal to the results of a search for
      // that group alone, sorted by length
      Query filtered = new BooleanQuery.Builder()
          .add(topLevel, BooleanClause.Occur.MUST)
          .add(new TermQuery(new Term("book", bookName)), BooleanClause.Occur.FILTER)
          .build();
      TopDocs td = searcher.search(filtered, 10, sort);
      assertFieldDocsEquals(td.scoreDocs, tg.groups[i].scoreDocs);
      // We're sorting by score, so the group sort value for each group should be a float,
      // and the value for the previous group should be higher or equal to the value for this one
      if (i > 0) {
        float prevScore = (float) tg.groups[i - 1].groupSortValues[0];
        float thisScore = (float) tg.groups[i].groupSortValues[0];
        assertTrue(prevScore >= thisScore);
      }
    }

    shard.close();
  }

  private static void indexRandomDocs(RandomIndexWriter writer) throws IOException {
    int bookCount = atLeast(20);
    for (int i = 0; i < bookCount; i++) {
      writer.addDocuments(createRandomBlock(i));
    }
  }

  private static List<Document> createRandomBlock(int book) {
    List<Document> block = new ArrayList<>();
    String bookName = "book" + book;
    int chapterCount = atLeast(10);
    for (int j = 0; j < chapterCount; j++) {
      Document doc = new Document();
      String chapterName = "chapter" + j;
      String chapterText = randomText();
      doc.add(new TextField("book", bookName, Field.Store.YES));
      doc.add(new TextField("chapter", chapterName, Field.Store.YES));
      doc.add(new TextField("text", chapterText, Field.Store.NO));
      doc.add(new NumericDocValuesField("length", chapterText.length()));
      doc.add(new SortedDocValuesField("book", new BytesRef(bookName)));
      if (j == chapterCount - 1) {
        doc.add(new TextField("blockEnd", "true", Field.Store.NO));
      }
      block.add(doc);
    }
    return block;
  }

  private static final String[] TEXT = new String[]{
      "It was the day my grandmother exploded",
      "It was the best of times, it was the worst of times",
      "It was a bright cold morning in April",
      "It is a truth universally acknowledged",
      "I have just returned from a visit to my landlord",
      "I've been here and I've been there"
  };

  private static String randomText() {
    StringBuilder sb = new StringBuilder(TEXT[random().nextInt(TEXT.length)]);
    int sentences = random().nextInt(20);
    for (int i = 0; i < sentences; i++) {
      sb.append(" ").append(TEXT[random().nextInt(TEXT.length)]);
    }
    return sb.toString();
  }

  private void assertSortsBefore(GroupDocs<?> first, GroupDocs<?> second) {
    Object[] groupSortValues = second.groupSortValues;
    Object[] prevSortValues = first.groupSortValues;
    assertTrue(((Long)prevSortValues[0]).compareTo((Long)groupSortValues[0]) <= 0);
  }

  protected static void assertFieldDocsEquals(ScoreDoc[] expected, ScoreDoc[] actual) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i].doc, actual[i].doc);
      FieldDoc e = (FieldDoc) expected[i];
      FieldDoc a = (FieldDoc) actual[i];
      assertArrayEquals(e.fields, a.fields);
    }
  }

}
