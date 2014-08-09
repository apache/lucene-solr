package org.apache.lucene.search;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests IndexSearcher's searchAfter() method
 */
public class TestSearchAfter extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private int iter;
  private List<SortField> allSortFields;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    allSortFields = new ArrayList<>(Arrays.asList(new SortField[] {
          new SortField("int", SortField.Type.INT, false),
          new SortField("long", SortField.Type.LONG, false),
          new SortField("float", SortField.Type.FLOAT, false),
          new SortField("double", SortField.Type.DOUBLE, false),
          new SortField("bytes", SortField.Type.STRING, false),
          new SortField("bytesval", SortField.Type.STRING_VAL, false),
          new SortField("intdocvalues", SortField.Type.INT, false),
          new SortField("floatdocvalues", SortField.Type.FLOAT, false),
          new SortField("sortedbytesdocvalues", SortField.Type.STRING, false),
          new SortField("sortedbytesdocvaluesval", SortField.Type.STRING_VAL, false),
          new SortField("straightbytesdocvalues", SortField.Type.STRING_VAL, false),
          new SortField("int", SortField.Type.INT, true),
          new SortField("long", SortField.Type.LONG, true),
          new SortField("float", SortField.Type.FLOAT, true),
          new SortField("double", SortField.Type.DOUBLE, true),
          new SortField("bytes", SortField.Type.STRING, true),
          new SortField("bytesval", SortField.Type.STRING_VAL, true),
          new SortField("intdocvalues", SortField.Type.INT, true),
          new SortField("floatdocvalues", SortField.Type.FLOAT, true),
          new SortField("sortedbytesdocvalues", SortField.Type.STRING, true),
          new SortField("sortedbytesdocvaluesval", SortField.Type.STRING_VAL, true),
          new SortField("straightbytesdocvalues", SortField.Type.STRING_VAL, true),
          SortField.FIELD_SCORE,
          SortField.FIELD_DOC,
        }));

    // Also test missing first / last for the "string" sorts:
    for(String field : new String[] {"bytes", "sortedbytesdocvalues"}) {
      for(int rev=0;rev<2;rev++) {
        boolean reversed = rev == 0;
        SortField sf = new SortField(field, SortField.Type.STRING, reversed);
        sf.setMissingValue(SortField.STRING_FIRST);
        allSortFields.add(sf);

        sf = new SortField(field, SortField.Type.STRING, reversed);
        sf.setMissingValue(SortField.STRING_LAST);
        allSortFields.add(sf);
      }
    }

    // Also test missing first / last for the "string_val" sorts:
    for(String field : new String[] {"sortedbytesdocvaluesval", "straightbytesdocvalues"}) {
      for(int rev=0;rev<2;rev++) {
        boolean reversed = rev == 0;
        SortField sf = new SortField(field, SortField.Type.STRING_VAL, reversed);
        sf.setMissingValue(SortField.STRING_FIRST);
        allSortFields.add(sf);

        sf = new SortField(field, SortField.Type.STRING_VAL, reversed);
        sf.setMissingValue(SortField.STRING_LAST);
        allSortFields.add(sf);
      }
    }

    int limit = allSortFields.size();
    for(int i=0;i<limit;i++) {
      SortField sf = allSortFields.get(i);
      if (sf.getType() == SortField.Type.INT) {
        SortField sf2 = new SortField(sf.getField(), SortField.Type.INT, sf.getReverse());
        sf2.setMissingValue(random().nextInt());
        allSortFields.add(sf2);
      } else if (sf.getType() == SortField.Type.LONG) {
        SortField sf2 = new SortField(sf.getField(), SortField.Type.LONG, sf.getReverse());
        sf2.setMissingValue(random().nextLong());
        allSortFields.add(sf2);
      } else if (sf.getType() == SortField.Type.FLOAT) {
        SortField sf2 = new SortField(sf.getField(), SortField.Type.FLOAT, sf.getReverse());
        sf2.setMissingValue(random().nextFloat());
        allSortFields.add(sf2);
      } else if (sf.getType() == SortField.Type.DOUBLE) {
        SortField sf2 = new SortField(sf.getField(), SortField.Type.DOUBLE, sf.getReverse());
        sf2.setMissingValue(random().nextDouble());
        allSortFields.add(sf2);
      }
    }

    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(200);
    for (int i = 0; i < numDocs; i++) {
      List<Field> fields = new ArrayList<>();
      fields.add(newTextField("english", English.intToEnglish(i), Field.Store.NO));
      fields.add(newTextField("oddeven", (i % 2 == 0) ? "even" : "odd", Field.Store.NO));
      fields.add(newStringField("byte", "" + ((byte) random().nextInt()), Field.Store.NO));
      fields.add(newStringField("short", "" + ((short) random().nextInt()), Field.Store.NO));
      fields.add(new IntField("int", random().nextInt(), Field.Store.NO));
      fields.add(new LongField("long", random().nextLong(), Field.Store.NO));

      fields.add(new FloatField("float", random().nextFloat(), Field.Store.NO));
      fields.add(new DoubleField("double", random().nextDouble(), Field.Store.NO));
      fields.add(newStringField("bytes", TestUtil.randomRealisticUnicodeString(random()), Field.Store.NO));
      fields.add(newStringField("bytesval", TestUtil.randomRealisticUnicodeString(random()), Field.Store.NO));
      fields.add(new DoubleField("double", random().nextDouble(), Field.Store.NO));

      fields.add(new NumericDocValuesField("intdocvalues", random().nextInt()));
      fields.add(new FloatDocValuesField("floatdocvalues", random().nextFloat()));
      fields.add(new SortedDocValuesField("sortedbytesdocvalues", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
      fields.add(new SortedDocValuesField("sortedbytesdocvaluesval", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
      fields.add(new BinaryDocValuesField("straightbytesdocvalues", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));

      Document document = new Document();
      document.add(new StoredField("id", ""+i));
      if (VERBOSE) {
        System.out.println("  add doc id=" + i);
      }
      for(Field field : fields) {
        // So we are sometimes missing that field:
        if (random().nextInt(5) != 4) {
          document.add(field);
          if (VERBOSE) {
            System.out.println("    " + field);
          }
        }
      }

      iw.addDocument(document);

      if (random().nextInt(50) == 17) {
        iw.commit();
      }
    }
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
    if (VERBOSE) {
      System.out.println("  searcher=" + searcher);
    }
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  public void testQueries() throws Exception {
    // because the first page has a null 'after', we get a normal collector.
    // so we need to run the test a few times to ensure we will collect multiple
    // pages.
    int n = atLeast(20);
    for (int i = 0; i < n; i++) {
      Filter odd = new QueryWrapperFilter(new TermQuery(new Term("oddeven", "odd")));
      assertQuery(new MatchAllDocsQuery(), null);
      assertQuery(new TermQuery(new Term("english", "one")), null);
      assertQuery(new MatchAllDocsQuery(), odd);
      assertQuery(new TermQuery(new Term("english", "four")), odd);
      BooleanQuery bq = new BooleanQuery();
      bq.add(new TermQuery(new Term("english", "one")), BooleanClause.Occur.SHOULD);
      bq.add(new TermQuery(new Term("oddeven", "even")), BooleanClause.Occur.SHOULD);
      assertQuery(bq, null);
    }
  }

  void assertQuery(Query query, Filter filter) throws Exception {
    assertQuery(query, filter, null);
    assertQuery(query, filter, Sort.RELEVANCE);
    assertQuery(query, filter, Sort.INDEXORDER);
    for(SortField sortField : allSortFields) {
      assertQuery(query, filter, new Sort(new SortField[] {sortField}));
    }
    for(int i=0;i<20;i++) {
      assertQuery(query, filter, getRandomSort());
    }
  }

  Sort getRandomSort() {
    SortField[] sortFields = new SortField[TestUtil.nextInt(random(), 2, 7)];
    for(int i=0;i<sortFields.length;i++) {
      sortFields[i] = allSortFields.get(random().nextInt(allSortFields.size()));
    }
    return new Sort(sortFields);
  }

  void assertQuery(Query query, Filter filter, Sort sort) throws Exception {
    int maxDoc = searcher.getIndexReader().maxDoc();
    TopDocs all;
    int pageSize = TestUtil.nextInt(random(), 1, maxDoc * 2);
    if (VERBOSE) {
      System.out.println("\nassertQuery " + (iter++) + ": query=" + query + " filter=" + filter + " sort=" + sort + " pageSize=" + pageSize);
    }
    final boolean doMaxScore = random().nextBoolean();
    final boolean doScores = random().nextBoolean();
    if (sort == null) {
      all = searcher.search(query, filter, maxDoc);
    } else if (sort == Sort.RELEVANCE) {
      all = searcher.search(query, filter, maxDoc, sort, true, doMaxScore);
    } else {
      all = searcher.search(query, filter, maxDoc, sort, doScores, doMaxScore);
    }
    if (VERBOSE) {
      System.out.println("  all.totalHits=" + all.totalHits);
      int upto = 0;
      for(ScoreDoc scoreDoc : all.scoreDocs) {
        System.out.println("    hit " + (upto++) + ": id=" + searcher.doc(scoreDoc.doc).get("id") + " " + scoreDoc);
      }
    }
    int pageStart = 0;
    ScoreDoc lastBottom = null;
    while (pageStart < all.totalHits) {
      TopDocs paged;
      if (sort == null) {
        if (VERBOSE) {
          System.out.println("  iter lastBottom=" + lastBottom);
        }
        paged = searcher.searchAfter(lastBottom, query, filter, pageSize);
      } else {
        if (VERBOSE) {
          System.out.println("  iter lastBottom=" + lastBottom);
        }
        if (sort == Sort.RELEVANCE) {
          paged = searcher.searchAfter(lastBottom, query, filter, pageSize, sort, true, doMaxScore);
        } else {
          paged = searcher.searchAfter(lastBottom, query, filter, pageSize, sort, doScores, doMaxScore);
        }
      }
      if (VERBOSE) {
        System.out.println("    " + paged.scoreDocs.length + " hits on page");
      }

      if (paged.scoreDocs.length == 0) {
        break;
      }
      assertPage(pageStart, all, paged);
      pageStart += paged.scoreDocs.length;
      lastBottom = paged.scoreDocs[paged.scoreDocs.length - 1];
    }
    assertEquals(all.scoreDocs.length, pageStart);
  }

  void assertPage(int pageStart, TopDocs all, TopDocs paged) throws IOException {
    assertEquals(all.totalHits, paged.totalHits);
    for (int i = 0; i < paged.scoreDocs.length; i++) {
      ScoreDoc sd1 = all.scoreDocs[pageStart + i];
      ScoreDoc sd2 = paged.scoreDocs[i];
      if (VERBOSE) {
        System.out.println("    hit " + (pageStart + i));
        System.out.println("      expected id=" + searcher.doc(sd1.doc).get("id") + " " + sd1);
        System.out.println("        actual id=" + searcher.doc(sd2.doc).get("id") + " " + sd2);
      }
      assertEquals(sd1.doc, sd2.doc);
      assertEquals(sd1.score, sd2.score, 0f);
      if (sd1 instanceof FieldDoc) {
        assertTrue(sd2 instanceof FieldDoc);
        assertEquals(((FieldDoc) sd1).fields, ((FieldDoc) sd2).fields);
      }
    }
  }
}
