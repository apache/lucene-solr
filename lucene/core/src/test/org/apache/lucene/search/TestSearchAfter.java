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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
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
          new SortField("int", SortField.Type.INT, true),
          new SortField("long", SortField.Type.LONG, true),
          new SortField("float", SortField.Type.FLOAT, true),
          new SortField("double", SortField.Type.DOUBLE, true),
          new SortField("bytes", SortField.Type.STRING, true),
          new SortField("bytesval", SortField.Type.STRING_VAL, true),
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
    Random r = random();
    for (int i = 0; i < numDocs; i++) {
      List<Field> fields = new ArrayList<>();
      fields.add(newTextField("english", English.intToEnglish(i), Field.Store.NO));
      fields.add(newTextField("oddeven", (i % 2 == 0) ? "even" : "odd", Field.Store.NO));
      fields.add(new NumericDocValuesField("byte", (byte) r.nextInt()));
      fields.add(new NumericDocValuesField("short", (short) r.nextInt()));
      fields.add(new NumericDocValuesField("int", r.nextInt()));
      fields.add(new NumericDocValuesField("long", r.nextLong()));
      fields.add(new FloatDocValuesField("float", r.nextFloat()));
      fields.add(new DoubleDocValuesField("double", r.nextDouble()));
      fields.add(new SortedDocValuesField("bytes", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
      fields.add(new BinaryDocValuesField("bytesval", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));

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
      assertQuery(new MatchAllDocsQuery(), null);
      assertQuery(new TermQuery(new Term("english", "one")), null);
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(new TermQuery(new Term("english", "one")), BooleanClause.Occur.SHOULD);
      bq.add(new TermQuery(new Term("oddeven", "even")), BooleanClause.Occur.SHOULD);
      assertQuery(bq.build(), null);
    }
  }

  void assertQuery(Query query) throws Exception {
    assertQuery(query, null);
    assertQuery(query, Sort.RELEVANCE);
    assertQuery(query, Sort.INDEXORDER);
    for(SortField sortField : allSortFields) {
      assertQuery(query, new Sort(new SortField[] {sortField}));
    }
    for(int i=0;i<20;i++) {
      assertQuery(query, getRandomSort());
    }
  }

  Sort getRandomSort() {
    SortField[] sortFields = new SortField[TestUtil.nextInt(random(), 2, 7)];
    for(int i=0;i<sortFields.length;i++) {
      sortFields[i] = allSortFields.get(random().nextInt(allSortFields.size()));
    }
    return new Sort(sortFields);
  }

  void assertQuery(Query query, Sort sort) throws Exception {
    int maxDoc = searcher.getIndexReader().maxDoc();
    TopDocs all;
    int pageSize = TestUtil.nextInt(random(), 1, maxDoc * 2);
    if (VERBOSE) {
      System.out.println("\nassertQuery " + (iter++) + ": query=" + query + " sort=" + sort + " pageSize=" + pageSize);
    }
    final boolean doScores;
    final TopDocsCollector<?> allCollector;
    if (sort == null) {
      allCollector = TopScoreDocCollector.create(maxDoc, null, Integer.MAX_VALUE);
      doScores = false;
    } else if (sort == Sort.RELEVANCE) {
      allCollector = TopFieldCollector.create(sort, maxDoc, Integer.MAX_VALUE);
      doScores = true;
    } else {
      allCollector = TopFieldCollector.create(sort, maxDoc, Integer.MAX_VALUE);
      doScores = random().nextBoolean();
    }
    searcher.search(query, allCollector);
    all = allCollector.topDocs();
    if (doScores) {
      TopFieldCollector.populateScores(all.scoreDocs, searcher, query);
    }

    if (VERBOSE) {
      System.out.println("  all.totalHits.value=" + all.totalHits.value);
      int upto = 0;
      for(ScoreDoc scoreDoc : all.scoreDocs) {
        System.out.println("    hit " + (upto++) + ": id=" + searcher.doc(scoreDoc.doc).get("id") + " " + scoreDoc);
      }
    }
    int pageStart = 0;
    ScoreDoc lastBottom = null;
    while (pageStart < all.totalHits.value) {
      TopDocs paged;
      final TopDocsCollector<?> pagedCollector;
      if (sort == null) {
        if (VERBOSE) {
          System.out.println("  iter lastBottom=" + lastBottom);
        }
        pagedCollector = TopScoreDocCollector.create(pageSize, lastBottom, Integer.MAX_VALUE);
      } else {
        if (VERBOSE) {
          System.out.println("  iter lastBottom=" + lastBottom);
        }
        if (sort == Sort.RELEVANCE) {
          pagedCollector = TopFieldCollector.create(sort, pageSize, (FieldDoc) lastBottom, Integer.MAX_VALUE);
        } else {
          pagedCollector = TopFieldCollector.create(sort, pageSize, (FieldDoc) lastBottom, Integer.MAX_VALUE);
        }
      }
      searcher.search(query, pagedCollector);
      paged = pagedCollector.topDocs();
      if (doScores) {
        TopFieldCollector.populateScores(paged.scoreDocs, searcher, query);
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
    assertEquals(all.totalHits.value, paged.totalHits.value);
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
