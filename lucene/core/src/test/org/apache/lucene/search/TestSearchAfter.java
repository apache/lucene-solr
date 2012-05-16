package org.apache.lucene.search;

/**
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

import java.util.Arrays;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntDocValuesField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedBytesDocValuesField;
import org.apache.lucene.document.StraightBytesDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Tests IndexSearcher's searchAfter() method
 */

public class TestSearchAfter extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
   
  boolean supportsDocValues = Codec.getDefault().getName().equals("Lucene3x") == false;

  private static SortField useDocValues(SortField field) {
    field.setUseIndexValues(true);
    return field;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(200);
    for (int i = 0; i < numDocs; i++) {
      Document document = new Document();
      document.add(newField("english", English.intToEnglish(i), TextField.TYPE_UNSTORED));
      document.add(newField("oddeven", (i % 2 == 0) ? "even" : "odd", TextField.TYPE_UNSTORED));
      document.add(newField("byte", "" + ((byte) random().nextInt()), StringField.TYPE_UNSTORED));
      document.add(newField("short", "" + ((short) random().nextInt()), StringField.TYPE_UNSTORED));
      document.add(new IntField("int", random().nextInt()));
      document.add(new LongField("long", random().nextLong()));

      document.add(new FloatField("float", random().nextFloat()));
      document.add(new DoubleField("double", random().nextDouble()));
      document.add(newField("bytes", _TestUtil.randomRealisticUnicodeString(random()), StringField.TYPE_UNSTORED));
      document.add(newField("bytesval", _TestUtil.randomRealisticUnicodeString(random()), StringField.TYPE_UNSTORED));
      document.add(new DoubleField("double", random().nextDouble()));

      if (supportsDocValues) {
        document.add(new IntDocValuesField("intdocvalues", random().nextInt()));
        document.add(new FloatDocValuesField("floatdocvalues", random().nextFloat()));
        document.add(new SortedBytesDocValuesField("sortedbytesdocvalues", new BytesRef(_TestUtil.randomRealisticUnicodeString(random()))));
        document.add(new SortedBytesDocValuesField("sortedbytesdocvaluesval", new BytesRef(_TestUtil.randomRealisticUnicodeString(random()))));
        document.add(new StraightBytesDocValuesField("straightbytesdocvalues", new BytesRef(_TestUtil.randomRealisticUnicodeString(random()))));
      }

      iw.addDocument(document);
    }
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
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
    for(int rev=0;rev<2;rev++) {
      boolean reversed = rev == 1;
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("byte", SortField.Type.BYTE, reversed)}));
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("short", SortField.Type.SHORT, reversed)}));
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("int", SortField.Type.INT, reversed)}));
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("long", SortField.Type.LONG, reversed)}));
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("float", SortField.Type.FLOAT, reversed)}));
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("double", SortField.Type.DOUBLE, reversed)}));
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("bytes", SortField.Type.STRING, reversed)}));
      assertQuery(query, filter, new Sort(new SortField[] {new SortField("bytesval", SortField.Type.STRING_VAL, reversed)}));
      if (supportsDocValues) {
        assertQuery(query, filter, new Sort(new SortField[] {useDocValues(new SortField("intdocvalues", SortField.Type.INT, reversed))}));
        assertQuery(query, filter, new Sort(new SortField[] {useDocValues(new SortField("floatdocvalues", SortField.Type.FLOAT, reversed))}));
        assertQuery(query, filter, new Sort(new SortField[] {useDocValues(new SortField("sortedbytesdocvalues", SortField.Type.STRING, reversed))}));
        assertQuery(query, filter, new Sort(new SortField[] {useDocValues(new SortField("sortedbytesdocvaluesval", SortField.Type.STRING_VAL, reversed))}));
        assertQuery(query, filter, new Sort(new SortField[] {useDocValues(new SortField("straightbytesdocvalues", SortField.Type.STRING_VAL, reversed))}));
      }
    }
  }

  void assertQuery(Query query, Filter filter, Sort sort) throws Exception {
    int maxDoc = searcher.getIndexReader().maxDoc();
    TopDocs all;
    int pageSize = _TestUtil.nextInt(random(), 1, maxDoc*2);
    if (VERBOSE) {
      System.out.println("\nassertQuery: query=" + query + " filter=" + filter + " sort=" + sort + " pageSize=" + pageSize);
    }
    final boolean doMaxScore = random().nextBoolean();
    if (sort == null) {
      all = searcher.search(query, filter, maxDoc);
    } else if (sort == Sort.RELEVANCE) {
      all = searcher.search(query, filter, maxDoc, sort, true, doMaxScore);
    } else {
      all = searcher.search(query, filter, maxDoc, sort);
    }
    if (VERBOSE) {
      System.out.println("  all.totalHits=" + all.totalHits);
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
          System.out.println("  iter lastBottom=" + lastBottom + (lastBottom == null ? "" : " fields=" + Arrays.toString(((FieldDoc) lastBottom).fields)));
        }
        if (sort == Sort.RELEVANCE) {
          paged = searcher.searchAfter(lastBottom, query, filter, pageSize, sort, true, doMaxScore);
        } else {
          paged = searcher.searchAfter(lastBottom, query, filter, pageSize, sort);
        }
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

  static void assertPage(int pageStart, TopDocs all, TopDocs paged) {
    assertEquals(all.totalHits, paged.totalHits);
    for (int i = 0; i < paged.scoreDocs.length; i++) {
      ScoreDoc sd1 = all.scoreDocs[pageStart + i];
      ScoreDoc sd2 = paged.scoreDocs[i];
      assertEquals(sd1.doc, sd2.doc);
      assertEquals(sd1.score, sd2.score, 0f);
      if (sd1 instanceof FieldDoc) {
        assertTrue(sd2 instanceof FieldDoc);
        assertEquals(((FieldDoc) sd1).fields, ((FieldDoc) sd2).fields);
      }
    }
  }
}
