package org.apache.lucene.expressions;

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

import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/**
 * Tests some basic expressions against different queries,
 * and fieldcache/docvalues fields against an equivalent sort.
 */
@SuppressCodecs("Lucene3x")
public class TestExpressionSorts extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = TestUtil.nextInt(random(), 2049, 4000);
    for (int i = 0; i < numDocs; i++) {
      Document document = new Document();
      document.add(newTextField("english", English.intToEnglish(i), Field.Store.NO));
      document.add(newTextField("oddeven", (i % 2 == 0) ? "even" : "odd", Field.Store.NO));
      document.add(newStringField("byte", "" + ((byte) random().nextInt()), Field.Store.NO));
      document.add(newStringField("short", "" + ((short) random().nextInt()), Field.Store.NO));
      document.add(new IntField("int", random().nextInt(), Field.Store.NO));
      document.add(new LongField("long", random().nextLong(), Field.Store.NO));

      document.add(new FloatField("float", random().nextFloat(), Field.Store.NO));
      document.add(new DoubleField("double", random().nextDouble(), Field.Store.NO));

      document.add(new NumericDocValuesField("intdocvalues", random().nextInt()));
      document.add(new FloatDocValuesField("floatdocvalues", random().nextFloat()));
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
    int n = atLeast(4);
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
      // force in order
      bq.add(new TermQuery(new Term("english", "two")), BooleanClause.Occur.SHOULD);
      bq.setMinimumNumberShouldMatch(2);
      assertQuery(bq, null);
    }
  }
  
  void assertQuery(Query query, Filter filter) throws Exception {
    for (int i = 0; i < 10; i++) {
      boolean reversed = random().nextBoolean();
      SortField fields[] = new SortField[] {
          new SortField("int", SortField.Type.INT, reversed),
          new SortField("long", SortField.Type.LONG, reversed),
          new SortField("float", SortField.Type.FLOAT, reversed),
          new SortField("double", SortField.Type.DOUBLE, reversed),
          new SortField("intdocvalues", SortField.Type.INT, reversed),
          new SortField("floatdocvalues", SortField.Type.FLOAT, reversed),
          new SortField("score", SortField.Type.SCORE)
      };
      Collections.shuffle(Arrays.asList(fields), random());
      int numSorts = TestUtil.nextInt(random(), 1, fields.length);
      assertQuery(query, filter, new Sort(Arrays.copyOfRange(fields, 0, numSorts)));
    }
  }

  void assertQuery(Query query, Filter filter, Sort sort) throws Exception {
    int size = TestUtil.nextInt(random(), 1, searcher.getIndexReader().maxDoc() / 5);
    TopDocs expected = searcher.search(query, filter, size, sort, random().nextBoolean(), random().nextBoolean());
    
    // make our actual sort, mutating original by replacing some of the 
    // sortfields with equivalent expressions
    
    SortField original[] = sort.getSort();
    SortField mutated[] = new SortField[original.length];
    for (int i = 0; i < mutated.length; i++) {
      if (random().nextInt(3) > 0) {
        SortField s = original[i];
        Expression expr = JavascriptCompiler.compile(s.getField());
        SimpleBindings simpleBindings = new SimpleBindings();
        simpleBindings.add(s);
        boolean reverse = s.getType() == SortField.Type.SCORE || s.getReverse();
        mutated[i] = expr.getSortField(simpleBindings, reverse);
      } else {
        mutated[i] = original[i];
      }
    }
    
    Sort mutatedSort = new Sort(mutated);
    TopDocs actual = searcher.search(query, filter, size, mutatedSort, random().nextBoolean(), random().nextBoolean());
    CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);
    
    if (size < actual.totalHits) {
      expected = searcher.searchAfter(expected.scoreDocs[size-1], query, filter, size, sort);
      actual = searcher.searchAfter(actual.scoreDocs[size-1], query, filter, size, mutatedSort);
      CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);
    }
  }
}

