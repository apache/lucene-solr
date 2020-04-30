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
package org.apache.lucene.expressions;


import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests some basic expressions against different queries,
 * and fieldcache/docvalues fields against an equivalent sort.
 */
public class TestExpressionSorts extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      Document document = new Document();
      document.add(newTextField("english", English.intToEnglish(i), Field.Store.NO));
      document.add(newTextField("oddeven", (i % 2 == 0) ? "even" : "odd", Field.Store.NO));
      document.add(new NumericDocValuesField("int", random().nextInt()));
      document.add(new NumericDocValuesField("long", random().nextLong()));
      document.add(new FloatDocValuesField("float", random().nextFloat()));
      document.add(new DoubleDocValuesField("double", random().nextDouble()));
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
    int n = atLeast(1);
    for (int i = 0; i < n; i++) {
      assertQuery(new MatchAllDocsQuery());
      assertQuery(new TermQuery(new Term("english", "one")));
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(new TermQuery(new Term("english", "one")), BooleanClause.Occur.SHOULD);
      bq.add(new TermQuery(new Term("oddeven", "even")), BooleanClause.Occur.SHOULD);
      assertQuery(bq.build());
      // force in order
      bq.add(new TermQuery(new Term("english", "two")), BooleanClause.Occur.SHOULD);
      bq.setMinimumNumberShouldMatch(2);
      assertQuery(bq.build());
    }
  }
  
  void assertQuery(Query query) throws Exception {
    for (int i = 0; i < 10; i++) {
      boolean reversed = random().nextBoolean();
      SortField fields[] = new SortField[] {
          new SortField("int", SortField.Type.INT, reversed),
          new SortField("long", SortField.Type.LONG, reversed),
          new SortField("float", SortField.Type.FLOAT, reversed),
          new SortField("double", SortField.Type.DOUBLE, reversed),
          new SortField("score", SortField.Type.SCORE)
      };
      Collections.shuffle(Arrays.asList(fields), random());
      int numSorts = TestUtil.nextInt(random(), 1, fields.length);
      assertQuery(query, new Sort(ArrayUtil.copyOfSubArray(fields, 0, numSorts)));
    }
  }

  void assertQuery(Query query, Sort sort) throws Exception {
    int size = TestUtil.nextInt(random(), 1, searcher.getIndexReader().maxDoc() / 5);
    TopDocs expected = searcher.search(query, size, sort, random().nextBoolean());
    
    // make our actual sort, mutating original by replacing some of the 
    // sortfields with equivalent expressions
    
    SortField original[] = sort.getSort();
    SortField mutated[] = new SortField[original.length];
    for (int i = 0; i < mutated.length; i++) {
      if (random().nextInt(3) > 0) {
        SortField s = original[i];
        Expression expr = JavascriptCompiler.compile(s.getField());
        SimpleBindings simpleBindings = new SimpleBindings();
        simpleBindings.add(s.getField(), fromSortField(s));
        boolean reverse = s.getType() == SortField.Type.SCORE || s.getReverse();
        mutated[i] = expr.getSortField(simpleBindings, reverse);
      } else {
        mutated[i] = original[i];
      }
    }
    
    Sort mutatedSort = new Sort(mutated);
    TopDocs actual = searcher.search(query, size, mutatedSort, random().nextBoolean());
    CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);
    
    if (size < actual.totalHits.value) {
      expected = searcher.searchAfter(expected.scoreDocs[size-1], query, size, sort);
      actual = searcher.searchAfter(actual.scoreDocs[size-1], query, size, mutatedSort);
      CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);
    }
  }

  private DoubleValuesSource fromSortField(SortField field) {
    switch(field.getType()) {
      case INT:
        return DoubleValuesSource.fromIntField(field.getField());
      case LONG:
        return DoubleValuesSource.fromLongField(field.getField());
      case FLOAT:
        return DoubleValuesSource.fromFloatField(field.getField());
      case DOUBLE:
        return DoubleValuesSource.fromDoubleField(field.getField());
      case SCORE:
        return DoubleValuesSource.SCORES;
      default:
        throw new UnsupportedOperationException();
    }
  }
}

