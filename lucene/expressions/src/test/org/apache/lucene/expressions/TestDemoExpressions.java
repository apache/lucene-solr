package org.apache.lucene.expressions;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

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

/** simple demo of using expressions */
public class  TestDemoExpressions extends LuceneTestCase {
  IndexSearcher searcher;
  DirectoryReader reader;
  Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    doc.add(newTextField("body", "some contents and more contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 5));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(newStringField("id", "2", Field.Store.YES));
    doc.add(newTextField("body", "another document with different contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 20));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    doc.add(newTextField("body", "crappy contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 2));
    iw.addDocument(doc);
    
    reader = iw.getReader();
    searcher = new IndexSearcher(reader);
    iw.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  /** an example of how to rank by an expression */
  public void test() throws Exception {
    // compile an expression:
    Expression expr = JavascriptCompiler.compile("sqrt(_score) + ln(popularity)");
    
    // we use SimpleBindings: which just maps variables to SortField instances
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    bindings.add(new SortField("popularity", SortField.Type.INT));
    
    // create a sort field and sort by it (reverse order)
    Sort sort = new Sort(expr.getSortField(bindings, true));
    Query query = new TermQuery(new Term("body", "contents"));
    searcher.search(query, null, 3, sort);
  }
  
  /** tests the returned sort values are correct */
  public void testSortValues() throws Exception {
    Expression expr = JavascriptCompiler.compile("sqrt(_score)");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    
    Sort sort = new Sort(expr.getSortField(bindings, true));
    Query query = new TermQuery(new Term("body", "contents"));
    TopFieldDocs td = searcher.search(query, null, 3, sort, true, true);
    for (int i = 0; i < 3; i++) {
      FieldDoc d = (FieldDoc) td.scoreDocs[i];
      float expected = (float) Math.sqrt(d.score);
      float actual = ((Double)d.fields[0]).floatValue();
      assertEquals(expected, actual, CheckHits.explainToleranceDelta(expected, actual));
    }
  }
  
  /** tests huge amounts of variables in the expression */
  public void testLotsOfBindings() throws Exception {
    doTestLotsOfBindings(Byte.MAX_VALUE-1);
    doTestLotsOfBindings(Byte.MAX_VALUE);
    doTestLotsOfBindings(Byte.MAX_VALUE+1);
    // TODO: ideally we'd test > Short.MAX_VALUE too, but compilation is currently recursive.
    // so if we want to test such huge expressions, we need to instead change parser to use an explicit Stack
  }
  
  private void doTestLotsOfBindings(int n) throws Exception {
    SimpleBindings bindings = new SimpleBindings();    
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append("+");
      }
      sb.append("x" + i);
      bindings.add(new SortField("x" + i, SortField.Type.SCORE));
    }
    
    Expression expr = JavascriptCompiler.compile(sb.toString());
    Sort sort = new Sort(expr.getSortField(bindings, true));
    Query query = new TermQuery(new Term("body", "contents"));
    TopFieldDocs td = searcher.search(query, null, 3, sort, true, true);
    for (int i = 0; i < 3; i++) {
      FieldDoc d = (FieldDoc) td.scoreDocs[i];
      float expected = n*d.score;
      float actual = ((Double)d.fields[0]).floatValue();
      assertEquals(expected, actual, CheckHits.explainToleranceDelta(expected, actual));
    }
  }
}
