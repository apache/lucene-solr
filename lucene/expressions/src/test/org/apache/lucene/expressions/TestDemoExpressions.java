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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.expressions.js.VariableContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.expressions.js.VariableContext.Type.MEMBER;
import static org.apache.lucene.expressions.js.VariableContext.Type.STR_INDEX;
import static org.apache.lucene.expressions.js.VariableContext.Type.INT_INDEX;


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
    doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.759011)));
    doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits(-73.9844722)));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(newStringField("id", "2", Field.Store.YES));
    doc.add(newTextField("body", "another document with different contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 20));
    doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.718266)));
    doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits(-74.007819)));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    doc.add(newTextField("body", "crappy contents", Field.Store.NO));
    doc.add(new NumericDocValuesField("popularity", 2));
    doc.add(new NumericDocValuesField("latitude", Double.doubleToRawLongBits(40.7051157)));
    doc.add(new NumericDocValuesField("longitude", Double.doubleToRawLongBits(-74.0088305)));
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
    searcher.search(query, 3, sort);
  }
  
  /** tests the returned sort values are correct */
  public void testSortValues() throws Exception {
    Expression expr = JavascriptCompiler.compile("sqrt(_score)");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    
    Sort sort = new Sort(expr.getSortField(bindings, true));
    Query query = new TermQuery(new Term("body", "contents"));
    TopFieldDocs td = searcher.search(query, 3, sort, true, true);
    for (int i = 0; i < 3; i++) {
      FieldDoc d = (FieldDoc) td.scoreDocs[i];
      float expected = (float) Math.sqrt(d.score);
      float actual = ((Double)d.fields[0]).floatValue();
      assertEquals(expected, actual, CheckHits.explainToleranceDelta(expected, actual));
    }
  }
  
  /** tests same binding used more than once in an expression */
  public void testTwoOfSameBinding() throws Exception {
    Expression expr = JavascriptCompiler.compile("_score + _score");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    
    Sort sort = new Sort(expr.getSortField(bindings, true));
    Query query = new TermQuery(new Term("body", "contents"));
    TopFieldDocs td = searcher.search(query, 3, sort, true, true);
    for (int i = 0; i < 3; i++) {
      FieldDoc d = (FieldDoc) td.scoreDocs[i];
      float expected = 2*d.score;
      float actual = ((Double)d.fields[0]).floatValue();
      assertEquals(expected, actual, CheckHits.explainToleranceDelta(expected, actual));
    }
  }
  
  /** Uses variables with $ */
  public void testDollarVariable() throws Exception {
    Expression expr = JavascriptCompiler.compile("$0+$score");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("$0", SortField.Type.SCORE));
    bindings.add(new SortField("$score", SortField.Type.SCORE));
    
    Sort sort = new Sort(expr.getSortField(bindings, true));
    Query query = new TermQuery(new Term("body", "contents"));
    TopFieldDocs td = searcher.search(query, 3, sort, true, true);
    for (int i = 0; i < 3; i++) {
      FieldDoc d = (FieldDoc) td.scoreDocs[i];
      float expected = 2*d.score;
      float actual = ((Double)d.fields[0]).floatValue();
      assertEquals(expected, actual, CheckHits.explainToleranceDelta(expected, actual));
    }
  }
  
  /** tests expression referring to another expression */
  public void testExpressionRefersToExpression() throws Exception {
    Expression expr1 = JavascriptCompiler.compile("_score");
    Expression expr2 = JavascriptCompiler.compile("2*expr1");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    bindings.add("expr1", expr1);
    
    Sort sort = new Sort(expr2.getSortField(bindings, true));
    Query query = new TermQuery(new Term("body", "contents"));
    TopFieldDocs td = searcher.search(query, 3, sort, true, true);
    for (int i = 0; i < 3; i++) {
      FieldDoc d = (FieldDoc) td.scoreDocs[i];
      float expected = 2*d.score;
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
    TopFieldDocs td = searcher.search(query, 3, sort, true, true);
    for (int i = 0; i < 3; i++) {
      FieldDoc d = (FieldDoc) td.scoreDocs[i];
      float expected = n*d.score;
      float actual = ((Double)d.fields[0]).floatValue();
      assertEquals(expected, actual, CheckHits.explainToleranceDelta(expected, actual));
    }
  }
  
  public void testDistanceSort() throws Exception {
    Expression distance = JavascriptCompiler.compile("haversin(40.7143528,-74.0059731,latitude,longitude)");
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("latitude", SortField.Type.DOUBLE));
    bindings.add(new SortField("longitude", SortField.Type.DOUBLE));
    Sort sort = new Sort(distance.getSortField(bindings, false));
    TopFieldDocs td = searcher.search(new MatchAllDocsQuery(), 3, sort);
    
    FieldDoc d = (FieldDoc) td.scoreDocs[0];
    assertEquals(0.4619D, (Double)d.fields[0], 1E-4);
    
    d = (FieldDoc) td.scoreDocs[1];
    assertEquals(1.0546D, (Double)d.fields[0], 1E-4);
    
    d = (FieldDoc) td.scoreDocs[2];
    assertEquals(5.2842D, (Double)d.fields[0], 1E-4);
  }

  public void testStaticExtendedVariableExample() throws Exception {
    Expression popularity = JavascriptCompiler.compile("doc[\"popularity\"].value");
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("doc['popularity'].value", new IntFieldSource("popularity"));
    Sort sort = new Sort(popularity.getSortField(bindings, true));
    TopFieldDocs td = searcher.search(new MatchAllDocsQuery(), 3, sort);

    FieldDoc d = (FieldDoc)td.scoreDocs[0];
    assertEquals(20D, (Double)d.fields[0], 1E-4);

    d = (FieldDoc)td.scoreDocs[1];
    assertEquals(5D, (Double)d.fields[0], 1E-4);

    d = (FieldDoc)td.scoreDocs[2];
    assertEquals(2D, (Double)d.fields[0], 1E-4);
  }

  public void testDynamicExtendedVariableExample() throws Exception {
    Expression popularity = JavascriptCompiler.compile("doc['popularity'].value + magicarray[0] + fourtytwo");

    // The following is an example of how to write bindings which parse the variable name into pieces.
    // Note, however, that this requires a lot of error checking.  Each "error case" below should be
    // filled in with proper error messages for a real use case.
    Bindings bindings = new Bindings() {
      @Override
      public ValueSource getValueSource(String name) {
        VariableContext[] var = VariableContext.parse(name);
        assert var[0].type == MEMBER;
        String base = var[0].text;
        if (base.equals("doc")) {
          if (var.length > 1 && var[1].type == STR_INDEX) {
            String field = var[1].text;
            if (var.length > 2 && var[2].type == MEMBER && var[2].text.equals("value")) {
              return new IntFieldSource(field);
            } else {
              fail("member: " + var[2].text);// error case, non/missing "value" member access
            }
          } else {
            fail();// error case, doc should be a str indexed array
          }
        } else if (base.equals("magicarray")) {
          if (var.length > 1 && var[1].type == INT_INDEX) {
            return new DoubleConstValueSource(2048);
          } else {
            fail();// error case, magic array isn't an array
          }
        } else if (base.equals("fourtytwo")) {
          return new DoubleConstValueSource(42);
        } else {
          fail();// error case (variable doesn't exist)
        }
        throw new IllegalArgumentException("Illegal reference '" + name + "'");
      }
    };
    Sort sort = new Sort(popularity.getSortField(bindings, false));
    TopFieldDocs td = searcher.search(new MatchAllDocsQuery(), 3, sort);

    FieldDoc d = (FieldDoc)td.scoreDocs[0];
    assertEquals(2092D, (Double)d.fields[0], 1E-4);

    d = (FieldDoc)td.scoreDocs[1];
    assertEquals(2095D, (Double)d.fields[0], 1E-4);

    d = (FieldDoc)td.scoreDocs[2];
    assertEquals(2110D, (Double)d.fields[0], 1E-4);
  }
}
