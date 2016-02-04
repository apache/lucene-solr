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


import java.util.HashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestExpressionValueSource extends LuceneTestCase {
  DirectoryReader reader;
  Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
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
    iw.forceMerge(1);
    
    reader = iw.getReader();
    iw.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  public void testTypes() throws Exception {
    Expression expr = JavascriptCompiler.compile("2*popularity");
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("popularity", SortField.Type.LONG));
    ValueSource vs = expr.getValueSource(bindings);
    
    assertEquals(1, reader.leaves().size());
    LeafReaderContext leaf = reader.leaves().get(0);
    FunctionValues values = vs.getValues(new HashMap<String,Object>(), leaf);
    
    assertEquals(10, values.doubleVal(0), 0);
    assertEquals(10, values.floatVal(0), 0);
    assertEquals(10, values.longVal(0));
    assertEquals(10, values.intVal(0));
    assertEquals(10, values.shortVal(0));
    assertEquals(10, values.byteVal(0));
    assertEquals("10.0", values.strVal(0));
    assertEquals(new Double(10), values.objectVal(0));
    
    assertEquals(40, values.doubleVal(1), 0);
    assertEquals(40, values.floatVal(1), 0);
    assertEquals(40, values.longVal(1));
    assertEquals(40, values.intVal(1));
    assertEquals(40, values.shortVal(1));
    assertEquals(40, values.byteVal(1));
    assertEquals("40.0", values.strVal(1));
    assertEquals(new Double(40), values.objectVal(1));
    
    assertEquals(4, values.doubleVal(2), 0);
    assertEquals(4, values.floatVal(2), 0);
    assertEquals(4, values.longVal(2));
    assertEquals(4, values.intVal(2));
    assertEquals(4, values.shortVal(2));
    assertEquals(4, values.byteVal(2));
    assertEquals("4.0", values.strVal(2));
    assertEquals(new Double(4), values.objectVal(2));    
  }
  
  public void testRangeScorer() throws Exception {
    Expression expr = JavascriptCompiler.compile("2*popularity");
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("popularity", SortField.Type.LONG));
    ValueSource vs = expr.getValueSource(bindings);
    
    assertEquals(1, reader.leaves().size());
    LeafReaderContext leaf = reader.leaves().get(0);
    FunctionValues values = vs.getValues(new HashMap<String,Object>(), leaf);
    
    // everything
    ValueSourceScorer scorer = values.getRangeScorer(leaf.reader(), "4", "40", true, true);
    DocIdSetIterator iter = scorer.iterator();
    assertEquals(-1, iter.docID());
    assertEquals(0, iter.nextDoc());
    assertEquals(1, iter.nextDoc());
    assertEquals(2, iter.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.nextDoc());

    // just the first doc
    scorer = values.getRangeScorer(leaf.reader(), "4", "40", false, false);
    iter = scorer.iterator();
    assertEquals(-1, scorer.docID());
    assertEquals(0, iter.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.nextDoc());
  }
  
  public void testEquals() throws Exception {
    Expression expr = JavascriptCompiler.compile("sqrt(a) + ln(b)");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("a", SortField.Type.INT));
    bindings.add(new SortField("b", SortField.Type.INT));
    
    ValueSource vs1 = expr.getValueSource(bindings);
    // same instance
    assertEquals(vs1, vs1);
    // null
    assertFalse(vs1.equals(null));
    // other object
    assertFalse(vs1.equals("foobar"));
    // same bindings and expression instances
    ValueSource vs2 = expr.getValueSource(bindings);
    assertEquals(vs1.hashCode(), vs2.hashCode());
    assertEquals(vs1, vs2);
    // equiv bindings (different instance)
    SimpleBindings bindings2 = new SimpleBindings();    
    bindings2.add(new SortField("a", SortField.Type.INT));
    bindings2.add(new SortField("b", SortField.Type.INT));
    ValueSource vs3 = expr.getValueSource(bindings2);
    assertEquals(vs1, vs3);
    // different bindings (same names, different types)
    SimpleBindings bindings3 = new SimpleBindings();    
    bindings3.add(new SortField("a", SortField.Type.LONG));
    bindings3.add(new SortField("b", SortField.Type.INT));
    ValueSource vs4 = expr.getValueSource(bindings3);
    assertFalse(vs1.equals(vs4));
  }
}
