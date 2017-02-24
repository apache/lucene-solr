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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
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
    doc.add(new NumericDocValuesField("count", 1));
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

  public void testDoubleValuesSourceTypes() throws Exception {
    Expression expr = JavascriptCompiler.compile("2*popularity + count");
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("popularity", SortField.Type.LONG));
    bindings.add(new SortField("count", SortField.Type.LONG));
    DoubleValuesSource vs = expr.getDoubleValuesSource(bindings);

    assertEquals(1, reader.leaves().size());
    LeafReaderContext leaf = reader.leaves().get(0);
    DoubleValues values = vs.getValues(leaf, null);

    assertTrue(values.advanceExact(0));
    assertEquals(10, values.doubleValue(), 0);
    assertTrue(values.advanceExact(1));
    assertEquals(41, values.doubleValue(), 0);
    assertTrue(values.advanceExact(2));
    assertEquals(4, values.doubleValue(), 0);
  }

  public void testDoubleValuesSourceEquals() throws Exception {
    Expression expr = JavascriptCompiler.compile("sqrt(a) + ln(b)");

    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("a", SortField.Type.INT));
    bindings.add(new SortField("b", SortField.Type.INT));

    DoubleValuesSource vs1 = expr.getDoubleValuesSource(bindings);
    // same instance
    assertEquals(vs1, vs1);
    // null
    assertFalse(vs1.equals(null));
    // other object
    assertFalse(vs1.equals("foobar"));
    // same bindings and expression instances
    DoubleValuesSource vs2 = expr.getDoubleValuesSource(bindings);
    assertEquals(vs1.hashCode(), vs2.hashCode());
    assertEquals(vs1, vs2);
    // equiv bindings (different instance)
    SimpleBindings bindings2 = new SimpleBindings();
    bindings2.add(new SortField("a", SortField.Type.INT));
    bindings2.add(new SortField("b", SortField.Type.INT));
    DoubleValuesSource vs3 = expr.getDoubleValuesSource(bindings2);
    assertEquals(vs1, vs3);
    // different bindings (same names, different types)
    SimpleBindings bindings3 = new SimpleBindings();
    bindings3.add(new SortField("a", SortField.Type.LONG));
    bindings3.add(new SortField("b", SortField.Type.FLOAT));
    DoubleValuesSource vs4 = expr.getDoubleValuesSource(bindings3);
    assertFalse(vs1.equals(vs4));
  }

}
