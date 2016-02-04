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
package org.apache.lucene.queries.function;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.queries.function.valuesource.SumFloatFunction;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Test that functionquery's getSortField() actually works */
public class TestFunctionQuerySort extends LuceneTestCase {
  
  public void testOptimizedFieldSourceFunctionSorting() throws IOException {
    // index contents don't matter for this test.
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    final boolean reverse = random().nextBoolean();
    ValueSource vs;
    SortField sf, vssf;

    vs = new IntFieldSource("int_field");
    sf = new SortField("int_field", Type.INT, reverse);
    vssf = vs.getSortField(reverse);
    assertEquals(sf, vssf);
    sf = sf.rewrite(searcher);
    vssf = vssf.rewrite(searcher);
    assertEquals(sf, vssf);
    
    vs = new FloatFieldSource("float_field");
    sf = new SortField("float_field", Type.FLOAT, reverse);
    vssf = vs.getSortField(reverse);
    assertEquals(sf, vssf);
    sf = sf.rewrite(searcher);
    vssf = vssf.rewrite(searcher);
    assertEquals(sf, vssf);
    
    vs = new DoubleFieldSource("double_field");
    sf = new SortField("double_field", Type.DOUBLE, reverse);
    vssf = vs.getSortField(reverse);
    assertEquals(sf, vssf);
    sf = sf.rewrite(searcher);
    vssf = vssf.rewrite(searcher);
    assertEquals(sf, vssf);
    
    vs = new LongFieldSource("long_field");
    sf = new SortField("long_field", Type.LONG, reverse);
    vssf = vs.getSortField(reverse);
    assertEquals(sf, vssf);
    sf = sf.rewrite(searcher);
    vssf = vssf.rewrite(searcher);
    assertEquals(sf, vssf);
     
    reader.close();
    dir.close();
  }
  
  public void testSearchAfterWhenSortingByFunctionValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    iwc.setMergePolicy(newLogMergePolicy()); // depends on docid order
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    Document doc = new Document();
    Field field = new IntField("value", 0, Field.Store.YES);
    Field dvField = new NumericDocValuesField("value", 0);
    doc.add(field);
    doc.add(dvField);

    // Save docs unsorted (decreasing value n, n-1, ...)
    final int NUM_VALS = 5;
    for (int val = NUM_VALS; val > 0; val--) {
      field.setIntValue(val);
      dvField.setLongValue(val);
      writer.addDocument(doc);
    }

    // Open index
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    // Trivial ValueSource function that bypasses single field ValueSource sort optimization
    ValueSource src = new SumFloatFunction(new ValueSource[] { new IntFieldSource("value"),
                                                               new DoubleConstValueSource(1.0D) });
    // ...and make it a sort criterion
    SortField sf = src.getSortField(false).rewrite(searcher);
    Sort orderBy = new Sort(sf);

    // Get hits sorted by our FunctionValues (ascending values)
    Query q = new MatchAllDocsQuery();
    TopDocs hits = searcher.search(q, reader.maxDoc(), orderBy);
    assertEquals(NUM_VALS, hits.scoreDocs.length);
    // Verify that sorting works in general
    int i = 0;
    for (ScoreDoc hit : hits.scoreDocs) {
      int valueFromDoc = Integer.parseInt(reader.document(hit.doc).get("value"));
      assertEquals(++i, valueFromDoc);
    }

    // Now get hits after hit #2 using IS.searchAfter()
    int afterIdx = 1;
    FieldDoc afterHit = (FieldDoc) hits.scoreDocs[afterIdx];
    hits = searcher.searchAfter(afterHit, q, reader.maxDoc(), orderBy);

    // Expected # of hits: NUM_VALS - 2
    assertEquals(NUM_VALS - (afterIdx + 1), hits.scoreDocs.length);

    // Verify that hits are actually "after"
    int afterValue = ((Double) afterHit.fields[0]).intValue();
    for (ScoreDoc hit : hits.scoreDocs) {
      int val = Integer.parseInt(reader.document(hit.doc).get("value"));
      assertTrue(afterValue <= val);
      assertFalse(hit.doc == afterHit.doc);
    }
    reader.close();
    dir.close();
  }
}
