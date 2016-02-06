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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestExpressionRescorer extends LuceneTestCase {
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
    // TODO: fix this test to not be so flaky and use newSearcher
    searcher.setSimilarity(new ClassicSimilarity());
    iw.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  public void testBasic() throws Exception {

    // create a sort field and sort by it (reverse order)
    Query query = new TermQuery(new Term("body", "contents"));
    IndexReader r = searcher.getIndexReader();

    // Just first pass query
    TopDocs hits = searcher.search(query, 10);
    assertEquals(3, hits.totalHits);
    assertEquals("3", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", r.document(hits.scoreDocs[1].doc).get("id"));
    assertEquals("2", r.document(hits.scoreDocs[2].doc).get("id"));

    // Now, rescore:

    Expression e = JavascriptCompiler.compile("sqrt(_score) + ln(popularity)");
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("popularity", SortField.Type.INT));
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    Rescorer rescorer = e.getRescorer(bindings);

    hits = rescorer.rescore(searcher, hits, 10);
    assertEquals(3, hits.totalHits);
    assertEquals("2", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", r.document(hits.scoreDocs[1].doc).get("id"));
    assertEquals("3", r.document(hits.scoreDocs[2].doc).get("id"));

    String expl = rescorer.explain(searcher,
                                   searcher.explain(query, hits.scoreDocs[0].doc),
                                   hits.scoreDocs[0].doc).toString();

    // Confirm the explanation breaks out the individual
    // variables:
    assertTrue(expl.contains("= variable \"popularity\""));

    // Confirm the explanation includes first pass details:
    assertTrue(expl.contains("= first pass score"));
    assertTrue(expl.contains("body:contents in"));
  }
}
