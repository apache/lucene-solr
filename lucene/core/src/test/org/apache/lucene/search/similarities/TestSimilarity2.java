package org.apache.lucene.search.similarities;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests against all the similarities we have
 */
public class TestSimilarity2 extends LuceneTestCase {
  List<Similarity> sims;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    sims = new ArrayList<Similarity>();
    sims.add(new DefaultSimilarity());
    sims.add(new BM25Similarity());
    // TODO: not great that we dup this all with TestSimilarityBase
    for (BasicModel basicModel : TestSimilarityBase.BASIC_MODELS) {
      for (AfterEffect afterEffect : TestSimilarityBase.AFTER_EFFECTS) {
        for (Normalization normalization : TestSimilarityBase.NORMALIZATIONS) {
          sims.add(new DFRSimilarity(basicModel, afterEffect, normalization));
        }
      }
    }
    for (Distribution distribution : TestSimilarityBase.DISTRIBUTIONS) {
      for (Lambda lambda : TestSimilarityBase.LAMBDAS) {
        for (Normalization normalization : TestSimilarityBase.NORMALIZATIONS) {
          sims.add(new IBSimilarity(distribution, lambda, normalization));
        }
      }
    }
    sims.add(new LMDirichletSimilarity());
    sims.add(new LMJelinekMercerSimilarity(0.1f));
    sims.add(new LMJelinekMercerSimilarity(0.7f));
  }
  
  /** because of stupid things like querynorm, its possible we computeStats on a field that doesnt exist at all
   *  test this against a totally empty index, to make sure sims handle it
   */
  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      assertEquals(0, is.search(new TermQuery(new Term("foo", "bar")), 10).totalHits);
    }
    ir.close();
    dir.close();
  }
  
  /** similar to the above, but ORs the query with a real field */
  public void testEmptyField() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      BooleanQuery query = new BooleanQuery(true);
      query.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
      query.add(new TermQuery(new Term("bar", "baz")), BooleanClause.Occur.SHOULD);
      assertEquals(1, is.search(query, 10).totalHits);
    }
    ir.close();
    dir.close();
  }
  
  /** similar to the above, however the field exists, but we query with a term that doesnt exist too */
  public void testEmptyTerm() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      BooleanQuery query = new BooleanQuery(true);
      query.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
      query.add(new TermQuery(new Term("foo", "baz")), BooleanClause.Occur.SHOULD);
      assertEquals(1, is.search(query, 10).totalHits);
    }
    ir.close();
    dir.close();
  }
  
  /** make sure we can retrieve when norms are disabled */
  public void testNoNorms() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setOmitNorms(true);
    ft.freeze();
    doc.add(newField("foo", "bar", ft));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      BooleanQuery query = new BooleanQuery(true);
      query.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
      assertEquals(1, is.search(query, 10).totalHits);
    }
    ir.close();
    dir.close();
  }
  
  /** make sure all sims work if TF is omitted */
  public void testOmitTF() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    ft.freeze();
    Field f = newField("foo", "bar", ft);
    doc.add(f);
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      BooleanQuery query = new BooleanQuery(true);
      query.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
      assertEquals(1, is.search(query, 10).totalHits);
    }
    ir.close();
    dir.close();
  }
  
  /** make sure all sims work if TF and norms is omitted */
  public void testOmitTFAndNorms() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    ft.setOmitNorms(true);
    ft.freeze();
    Field f = newField("foo", "bar", ft);
    doc.add(f);
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      BooleanQuery query = new BooleanQuery(true);
      query.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
      assertEquals(1, is.search(query, 10).totalHits);
    }
    ir.close();
    dir.close();
  }
  
  /** make sure all sims work with spanOR(termX, termY) where termY does not exist */
  public void testCrazySpans() throws Exception {
    // The problem: "normal" lucene queries create scorers, returning null if terms dont exist
    // This means they never score a term that does not exist.
    // however with spans, there is only one scorer for the whole hierarchy:
    // inner queries are not real queries, their boosts are ignored, etc.
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    doc.add(newField("foo", "bar", ft));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    
    for (Similarity sim : sims) {
      is.setSimilarity(sim);
      SpanTermQuery s1 = new SpanTermQuery(new Term("foo", "bar"));
      SpanTermQuery s2 = new SpanTermQuery(new Term("foo", "baz"));
      Query query = new SpanOrQuery(s1, s2);
      TopDocs td = is.search(query, 10);
      assertEquals(1, td.totalHits);
      float score = td.scoreDocs[0].score;
      assertTrue(score >= 0.0f);
      assertFalse("inf score for " + sim, Float.isInfinite(score));
    }
    ir.close();
    dir.close();
  }
}
