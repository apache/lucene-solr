package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestParallelReader extends LuceneTestCase {

  private Searcher parallel;
  private Searcher single;
  
  protected void setUp() throws Exception {
    super.setUp();
    single = single();
    parallel = parallel();
  }

  public void testQueries() throws Exception {
    queryTest(new TermQuery(new Term("f1", "v1")));
    queryTest(new TermQuery(new Term("f1", "v2")));
    queryTest(new TermQuery(new Term("f2", "v1")));
    queryTest(new TermQuery(new Term("f2", "v2")));
    queryTest(new TermQuery(new Term("f3", "v1")));
    queryTest(new TermQuery(new Term("f3", "v2")));
    queryTest(new TermQuery(new Term("f4", "v1")));
    queryTest(new TermQuery(new Term("f4", "v2")));

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(new TermQuery(new Term("f1", "v1")), Occur.MUST);
    bq1.add(new TermQuery(new Term("f4", "v1")), Occur.MUST);
    queryTest(bq1);
  }

  public void testFieldNames() throws Exception {
    Directory dir1 = getDir1();
    Directory dir2 = getDir2();
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    pr.add(IndexReader.open(dir2));
    Collection fieldNames = pr.getFieldNames(IndexReader.FieldOption.ALL);
    assertEquals(4, fieldNames.size());
    assertTrue(fieldNames.contains("f1"));
    assertTrue(fieldNames.contains("f2"));
    assertTrue(fieldNames.contains("f3"));
    assertTrue(fieldNames.contains("f4"));
  }
  
  public void testDocument() throws IOException {
    Directory dir1 = getDir1();
    Directory dir2 = getDir2();
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    pr.add(IndexReader.open(dir2));

    Document doc11 = pr.document(0, new MapFieldSelector(new String[] {"f1"}));
    Document doc24 = pr.document(1, new MapFieldSelector(Arrays.asList(new String[] {"f4"})));
    Document doc223 = pr.document(1, new MapFieldSelector(new String[] {"f2", "f3"}));
    
    assertEquals(1, doc11.getFields().size());
    assertEquals(1, doc24.getFields().size());
    assertEquals(2, doc223.getFields().size());
    
    assertEquals("v1", doc11.get("f1"));
    assertEquals("v2", doc24.get("f4"));
    assertEquals("v2", doc223.get("f2"));
    assertEquals("v2", doc223.get("f3"));
  }
  
  public void testIncompatibleIndexes() throws IOException {
    // two documents:
    Directory dir1 = getDir1();

    // one document only:
    Directory dir2 = new MockRAMDirectory();
    IndexWriter w2 = new IndexWriter(dir2, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document d3 = new Document();
    d3.add(new Field("f3", "v1", Field.Store.YES, Field.Index.ANALYZED));
    w2.addDocument(d3);
    w2.close();
    
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    try {
      pr.add(IndexReader.open(dir2));
      fail("didn't get exptected exception: indexes don't have same number of documents");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
  }
  
  public void testIsCurrent() throws IOException {
    Directory dir1 = getDir1();
    Directory dir2 = getDir1();
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    pr.add(IndexReader.open(dir2));
    
    assertTrue(pr.isCurrent());
    IndexReader modifier = IndexReader.open(dir1);
    modifier.setNorm(0, "f1", 100);
    modifier.close();
    
    // one of the two IndexReaders which ParallelReader is using
    // is not current anymore
    assertFalse(pr.isCurrent());
    
    modifier = IndexReader.open(dir2);
    modifier.setNorm(0, "f3", 100);
    modifier.close();
    
    // now both are not current anymore
    assertFalse(pr.isCurrent());
  }

  public void testIsOptimized() throws IOException {
    Directory dir1 = getDir1();
    Directory dir2 = getDir1();
    
    // add another document to ensure that the indexes are not optimized
    IndexWriter modifier = new IndexWriter(dir1, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    Document d = new Document();
    d.add(new Field("f1", "v1", Field.Store.YES, Field.Index.ANALYZED));
    modifier.addDocument(d);
    modifier.close();
    
    modifier = new IndexWriter(dir2, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    d = new Document();
    d.add(new Field("f2", "v2", Field.Store.YES, Field.Index.ANALYZED));
    modifier.addDocument(d);
    modifier.close();

    
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    pr.add(IndexReader.open(dir2));
    assertFalse(pr.isOptimized());
    pr.close();
    
    modifier = new IndexWriter(dir1, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    modifier.optimize();
    modifier.close();
    
    pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    pr.add(IndexReader.open(dir2));
    // just one of the two indexes are optimized
    assertFalse(pr.isOptimized());
    pr.close();

    
    modifier = new IndexWriter(dir2, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    modifier.optimize();
    modifier.close();
    
    pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    pr.add(IndexReader.open(dir2));
    // now both indexes are optimized
    assertTrue(pr.isOptimized());
    pr.close();

  }

  
  private void queryTest(Query query) throws IOException {
    ScoreDoc[] parallelHits = parallel.search(query, null, 1000).scoreDocs;
    ScoreDoc[] singleHits = single.search(query, null, 1000).scoreDocs;
    assertEquals(parallelHits.length, singleHits.length);
    for(int i = 0; i < parallelHits.length; i++) {
      assertEquals(parallelHits[i].score, singleHits[i].score, 0.001f);
      Document docParallel = parallel.doc(parallelHits[i].doc);
      Document docSingle = single.doc(singleHits[i].doc);
      assertEquals(docParallel.get("f1"), docSingle.get("f1"));
      assertEquals(docParallel.get("f2"), docSingle.get("f2"));
      assertEquals(docParallel.get("f3"), docSingle.get("f3"));
      assertEquals(docParallel.get("f4"), docSingle.get("f4"));
    }
  }

  // Fields 1-4 indexed together:
  private Searcher single() throws IOException {
    Directory dir = new MockRAMDirectory();
    IndexWriter w = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document d1 = new Document();
    d1.add(new Field("f1", "v1", Field.Store.YES, Field.Index.ANALYZED));
    d1.add(new Field("f2", "v1", Field.Store.YES, Field.Index.ANALYZED));
    d1.add(new Field("f3", "v1", Field.Store.YES, Field.Index.ANALYZED));
    d1.add(new Field("f4", "v1", Field.Store.YES, Field.Index.ANALYZED));
    w.addDocument(d1);
    Document d2 = new Document();
    d2.add(new Field("f1", "v2", Field.Store.YES, Field.Index.ANALYZED));
    d2.add(new Field("f2", "v2", Field.Store.YES, Field.Index.ANALYZED));
    d2.add(new Field("f3", "v2", Field.Store.YES, Field.Index.ANALYZED));
    d2.add(new Field("f4", "v2", Field.Store.YES, Field.Index.ANALYZED));
    w.addDocument(d2);
    w.close();

    return new IndexSearcher(dir);
  }

  // Fields 1 & 2 in one index, 3 & 4 in other, with ParallelReader:
  private Searcher parallel() throws IOException {
    Directory dir1 = getDir1();
    Directory dir2 = getDir2();
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(dir1));
    pr.add(IndexReader.open(dir2));
    return new IndexSearcher(pr);
  }

  private Directory getDir1() throws IOException {
    Directory dir1 = new MockRAMDirectory();
    IndexWriter w1 = new IndexWriter(dir1, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document d1 = new Document();
    d1.add(new Field("f1", "v1", Field.Store.YES, Field.Index.ANALYZED));
    d1.add(new Field("f2", "v1", Field.Store.YES, Field.Index.ANALYZED));
    w1.addDocument(d1);
    Document d2 = new Document();
    d2.add(new Field("f1", "v2", Field.Store.YES, Field.Index.ANALYZED));
    d2.add(new Field("f2", "v2", Field.Store.YES, Field.Index.ANALYZED));
    w1.addDocument(d2);
    w1.close();
    return dir1;
  }

  private Directory getDir2() throws IOException {
    Directory dir2 = new RAMDirectory();
    IndexWriter w2 = new IndexWriter(dir2, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document d3 = new Document();
    d3.add(new Field("f3", "v1", Field.Store.YES, Field.Index.ANALYZED));
    d3.add(new Field("f4", "v1", Field.Store.YES, Field.Index.ANALYZED));
    w2.addDocument(d3);
    Document d4 = new Document();
    d4.add(new Field("f3", "v2", Field.Store.YES, Field.Index.ANALYZED));
    d4.add(new Field("f4", "v2", Field.Store.YES, Field.Index.ANALYZED));
    w2.addDocument(d4);
    w2.close();
    return dir2;
  }

}
