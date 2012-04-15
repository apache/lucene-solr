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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestParallelCompositeReader extends LuceneTestCase {

  private IndexSearcher parallel, single;
  private Directory dir, dir1, dir2;

  public void testQueries() throws Exception {
    single = single(random(), false);
    parallel = parallel(random(), false);
    
    queries();
    
    single.getIndexReader().close(); single = null;
    parallel.getIndexReader().close(); parallel = null;
    dir.close(); dir = null;
    dir1.close(); dir1 = null;
    dir2.close(); dir2 = null;
  }

  public void testQueriesCompositeComposite() throws Exception {
    single = single(random(), true);
    parallel = parallel(random(), true);
    
    queries();
    
    single.getIndexReader().close(); single = null;
    parallel.getIndexReader().close(); parallel = null;
    dir.close(); dir = null;
    dir1.close(); dir1 = null;
    dir2.close(); dir2 = null;
  }
  
  private void queries() throws Exception {
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

  public void testRefCounts1() throws IOException {
    Directory dir1 = getDir1(random());
    Directory dir2 = getDir2(random());
    DirectoryReader ir1, ir2;
    // close subreaders, ParallelReader will not change refCounts, but close on its own close
    ParallelCompositeReader pr = new ParallelCompositeReader(ir1 = DirectoryReader.open(dir1),
                                                             ir2 = DirectoryReader.open(dir2));
    // check RefCounts
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    pr.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();    
  }
  
  public void testRefCounts2() throws IOException {
    Directory dir1 = getDir1(random());
    Directory dir2 = getDir2(random());
    DirectoryReader ir1 = DirectoryReader.open(dir1);
    DirectoryReader ir2 = DirectoryReader.open(dir2);

    // don't close subreaders, so ParallelReader will increment refcounts
    ParallelCompositeReader pr = new ParallelCompositeReader(false, ir1, ir2);
    // check RefCounts
    assertEquals(2, ir1.getRefCount());
    assertEquals(2, ir2.getRefCount());
    pr.close();
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    ir1.close();
    ir2.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();    
  }
  
  public void testIncompatibleIndexes1() throws IOException {
    // two documents:
    Directory dir1 = getDir1(random());

    // one document only:
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document d3 = new Document();

    d3.add(newField("f3", "v1", TextField.TYPE_STORED));
    w2.addDocument(d3);
    w2.close();
    
    DirectoryReader ir1 = DirectoryReader.open(dir1),
        ir2 = DirectoryReader.open(dir2);
    try {
      new ParallelCompositeReader(ir1, ir2);
      fail("didn't get expected exception: indexes don't have same number of documents");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    try {
      new ParallelCompositeReader(random().nextBoolean(), ir1, ir2);
      fail("didn't get expected exception: indexes don't have same number of documents");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    ir1.close();
    ir2.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();
  }
  
  public void testIncompatibleIndexes2() throws IOException {
    Directory dir1 = getDir1(random());
    Directory dir2 = getInvalidStructuredDir2(random());

    DirectoryReader ir1 = DirectoryReader.open(dir1),
        ir2 = DirectoryReader.open(dir2);
    CompositeReader[] readers = new CompositeReader[] {ir1, ir2};
    try {
      new ParallelCompositeReader(readers);
      fail("didn't get expected exception: indexes don't have same subreader structure");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    try {
      new ParallelCompositeReader(random().nextBoolean(), readers, readers);
      fail("didn't get expected exception: indexes don't have same subreader structure");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    ir1.close();
    ir2.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();
  }
  
  public void testIncompatibleIndexes3() throws IOException {
    Directory dir1 = getDir1(random());
    Directory dir2 = getDir2(random());

    CompositeReader ir1 = new MultiReader(DirectoryReader.open(dir1), SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir1))),
        ir2 = new MultiReader(DirectoryReader.open(dir2), DirectoryReader.open(dir2));
    CompositeReader[] readers = new CompositeReader[] {ir1, ir2};
    try {
      new ParallelCompositeReader(readers);
      fail("didn't get expected exception: indexes don't have same subreader structure");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    try {
      new ParallelCompositeReader(random().nextBoolean(), readers, readers);
      fail("didn't get expected exception: indexes don't have same subreader structure");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    ir1.close();
    ir2.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();
  }
  
  public void testIgnoreStoredFields() throws IOException {
    Directory dir1 = getDir1(random());
    Directory dir2 = getDir2(random());
    CompositeReader ir1 = DirectoryReader.open(dir1);
    CompositeReader ir2 = DirectoryReader.open(dir2);
    
    // with overlapping
    ParallelCompositeReader pr = new ParallelCompositeReader(false,
     new CompositeReader[] {ir1, ir2},
     new CompositeReader[] {ir1});
    assertEquals("v1", pr.document(0).get("f1"));
    assertEquals("v1", pr.document(0).get("f2"));
    assertNull(pr.document(0).get("f3"));
    assertNull(pr.document(0).get("f4"));
    // check that fields are there
    AtomicReader slow = SlowCompositeReaderWrapper.wrap(pr);
    assertNotNull(slow.terms("f1"));
    assertNotNull(slow.terms("f2"));
    assertNotNull(slow.terms("f3"));
    assertNotNull(slow.terms("f4"));
    pr.close();
    
    // no stored fields at all
    pr = new ParallelCompositeReader(false,
        new CompositeReader[] {ir2},
        new CompositeReader[0]);
    assertNull(pr.document(0).get("f1"));
    assertNull(pr.document(0).get("f2"));
    assertNull(pr.document(0).get("f3"));
    assertNull(pr.document(0).get("f4"));
    // check that fields are there
    slow = SlowCompositeReaderWrapper.wrap(pr);
    assertNull(slow.terms("f1"));
    assertNull(slow.terms("f2"));
    assertNotNull(slow.terms("f3"));
    assertNotNull(slow.terms("f4"));
    pr.close();
    
    // without overlapping
    pr = new ParallelCompositeReader(true,
      new CompositeReader[] {ir2},
      new CompositeReader[] {ir1});
    assertEquals("v1", pr.document(0).get("f1"));
    assertEquals("v1", pr.document(0).get("f2"));
    assertNull(pr.document(0).get("f3"));
    assertNull(pr.document(0).get("f4"));
    // check that fields are there
    slow = SlowCompositeReaderWrapper.wrap(pr);
    assertNull(slow.terms("f1"));
    assertNull(slow.terms("f2"));
    assertNotNull(slow.terms("f3"));
    assertNotNull(slow.terms("f4"));
    pr.close();
    
    // no main readers
    try {
      new ParallelCompositeReader(true,
        new CompositeReader[0],
        new CompositeReader[] {ir1});
      fail("didn't get expected exception: need a non-empty main-reader array");
    } catch (IllegalArgumentException iae) {
      // pass
    }
    
    dir1.close();
    dir2.close();
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
  private IndexSearcher single(Random random, boolean compositeComposite) throws IOException {
    dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d1 = new Document();
    d1.add(newField("f1", "v1", TextField.TYPE_STORED));
    d1.add(newField("f2", "v1", TextField.TYPE_STORED));
    d1.add(newField("f3", "v1", TextField.TYPE_STORED));
    d1.add(newField("f4", "v1", TextField.TYPE_STORED));
    w.addDocument(d1);
    Document d2 = new Document();
    d2.add(newField("f1", "v2", TextField.TYPE_STORED));
    d2.add(newField("f2", "v2", TextField.TYPE_STORED));
    d2.add(newField("f3", "v2", TextField.TYPE_STORED));
    d2.add(newField("f4", "v2", TextField.TYPE_STORED));
    w.addDocument(d2);
    Document d3 = new Document();
    d3.add(newField("f1", "v3", TextField.TYPE_STORED));
    d3.add(newField("f2", "v3", TextField.TYPE_STORED));
    d3.add(newField("f3", "v3", TextField.TYPE_STORED));
    d3.add(newField("f4", "v3", TextField.TYPE_STORED));
    w.addDocument(d3);
    Document d4 = new Document();
    d4.add(newField("f1", "v4", TextField.TYPE_STORED));
    d4.add(newField("f2", "v4", TextField.TYPE_STORED));
    d4.add(newField("f3", "v4", TextField.TYPE_STORED));
    d4.add(newField("f4", "v4", TextField.TYPE_STORED));
    w.addDocument(d4);
    w.close();

    final CompositeReader ir;
    if (compositeComposite) {
      ir = new MultiReader(DirectoryReader.open(dir), DirectoryReader.open(dir));
    } else {
      ir = DirectoryReader.open(dir);
    }
    return newSearcher(ir);
  }

  // Fields 1 & 2 in one index, 3 & 4 in other, with ParallelReader:
  private IndexSearcher parallel(Random random, boolean compositeComposite) throws IOException {
    dir1 = getDir1(random);
    dir2 = getDir2(random);
    final CompositeReader rd1, rd2;
    if (compositeComposite) {
      rd1 = new MultiReader(DirectoryReader.open(dir1), DirectoryReader.open(dir1));
      rd2 = new MultiReader(DirectoryReader.open(dir2), DirectoryReader.open(dir2));
      assertEquals(2, rd1.getSequentialSubReaders().length);
      assertEquals(2, rd2.getSequentialSubReaders().length);
    } else {
      rd1 = DirectoryReader.open(dir1);
      rd2 = DirectoryReader.open(dir2);
      assertEquals(3, rd1.getSequentialSubReaders().length);
      assertEquals(3, rd2.getSequentialSubReaders().length);
    }
    ParallelCompositeReader pr = new ParallelCompositeReader(rd1, rd2);
    return newSearcher(pr);
  }

  // subreader structure: (1,2,1) 
  private Directory getDir1(Random random) throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES));
    Document d1 = new Document();
    d1.add(newField("f1", "v1", TextField.TYPE_STORED));
    d1.add(newField("f2", "v1", TextField.TYPE_STORED));
    w1.addDocument(d1);
    w1.commit();
    Document d2 = new Document();
    d2.add(newField("f1", "v2", TextField.TYPE_STORED));
    d2.add(newField("f2", "v2", TextField.TYPE_STORED));
    w1.addDocument(d2);
    Document d3 = new Document();
    d3.add(newField("f1", "v3", TextField.TYPE_STORED));
    d3.add(newField("f2", "v3", TextField.TYPE_STORED));
    w1.addDocument(d3);
    w1.commit();
    Document d4 = new Document();
    d4.add(newField("f1", "v4", TextField.TYPE_STORED));
    d4.add(newField("f2", "v4", TextField.TYPE_STORED));
    w1.addDocument(d4);
    w1.close();
    return dir1;
  }

  // subreader structure: (1,2,1) 
  private Directory getDir2(Random random) throws IOException {
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES));
    Document d1 = new Document();
    d1.add(newField("f3", "v1", TextField.TYPE_STORED));
    d1.add(newField("f4", "v1", TextField.TYPE_STORED));
    w2.addDocument(d1);
    w2.commit();
    Document d2 = new Document();
    d2.add(newField("f3", "v2", TextField.TYPE_STORED));
    d2.add(newField("f4", "v2", TextField.TYPE_STORED));
    w2.addDocument(d2);
    Document d3 = new Document();
    d3.add(newField("f3", "v3", TextField.TYPE_STORED));
    d3.add(newField("f4", "v3", TextField.TYPE_STORED));
    w2.addDocument(d3);
    w2.commit();
    Document d4 = new Document();
    d4.add(newField("f3", "v4", TextField.TYPE_STORED));
    d4.add(newField("f4", "v4", TextField.TYPE_STORED));
    w2.addDocument(d4);
    w2.close();
    return dir2;
  }

  // this dir has a different subreader structure (1,1,2);
  private Directory getInvalidStructuredDir2(Random random) throws IOException {
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES));
    Document d1 = new Document();
    d1.add(newField("f3", "v1", TextField.TYPE_STORED));
    d1.add(newField("f4", "v1", TextField.TYPE_STORED));
    w2.addDocument(d1);
    w2.commit();
    Document d2 = new Document();
    d2.add(newField("f3", "v2", TextField.TYPE_STORED));
    d2.add(newField("f4", "v2", TextField.TYPE_STORED));
    w2.addDocument(d2);
    w2.commit();
    Document d3 = new Document();
    d3.add(newField("f3", "v3", TextField.TYPE_STORED));
    d3.add(newField("f4", "v3", TextField.TYPE_STORED));
    w2.addDocument(d3);
    Document d4 = new Document();
    d4.add(newField("f3", "v4", TextField.TYPE_STORED));
    d4.add(newField("f4", "v4", TextField.TYPE_STORED));
    w2.addDocument(d4);
    w2.close();
    return dir2;
  }

}
