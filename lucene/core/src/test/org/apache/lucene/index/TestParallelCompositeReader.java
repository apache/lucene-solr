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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
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

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(new TermQuery(new Term("f1", "v1")), Occur.MUST);
    bq1.add(new TermQuery(new Term("f4", "v1")), Occur.MUST);
    queryTest(bq1.build());
  }

  public void testRefCounts1() throws IOException {
    Directory dir1 = getDir1(random());
    Directory dir2 = getDir2(random());
    DirectoryReader ir1, ir2;
    // close subreaders, ParallelReader will not change refCounts, but close on its own close
    ParallelCompositeReader pr = new ParallelCompositeReader(ir1 = DirectoryReader.open(dir1),
                                                             ir2 = DirectoryReader.open(dir2));
    IndexReader psub1 = pr.getSequentialSubReaders().get(0);
    // check RefCounts
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    assertEquals(1, psub1.getRefCount());
    pr.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    assertEquals(0, psub1.getRefCount());
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
    IndexReader psub1 = pr.getSequentialSubReaders().get(0);
    // check RefCounts
    assertEquals(2, ir1.getRefCount());
    assertEquals(2, ir2.getRefCount());
    assertEquals("refCount must be 1, as the synthetic reader was created by ParallelCompositeReader", 1, psub1.getRefCount());
    pr.close();
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    assertEquals("refcount must be 0 because parent was closed", 0, psub1.getRefCount());
    ir1.close();
    ir2.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    assertEquals("refcount should not change anymore", 0, psub1.getRefCount());
    dir1.close();
    dir2.close();    
  }
  
  private void testReaderClosedListener(boolean closeSubReaders, int wrapMultiReaderType) throws IOException {
    final Directory dir1 = getDir1(random());
    final CompositeReader ir2, ir1 = DirectoryReader.open(dir1);
    switch (wrapMultiReaderType) {
      case 0:
        ir2 = ir1;
        break;
      case 1:
        // default case, does close subreaders:
        ir2 = new MultiReader(ir1); break;
      case 2:
        ir2 = new MultiReader(new CompositeReader[] {ir1}, false); break;
      default:
        throw new AssertionError();
    }
    
    // with overlapping
    ParallelCompositeReader pr = new ParallelCompositeReader(closeSubReaders,
     new CompositeReader[] {ir2},
     new CompositeReader[] {ir2});

    final int[] listenerClosedCount = new int[1];

    assertEquals(3, pr.leaves().size());

    for(LeafReaderContext cxt : pr.leaves()) {
      cxt.reader().addReaderClosedListener(new ReaderClosedListener() {
          @Override
          public void onClose(IndexReader reader) {
            listenerClosedCount[0]++;
          }
        });
    }
    pr.close();
    if (!closeSubReaders) {
      ir1.close();
    }
    assertEquals(3, listenerClosedCount[0]);
    
    // We have to close the extra MultiReader, because it will not close its own subreaders:
    if (wrapMultiReaderType == 2) {
      ir2.close();
    }
    dir1.close();
  }

  public void testReaderClosedListener1() throws Exception {
    testReaderClosedListener(false, 0);
  }

  public void testReaderClosedListener2() throws Exception {
    testReaderClosedListener(true, 0);
  }

  public void testReaderClosedListener3() throws Exception {
    testReaderClosedListener(false, 1);
  }

  public void testReaderClosedListener4() throws Exception {
    testReaderClosedListener(true, 1);
  }

  public void testReaderClosedListener5() throws Exception {
    testReaderClosedListener(false, 2);
  }

  public void testCloseInnerReader() throws Exception {
    Directory dir1 = getDir1(random());
    CompositeReader ir1 = DirectoryReader.open(dir1);
    assertEquals(1, ir1.getSequentialSubReaders().get(0).getRefCount());
    
    // with overlapping
    ParallelCompositeReader pr = new ParallelCompositeReader(true,
     new CompositeReader[] {ir1},
     new CompositeReader[] {ir1});

    IndexReader psub = pr.getSequentialSubReaders().get(0);
    assertEquals(1, psub.getRefCount());

    ir1.close();

    assertEquals("refCount of synthetic subreader should be unchanged", 1, psub.getRefCount());
    try {
      psub.document(0);
      fail("Subreader should be already closed because inner reader was closed!");
    } catch (AlreadyClosedException e) {
      // pass
    }
    
    try {
      pr.document(0);
      fail("ParallelCompositeReader should be already closed because inner reader was closed!");
    } catch (AlreadyClosedException e) {
      // pass
    }
    
    // noop:
    pr.close();
    assertEquals(0, psub.getRefCount());
    dir1.close();
  }

  public void testIncompatibleIndexes1() throws IOException {
    // two documents:
    Directory dir1 = getDir1(random());

    // one document only:
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random())));
    Document d3 = new Document();

    d3.add(newTextField("f3", "v1", Field.Store.YES));
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
    LeafReader slow = SlowCompositeReaderWrapper.wrap(pr);
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
  
  public void testToString() throws IOException {
    Directory dir1 = getDir1(random());
    CompositeReader ir1 = DirectoryReader.open(dir1);
    ParallelCompositeReader pr = new ParallelCompositeReader(new CompositeReader[] {ir1});
    
    final String s = pr.toString();
    assertTrue("toString incorrect: " + s, s.startsWith("ParallelCompositeReader(ParallelLeafReader("));

    pr.close();
    dir1.close();
  }
  
  public void testToStringCompositeComposite() throws IOException {
    Directory dir1 = getDir1(random());
    CompositeReader ir1 = DirectoryReader.open(dir1);
    ParallelCompositeReader pr = new ParallelCompositeReader(new CompositeReader[] {new MultiReader(ir1)});
    
    final String s = pr.toString();
    assertTrue("toString incorrect (should be flattened): " + s, s.startsWith("ParallelCompositeReader(ParallelLeafReader("));

    pr.close();
    dir1.close();
  }
  
  private void queryTest(Query query) throws IOException {
    ScoreDoc[] parallelHits = parallel.search(query, 1000).scoreDocs;
    ScoreDoc[] singleHits = single.search(query, 1000).scoreDocs;
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random)));
    Document d1 = new Document();
    d1.add(newTextField("f1", "v1", Field.Store.YES));
    d1.add(newTextField("f2", "v1", Field.Store.YES));
    d1.add(newTextField("f3", "v1", Field.Store.YES));
    d1.add(newTextField("f4", "v1", Field.Store.YES));
    w.addDocument(d1);
    Document d2 = new Document();
    d2.add(newTextField("f1", "v2", Field.Store.YES));
    d2.add(newTextField("f2", "v2", Field.Store.YES));
    d2.add(newTextField("f3", "v2", Field.Store.YES));
    d2.add(newTextField("f4", "v2", Field.Store.YES));
    w.addDocument(d2);
    Document d3 = new Document();
    d3.add(newTextField("f1", "v3", Field.Store.YES));
    d3.add(newTextField("f2", "v3", Field.Store.YES));
    d3.add(newTextField("f3", "v3", Field.Store.YES));
    d3.add(newTextField("f4", "v3", Field.Store.YES));
    w.addDocument(d3);
    Document d4 = new Document();
    d4.add(newTextField("f1", "v4", Field.Store.YES));
    d4.add(newTextField("f2", "v4", Field.Store.YES));
    d4.add(newTextField("f3", "v4", Field.Store.YES));
    d4.add(newTextField("f4", "v4", Field.Store.YES));
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
      assertEquals(2, rd1.getContext().children().size());
      assertEquals(2, rd2.getContext().children().size());
    } else {
      rd1 = DirectoryReader.open(dir1);
      rd2 = DirectoryReader.open(dir2);
      assertEquals(3, rd1.getContext().children().size());
      assertEquals(3, rd2.getContext().children().size());
    }
    ParallelCompositeReader pr = new ParallelCompositeReader(rd1, rd2);
    return newSearcher(pr);
  }

  // subreader structure: (1,2,1) 
  private Directory getDir1(Random random) throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random))
                                             .setMergePolicy(NoMergePolicy.INSTANCE));
    Document d1 = new Document();
    d1.add(newTextField("f1", "v1", Field.Store.YES));
    d1.add(newTextField("f2", "v1", Field.Store.YES));
    w1.addDocument(d1);
    w1.commit();
    Document d2 = new Document();
    d2.add(newTextField("f1", "v2", Field.Store.YES));
    d2.add(newTextField("f2", "v2", Field.Store.YES));
    w1.addDocument(d2);
    Document d3 = new Document();
    d3.add(newTextField("f1", "v3", Field.Store.YES));
    d3.add(newTextField("f2", "v3", Field.Store.YES));
    w1.addDocument(d3);
    w1.commit();
    Document d4 = new Document();
    d4.add(newTextField("f1", "v4", Field.Store.YES));
    d4.add(newTextField("f2", "v4", Field.Store.YES));
    w1.addDocument(d4);
    w1.close();
    return dir1;
  }

  // subreader structure: (1,2,1) 
  private Directory getDir2(Random random) throws IOException {
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random))
                                             .setMergePolicy(NoMergePolicy.INSTANCE));
    Document d1 = new Document();
    d1.add(newTextField("f3", "v1", Field.Store.YES));
    d1.add(newTextField("f4", "v1", Field.Store.YES));
    w2.addDocument(d1);
    w2.commit();
    Document d2 = new Document();
    d2.add(newTextField("f3", "v2", Field.Store.YES));
    d2.add(newTextField("f4", "v2", Field.Store.YES));
    w2.addDocument(d2);
    Document d3 = new Document();
    d3.add(newTextField("f3", "v3", Field.Store.YES));
    d3.add(newTextField("f4", "v3", Field.Store.YES));
    w2.addDocument(d3);
    w2.commit();
    Document d4 = new Document();
    d4.add(newTextField("f3", "v4", Field.Store.YES));
    d4.add(newTextField("f4", "v4", Field.Store.YES));
    w2.addDocument(d4);
    w2.close();
    return dir2;
  }

  // this dir has a different subreader structure (1,1,2);
  private Directory getInvalidStructuredDir2(Random random) throws IOException {
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random))
                                             .setMergePolicy(NoMergePolicy.INSTANCE));
    Document d1 = new Document();
    d1.add(newTextField("f3", "v1", Field.Store.YES));
    d1.add(newTextField("f4", "v1", Field.Store.YES));
    w2.addDocument(d1);
    w2.commit();
    Document d2 = new Document();
    d2.add(newTextField("f3", "v2", Field.Store.YES));
    d2.add(newTextField("f4", "v2", Field.Store.YES));
    w2.addDocument(d2);
    w2.commit();
    Document d3 = new Document();
    d3.add(newTextField("f3", "v3", Field.Store.YES));
    d3.add(newTextField("f4", "v3", Field.Store.YES));
    w2.addDocument(d3);
    Document d4 = new Document();
    d4.add(newTextField("f3", "v4", Field.Store.YES));
    d4.add(newTextField("f4", "v4", Field.Store.YES));
    w2.addDocument(d4);
    w2.close();
    return dir2;
  }

}
