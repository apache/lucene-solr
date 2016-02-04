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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

public class TestTieredMergePolicy extends BaseMergePolicyTestCase {

  public MergePolicy mergePolicy() {
    return newTieredMergePolicy();
  }

  public void testForceMergeDeletes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    TieredMergePolicy tmp = newTieredMergePolicy();
    conf.setMergePolicy(tmp);
    conf.setMaxBufferedDocs(4);
    tmp.setMaxMergeAtOnce(100);
    tmp.setSegmentsPerTier(100);
    tmp.setForceMergeDeletesPctAllowed(30.0);
    IndexWriter w = new IndexWriter(dir, conf);
    for(int i=0;i<80;i++) {
      Document doc = new Document();
      doc.add(newTextField("content", "aaa " + (i%4), Field.Store.NO));
      w.addDocument(doc);
    }
    assertEquals(80, w.maxDoc());
    assertEquals(80, w.numDocs());

    if (VERBOSE) {
      System.out.println("\nTEST: delete docs");
    }
    w.deleteDocuments(new Term("content", "0"));
    w.forceMergeDeletes();

    assertEquals(80, w.maxDoc());
    assertEquals(60, w.numDocs());

    if (VERBOSE) {
      System.out.println("\nTEST: forceMergeDeletes2");
    }
    ((TieredMergePolicy) w.getConfig().getMergePolicy()).setForceMergeDeletesPctAllowed(10.0);
    w.forceMergeDeletes();
    assertEquals(60, w.maxDoc());
    assertEquals(60, w.numDocs());
    w.close();
    dir.close();
  }

  public void testPartialMerge() throws Exception {
    int num = atLeast(10);
    for(int iter=0;iter<num;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      Directory dir = newDirectory();
      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
      conf.setMergeScheduler(new SerialMergeScheduler());
      TieredMergePolicy tmp = newTieredMergePolicy();
      conf.setMergePolicy(tmp);
      conf.setMaxBufferedDocs(2);
      tmp.setMaxMergeAtOnce(3);
      tmp.setSegmentsPerTier(6);

      IndexWriter w = new IndexWriter(dir, conf);
      int maxCount = 0;
      final int numDocs = TestUtil.nextInt(random(), 20, 100);
      for(int i=0;i<numDocs;i++) {
        Document doc = new Document();
        doc.add(newTextField("content", "aaa " + (i%4), Field.Store.NO));
        w.addDocument(doc);
        int count = w.getSegmentCount();
        maxCount = Math.max(count, maxCount);
        assertTrue("count=" + count + " maxCount=" + maxCount, count >= maxCount-3);
      }

      w.flush(true, true);

      int segmentCount = w.getSegmentCount();
      int targetCount = TestUtil.nextInt(random(), 1, segmentCount);
      if (VERBOSE) {
        System.out.println("TEST: merge to " + targetCount + " segs (current count=" + segmentCount + ")");
      }
      w.forceMerge(targetCount);
      assertEquals(targetCount, w.getSegmentCount());

      w.close();
      dir.close();
    }
  }

  public void testForceMergeDeletesMaxSegSize() throws Exception {
    final Directory dir = newDirectory();
    final IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    final TieredMergePolicy tmp = new TieredMergePolicy();
    tmp.setMaxMergedSegmentMB(0.01);
    tmp.setForceMergeDeletesPctAllowed(0.0);
    conf.setMergePolicy(tmp);

    final IndexWriter w = new IndexWriter(dir, conf);

    final int numDocs = atLeast(200);
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + i, Field.Store.NO));
      doc.add(newTextField("content", "aaa " + i, Field.Store.NO));
      w.addDocument(doc);
    }

    w.forceMerge(1);
    IndexReader r = w.getReader();
    assertEquals(numDocs, r.maxDoc());
    assertEquals(numDocs, r.numDocs());
    r.close();

    if (VERBOSE) {
      System.out.println("\nTEST: delete doc");
    }

    w.deleteDocuments(new Term("id", ""+(42+17)));

    r = w.getReader();
    assertEquals(numDocs, r.maxDoc());
    assertEquals(numDocs-1, r.numDocs());
    r.close();

    w.forceMergeDeletes();

    r = w.getReader();
    assertEquals(numDocs-1, r.maxDoc());
    assertEquals(numDocs-1, r.numDocs());
    r.close();

    w.close();

    dir.close();
  }
  
  private static final double EPSILON = 1E-14;
  
  public void testSetters() {
    final TieredMergePolicy tmp = new TieredMergePolicy();
    
    tmp.setMaxMergedSegmentMB(0.5);
    assertEquals(0.5, tmp.getMaxMergedSegmentMB(), EPSILON);
    
    tmp.setMaxMergedSegmentMB(Double.POSITIVE_INFINITY);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getMaxMergedSegmentMB(), EPSILON*Long.MAX_VALUE);
    
    tmp.setMaxMergedSegmentMB(Long.MAX_VALUE/1024/1024.);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getMaxMergedSegmentMB(), EPSILON*Long.MAX_VALUE);
    
    try {
      tmp.setMaxMergedSegmentMB(-2.0);
      fail("Didn't throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
    
    tmp.setFloorSegmentMB(2.0);
    assertEquals(2.0, tmp.getFloorSegmentMB(), EPSILON);
    
    tmp.setFloorSegmentMB(Double.POSITIVE_INFINITY);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getFloorSegmentMB(), EPSILON*Long.MAX_VALUE);
    
    tmp.setFloorSegmentMB(Long.MAX_VALUE/1024/1024.);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getFloorSegmentMB(), EPSILON*Long.MAX_VALUE);
    
    try {
      tmp.setFloorSegmentMB(-2.0);
      fail("Didn't throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
    
    tmp.setMaxCFSSegmentSizeMB(2.0);
    assertEquals(2.0, tmp.getMaxCFSSegmentSizeMB(), EPSILON);
    
    tmp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getMaxCFSSegmentSizeMB(), EPSILON*Long.MAX_VALUE);
    
    tmp.setMaxCFSSegmentSizeMB(Long.MAX_VALUE/1024/1024.);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getMaxCFSSegmentSizeMB(), EPSILON*Long.MAX_VALUE);
    
    try {
      tmp.setMaxCFSSegmentSizeMB(-2.0);
      fail("Didn't throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
    
    // TODO: Add more checks for other non-double setters!
  }

  // LUCENE-5668
  public void testUnbalancedMergeSelection() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    TieredMergePolicy tmp = (TieredMergePolicy) iwc.getMergePolicy();
    tmp.setFloorSegmentMB(0.00001);
    // We need stable sizes for each segment:
    iwc.setCodec(TestUtil.getDefaultCodec());
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setMaxBufferedDocs(100);
    iwc.setRAMBufferSizeMB(-1);
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<15000*RANDOM_MULTIPLIER;i++) {
      Document doc = new Document();
      doc.add(newTextField("id", random().nextLong() + "" + random().nextLong(), Field.Store.YES));
      w.addDocument(doc);
    }
    IndexReader r = DirectoryReader.open(w);

    // Make sure TMP always merged equal-number-of-docs segments:
    for(LeafReaderContext ctx : r.leaves()) {
      int numDocs = ctx.reader().numDocs();
      assertTrue("got numDocs=" + numDocs, numDocs == 100 || numDocs == 1000 || numDocs == 10000);
    }
    r.close();
    w.close();
    dir.close();
  }
}
