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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
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

      final long max125Pct = (long) ((tmp.getMaxMergedSegmentMB() * 1024.0 * 1024.0) * 1.25);
      // Other than in the case where the target count is 1 we can't say much except no segment should be > 125% of max seg size.
      if (targetCount == 1) {
        assertEquals("Should have merged down to one segment", targetCount, w.getSegmentCount());
      } else {
        // why can't we say much? Well...
        // 1> the random numbers generated above mean we could have 10 segments and a target max count of, say, 9. we
        //    could get there by combining only 2 segments. So tests like "no pair of segments should total less than
        //    125% max segment size" aren't valid.
        //
        // 2> We could have 10 segments and a target count of 2. In that case there could be 5 segments resulting.
        //    as long as they're all < 125% max seg size, that's valid.
        Iterator<SegmentCommitInfo> iterator = w.segmentInfos.iterator();
        while (iterator.hasNext()) {
          SegmentCommitInfo info = iterator.next();
          assertTrue("No segment should be more than 125% of max segment size ",
              max125Pct >= info.sizeInBytes());
        }
      }

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
    for (int i = 0; i < numDocs; i++) {
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

  // LUCENE-7976 makes findForceMergeDeletes and findForcedDeletes respect max segment size by default,
  // so insure that this works.
  public void testForcedMergesRespectSegSize() throws Exception {
    final Directory dir = newDirectory();
    final IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    final TieredMergePolicy tmp = new TieredMergePolicy();

    // Empirically, 100 docs the size below give us segments of 3,330 bytes. It's not all that reliable in terms
    // of how big a segment _can_ get, so set it to prevent merges on commit.
    double mbSize = 0.004;
    long maxSegBytes = (long) ((1024.0 * 1024.0)); // fudge it up, we're trying to catch egregious errors and segbytes don't really reflect the number for original merges.
    tmp.setMaxMergedSegmentMB(mbSize);
    conf.setMaxBufferedDocs(100);
    conf.setMergePolicy(tmp);

    final IndexWriter w = new IndexWriter(dir, conf);

    final int numDocs = atLeast(2400);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + i, Field.Store.NO));
      doc.add(newTextField("content", "aaa " + i, Field.Store.NO));
      w.addDocument(doc);
    }

    w.commit();

    // These should be no-ops on an index with no deletions and segments are pretty big.
    List<String> segNamesBefore = getSegmentNames(w);
    w.forceMergeDeletes();
    checkSegmentsInExpectations(w, segNamesBefore, false);  // There should have been no merges.

    w.forceMerge(Integer.MAX_VALUE);
    checkSegmentsInExpectations(w, segNamesBefore, true);
    checkSegmentSizeNotExceeded(w.segmentInfos, maxSegBytes);


    // Delete 12-17% of each segment and expungeDeletes. This should result in:
    // > the same number of segments as before.
    // > no segments larger than maxSegmentSize.
    // > no deleted docs left.
    int remainingDocs = numDocs - deletePctDocsFromEachSeg(w, random().nextInt(5) + 12, true);
    w.forceMergeDeletes();
    w.commit();
    checkSegmentSizeNotExceeded(w.segmentInfos, maxSegBytes);
    assertFalse("There should be no deleted docs in the index.", w.hasDeletions());

    // Check that deleting _fewer_ than 10% doesn't merge inappropriately. Nothing should be merged since no segment
    // has had more than 10% of its docs deleted.
    segNamesBefore = getSegmentNames(w);
    int deletedThisPass = deletePctDocsFromEachSeg(w, random().nextInt(4) + 3, false);
    w.forceMergeDeletes();
    remainingDocs -= deletedThisPass;
    checkSegmentsInExpectations(w, segNamesBefore, false); // There should have been no merges
    assertEquals("NumDocs should reflect removed documents ", remainingDocs, w.numDocs());
    assertTrue("Should still be deleted docs in the index", w.numDocs() < w.maxDoc());

    // This time, forceMerge. By default this should respect max segment size.
    // Will change for LUCENE-8236
    w.forceMerge(Integer.MAX_VALUE);
    checkSegmentSizeNotExceeded(w.segmentInfos, maxSegBytes);

    // Now forceMerge down to one segment, there should be exactly remainingDocs in exactly one segment.
    w.forceMerge(1);
    assertEquals("There should be exaclty one segment now", 1, w.getSegmentCount());
    assertEquals("maxDoc and numDocs should be identical", w.numDocs(), w.maxDoc());
    assertEquals("There should be an exact number of documents in that one segment", remainingDocs, w.numDocs());

    // Delete 5% and expunge, should be no change.
    segNamesBefore = getSegmentNames(w);
    remainingDocs -= deletePctDocsFromEachSeg(w, random().nextInt(5) + 1, false);
    w.forceMergeDeletes();
    checkSegmentsInExpectations(w, segNamesBefore, false);
    assertEquals("There should still be only one segment. ", 1, w.getSegmentCount());
    assertTrue("The segment should have deleted documents", w.numDocs() < w.maxDoc());

    w.forceMerge(1); // back to one segment so deletePctDocsFromEachSeg still works

    // Test singleton merge for expungeDeletes
    remainingDocs -= deletePctDocsFromEachSeg(w, random().nextInt(5) + 20, true);
    w.forceMergeDeletes();

    assertEquals("There should still be only one segment. ", 1, w.getSegmentCount());
    assertEquals("The segment should have no deleted documents", w.numDocs(), w.maxDoc());


    // sanity check, at this point we should have an over`-large segment, we know we have exactly one.
    assertTrue("Our single segment should have quite a few docs", w.numDocs() > 1_000);

    // Delete 60% of the documents and then add a few more docs and commit. This should "singleton merge" the large segment
    // created above. 60% leaves some wriggle room, LUCENE-8263 will change this assumption and should be tested
    // when we deal with that JIRA.

    deletedThisPass = deletePctDocsFromEachSeg(w, (w.numDocs() * 60) / 100, true);
    remainingDocs -= deletedThisPass;

    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + i + numDocs, Field.Store.NO));
      doc.add(newTextField("content", "aaa " + i, Field.Store.NO));
      w.addDocument(doc);
    }

    w.commit(); // want to trigger merge no matter what.

    assertEquals("There should be exactly one very large and one small segment", w.segmentInfos.size(), 2);
    SegmentCommitInfo info0 = w.segmentInfos.info(0);
    SegmentCommitInfo info1 = w.segmentInfos.info(1);
    int largeSegDocCount = Math.max(info0.info.maxDoc(), info1.info.maxDoc());
    int smallSegDocCount = Math.min(info0.info.maxDoc(), info1.info.maxDoc());
    assertEquals("The large segment should have a bunch of docs", largeSegDocCount, remainingDocs);
    assertEquals("Small segment shold have fewer docs", smallSegDocCount, 50);

    w.close();

    dir.close();
  }

  // Having a segment with very few documents in it can happen because of the random nature of the
  // docs added to the index. For instance, let's say it just happens that the last segment has 3 docs in it.
  // It can easily be merged with a close-to-max sized segment during a forceMerge and still respect the max segment
  // size.
  //
  // If the above is possible, the "twoMayHaveBeenMerged" will be true and we allow for a little slop, checking that
  // exactly two segments are gone from the old list and exactly one is in the new list. Otherwise, the lists must match
  // exactly.
  //
  // So forceMerge may not be a no-op, allow for that. There are two possibilities in forceMerge only:
  // > there were no small segments, in which case the two lists will be identical
  // > two segments in the original list are replaced by one segment in the final list.
  //
  // finally, there are some cases of forceMerge where the expectation is that there be exactly no differences.
  // this should be called after forceDeletesMerges with the boolean always false,
  // Depending on the state, forceMerge may call with the boolean true or false.

  void checkSegmentsInExpectations(IndexWriter w, List<String> segNamesBefore, boolean twoMayHaveBeenMerged) {

    List<String> segNamesAfter = getSegmentNames(w);

    if (twoMayHaveBeenMerged == false || segNamesAfter.size() == segNamesBefore.size()) {
      if (segNamesAfter.size() != segNamesBefore.size()) {
        fail("Segment lists different sizes!: " + segNamesBefore.toString() + " After list: " + segNamesAfter.toString());
      }

      if (segNamesAfter.containsAll(segNamesBefore) == false) {
        fail("Segment lists should be identical: " + segNamesBefore.toString() + " After list: " + segNamesAfter.toString());
      }
      return;
    }

    // forceMerge merged a tiny segment into a not-quite-max-sized segment so check that:
    // Two segments in the before list have been merged into one segment in the after list.
    if (segNamesAfter.size() != segNamesBefore.size() - 1) {
      fail("forceMerge didn't merge a small and large segment into one segment as expected: "
          + segNamesBefore.toString() + " After list: " + segNamesAfter.toString());
    }


    // There shold be exactly two segments in the before not in after and one in after not in before.
    List<String> testBefore = new ArrayList<>(segNamesBefore);
    List<String> testAfter = new ArrayList<>(segNamesAfter);

    testBefore.removeAll(segNamesAfter);
    testAfter.removeAll(segNamesBefore);

    if (testBefore.size() != 2 || testAfter.size() != 1) {
      fail("Segment lists different sizes!: " + segNamesBefore.toString() + " After list: " + segNamesAfter.toString());
    }
  }

  List<String> getSegmentNames(IndexWriter w) {
    List<String> names = new ArrayList<>();
    for (SegmentCommitInfo info : w.segmentInfos) {
      names.add(info.info.name);
    }
    return names;
  }

  // Deletes some docs from each segment
  int deletePctDocsFromEachSeg(IndexWriter w, int pct, boolean roundUp) throws IOException {
    IndexReader reader = DirectoryReader.open(w);
    List<Term> toDelete = new ArrayList<>();
    for (LeafReaderContext ctx : reader.leaves()) {
      toDelete.addAll(getRandTerms(ctx, pct, roundUp));
    }
    reader.close();

    Term[] termsToDel = new Term[toDelete.size()];
    toDelete.toArray(termsToDel);
    w.deleteDocuments(termsToDel);
    w.commit();
    return toDelete.size();
  }

  // Get me some Ids to delete.
  // So far this supposes that there are no deleted docs in the segment.
  // When the numbers of docs in segments is small, rounding matters. So tests that want over a percentage
  // pass "true" for roundUp, tests that want to be sure they're under some limit pass false.
  private List<Term> getRandTerms(LeafReaderContext ctx, int pct, boolean roundUp) throws IOException {

    assertFalse("This method assumes no deleted documents", ctx.reader().hasDeletions());
    // The indeterminate last segment is a pain, if we're there the number of docs is much less than we expect
    List<Term> ret = new ArrayList<>(100);

    double numDocs = ctx.reader().numDocs();
    double tmp = (numDocs * (double) pct) / 100.0;

    if (tmp <= 1.0) { // Calculations break down for segments with very few documents, the "tail end Charlie"
      return ret;
    }
    int mod = (int) (numDocs / tmp);

    if (mod == 0) return ret;

    Terms terms = ctx.reader().terms("id");
    TermsEnum iter = terms.iterator();
    int counter = 0;

    // Small numbers are tricky, they're subject to off-by-one errors. bail if we're going to exceed our target if we add another doc.
    int lim = (int) (numDocs * (double) pct / 100.0);
    if (roundUp) ++lim;

    for (BytesRef br = iter.next(); br != null && ret.size() < lim; br = iter.next()) {
      if ((counter % mod) == 0) {
        ret.add(new Term("id", br));
      }
      ++counter;
    }
    return ret;
  }

  private void checkSegmentSizeNotExceeded(SegmentInfos infos, long maxSegBytes) throws IOException {
    for (SegmentCommitInfo info : infos) {
      //assertTrue("Found an unexpectedly large segment: " + info.toString(), info.info.maxDoc() - info.getDelCount() <= docLim);
      assertTrue("Found an unexpectedly large segment: " + info.toString(), info.sizeInBytes() <= maxSegBytes);
    }
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
    
    expectThrows(IllegalArgumentException.class, () -> {
      tmp.setMaxMergedSegmentMB(-2.0);
    });
    
    tmp.setFloorSegmentMB(2.0);
    assertEquals(2.0, tmp.getFloorSegmentMB(), EPSILON);
    
    tmp.setFloorSegmentMB(Double.POSITIVE_INFINITY);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getFloorSegmentMB(), EPSILON*Long.MAX_VALUE);
    
    tmp.setFloorSegmentMB(Long.MAX_VALUE/1024/1024.);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getFloorSegmentMB(), EPSILON*Long.MAX_VALUE);
    
    expectThrows(IllegalArgumentException.class, () -> {
      tmp.setFloorSegmentMB(-2.0);
    });
    
    tmp.setMaxCFSSegmentSizeMB(2.0);
    assertEquals(2.0, tmp.getMaxCFSSegmentSizeMB(), EPSILON);
    
    tmp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getMaxCFSSegmentSizeMB(), EPSILON*Long.MAX_VALUE);
    
    tmp.setMaxCFSSegmentSizeMB(Long.MAX_VALUE/1024/1024.);
    assertEquals(Long.MAX_VALUE/1024/1024., tmp.getMaxCFSSegmentSizeMB(), EPSILON*Long.MAX_VALUE);
    
    expectThrows(IllegalArgumentException.class, () -> {
      tmp.setMaxCFSSegmentSizeMB(-2.0);
    });
    
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
