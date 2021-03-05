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
package org.apache.solr.update;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.TestHarness;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test auto commit functionality in a way that doesn't suck.
 * <p>
 * AutoCommitTest is an abomination that is way to brittle in how it 
 * tries to check that commits happened, and when they happened.
 * The goal of this test class is to (ultimately) completely replace all 
 * of the functionality of that test class using:
 * </p>
 * <ul>
 *   <li>A more robust monitor of commit/newSearcher events that records 
 *       the times of those events in a queue that can be polled.  
 *       Multiple events in rapid succession are not lost.
 *   </li>
 *   <li>Timing checks that are forgiving of slow machines and use 
 *       knowledge of how slow A-&gt;B was to affect the expectation of 
 *       how slow B-&gt;C will be
 *   </li>
 * </ul>
 */
@Slow
public class SoftAutoCommitTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  private MockEventListener monitor;
  private DirectUpdateHandler2 updater;
    
  @Before
  public void createMonitor() throws Exception {
    assumeFalse("This test is not working on Windows (or maybe machines with only 2 CPUs)",
      Constants.WINDOWS);
  
    SolrCore core = h.getCore();

    updater = (DirectUpdateHandler2) core.getUpdateHandler();
    updater.setCommitWithinSoftCommit(true); // foce to default, let tests change as needed
    monitor = new MockEventListener();

    core.registerNewSearcherListener(monitor);
    updater.registerSoftCommitCallback(monitor);
    updater.registerCommitCallback(monitor);

    // isolate searcher getting ready from this test
    monitor.searcher.poll(5000, MILLISECONDS);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();

  }
  
  public void testSoftAndHardCommitMaxDocs() throws Exception {

    // NOTE WHEN READING THIS TEST...
    // The maxDocs settings on the CommitTrackers are the "upper bound"
    // of how many docs can be added with out doing a commit.
    // That means they are one less then the actual number of docs that will trigger a commit.
    final int softCommitMaxDocs = 5;
    final int hardCommitMaxDocs = 7;

    assert softCommitMaxDocs < hardCommitMaxDocs; // remainder of test designed with these assumptions
    
    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    
    // wait out any leaked commits
    monitor.hard.poll(3000, MILLISECONDS);
    monitor.soft.poll(0, MILLISECONDS);
    monitor.clear();
    
    softTracker.setDocsUpperBound(softCommitMaxDocs);
    softTracker.setTimeUpperBound(-1);
    hardTracker.setDocsUpperBound(hardCommitMaxDocs);
    hardTracker.setTimeUpperBound(-1);
    // simplify whats going on by only having soft auto commits trigger new searchers
    hardTracker.setOpenSearcher(false);

    // Note: doc id counting starts at 0, see comment at start of test regarding "upper bound"

    // add num docs up to the soft commit upper bound
    for (int i = 0; i < softCommitMaxDocs; i++) {
      assertU(adoc("id", ""+(8000 + i), "subject", "testMaxDocs"));
    }
    // the first soft commit we see must be after this.
    final long minSoftCommitNanos = System.nanoTime();
    
    // now add the doc that will trigger the soft commit,
    // as well as additional docs up to the hard commit upper bound
    for (int i = softCommitMaxDocs; i < hardCommitMaxDocs; i++) {
      assertU(adoc("id", ""+(8000 + i), "subject", "testMaxDocs"));
    }
    // the first hard commit we see must be after this.
    final long minHardCommitNanos = System.nanoTime();

    // a final doc to trigger the hard commit
    assertU(adoc("id", ""+(8000 + hardCommitMaxDocs), "subject", "testMaxDocs"));

    // now poll our monitors for the timestamps on the first commits
    final Long firstSoftNanos = monitor.soft.poll(5000, MILLISECONDS);
    final Long firstHardNanos = monitor.hard.poll(5000, MILLISECONDS);

    assertNotNull("didn't get a single softCommit after adding the max docs", firstSoftNanos);
    assertNotNull("didn't get a single hardCommit after adding the max docs", firstHardNanos);
                  
    assertTrue("softCommit @ " + firstSoftNanos + "ns is before the maxDocs should have triggered it @ " +
               minSoftCommitNanos + "ns",
               minSoftCommitNanos < firstSoftNanos);
    assertTrue("hardCommit @ " + firstHardNanos + "ns is before the maxDocs should have triggered it @ " +
               minHardCommitNanos + "ns",
               minHardCommitNanos < firstHardNanos);

    // wait a bit, w/o other action we shouldn't see any new hard/soft commits 
    assertNull("Got a hard commit we weren't expecting",
               monitor.hard.poll(1000, MILLISECONDS));
    assertNull("Got a soft commit we weren't expecting",
               monitor.soft.poll(0, MILLISECONDS));
    
    monitor.assertSaneOffers();
    monitor.clear();
  }

  public void testSoftAndHardCommitMaxTimeMixedAdds() throws Exception {
   doTestSoftAndHardCommitMaxTimeMixedAdds(CommitWithinType.NONE);
  }
  public void testSoftCommitWithinAndHardCommitMaxTimeMixedAdds() throws Exception {
    doTestSoftAndHardCommitMaxTimeMixedAdds(CommitWithinType.SOFT);
  }
  public void testHardCommitWithinAndSoftCommitMaxTimeMixedAdds() throws Exception {
    doTestSoftAndHardCommitMaxTimeMixedAdds(CommitWithinType.HARD);
  }
  private void doTestSoftAndHardCommitMaxTimeMixedAdds(final CommitWithinType commitWithinType)
    throws Exception {
    
    final int softCommitWaitMillis = 500;
    final int hardCommitWaitMillis = 1200;
    final int commitWithin = commitWithinType.useValue(softCommitWaitMillis, hardCommitWaitMillis);
    
    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    updater.setCommitWithinSoftCommit(commitWithinType.equals(CommitWithinType.SOFT));
    
    // wait out any leaked commits
    monitor.soft.poll(softCommitWaitMillis * 2, MILLISECONDS);
    monitor.hard.poll(hardCommitWaitMillis * 2, MILLISECONDS);
    
    int startingHardCommits = hardTracker.getCommitCount();
    int startingSoftCommits = softTracker.getCommitCount();
    
    softTracker.setTimeUpperBound(commitWithinType.equals(CommitWithinType.SOFT) ? -1 : softCommitWaitMillis);
    softTracker.setDocsUpperBound(-1);
    hardTracker.setTimeUpperBound(commitWithinType.equals(CommitWithinType.HARD) ? -1 : hardCommitWaitMillis);
    hardTracker.setDocsUpperBound(-1);
    // simplify whats going on by only having soft auto commits trigger new searchers
    hardTracker.setOpenSearcher(false);

    // Add a single document
    long add529 = System.nanoTime();
    assertU(adoc(commitWithin, "id", "529", "subject", "the doc we care about in this test"));

    monitor.assertSaneOffers();

    // Wait for the soft commit with some fudge
    Long soft529 = monitor.soft.poll(softCommitWaitMillis * 5, MILLISECONDS);
    assertNotNull("soft529 wasn't fast enough", soft529);
    monitor.assertSaneOffers();

    
    // wait for the hard commit
    Long hard529 = monitor.hard.poll(hardCommitWaitMillis * 5, MILLISECONDS);
    assertNotNull("hard529 wasn't fast enough", hard529);
    
    // check for the searcher, should have happened right after soft commit
    Long searcher529 = monitor.searcher.poll(5000, MILLISECONDS);
    assertNotNull("searcher529 wasn't fast enough", searcher529);
    monitor.assertSaneOffers();

    // toss in another doc, shouldn't affect first hard commit time we poll
    assertU(adoc(commitWithin, "id", "530", "subject", "just for noise/activity"));


    monitor.assertSaneOffers();

    final long soft529Ms = TimeUnit.MILLISECONDS.convert(soft529 - add529, TimeUnit.NANOSECONDS);
    assertTrue("soft529 occurred too fast, in " +
            soft529Ms + "ms, less than soft commit interval " + softCommitWaitMillis,
        soft529Ms >= softCommitWaitMillis);
    final long hard529Ms = TimeUnit.MILLISECONDS.convert(hard529 - add529, TimeUnit.NANOSECONDS);
    assertTrue("hard529 occurred too fast, in " +
            hard529Ms + "ms, less than hard commit interval " + hardCommitWaitMillis,
        hard529Ms >= hardCommitWaitMillis);

    // however slow the machine was to do the soft commit compared to expected,
    // assume newSearcher had some magnitude of that much overhead as well 
    long slowTestFudge = Math.max(300, 12 * (soft529Ms - softCommitWaitMillis));
    final long softCommitToSearcherOpenMs = TimeUnit.MILLISECONDS.convert(searcher529 - soft529, TimeUnit.NANOSECONDS);
    assertTrue("searcher529 wasn't soon enough after soft529: Took " +
            softCommitToSearcherOpenMs + "ms, >= acceptable " + slowTestFudge + "ms (fudge)",
        softCommitToSearcherOpenMs < slowTestFudge);

    assertTrue("hard529 was before searcher529: " +
               searcher529 + " !<= " + hard529,
               searcher529 <= hard529);

    monitor.assertSaneOffers();

    // there may have been (or will be) a second hard commit for 530
    Long hard530 = monitor.hard.poll(hardCommitWaitMillis * 5, MILLISECONDS);
    assertEquals("Tracker reports too many hard commits",
                 (null == hard530 ? 1 : 2),
                 hardTracker.getCommitCount() - startingHardCommits);

    // there may have been a second soft commit for 530, 
    // but if so it must have already happend
    Long soft530 = monitor.soft.poll(0, MILLISECONDS);
    if (null != soft530) {
      assertEquals("Tracker reports too many soft commits",
                   2, softTracker.getCommitCount() - startingSoftCommits);
      if (null != hard530) {
        assertTrue("soft530 after hard530: " +
                   soft530 + " !<= " + hard530,
                   soft530 <= hard530);
      } else {
        assertTrue("soft530 after hard529 but no hard530: " +
                   soft530 + " !<= " + hard529,
                   soft530 <= hard529);
      }
    } else {
      assertEquals("Tracker reports too many soft commits",
                   1, softTracker.getCommitCount() - startingSoftCommits);
    }
      
    if (null != soft530 || null != hard530) {
      assertNotNull("at least one extra commit for 530, but no searcher",
                    monitor.searcher.poll(0, MILLISECONDS));
    }

    // clear commits
    monitor.hard.clear();
    monitor.soft.clear();

    // wait a bit, w/o other action we shouldn't see any 
    // new hard/soft commits 
    assertNull("Got a hard commit we weren't expecting",
               monitor.hard.poll(1000, MILLISECONDS));
    assertNull("Got a soft commit we weren't expecting",
               monitor.soft.poll(0, MILLISECONDS));

    monitor.assertSaneOffers();
    monitor.searcher.clear();
  }

  public void testSoftAndHardCommitMaxTimeDelete() throws Exception {
    doTestSoftAndHardCommitMaxTimeDelete(CommitWithinType.NONE);
  }
  public void testSoftCommitWithinAndHardCommitMaxTimeDelete() throws Exception {
    doTestSoftAndHardCommitMaxTimeDelete(CommitWithinType.SOFT);
  }
  public void testHardCommitWithinAndSoftCommitMaxTimeDelete() throws Exception {
    doTestSoftAndHardCommitMaxTimeDelete(CommitWithinType.HARD);
  }
  private void doTestSoftAndHardCommitMaxTimeDelete(final CommitWithinType commitWithinType)
    throws Exception {
    
    final int softCommitWaitMillis = 500;
    final int hardCommitWaitMillis = 1200;
    final int commitWithin = commitWithinType.useValue(softCommitWaitMillis, hardCommitWaitMillis);

    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    updater.setCommitWithinSoftCommit(commitWithinType.equals(CommitWithinType.SOFT));
    
    int startingHardCommits = hardTracker.getCommitCount();
    int startingSoftCommits = softTracker.getCommitCount();
    
    softTracker.setTimeUpperBound(commitWithinType.equals(CommitWithinType.SOFT) ? -1 : softCommitWaitMillis);
    softTracker.setDocsUpperBound(-1);
    hardTracker.setTimeUpperBound(commitWithinType.equals(CommitWithinType.HARD) ? -1 : hardCommitWaitMillis);
    hardTracker.setDocsUpperBound(-1);
    // we don't want to overlap soft and hard opening searchers - this now blocks commits and we
    // are looking for prompt timings
    hardTracker.setOpenSearcher(false);
    
    // add a doc and force a commit
    assertU(adoc(commitWithin, "id", "529", "subject", "the doc we care about in this test"));
    assertU(commit());

    Long soft529;
    Long hard529;

    monitor.clear();

    // Delete the document
    long del529 = System.nanoTime();
    assertU( delI("529", "commitWithin", ""+commitWithin));

    monitor.assertSaneOffers();

    // Wait for the soft commit with some fudge
    soft529 = monitor.soft.poll(softCommitWaitMillis * 5, MILLISECONDS);
    assertNotNull("soft529 wasn't fast enough", soft529);
    monitor.assertSaneOffers();
 
    // check for the searcher, should have happened right after soft commit
    Long searcher529 = monitor.searcher.poll(softCommitWaitMillis, MILLISECONDS);
    assertNotNull("searcher529 wasn't fast enough", searcher529);
    monitor.assertSaneOffers();

    // toss in another doc, shouldn't affect first hard commit time we poll
    assertU(adoc(commitWithin, "id", "550", "subject", "just for noise/activity"));

    // wait for the hard commit
    hard529 = monitor.hard.poll(hardCommitWaitMillis * 5, MILLISECONDS);
    assertNotNull("hard529 wasn't fast enough", hard529);
    monitor.assertSaneOffers();

    final long soft529Ms = TimeUnit.MILLISECONDS.convert(soft529 - del529, TimeUnit.NANOSECONDS);
    assertTrue("soft529 occurred too fast, in " + soft529Ms +
            "ms, less than soft commit interval " + softCommitWaitMillis,
        soft529Ms >= softCommitWaitMillis);
    final long hard529Ms = TimeUnit.MILLISECONDS.convert(hard529 - del529, TimeUnit.NANOSECONDS);
    assertTrue("hard529 occurred too fast, in " +
            hard529Ms + "ms, less than hard commit interval " + hardCommitWaitMillis,
        hard529Ms >= hardCommitWaitMillis);

    // however slow the machine was to do the soft commit compared to expected,
    // assume newSearcher had some magnitude of that much overhead as well
    long slowTestFudge = Math.max(300, 12 * (soft529Ms - softCommitWaitMillis));
    final long softCommitToSearcherOpenMs = TimeUnit.MILLISECONDS.convert(searcher529 - soft529, TimeUnit.NANOSECONDS);
    assertTrue("searcher529 wasn't soon enough after soft529: Took " +
            softCommitToSearcherOpenMs + "ms, >= acceptable " + slowTestFudge + "ms (fudge)",
        softCommitToSearcherOpenMs < slowTestFudge);

    assertTrue("hard529 was before searcher529: " +
               searcher529 + " !<= " + hard529,
               searcher529 <= hard529);

    // ensure we wait for the last searcher we triggered with 550
    monitor.searcher.poll(5000, MILLISECONDS);
    
    // ensure we wait for the commits on 550
    monitor.hard.poll(5000, MILLISECONDS);
    monitor.soft.poll(5000, MILLISECONDS);
    
    // clear commits
    monitor.hard.clear();
    monitor.soft.clear();
    
    // wait a bit, w/o other action we shouldn't see any 
    // new hard/soft commits 
    assertNull("Got a hard commit we weren't expecting",
        monitor.hard.poll(1000, MILLISECONDS));
    assertNull("Got a soft commit we weren't expecting",
        monitor.soft.poll(0, MILLISECONDS));

    monitor.assertSaneOffers();
    
    monitor.searcher.clear();
  }

  public void testSoftAndHardCommitMaxTimeRapidAdds() throws Exception {
    doTestSoftAndHardCommitMaxTimeRapidAdds(CommitWithinType.NONE);
  }
  public void testSoftCommitWithinAndHardCommitMaxTimeRapidAdds() throws Exception {
    doTestSoftAndHardCommitMaxTimeRapidAdds(CommitWithinType.SOFT);
  }
  public void testHardCommitWithinAndSoftCommitMaxTimeRapidAdds() throws Exception {
    doTestSoftAndHardCommitMaxTimeRapidAdds(CommitWithinType.HARD);
  }
  public void doTestSoftAndHardCommitMaxTimeRapidAdds(final CommitWithinType commitWithinType)
    throws Exception {
 
    final int softCommitWaitMillis = 500;
    final int hardCommitWaitMillis = 1200;
    final int commitWithin = commitWithinType.useValue(softCommitWaitMillis, hardCommitWaitMillis);

    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    updater.setCommitWithinSoftCommit(commitWithinType.equals(CommitWithinType.SOFT));
    
    softTracker.setTimeUpperBound(commitWithinType.equals(CommitWithinType.SOFT) ? -1 : softCommitWaitMillis);
    softTracker.setDocsUpperBound(-1);
    hardTracker.setTimeUpperBound(commitWithinType.equals(CommitWithinType.HARD) ? -1 : hardCommitWaitMillis);
    hardTracker.setDocsUpperBound(-1);
    // we don't want to overlap soft and hard opening searchers - this now blocks commits and we
    // are looking for prompt timings
    hardTracker.setOpenSearcher(false);
    
    // try to add 5 docs really fast

    final long preFirstNanos = System.nanoTime();
    for( int i=0;i<5; i++ ) {
      assertU(adoc(commitWithin, "id", ""+500 + i, "subject", "five fast docs"));
    }
    final long postLastNanos = System.nanoTime();
    
    monitor.assertSaneOffers();

    final long maxTimeMillis = MILLISECONDS.convert(postLastNanos - preFirstNanos, NANOSECONDS);
    log.info("maxTimeMillis: {}ns - {}ns == {}ms", postLastNanos, preFirstNanos, maxTimeMillis);
    
    // NOTE: explicitly using truncated division of longs to round down
    // even if evenly divisible, need +1 to account for possible "last" commit triggered by "last" doc
    final long maxExpectedSoft = 1L + (maxTimeMillis / softCommitWaitMillis);
    final long maxExpectedHard = 1L + (maxTimeMillis / hardCommitWaitMillis);

    log.info("maxExpectedSoft={}", maxExpectedSoft);
    log.info("maxExpectedHard={}", maxExpectedHard);

    // do a loop pool over each monitor queue, asserting that:
    // - we get at least one commit
    // - we don't get more then the max possible commits expected
    // - any commit we do get doesn't happen "too fast" relative the previous commit
    //   (or first doc added for the first commit)
    monitor.assertSaneOffers();
    assertRapidMultiCommitQueues("softCommit", preFirstNanos, softCommitWaitMillis,
                                 maxExpectedSoft, monitor.soft);
    monitor.assertSaneOffers();
    assertRapidMultiCommitQueues("hardCommit", preFirstNanos, hardCommitWaitMillis,
                                 maxExpectedHard, monitor.hard);

    // now wait a bit...
    // w/o other action we shouldn't see any additional hard/soft commits

    assertNull("Got a hard commit we weren't expecting",
               monitor.hard.poll(1000, MILLISECONDS));
    assertNull("Got a soft commit we weren't expecting",
               monitor.soft.poll(0, MILLISECONDS));

    monitor.assertSaneOffers();
    
  }

  /**
   * Helper method
   * @see #testSoftAndHardCommitMaxTimeRapidAdds
   */
  private static void assertRapidMultiCommitQueues
    (final String debug, final long startTimestampNanos, final long commitWaitMillis,
     final long maxNumCommits, final BlockingQueue<Long> queue) throws InterruptedException {

    assert 0 < maxNumCommits;
    
    // do all our math/comparisons in Nanos...
    final long commitWaitNanos = NANOSECONDS.convert(commitWaitMillis, MILLISECONDS);

    // these will be modified in each iteration of our assertion loop
    long prevTimestampNanos = startTimestampNanos;
    int count = 1;
    Long commitNanos = queue.poll(commitWaitMillis * 6, MILLISECONDS);
    assertNotNull(debug + ": did not find a single commit", commitNanos);
    
    while (null != commitNanos) {
      if (commitNanos < prevTimestampNanos + commitWaitMillis) {
        fail(debug + ": commit#" + count + " has TS too low relative to previous TS & commitWait: " +
             "commitNanos=" + commitNanos + ", prevTimestampNanos=" + prevTimestampNanos +
             ", commitWaitMillis=" + commitWaitMillis);
      }
      if (maxNumCommits < count) {
        fail(debug + ": commit#" + count + " w/ commitNanos=" + commitNanos +
             ", but maxNumCommits=" +maxNumCommits);
      }
      
      prevTimestampNanos = commitNanos;
      count++;
      commitNanos = queue.poll(commitWaitMillis * 3, MILLISECONDS);
    }
  }

  /** enum for indicating if a test should use commitWithin, and if so what type: hard or soft */
  private static enum CommitWithinType {
    NONE {
      @Override public int useValue(final int softCommitWaitMillis, final int hardCommitWaitMillis) {
        return -1;
      }
    },
    SOFT {
      @Override public int useValue(final int softCommitWaitMillis, final int hardCommitWaitMillis) {
        return softCommitWaitMillis;
      }
    },
    HARD {
      @Override public int useValue(final int softCommitWaitMillis, final int hardCommitWaitMillis) {
        return hardCommitWaitMillis;
      }
    };
    public abstract int useValue(final int softCommitWaitMillis, final int hardCommitWaitMillis);
  }

  public String delI(String id, String... args) {
    return TestHarness.deleteById(id, args);
  }

  public String adoc(int commitWithin, String... fieldsAndValues) {
    XmlDoc d = doc(fieldsAndValues);
    return add(d, "commitWithin", String.valueOf(commitWithin));
  }
}

class MockEventListener implements SolrEventListener {

  // use capacity bound Queues just so we're sure we don't OOM 
  public final BlockingQueue<Long> soft = new LinkedBlockingQueue<>(1000);
  public final BlockingQueue<Long> hard = new LinkedBlockingQueue<>(1000);
  public final BlockingQueue<Long> searcher = new LinkedBlockingQueue<>(1000);

  // if non enpty, then at least one offer failed (queues full)
  private StringBuffer fail = new StringBuffer();

  public MockEventListener() { /* NOOP */ }
  
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {}
  
  @Override
  public void newSearcher(SolrIndexSearcher newSearcher,
                          SolrIndexSearcher currentSearcher) {
    Long now = System.nanoTime();
    if (!searcher.offer(now)) fail.append(", newSearcher @ " + now);
  }
  
  @Override
  public void postCommit() {
    Long now = System.nanoTime();
    if (!hard.offer(now)) fail.append(", hardCommit @ " + now);
  }
  
  @Override
  public void postSoftCommit() {
    Long now = System.nanoTime();
    if (!soft.offer(now)) fail.append(", softCommit @ " + now);
  }
  
  public void clear() {
    soft.clear();
    hard.clear();
    searcher.clear();
    fail.setLength(0);
  }
  
  public void assertSaneOffers() {
    assertEquals("Failure of MockEventListener" + fail.toString(), 
                 0, fail.length());
  }
}

