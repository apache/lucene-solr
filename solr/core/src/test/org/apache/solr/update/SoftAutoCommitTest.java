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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

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
public class SoftAutoCommitTest extends AbstractSolrTestCase {


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
    monitor = new MockEventListener();

    core.registerNewSearcherListener(monitor);
    updater.registerSoftCommitCallback(monitor);
    updater.registerCommitCallback(monitor);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // reset stats
    h.getCoreContainer().reload("collection1");
  }

  public void testSoftAndHardCommitMaxTimeMixedAdds() throws Exception {

    final int softCommitWaitMillis = 500;
    final int hardCommitWaitMillis = 1200;

    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    
    softTracker.setTimeUpperBound(softCommitWaitMillis);
    softTracker.setDocsUpperBound(-1);
    hardTracker.setTimeUpperBound(hardCommitWaitMillis);
    hardTracker.setDocsUpperBound(-1);
    
    // Add a single document
    long add529 = System.nanoTime();
    assertU(adoc("id", "529", "subject", "the doc we care about in this test"));

    monitor.assertSaneOffers();

    // Wait for the soft commit with some fudge
    Long soft529 = monitor.soft.poll(softCommitWaitMillis * 2, MILLISECONDS);
    assertNotNull("soft529 wasn't fast enough", soft529);
    monitor.assertSaneOffers();

    // check for the searcher, should have happened right after soft commit
    Long searcher529 = monitor.searcher.poll(softCommitWaitMillis * 3, MILLISECONDS);
    assertNotNull("searcher529 wasn't fast enough", searcher529);
    monitor.assertSaneOffers();

    // toss in another doc, shouldn't affect first hard commit time we poll
    assertU(adoc("id", "530", "subject", "just for noise/activity"));

    // wait for the hard commit
    Long hard529 = monitor.hard.poll(hardCommitWaitMillis * 5, MILLISECONDS);
    assertNotNull("hard529 wasn't fast enough", hard529);
    monitor.assertSaneOffers();

    final long soft529Ms = TimeUnit.MILLISECONDS.convert(soft529 - add529, TimeUnit.NANOSECONDS);
    assertTrue("soft529 occured too fast, in " +
            soft529Ms + "ms, less than soft commit interval " + softCommitWaitMillis,
        soft529Ms >= softCommitWaitMillis);
    final long hard529Ms = TimeUnit.MILLISECONDS.convert(hard529 - add529, TimeUnit.NANOSECONDS);
    assertTrue("hard529 occured too fast, in " +
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
    Long hard530 = monitor.hard.poll(hardCommitWaitMillis, MILLISECONDS);
    assertEquals("Tracker reports too many hard commits",
                 (null == hard530 ? 1 : 2),
                 hardTracker.getCommitCount());

    // there may have been a second soft commit for 530, 
    // but if so it must have already happend
    Long soft530 = monitor.soft.poll(0, MILLISECONDS);
    if (null != soft530) {
      assertEquals("Tracker reports too many soft commits",
                   2, softTracker.getCommitCount());
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
                   1, softTracker.getCommitCount());
    }
      
    if (null != soft530 || null != hard530) {
      assertNotNull("at least one extra commit for 530, but no searcher",
                    monitor.searcher.poll(0, MILLISECONDS));
    }

    monitor.assertSaneOffers();

    // wait a bit, w/o other action we definitley shouldn't see any 
    // new hard/soft commits 
    assertNull("Got a hard commit we weren't expecting",
               monitor.hard.poll(2, SECONDS));
    assertNull("Got a soft commit we weren't expecting",
               monitor.soft.poll(0, MILLISECONDS));

    monitor.assertSaneOffers();
  }

  public void testSoftAndHardCommitMaxTimeDelete() throws Exception {
    
    final int softCommitWaitMillis = 500;
    final int hardCommitWaitMillis = 1200;

    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    
    softTracker.setTimeUpperBound(softCommitWaitMillis);
    softTracker.setDocsUpperBound(-1);
    hardTracker.setTimeUpperBound(hardCommitWaitMillis);
    hardTracker.setDocsUpperBound(-1);
    
    // add a doc and force a commit
    assertU(adoc("id", "529", "subject", "the doc we care about in this test"));
    assertU(commit());

    Long soft529;
    Long hard529;

/*** an explicit commit can (and should) clear pending auto-commits
    long postAdd529 = System.currentTimeMillis();

    // wait for first hard/soft commit
    Long soft529 = monitor.soft.poll(softCommitWaitMillis * 3, MILLISECONDS);
    assertNotNull("soft529 wasn't fast enough", soft529);
    Long manCommit = monitor.hard.poll(0, MILLISECONDS);

    assertNotNull("manCommit wasn't fast enough", manCommit);
    assertTrue("forced manCommit didn't happen when it should have: " + 
        manCommit + " !<= " + postAdd529, 
        manCommit <= postAdd529);
    
    Long hard529 = monitor.hard.poll(hardCommitWaitMillis * 2, MILLISECONDS);
    assertNotNull("hard529 wasn't fast enough", hard529);

    monitor.assertSaneOffers();
 ***/

    monitor.clear();

    // Delete the document
    long del529 = System.nanoTime();
    assertU( delI("529") );

    monitor.assertSaneOffers();

    // Wait for the soft commit with some fudge
    soft529 = monitor.soft.poll(softCommitWaitMillis * 3, MILLISECONDS);
    assertNotNull("soft529 wasn't fast enough", soft529);
    monitor.assertSaneOffers();
 
    // check for the searcher, should have happened right after soft commit
    Long searcher529 = monitor.searcher.poll(softCommitWaitMillis, MILLISECONDS);
    assertNotNull("searcher529 wasn't fast enough", searcher529);
    monitor.assertSaneOffers();

    // toss in another doc, shouldn't affect first hard commit time we poll
    assertU(adoc("id", "550", "subject", "just for noise/activity"));

    // wait for the hard commit
    hard529 = monitor.hard.poll(hardCommitWaitMillis * 3, MILLISECONDS);
    assertNotNull("hard529 wasn't fast enough", hard529);
    monitor.assertSaneOffers();

    final long soft529Ms = TimeUnit.MILLISECONDS.convert(soft529 - del529, TimeUnit.NANOSECONDS);
    assertTrue("soft529 occured too fast, in " + soft529Ms +
            "ms, less than soft commit interval " + softCommitWaitMillis,
        soft529Ms >= softCommitWaitMillis);
    final long hard529Ms = TimeUnit.MILLISECONDS.convert(hard529 - del529, TimeUnit.NANOSECONDS);
    assertTrue("hard529 occured too fast, in " +
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

    // clear commmits
    monitor.hard.clear();
    monitor.soft.clear();
    
    // wait a bit, w/o other action we definitely shouldn't see any 
    // new hard/soft commits 
    assertNull("Got a hard commit we weren't expecting",
               monitor.hard.poll(2, SECONDS));
    assertNull("Got a soft commit we weren't expecting",
               monitor.soft.poll(0, MILLISECONDS));

    monitor.assertSaneOffers();
  }

  public void testSoftAndHardCommitMaxTimeRapidAdds() throws Exception {
 
    final int softCommitWaitMillis = 500;
    final int hardCommitWaitMillis = 1200;

    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    
    softTracker.setTimeUpperBound(softCommitWaitMillis);
    softTracker.setDocsUpperBound(-1);
    hardTracker.setTimeUpperBound(hardCommitWaitMillis);
    hardTracker.setDocsUpperBound(-1);
    
    // try to add 5 docs really fast
    long fast5start = System.nanoTime();
    for( int i=0;i<5; i++ ) {
      assertU(adoc("id", ""+500 + i, "subject", "five fast docs"));
    }
    long fast5end = System.nanoTime() - TimeUnit.NANOSECONDS.convert(200, TimeUnit.MILLISECONDS); // minus a tad of slop
    long fast5time = 1 + TimeUnit.MILLISECONDS.convert(fast5end - fast5start, TimeUnit.NANOSECONDS);

    // total time for all 5 adds determines the number of soft to expect
    long expectedSoft = (long)Math.ceil((double) fast5time / softCommitWaitMillis);
    long expectedHard = (long)Math.ceil((double) fast5time / hardCommitWaitMillis);

    // note: counting from 1 for multiplication
    for (int i = 1; i <= expectedSoft; i++) {
      // Wait for the soft commit with some fudge
      Long soft = monitor.soft.poll(softCommitWaitMillis * 2, MILLISECONDS);
      assertNotNull(i + ": soft wasn't fast enough", soft);
      monitor.assertSaneOffers();

      // have to assume none of the docs were added until
      // very end of the add window
      long softMs = TimeUnit.MILLISECONDS.convert(soft - fast5end, TimeUnit.NANOSECONDS);
      assertTrue(i + ": soft occured too fast: " +
              softMs + " < (" + softCommitWaitMillis + " * " + i + ")",
          softMs >= (softCommitWaitMillis * i));
    }

    // note: counting from 1 for multiplication
    for (int i = 1; i <= expectedHard; i++) {
      // wait for the hard commit, shouldn't need any fudge given 
      // other actions already taken
      Long hard = monitor.hard.poll(hardCommitWaitMillis, MILLISECONDS);
      assertNotNull(i + ": hard wasn't fast enough", hard);
      monitor.assertSaneOffers();
      
      // have to assume none of the docs were added until
      // very end of the add window
      long hardMs = TimeUnit.MILLISECONDS.convert(hard - fast5end, TimeUnit.NANOSECONDS);
      assertTrue(i + ": hard occured too fast: " +
              hardMs + " < (" + hardCommitWaitMillis + " * " + i + ")",
          hardMs >= (hardCommitWaitMillis * i));
    }
 
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
  public void init(NamedList args) {}
  
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

