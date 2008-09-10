package org.apache.lucene.search;

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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.BitSet;

/**
 * Tests the TimeLimitedCollector.  This test checks (1) search
 * correctness (regardless of timeout), (2) expected timeout behavior,
 * and (3) a sanity test with multiple searching threads.
 */
public class TestTimeLimitedCollector extends LuceneTestCase {
  private static final int SLOW_DOWN = 47;
  private static final long TIME_ALLOWED = 17 * SLOW_DOWN; // so searches can find about 17 docs.
  
  // max time allowed is relaxed for multithreading tests. 
  // the multithread case fails when setting this to 1 (no slack) and launching many threads (>2000).  
  // but this is not a real failure, just noise.
  private static final double MULTI_THREAD_SLACK = 7;      
            
  private static final int N_DOCS = 3000;
  private static final int N_THREADS = 50;

  private Searcher searcher;
  private final String FIELD_NAME = "body";
  private Query query;

  public TestTimeLimitedCollector(String name) {
    super(name);
  }

  /**
   * initializes searcher with a document set
   */
  protected void setUp() throws Exception {
    final String docText[] = {
        "docThatNeverMatchesSoWeCanRequireLastDocCollectedToBeGreaterThanZero",
        "one blah three",
        "one foo three multiOne",
        "one foobar three multiThree",
        "blueberry pancakes",
        "blueberry pie",
        "blueberry strudel",
        "blueberry pizza",
    };
    Directory directory = new RAMDirectory();
    IndexWriter iw = new IndexWriter(directory, new WhitespaceAnalyzer(), true, MaxFieldLength.UNLIMITED);
    
    for (int i=0; i<N_DOCS; i++) {
      add(docText[i%docText.length], iw);
    }
    iw.close();
    searcher = new IndexSearcher(directory);

    String qtxt = "one";
    for (int i = 0; i < docText.length; i++) {
      qtxt += ' ' + docText[i]; // large query so that search will be longer
    }
    QueryParser queryParser = new QueryParser(FIELD_NAME, new WhitespaceAnalyzer());
    query = queryParser.parse(qtxt);
    
    // warm the searcher
    searcher.search(query, null, 1000);

  }

  public void tearDown() throws Exception {
    searcher.close();
  }

  private void add(String value, IndexWriter iw) throws IOException {
    Document d = new Document();
    d.add(new Field(FIELD_NAME, value, Field.Store.NO, Field.Index.ANALYZED));
    iw.addDocument(d);
  }

  private void search(HitCollector collector) throws Exception {
    searcher.search(query, collector);
  }

  /**
   * test search correctness with no timeout
   */
  public void testSearch() {
    doTestSearch();
  }
  
  private void doTestSearch() {
    int totalResults = 0;
    int totalTLCResults = 0;
    try {
      MyHitCollector myHc = new MyHitCollector();
      search(myHc);
      totalResults = myHc.hitCount();
      
      myHc = new MyHitCollector();
      long oneHour = 3600000;
      HitCollector tlCollector = createTimedCollector(myHc, oneHour, false);
      search(tlCollector);
      totalTLCResults = myHc.hitCount();
    } catch (Exception e) {
      assertTrue("Unexpected exception: "+e, false); //==fail
    }
    assertEquals( "Wrong number of results!", totalResults, totalTLCResults );
  }

  private HitCollector createTimedCollector(MyHitCollector hc, long timeAllowed, boolean greedy) {
    TimeLimitedCollector res = new TimeLimitedCollector(hc, timeAllowed);
    res.setGreedy(greedy); // set to true to make sure at least one doc is collected.
    return res;
  }

  /**
   * Test that timeout is obtained, and soon enough!
   */
  public void testTimeoutGreedy() {
    doTestTimeout(false, true);
  }
  
  /**
   * Test that timeout is obtained, and soon enough!
   */
  public void testTimeoutNotGreedy() {
    doTestTimeout(false, false);
  }

  private void doTestTimeout(boolean multiThreaded, boolean greedy) {
    // setup
    MyHitCollector myHc = new MyHitCollector();
    myHc.setSlowDown(SLOW_DOWN);
    HitCollector tlCollector = createTimedCollector(myHc, TIME_ALLOWED, greedy);

    // search
    TimeLimitedCollector.TimeExceededException timoutException = null;
    try {
      search(tlCollector);
    } catch (TimeLimitedCollector.TimeExceededException x) {
      timoutException = x;
    } catch (Exception e) {
      assertTrue("Unexpected exception: "+e, false); //==fail
    }
    
    // must get exception
    assertNotNull( "Timeout expected!", timoutException );

    // greediness affect last doc collected
    int exceptionDoc = timoutException.getLastDocCollected();
    int lastCollected = myHc.getLastDocCollected(); 
    assertTrue( "doc collected at timeout must be > 0!", exceptionDoc > 0 );
    if (greedy) {
      assertTrue("greedy="+greedy+" exceptionDoc="+exceptionDoc+" != lastCollected="+lastCollected, exceptionDoc==lastCollected);
      assertTrue("greedy, but no hits found!", myHc.hitCount() > 0 );
    } else {
      assertTrue("greedy="+greedy+" exceptionDoc="+exceptionDoc+" not > lastCollected="+lastCollected, exceptionDoc>lastCollected);
    }

    // verify that elapsed time at exception is within valid limits
    assertEquals( timoutException.getTimeAllowed(), TIME_ALLOWED);
    // a) Not too early
    assertTrue ( "elapsed="+timoutException.getTimeElapsed()+" <= (allowed-resolution)="+(TIME_ALLOWED-TimeLimitedCollector.getResolution()),
        timoutException.getTimeElapsed() > TIME_ALLOWED-TimeLimitedCollector.getResolution());
    // b) Not too late.
    //    This part is problematic in a busy test system, so we just print a warning.
    //    We already verified that a timeout occurred, we just can't be picky about how long it took.
    if (timoutException.getTimeElapsed() > maxTime(multiThreaded)) {
      System.out.println("Informative: timeout exceeded (no action required: most probably just " +
        " because the test machine is slower than usual):  " +
        "lastDoc="+exceptionDoc+
        " ,&& allowed="+timoutException.getTimeAllowed() +
        " ,&& elapsed="+timoutException.getTimeElapsed() +
        " >= " + maxTimeStr(multiThreaded));
    }
  }

  private long maxTime(boolean multiThreaded) {
    long res = 2 * TimeLimitedCollector.getResolution() + TIME_ALLOWED + SLOW_DOWN; // some slack for less noise in this test
    if (multiThreaded) {
      res *= MULTI_THREAD_SLACK; // larger slack  
    }
    return res;
  }

  private String maxTimeStr(boolean multiThreaded) {
    String s =
      "( " +
      "2*resolution +  TIME_ALLOWED + SLOW_DOWN = " +
      "2*" + TimeLimitedCollector.getResolution() + " + " + TIME_ALLOWED + " + " + SLOW_DOWN +
      ")";
    if (multiThreaded) {
      s = MULTI_THREAD_SLACK + " * "+s;  
    }
    return maxTime(multiThreaded) + " = " + s;
  }

  /**
   * Test timeout behavior when resolution is modified. 
   */
  public void testModifyResolution() {
    try {
      // increase and test
      long resolution = 20 * TimeLimitedCollector.DEFAULT_RESOLUTION; //400
      TimeLimitedCollector.setResolution(resolution);
      assertEquals(resolution, TimeLimitedCollector.getResolution());
      doTestTimeout(false,true);
      // decrease much and test
      resolution = 5;
      TimeLimitedCollector.setResolution(resolution);
      assertEquals(resolution, TimeLimitedCollector.getResolution());
      doTestTimeout(false,true);
      // return to default and test
      resolution = TimeLimitedCollector.DEFAULT_RESOLUTION;
      TimeLimitedCollector.setResolution(resolution);
      assertEquals(resolution, TimeLimitedCollector.getResolution());
      doTestTimeout(false,true);
    } finally {
      TimeLimitedCollector.setResolution(TimeLimitedCollector.DEFAULT_RESOLUTION);
    }
  }
  
  /** 
   * Test correctness with multiple searching threads.
   */
  public void testSearchMultiThreaded() {
    doTestMultiThreads(false);
  }

  /** 
   * Test correctness with multiple searching threads.
   */
  public void testTimeoutMultiThreaded() {
    doTestMultiThreads(true);
  }
  
  private void doTestMultiThreads(final boolean withTimeout) {
    Thread [] threadArray = new Thread[N_THREADS];
    final BitSet success = new BitSet(N_THREADS);
    for( int i = 0; i < threadArray.length; ++i ) {
      final int num = i;
      threadArray[num] = new Thread() {
          public void run() {
            if (withTimeout) {
              doTestTimeout(true,true);
            } else {
              doTestSearch();
            }
            synchronized(success) {
              success.set(num);
            }
          }
      };
    }
    for( int i = 0; i < threadArray.length; ++i ) {
      threadArray[i].start();
    }
    boolean interrupted = false;
    for( int i = 0; i < threadArray.length; ++i ) {
      try {
        threadArray[i].join();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    assertEquals("some threads failed!", N_THREADS,success.cardinality());
  }
  
  // counting hit collector that can slow down at collect().
  private class MyHitCollector extends HitCollector
  {
    private final BitSet bits = new BitSet();
    private int slowdown = 0;
    private int lastDocCollected = -1;

    /**
     * amount of time to wait on each collect to simulate a long iteration
     */
    public void setSlowDown( int milliseconds ) {
      slowdown = milliseconds;
    }
    
    public void collect( final int doc, final float score ) {
      if( slowdown > 0 ) {
        try {
          Thread.sleep(slowdown);
        }
        catch(InterruptedException x) {
          System.out.println("caught " + x);
        }
      }
      bits.set( doc );
      lastDocCollected = doc;
    }
    
    public int hitCount() {
      return bits.cardinality();
    }

    public int getLastDocCollected() {
      return lastDocCollected;
    }
    
  }

}
  
