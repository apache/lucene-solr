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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
  private static final int TIME_ALLOWED = 17 * SLOW_DOWN; // so searches can find about 17 docs.
  
  // max time allowed is relaxed for multithreading tests. 
  // the multithread case fails when setting this to 1 (no slack) and launching many threads (>2000).  
  // but this is not a real failure, just noise.
  private static final double MULTI_THREAD_SLACK = 3;      
            
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
        "one blah three",
        "one foo three multiOne",
        "one foobar three multiThree",
        "blueberry pancakes",
        "blueberry pie",
        "blueberry strudel",
        "blueberry pizza",
    };
    Directory directory = new RAMDirectory();
    IndexWriter iw = new IndexWriter(directory, new StandardAnalyzer(), true, MaxFieldLength.UNLIMITED);
    
    for (int i=0; i<N_DOCS; i++) {
      add(docText[i%docText.length], iw);
    }
    iw.close();
    searcher = new IndexSearcher(directory);

    String qtxt = "one";
    for (int i = 0; i < docText.length; i++) {
      qtxt += ' ' + docText[i]; // large query so that search will be longer
    }
    QueryParser queryParser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
    query = queryParser.parse(qtxt);
    
    // warm the searcher
    searcher.search(query);

  }

  public void tearDown() throws Exception {
    searcher.close();
  }

  private void add(String value, IndexWriter iw) throws IOException {
    Document d = new Document();
    d.add(new Field(FIELD_NAME, value, Field.Store.NO, Field.Index.TOKENIZED));
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
      HitCollector tlCollector = new TimeLimitedCollector(myHc, 3600000); // 1 hour
      search(tlCollector);
      totalTLCResults = myHc.hitCount();
    } catch (Exception e) {
      assertTrue("Unexpected exception: "+e, false); //==fail
    }
    assertEquals( "Wrong number of results!", totalResults, totalTLCResults );
  }

  /**
   * Test that timeout is obtained, and soon enough!
   */
  public void testTimeout() {
    doTestTimeout(false);
  }
  
  private void doTestTimeout(boolean multiThreaded) {
    MyHitCollector myHc = new MyHitCollector();
    myHc.setSlowDown(SLOW_DOWN);
    HitCollector tlCollector = new TimeLimitedCollector(myHc, TIME_ALLOWED);

    TimeLimitedCollector.TimeExceededException exception = null;
    try {
      search(tlCollector);
    } catch (TimeLimitedCollector.TimeExceededException x) {
      exception = x;
    } catch (Exception e) {
      assertTrue("Unexpected exception: "+e, false); //==fail
    }
    assertNotNull( "Timeout expected!", exception );
    assertTrue( "no hits found!", myHc.hitCount() > 0 );
    assertTrue( "last doc collected cannot be 0!", exception.getLastDocCollected() > 0 );
    assertEquals( exception.getTimeAllowed(), TIME_ALLOWED);
    assertTrue ( "elapsed="+exception.getTimeElapsed()+" <= (allowed-resolution)="+(TIME_ALLOWED-TimeLimitedCollector.getResolution()),
        exception.getTimeElapsed() > TIME_ALLOWED-TimeLimitedCollector.getResolution());
    assertTrue ( "lastDoc="+exception.getLastDocCollected()+
        " ,&& allowed="+exception.getTimeAllowed() +
        " ,&& elapsed="+exception.getTimeElapsed() +
        " >= " + maxTimeStr(multiThreaded),
        exception.getTimeElapsed() < maxTime(multiThreaded));
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
      doTestTimeout(false);
      // decrease much and test
      resolution = 5;
      TimeLimitedCollector.setResolution(resolution);
      assertEquals(resolution, TimeLimitedCollector.getResolution());
      doTestTimeout(false);
      // return to default and test
      resolution = TimeLimitedCollector.DEFAULT_RESOLUTION;
      TimeLimitedCollector.setResolution(resolution);
      assertEquals(resolution, TimeLimitedCollector.getResolution());
      doTestTimeout(false);
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
              doTestTimeout(true);
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
    }
    
    public int hitCount() {
      return bits.cardinality();
    }
    
  }

}
  
