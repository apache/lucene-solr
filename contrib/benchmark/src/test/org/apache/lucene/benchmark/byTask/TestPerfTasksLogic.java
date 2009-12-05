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

package org.apache.lucene.benchmark.byTask;

import java.io.StringReader;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.List;
import java.util.Iterator;

import org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker;
import org.apache.lucene.benchmark.byTask.tasks.CountingSearchTestTask;
import org.apache.lucene.benchmark.byTask.tasks.CountingHighlighterTestTask;
import org.apache.lucene.benchmark.byTask.stats.TaskStats;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.search.FieldCache.StringIndex;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test very simply that perf tasks - simple algorithms - are doing what they should.
 */
public class TestPerfTasksLogic extends LuceneTestCase {

  private static final boolean DEBUG = false;
  static final String NEW_LINE = System.getProperty("line.separator");
  
  // properties in effect in all tests here
  static final String propLines [] = {
    "directory=RAMDirectory",
    "print.props=false",
  };
  
  /**
   * @param name test name
   */
  public TestPerfTasksLogic(String name) {
    super(name);
  }

  /**
   * Test index creation logic
   */
  public void testIndexAndSearchTasks() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : 1000",
        "Optimize",
        "CloseIndex",
        "OpenReader",
        "{ CountingSearchTest } : 200",
        "CloseReader",
        "[ CountingSearchTest > : 70",
        "[ CountingSearchTest > : 9",
    };
    
    // 2. we test this value later
    CountingSearchTestTask.numSearches = 0;
    
    // 3. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 4. test specific checks after the benchmark run completed.
    assertEquals("TestSearchTask was supposed to be called!",279,CountingSearchTestTask.numSearches);
    assertTrue("Index does not exist?...!", IndexReader.indexExists(benchmark.getRunData().getDirectory()));
    // now we should be able to open the index for write. 
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false, IndexWriter.MaxFieldLength.LIMITED);
    iw.close();
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    assertEquals("1000 docs were added to the index, this is what we expect to find!",1000,ir.numDocs());
    ir.close();
  }

  /**
   * Test timed sequence task.
   */
  public void testTimedSearchTask() throws Exception {
    String algLines[] = {
        "log.step=100000",
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : 100",
        "Optimize",
        "CloseIndex",
        "OpenReader",
        "{ CountingSearchTest } : .5s",
        "CloseReader",
    };

    CountingSearchTestTask.numSearches = 0;
    execBenchmark(algLines);
    assertTrue(CountingSearchTestTask.numSearches > 0);
    long elapsed = CountingSearchTestTask.prevLastMillis - CountingSearchTestTask.startMillis;
    assertTrue("elapsed time was " + elapsed + " msec", elapsed <= 1500);
  }

  public void testBGSearchTaskThreads() throws Exception {
    String algLines[] = {
        "log.time.step.msec = 100",
        "log.step=100000",
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : 1000",
        "Optimize",
        "CloseIndex",
        "OpenReader",
        "{",
        "  [ \"XSearch\" { CountingSearchTest > : * ] : 2 &-1",
        "  Wait(0.5)",
        "}",
        "CloseReader",
        "RepSumByPref X"
    };

    CountingSearchTestTask.numSearches = 0;
    execBenchmark(algLines);
    assertTrue(CountingSearchTestTask.numSearches > 0);
  }

  public void testHighlighting() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "doc.stored=true",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "query.maker=" + ReutersQueryMaker.class.getName(),
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : 100",
        "Optimize",
        "CloseIndex",
        "OpenReader(true)",
        "{ CountingHighlighterTest(size[1],highlight[1],mergeContiguous[true],maxFrags[1],fields[body]) } : 200",
        "CloseReader",
    };

    // 2. we test this value later
    CountingHighlighterTestTask.numHighlightedResults = 0;
    CountingHighlighterTestTask.numDocsRetrieved = 0;
    // 3. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 4. test specific checks after the benchmark run completed.
    assertEquals("TestSearchTask was supposed to be called!",92,CountingHighlighterTestTask.numDocsRetrieved);
    //pretty hard to figure out a priori how many docs are going to have highlighted fragments returned, but we can never have more than the number of docs
    //we probably should use a different doc/query maker, but...
    assertTrue("TestSearchTask was supposed to be called!", CountingHighlighterTestTask.numDocsRetrieved >= CountingHighlighterTestTask.numHighlightedResults && CountingHighlighterTestTask.numHighlightedResults > 0);

    assertTrue("Index does not exist?...!", IndexReader.indexExists(benchmark.getRunData().getDirectory()));
    // now we should be able to open the index for write.
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false, IndexWriter.MaxFieldLength.LIMITED);
    iw.close();
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    assertEquals("100 docs were added to the index, this is what we expect to find!",100,ir.numDocs());
    ir.close();
  }

  public void testHighlightingTV() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "doc.stored=true",//doc storage is required in order to have text to highlight
        "doc.term.vector.offsets=true",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "query.maker=" + ReutersQueryMaker.class.getName(),
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : 1000",
        "Optimize",
        "CloseIndex",
        "OpenReader(false)",
        "{ CountingHighlighterTest(size[1],highlight[1],mergeContiguous[true],maxFrags[1],fields[body]) } : 200",
        "CloseReader",
    };

    // 2. we test this value later
    CountingHighlighterTestTask.numHighlightedResults = 0;
    CountingHighlighterTestTask.numDocsRetrieved = 0;
    // 3. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 4. test specific checks after the benchmark run completed.
    assertEquals("TestSearchTask was supposed to be called!",92,CountingHighlighterTestTask.numDocsRetrieved);
    //pretty hard to figure out a priori how many docs are going to have highlighted fragments returned, but we can never have more than the number of docs
    //we probably should use a different doc/query maker, but...
    assertTrue("TestSearchTask was supposed to be called!", CountingHighlighterTestTask.numDocsRetrieved >= CountingHighlighterTestTask.numHighlightedResults && CountingHighlighterTestTask.numHighlightedResults > 0);

    assertTrue("Index does not exist?...!", IndexReader.indexExists(benchmark.getRunData().getDirectory()));
    // now we should be able to open the index for write.
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false,IndexWriter.MaxFieldLength.UNLIMITED);
    iw.close();
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    assertEquals("1000 docs were added to the index, this is what we expect to find!",1000,ir.numDocs());
    ir.close();
  }

  public void testHighlightingNoTvNoStore() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "doc.stored=false",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "query.maker=" + ReutersQueryMaker.class.getName(),
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : 1000",
        "Optimize",
        "CloseIndex",
        "OpenReader",
        "{ CountingHighlighterTest(size[1],highlight[1],mergeContiguous[true],maxFrags[1],fields[body]) } : 200",
        "CloseReader",
    };

    // 2. we test this value later
    CountingHighlighterTestTask.numHighlightedResults = 0;
    CountingHighlighterTestTask.numDocsRetrieved = 0;
    // 3. execute the algorithm  (required in every "logic" test)
    try {
      Benchmark benchmark = execBenchmark(algLines);
      assertTrue("CountingHighlighterTest should have thrown an exception", false);
      assertNotNull(benchmark); // (avoid compile warning on unused variable)
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  /**
   * Test Exhasting Doc Maker logic
   */
  public void testExhaustContentSource() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.SingleDocSource",
        "content.source.log.step=1",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "doc.stored=false",
        "doc.tokenized=false",
        "# ----- alg ",
        "CreateIndex",
        "{ AddDoc } : * ",
        "Optimize",
        "CloseIndex",
        "OpenReader",
        "{ CountingSearchTest } : 100",
        "CloseReader",
        "[ CountingSearchTest > : 30",
        "[ CountingSearchTest > : 9",
    };
    
    // 2. we test this value later
    CountingSearchTestTask.numSearches = 0;
    
    // 3. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 4. test specific checks after the benchmark run completed.
    assertEquals("TestSearchTask was supposed to be called!",139,CountingSearchTestTask.numSearches);
    assertTrue("Index does not exist?...!", IndexReader.indexExists(benchmark.getRunData().getDirectory()));
    // now we should be able to open the index for write. 
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false,IndexWriter.MaxFieldLength.UNLIMITED);
    iw.close();
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    assertEquals("1 docs were added to the index, this is what we expect to find!",1,ir.numDocs());
    ir.close();
  }

  // LUCENE-1994: test thread safety of SortableSingleDocMaker
  public void testDocMakerThreadSafety() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.SortableSingleDocSource",
        "doc.term.vector=false",
        "log.step.AddDoc=10000",
        "content.source.forever=true",
        "directory=RAMDirectory",
        "doc.reuse.fields=false",
        "doc.stored=false",
        "doc.tokenized=false",
        "doc.index.props=true",
        "# ----- alg ",
        "CreateIndex",
        "[ { AddDoc > : 250 ] : 4",
        "CloseIndex",
    };
    
    // 2. we test this value later
    CountingSearchTestTask.numSearches = 0;
    
    // 3. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    IndexReader r = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    StringIndex idx = FieldCache.DEFAULT.getStringIndex(r, "country");
    final int maxDoc = r.maxDoc();
    assertEquals(1000, maxDoc);
    for(int i=0;i<1000;i++) {
      assertNotNull("doc " + i + " has null country", idx.lookup[idx.order[i]]);
    }
    r.close();
  }

  /**
   * Test Parallel Doc Maker logic (for LUCENE-940)
   */
  public void testParallelDocMaker() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=FSDirectory",
        "doc.stored=false",
        "doc.tokenized=false",
        "# ----- alg ",
        "CreateIndex",
        "[ { AddDoc } : * ] : 4 ",
        "CloseIndex",
    };
    
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 3. test number of docs in the index
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  /**
   * Test WriteLineDoc and LineDocSource.
   */
  public void testLineDocFile() throws Exception {
    File lineFile = new File(System.getProperty("tempDir"), "test.reuters.lines.txt");

    // We will call WriteLineDocs this many times
    final int NUM_TRY_DOCS = 50;

    // Creates a line file with first 50 docs from SingleDocSource
    String algLines1[] = {
      "# ----- properties ",
      "content.source=org.apache.lucene.benchmark.byTask.feeds.SingleDocSource",
      "content.source.forever=true",
      "line.file.out=" + lineFile.getAbsolutePath().replace('\\', '/'),
      "# ----- alg ",
      "{WriteLineDoc()}:" + NUM_TRY_DOCS,
    };

    // Run algo
    Benchmark benchmark = execBenchmark(algLines1);

    BufferedReader r = new BufferedReader(new FileReader(lineFile));
    int numLines = 0;
    while(r.readLine() != null)
      numLines++;
    r.close();
    assertEquals("did not see the right number of docs; should be " + NUM_TRY_DOCS + " but was " + numLines, NUM_TRY_DOCS, numLines);
    
    // Index the line docs
    String algLines2[] = {
      "# ----- properties ",
      "analyzer=org.apache.lucene.analysis.SimpleAnalyzer",
      "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
      "docs.file=" + lineFile.getAbsolutePath().replace('\\', '/'),
      "content.source.forever=false",
      "doc.reuse.fields=false",
      "ram.flush.mb=4",
      "# ----- alg ",
      "ResetSystemErase",
      "CreateIndex",
      "{AddDoc}: *",
      "CloseIndex",
    };
    
    // Run algo
    benchmark = execBenchmark(algLines2);

    // now we should be able to open the index for write. 
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false,IndexWriter.MaxFieldLength.UNLIMITED);
    iw.close();

    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    assertEquals(numLines + " lines were created but " + ir.numDocs() + " docs are in the index", numLines, ir.numDocs());
    ir.close();

    lineFile.delete();
  }
  
  /**
   * Test ReadTokensTask
   */
  public void testReadTokens() throws Exception {

    // We will call ReadTokens on this many docs
    final int NUM_DOCS = 20;

    // Read tokens from first NUM_DOCS docs from Reuters and
    // then build index from the same docs
    String algLines1[] = {
      "# ----- properties ",
      "analyzer=org.apache.lucene.analysis.WhitespaceAnalyzer",
      "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
      "docs.file=" + getReuters20LinesFile(),
      "# ----- alg ",
      "{ReadTokens}: " + NUM_DOCS,
      "ResetSystemErase",
      "CreateIndex",
      "{AddDoc}: " + NUM_DOCS,
      "CloseIndex",
    };

    // Run algo
    Benchmark benchmark = execBenchmark(algLines1);

    List<TaskStats> stats = benchmark.getRunData().getPoints().taskStats();

    // Count how many tokens all ReadTokens saw
    int totalTokenCount1 = 0;
    for (final TaskStats stat : stats) {
      if (stat.getTask().getName().equals("ReadTokens")) {
        totalTokenCount1 += stat.getCount();
      }
    }

    // Separately count how many tokens are actually in the index:
    IndexReader reader = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    assertEquals(NUM_DOCS, reader.numDocs());

    TermEnum terms = reader.terms();
    TermDocs termDocs = reader.termDocs();
    int totalTokenCount2 = 0;
    while(terms.next()) {
      termDocs.seek(terms.term());
      while(termDocs.next())
        totalTokenCount2 += termDocs.freq();
    }
    reader.close();

    // Make sure they are the same
    assertEquals(totalTokenCount1, totalTokenCount2);
  }
  
  /**
   * Test that " {[AddDoc(4000)]: 4} : * " works corrcetly (for LUCENE-941)
   */
  public void testParallelExhausted() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "doc.stored=false",
        "doc.tokenized=false",
        "task.max.depth.log=1",
        "# ----- alg ",
        "CreateIndex",
        "{ [ AddDoc]: 4} : * ",
        "ResetInputs ",
        "{ [ AddDoc]: 4} : * ",
        "CloseIndex",
    };
    
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 3. test number of docs in the index
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    int ndocsExpected = 2 * 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  // create the benchmark and execute it. 
  public static Benchmark execBenchmark(String[] algLines) throws Exception {
    String algText = algLinesToText(algLines);
    logTstLogic(algText);
    Benchmark benchmark = new Benchmark(new StringReader(algText));
    benchmark.execute();
    return benchmark;
  }
  
  // catenate alg lines to make the alg text
  private static String algLinesToText(String[] algLines) {
    String indent = "  ";
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < propLines.length; i++) {
      sb.append(indent).append(propLines[i]).append(NEW_LINE);
    }
    for (int i = 0; i < algLines.length; i++) {
      sb.append(indent).append(algLines[i]).append(NEW_LINE);
    }
    return sb.toString();
  }

  private static void logTstLogic (String txt) {
    if (!DEBUG) 
      return;
    System.out.println("Test logic of:");
    System.out.println(txt);
  }

  /**
   * Test that exhaust in loop works as expected (LUCENE-1115).
   */
  public void testExhaustedLooped() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "doc.stored=false",
        "doc.tokenized=false",
        "task.max.depth.log=1",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "  CloseIndex",
        "} : 2",
    };
    
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 3. test number of docs in the index
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    int ndocsExpected = 20;  // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }
  
  /**
   * Test that we can close IndexWriter with argument "false".
   */
  public void testCloseIndexFalse() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "ram.flush.mb=-1",
        "max.buffered=2",
        "content.source.log.step=3",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "doc.stored=false",
        "doc.tokenized=false",
        "debug.level=1",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "  CloseIndex(false)",
        "} : 2",
    };
    
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 3. test number of docs in the index
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  public static class MyMergeScheduler extends SerialMergeScheduler {
    boolean called;
    public MyMergeScheduler() {
      super();
      called = true;
    }
  }

  /**
   * Test that we can set merge scheduler".
   */
  public void testMergeScheduler() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "merge.scheduler=" + MyMergeScheduler.class.getName(),
        "doc.stored=false",
        "doc.tokenized=false",
        "debug.level=1",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "} : 2",
    };
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    assertTrue("did not use the specified MergeScheduler", ((MyMergeScheduler) benchmark.getRunData().getIndexWriter().getMergeScheduler()).called);
    benchmark.getRunData().getIndexWriter().close();

    // 3. test number of docs in the index
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  public static class MyMergePolicy extends LogDocMergePolicy {
    boolean called;
    public MyMergePolicy(IndexWriter writer) {
      super(writer);
      called = true;
    }
  }
  /**
   * Test that we can set merge policy".
   */
  public void testMergePolicy() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "ram.flush.mb=-1",
        "max.buffered=2",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "merge.policy=" + MyMergePolicy.class.getName(),
        "doc.stored=false",
        "doc.tokenized=false",
        "debug.level=1",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "} : 2",
    };

    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);
    assertTrue("did not use the specified MergeScheduler", ((MyMergePolicy) benchmark.getRunData().getIndexWriter().getMergePolicy()).called);
    benchmark.getRunData().getIndexWriter().close();
    
    // 3. test number of docs in the index
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  /**
   * Test that IndexWriter settings stick.
   */
  public void testIndexWriterSettings() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "ram.flush.mb=-1",
        "max.buffered=2",
        "compound=cmpnd:true:false",
        "doc.term.vector=vector:false:true",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "doc.stored=false",
        "merge.factor=3",
        "doc.tokenized=false",
        "debug.level=1",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "  NewRound",
        "} : 2",
    };

    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);
    final IndexWriter writer = benchmark.getRunData().getIndexWriter();
    assertEquals(2, writer.getMaxBufferedDocs());
    assertEquals(IndexWriter.DISABLE_AUTO_FLUSH, (int) writer.getRAMBufferSizeMB());
    assertEquals(3, writer.getMergeFactor());
    assertFalse(writer.getUseCompoundFile());
    writer.close();
    Directory dir = benchmark.getRunData().getDirectory();
    IndexReader reader = IndexReader.open(dir, true);
    TermFreqVector [] tfv = reader.getTermFreqVectors(0);
    assertNotNull(tfv);
    assertTrue(tfv.length > 0);
    reader.close();
  }

  /**
   * Test that we can call optimize(maxNumSegments).
   */
  public void testOptimizeMaxNumSegments() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "ram.flush.mb=-1",
        "max.buffered=3",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "merge.policy=org.apache.lucene.index.LogDocMergePolicy",
        "doc.stored=false",
        "doc.tokenized=false",
        "debug.level=1",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "  Optimize(3)",
        "  CloseIndex()",
        "} : 2",
    };
    
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 3. test number of docs in the index
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory(), true);
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();

    // Make sure we have 3 segments:
    final String[] files = benchmark.getRunData().getDirectory().listAll();
    int cfsCount = 0;
    for(int i=0;i<files.length;i++)
      if (files[i].endsWith(".cfs"))
        cfsCount++;
    assertEquals(3, cfsCount);
  }
  
  /**
   * Test disabling task count (LUCENE-1136).
   */
  public void testDisableCounting() throws Exception {
    doTestDisableCounting(true);
    doTestDisableCounting(false);
  }

  private void doTestDisableCounting(boolean disable) throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = disableCountingLines(disable);
    
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 3. test counters
    int n = disable ? 0 : 1;
    int nChecked = 0;
    for (final TaskStats stats : benchmark.getRunData().getPoints().taskStats()) {
      String taskName = stats.getTask().getName();
      if (taskName.equals("Rounds")) {
        assertEquals("Wrong total count!",20+2*n,stats.getCount());
        nChecked++;
      } else if (taskName.equals("CreateIndex")) {
        assertEquals("Wrong count for CreateIndex!",n,stats.getCount());
        nChecked++;
      } else if (taskName.equals("CloseIndex")) {
        assertEquals("Wrong count for CloseIndex!",n,stats.getCount());
        nChecked++;
      }
    }
    assertEquals("Missing some tasks to check!",3,nChecked);
  }

  private static String[] disableCountingLines (boolean disable) {
    String dis = disable ? "-" : "";
    return new String[] {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=30",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "doc.stored=false",
        "doc.tokenized=false",
        "task.max.depth.log=1",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  "+dis+"CreateIndex",            // optionally disable counting here
        "  { \"AddDocs\"  AddDoc > : * ",
        "  "+dis+"  CloseIndex",             // optionally disable counting here (with extra blanks)
        "}",
        "RepSumByName",
    };
  }

  private static String getReuters20LinesFile() {
    return System.getProperty("lucene.common.dir").replace('\\','/') +
      "/contrib/benchmark/src/test/org/apache/lucene/benchmark/reuters.first20.lines.txt";
  }  
}
