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

import org.apache.lucene.benchmark.byTask.Benchmark;
import org.apache.lucene.benchmark.byTask.feeds.DocData;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.feeds.ReutersDocMaker;
import org.apache.lucene.benchmark.byTask.tasks.CountingSearchTestTask;
import org.apache.lucene.benchmark.byTask.stats.TaskStats;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermDocs;

import junit.framework.TestCase;

/**
 * Test very simply that perf tasks - simple algorithms - are doing what they should.
 */
public class TestPerfTasksLogic extends TestCase {

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
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false);
    iw.close();
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory());
    assertEquals("1000 docs were added to the index, this is what we expect to find!",1000,ir.numDocs());
    ir.close();
  }

  /**
   * Test Exhasting Doc Maker logic
   */
  public void testExhaustDocMaker() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "doc.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleDocMaker",
        "doc.add.log.step=1",
        "doc.term.vector=false",
        "doc.maker.forever=false",
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
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false);
    iw.close();
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory());
    assertEquals("1 docs were added to the index, this is what we expect to find!",1,ir.numDocs());
    ir.close();
  }

  /**
   * Test Parallel Doc Maker logic (for LUCENE-940)
   */
  public void testParallelDocMaker() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "doc.maker="+Reuters20DocMaker.class.getName(),
        "doc.add.log.step=3",
        "doc.term.vector=false",
        "doc.maker.forever=false",
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
    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory());
    int ndocsExpected = 20; // Reuters20DocMaker exhausts after 20 docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  /**
   * Test WriteLineDoc and LineDocMaker.
   */
  public void testLineDocFile() throws Exception {
    File lineFile = new File(System.getProperty("tempDir"), "test.reuters.lines.txt");

    // We will call WriteLineDocs this many times
    final int NUM_TRY_DOCS = 500;

    // Creates a line file with first 500 docs from reuters
    String algLines1[] = {
      "# ----- properties ",
      "doc.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersDocMaker",
      "doc.maker.forever=false",
      "line.file.out=" + lineFile.getAbsolutePath().replace('\\', '/'),
      "# ----- alg ",
      "{WriteLineDoc()}:" + NUM_TRY_DOCS,
    };

    // Run algo
    Benchmark benchmark = execBenchmark(algLines1);

    // Verify we got somewhere between 1-500 lines (some
    // Reuters docs have no body, which WriteLineDoc task
    // skips).
    BufferedReader r = new BufferedReader(new FileReader(lineFile));
    int numLines = 0;
    while(r.readLine() != null)
      numLines++;
    r.close();
    assertTrue("did not see the right number of docs; should be > 0 and <= " + NUM_TRY_DOCS + " but was " + numLines, numLines > 0 && numLines <= NUM_TRY_DOCS);
    
    // Index the line docs
    String algLines2[] = {
      "# ----- properties ",
      "analyzer=org.apache.lucene.analysis.SimpleAnalyzer",
      "doc.maker=org.apache.lucene.benchmark.byTask.feeds.LineDocMaker",
      "docs.file=" + lineFile.getAbsolutePath().replace('\\', '/'),
      "doc.maker.forever=false",
      "autocommit=false",
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
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),null,false);
    iw.close();

    IndexReader ir = IndexReader.open(benchmark.getRunData().getDirectory());
    assertEquals(numLines + " lines were were created but " + ir.numDocs() + " docs are in the index", numLines, ir.numDocs());
    ir.close();

    lineFile.delete();
  }
  
  /**
   * Test ReadTokensTask
   */
  public void testReadTokens() throws Exception {

    // We will call ReadTokens on this many docs
    final int NUM_DOCS = 100;

    // Read tokens from first NUM_DOCS docs from Reuters and
    // then build index from the same docs
    String algLines1[] = {
      "# ----- properties ",
      "analyzer=org.apache.lucene.analysis.WhitespaceAnalyzer",
      "doc.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersDocMaker",
      "# ----- alg ",
      "{ReadTokens}: " + NUM_DOCS,
      "ResetSystemErase",
      "CreateIndex",
      "{AddDoc}: " + NUM_DOCS,
      "CloseIndex",
    };

    // Run algo
    Benchmark benchmark = execBenchmark(algLines1);

    List stats = benchmark.getRunData().getPoints().taskStats();

    // Count how many tokens all ReadTokens saw
    int totalTokenCount1 = 0;
    for (Iterator it = stats.iterator(); it.hasNext();) {
      TaskStats stat = (TaskStats) it.next();
      if (stat.getTask().getName().equals("ReadTokens")) {
        totalTokenCount1 += stat.getCount();
      }
    }

    // Separately count how many tokens are actually in the index:
    IndexReader reader = IndexReader.open(benchmark.getRunData().getDirectory());
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

  /** use reuters and the exhaust mechanism, but to be faster, add 20 docs only... */
  public static class Reuters20DocMaker extends ReutersDocMaker {
    private int nDocs=0;
    protected DocData getNextDocData() throws Exception {
      if (nDocs>=20 && !forever) {
        throw new NoMoreDataException();
      }
      nDocs++;
      return super.getNextDocData();
    }
  }
}
