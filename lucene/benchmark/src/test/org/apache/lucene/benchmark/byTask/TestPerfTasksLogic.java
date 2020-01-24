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
package org.apache.lucene.benchmark.byTask;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Collator;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.stats.TaskStats;
import org.apache.lucene.benchmark.byTask.tasks.CountingSearchTestTask;
import org.apache.lucene.benchmark.byTask.tasks.WriteLineDocTask;
import org.apache.lucene.collation.CollationKeyAnalyzer;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Test very simply that perf tasks - simple algorithms - are doing what they should.
 */
@LuceneTestCase.SuppressCodecs("SimpleText")
public class TestPerfTasksLogic extends BenchmarkTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    copyToWorkDir("reuters.first20.lines.txt");
    copyToWorkDir("test-mapping-ISOLatin1Accent-partial.txt");
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
        "ForceMerge(1)",
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
    assertTrue("Index does not exist?...!", DirectoryReader.indexExists(benchmark.getRunData().getDirectory()));
    // now we should be able to open the index for write. 
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(OpenMode.APPEND));
    iw.close();
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
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
        "ForceMerge(1)",
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

  // disabled until we fix BG thread prio -- this test
  // causes build to hang
  public void testBGSearchTaskThreads() throws Exception {
    String algLines[] = {
        "log.time.step.msec = 100",
        "log.step=100000",
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : 1000",
        "ForceMerge(1)",
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

    // NOTE: cannot assert this, because on a super-slow
    // system, it could be after waiting 0.5 seconds that
    // the search threads hadn't yet succeeded in starting
    // up and then they start up and do no searching:
    //assertTrue(CountingSearchTestTask.numSearches > 0);
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
        "ForceMerge(1)",
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
    assertTrue("Index does not exist?...!", DirectoryReader.indexExists(benchmark.getRunData().getDirectory()));
    // now we should be able to open the index for write. 
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(), new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
    iw.close();
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
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
        "doc.stored=true",
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

    DirectoryReader r = DirectoryReader.open(benchmark.getRunData().getDirectory());
    
    final int maxDoc = r.maxDoc();
    assertEquals(1000, maxDoc);
    for(int i=0;i<1000;i++) {
      assertNotNull("doc " + i + " has null country", r.document(i).getField("country"));
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
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  /**
   * Test WriteLineDoc and LineDocSource.
   */
  public void testLineDocFile() throws Exception {
    Path lineFile = createTempFile("test.reuters.lines", ".txt");

    // We will call WriteLineDocs this many times
    final int NUM_TRY_DOCS = 50;

    // Creates a line file with first 50 docs from SingleDocSource
    String algLines1[] = {
      "# ----- properties ",
      "content.source=org.apache.lucene.benchmark.byTask.feeds.SingleDocSource",
      "content.source.forever=true",
      "line.file.out=" + lineFile.toAbsolutePath().toString().replace('\\', '/'),
      "# ----- alg ",
      "{WriteLineDoc()}:" + NUM_TRY_DOCS,
    };

    // Run algo
    Benchmark benchmark = execBenchmark(algLines1);

    BufferedReader r = Files.newBufferedReader(lineFile, StandardCharsets.UTF_8);
    int numLines = 0;
    String line;
    while((line = r.readLine()) != null) {
      if (numLines==0 && line.startsWith(WriteLineDocTask.FIELDS_HEADER_INDICATOR)) {
        continue; // do not count the header line as a doc 
      }
      numLines++;
    }
    r.close();
    assertEquals("did not see the right number of docs; should be " + NUM_TRY_DOCS + " but was " + numLines, NUM_TRY_DOCS, numLines);
    
    // Index the line docs
    String algLines2[] = {
      "# ----- properties ",
      "analyzer=org.apache.lucene.analysis.core.WhitespaceAnalyzer",
      "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
      "docs.file=" + lineFile.toAbsolutePath().toString().replace('\\', '/'),
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
    IndexWriter iw = new IndexWriter(benchmark.getRunData().getDirectory(),
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setOpenMode(OpenMode.APPEND));
    iw.close();

    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
    assertEquals(numLines + " lines were created but " + ir.numDocs() + " docs are in the index", numLines, ir.numDocs());
    ir.close();
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
      "analyzer=org.apache.lucene.analysis.core.WhitespaceAnalyzer",
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
    IndexReader reader = DirectoryReader.open(benchmark.getRunData().getDirectory());
    assertEquals(NUM_DOCS, reader.numDocs());

    int totalTokenCount2 = 0;

    Collection<String> fields = FieldInfos.getIndexedFields(reader);

    for (String fieldName : fields) {
      if (fieldName.equals(DocMaker.ID_FIELD) || fieldName.equals(DocMaker.DATE_MSEC_FIELD) || fieldName.equals(DocMaker.TIME_SEC_FIELD)) {
        continue;
      }
      Terms terms = MultiTerms.getTerms(reader, fieldName);
      if (terms == null) {
        continue;
      }
      TermsEnum termsEnum = terms.iterator();
      PostingsEnum docs = null;
      while(termsEnum.next() != null) {
        docs = TestUtil.docs(random(), termsEnum, docs, PostingsEnum.FREQS);
        while(docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          totalTokenCount2 += docs.freq();
        }
      }
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
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
    int ndocsExpected = 2 * 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
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
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
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
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
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

    assertTrue("did not use the specified MergeScheduler",
        ((MyMergeScheduler) benchmark.getRunData().getIndexWriter().getConfig()
            .getMergeScheduler()).called);
    benchmark.getRunData().getIndexWriter().close();

    // 3. test number of docs in the index
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();
  }

  public static class MyMergePolicy extends LogDocMergePolicy {
    boolean called;
    public MyMergePolicy() {
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
    assertTrue("did not use the specified MergePolicy", ((MyMergePolicy) benchmark.getRunData().getIndexWriter().getConfig().getMergePolicy()).called);
    benchmark.getRunData().getIndexWriter().close();
    
    // 3. test number of docs in the index
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
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
    assertEquals(2, writer.getConfig().getMaxBufferedDocs());
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, (int) writer.getConfig().getRAMBufferSizeMB());
    assertEquals(3, ((LogMergePolicy) writer.getConfig().getMergePolicy()).getMergeFactor());
    assertEquals(0.0d, writer.getConfig().getMergePolicy().getNoCFSRatio(), 0.0);
    writer.close();
    Directory dir = benchmark.getRunData().getDirectory();
    IndexReader reader = DirectoryReader.open(dir);
    Fields tfv = reader.getTermVectors(0);
    assertNotNull(tfv);
    assertTrue(tfv.size() > 0);
    reader.close();
  }

  /**
   * Test indexing with facets tasks.
   */
  public void testIndexingWithFacets() throws Exception {
    // 1. alg definition (required in every "logic" test)
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=100",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "doc.stored=false",
        "merge.factor=3",
        "doc.tokenized=false",
        "debug.level=1",
        "# ----- alg ",
        "ResetSystemErase",
        "CreateIndex",
        "CreateTaxonomyIndex",
        "{ \"AddDocs\"  AddFacetedDoc > : * ",
        "CloseIndex",
        "CloseTaxonomyIndex",
        "OpenTaxonomyReader",
    };

    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);
    PerfRunData runData = benchmark.getRunData();
    assertNull("taxo writer was not properly closed",runData.getTaxonomyWriter());
    TaxonomyReader taxoReader = runData.getTaxonomyReader();
    assertNotNull("taxo reader was not opened", taxoReader);
    assertTrue("nothing was added to the taxnomy (expecting root and at least one addtional category)",taxoReader.getSize()>1);
    taxoReader.close();
  }
  
  /**
   * Test that we can call forceMerge(maxNumSegments).
   */
  public void testForceMerge() throws Exception {
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
        "  ForceMerge(3)",
        "  CloseIndex()",
        "} : 2",
    };
    
    // 2. execute the algorithm  (required in every "logic" test)
    Benchmark benchmark = execBenchmark(algLines);

    // 3. test number of docs in the index
    IndexReader ir = DirectoryReader.open(benchmark.getRunData().getDirectory());
    int ndocsExpected = 20; // first 20 reuters docs.
    assertEquals("wrong number of docs in the index!", ndocsExpected, ir.numDocs());
    ir.close();

    // Make sure we have 3 segments:
    SegmentInfos infos = SegmentInfos.readLatestCommit(benchmark.getRunData().getDirectory());
    assertEquals(3, infos.size());
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

  private String[] disableCountingLines (boolean disable) {
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

  /**
   * Test that we can change the Locale in the runData,
   * that it is parsed as we expect.
   */
  public void testLocale() throws Exception {
    // empty Locale: clear it (null)
    Benchmark benchmark = execBenchmark(getLocaleConfig(""));
    assertNull(benchmark.getRunData().getLocale());

    // ROOT locale
    benchmark = execBenchmark(getLocaleConfig("ROOT"));
    assertEquals(new Locale(""), benchmark.getRunData().getLocale());
    
    // specify just a language 
    benchmark = execBenchmark(getLocaleConfig("de"));
    assertEquals(new Locale("de"), benchmark.getRunData().getLocale());
    
    // specify language + country
    benchmark = execBenchmark(getLocaleConfig("en,US"));
    assertEquals(new Locale("en", "US"), benchmark.getRunData().getLocale());
    
    // specify language + country + variant
    benchmark = execBenchmark(getLocaleConfig("no,NO,NY"));
    assertEquals(new Locale("no", "NO", "NY"), benchmark.getRunData().getLocale());
  }
   
  private String[] getLocaleConfig(String localeParam) {
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  NewLocale(" + localeParam + ")",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "  NewRound",
        "} : 1",
    };
    return algLines;
  }
  
  /**
   * Test that we can create CollationAnalyzers.
   */
  public void testCollator() throws Exception {
    // ROOT locale
    Benchmark benchmark = execBenchmark(getCollatorConfig("ROOT", "impl:jdk"));
    CollationKeyAnalyzer expected = new CollationKeyAnalyzer(Collator.getInstance(new Locale("")));
    assertEqualCollation(expected, benchmark.getRunData().getAnalyzer(), "foobar");
    
    // specify just a language
    benchmark = execBenchmark(getCollatorConfig("de", "impl:jdk"));
    expected = new CollationKeyAnalyzer(Collator.getInstance(new Locale("de")));
    assertEqualCollation(expected, benchmark.getRunData().getAnalyzer(), "foobar");
    
    // specify language + country
    benchmark = execBenchmark(getCollatorConfig("en,US", "impl:jdk"));
    expected = new CollationKeyAnalyzer(Collator.getInstance(new Locale("en", "US")));
    assertEqualCollation(expected, benchmark.getRunData().getAnalyzer(), "foobar");
    
    // specify language + country + variant
    benchmark = execBenchmark(getCollatorConfig("no,NO,NY", "impl:jdk"));
    expected = new CollationKeyAnalyzer(Collator.getInstance(new Locale("no", "NO", "NY")));
    assertEqualCollation(expected, benchmark.getRunData().getAnalyzer(), "foobar");
  }
  
  private void assertEqualCollation(Analyzer a1, Analyzer a2, String text)
      throws Exception {
    TokenStream ts1 = a1.tokenStream("bogus", text);
    TokenStream ts2 = a2.tokenStream("bogus", text);
    ts1.reset();
    ts2.reset();
    TermToBytesRefAttribute termAtt1 = ts1.addAttribute(TermToBytesRefAttribute.class);
    TermToBytesRefAttribute termAtt2 = ts2.addAttribute(TermToBytesRefAttribute.class);
    assertTrue(ts1.incrementToken());
    assertTrue(ts2.incrementToken());
    BytesRef bytes1 = termAtt1.getBytesRef();
    BytesRef bytes2 = termAtt2.getBytesRef();
    assertEquals(bytes1, bytes2);
    assertFalse(ts1.incrementToken());
    assertFalse(ts2.incrementToken());
    ts1.close();
    ts2.close();
  }
  
  private String[] getCollatorConfig(String localeParam, 
      String collationParam) {
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "content.source.log.step=3",
        "content.source.forever=false",
        "directory=RAMDirectory",
        "# ----- alg ",
        "{ \"Rounds\"",
        "  ResetSystemErase",
        "  NewLocale(" + localeParam + ")",
        "  NewCollationAnalyzer(" + collationParam + ")",
        "  CreateIndex",
        "  { \"AddDocs\"  AddDoc > : * ",
        "  NewRound",
        "} : 1",
    };
    return algLines;
  }
  
  /**
   * Test that we can create shingle analyzers using AnalyzerFactory.
   */
  public void testShingleAnalyzer() throws Exception {
    String text = "one,two,three, four five six";
    
    // StandardTokenizer, maxShingleSize, and outputUnigrams
    Benchmark benchmark = execBenchmark(getAnalyzerFactoryConfig
        ("shingle-analyzer", "StandardTokenizer,ShingleFilter"));
    benchmark.getRunData().getAnalyzer().tokenStream
        ("bogus", text).close();
    BaseTokenStreamTestCase.assertAnalyzesTo(benchmark.getRunData().getAnalyzer(), text,
                                             new String[] { "one", "one two", "two", "two three",
                                                            "three", "three four", "four", "four five",
                                                            "five", "five six", "six" });
    // StandardTokenizer, maxShingleSize = 3, and outputUnigrams = false
    benchmark = execBenchmark
      (getAnalyzerFactoryConfig
          ("shingle-analyzer",
           "StandardTokenizer,ShingleFilter(maxShingleSize:3,outputUnigrams:false)"));
    BaseTokenStreamTestCase.assertAnalyzesTo(benchmark.getRunData().getAnalyzer(), text,
                                             new String[] { "one two", "one two three", "two three",
                                                            "two three four", "three four",
                                                            "three four five", "four five",
                                                            "four five six", "five six" });
    // WhitespaceTokenizer, default maxShingleSize and outputUnigrams
    benchmark = execBenchmark
      (getAnalyzerFactoryConfig("shingle-analyzer", "WhitespaceTokenizer,ShingleFilter"));
    BaseTokenStreamTestCase.assertAnalyzesTo(benchmark.getRunData().getAnalyzer(), text,
                                             new String[] { "one,two,three,", "one,two,three, four",
                                                            "four", "four five", "five", "five six",
                                                            "six" });
    
    // WhitespaceTokenizer, maxShingleSize=3 and outputUnigrams=false
    benchmark = execBenchmark
      (getAnalyzerFactoryConfig
        ("shingle-factory",
         "WhitespaceTokenizer,ShingleFilter(outputUnigrams:false,maxShingleSize:3)"));
    BaseTokenStreamTestCase.assertAnalyzesTo(benchmark.getRunData().getAnalyzer(), text,
                                             new String[] { "one,two,three, four",
                                                            "one,two,three, four five",
                                                            "four five", "four five six",
                                                            "five six" });
  }
  
  private String[] getAnalyzerFactoryConfig(String name, String params) {
    final String singleQuoteEscapedName = name.replaceAll("'", "\\\\'");
    String algLines[] = {
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "docs.file=" + getReuters20LinesFile(),
        "work.dir=" + getWorkDir().toAbsolutePath().toString().replaceAll("\\\\", "/"), // Fix Windows path
        "content.source.forever=false",
        "directory=RAMDirectory",
        "AnalyzerFactory(name:'" + singleQuoteEscapedName + "', " + params + ")",
        "NewAnalyzer('" + singleQuoteEscapedName + "')",
        "CreateIndex",
        "{ \"AddDocs\"  AddDoc > : * "
    };
    return algLines;
  }

  public void testAnalyzerFactory() throws Exception {
    String text = "Fortieth, Quarantième, Cuadragésimo";
    Benchmark benchmark = execBenchmark(getAnalyzerFactoryConfig
        ("ascii folded, pattern replaced, standard tokenized, downcased, bigrammed.'analyzer'",
         "positionIncrementGap:100,offsetGap:1111,"
         +"MappingCharFilter(mapping:'test-mapping-ISOLatin1Accent-partial.txt'),"
         +"PatternReplaceCharFilterFactory(pattern:'e(\\\\\\\\S*)m',replacement:\"$1xxx$1\"),"
         +"StandardTokenizer,LowerCaseFilter,NGramTokenFilter(minGramSize:2,maxGramSize:2)"));
    BaseTokenStreamTestCase.assertAnalyzesTo(benchmark.getRunData().getAnalyzer(), text,
        new String[] { "fo", "or", "rt", "ti", "ie", "et", "th",
                       "qu", "ua", "ar", "ra", "an", "nt", "ti", "ix", "xx", "xx", "xe",
                       "cu", "ua", "ad", "dr", "ra", "ag", "gs", "si", "ix", "xx", "xx", "xs", "si", "io"});
  }
  
  private String getReuters20LinesFile() {
    return getWorkDirResourcePath("reuters.first20.lines.txt");
  }  
}
