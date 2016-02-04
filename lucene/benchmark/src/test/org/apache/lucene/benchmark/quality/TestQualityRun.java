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
package org.apache.lucene.benchmark.quality;


import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.quality.trec.TrecJudge;
import org.apache.lucene.benchmark.quality.trec.TrecTopicsReader;
import org.apache.lucene.benchmark.quality.utils.SimpleQQParser;
import org.apache.lucene.benchmark.quality.utils.SubmissionReport;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Test that quality run does its job.
 * <p>
 * NOTE: if the default scoring or StandardAnalyzer is changed, then
 * this test will not work correctly, as it does not dynamically
 * generate its test trec topics/qrels!
 */
public class TestQualityRun extends BenchmarkTestCase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    copyToWorkDir("reuters.578.lines.txt.bz2");
  }

  public void testTrecQuality() throws Exception {
    // first create the partial reuters index
    createReutersIndex();
    
    int maxResults = 1000;
    String docNameField = "doctitle"; // orig docID is in the linedoc format title 
    
    PrintWriter logger = VERBOSE ? new PrintWriter(new OutputStreamWriter(System.out, Charset.defaultCharset()),true) : null;
   
    // prepare topics
    InputStream topics = getClass().getResourceAsStream("trecTopics.txt");
    TrecTopicsReader qReader = new TrecTopicsReader();
    QualityQuery qqs[] = qReader.readQueries(new BufferedReader(new InputStreamReader(topics, StandardCharsets.UTF_8)));
    
    // prepare judge
    InputStream qrels = getClass().getResourceAsStream("trecQRels.txt");
    Judge judge = new TrecJudge(new BufferedReader(new InputStreamReader(qrels, StandardCharsets.UTF_8)));
    
    // validate topics & judgments match each other
    judge.validateData(qqs, logger);
    
    Directory dir = newFSDirectory(getWorkDir().resolve("index"));
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    QualityQueryParser qqParser = new SimpleQQParser("title","body");
    QualityBenchmark qrun = new QualityBenchmark(qqs, qqParser, searcher, docNameField);
    
    SubmissionReport submitLog = VERBOSE ? new SubmissionReport(logger, "TestRun") : null;
    qrun.setMaxResults(maxResults);
    QualityStats stats[] = qrun.execute(judge, submitLog, logger);
    
    // --------- verify by the way judgments were altered for this test:
    // for some queries, depending on m = qnum % 8
    // m==0: avg_precision and recall are hurt, by marking fake docs as relevant
    // m==1: precision_at_n and avg_precision are hurt, by unmarking relevant docs
    // m==2: all precision, precision_at_n and recall are hurt.
    // m>=3: these queries remain perfect
    for (int i = 0; i < stats.length; i++) {
      QualityStats s = stats[i];
      switch (i%8) {

      case 0:
        assertTrue("avg-p should be hurt: "+s.getAvp(), 1.0 > s.getAvp());
        assertTrue("recall should be hurt: "+s.getRecall(), 1.0 > s.getRecall());
        for (int j = 1; j <= QualityStats.MAX_POINTS; j++) {
          assertEquals("p_at_"+j+" should be perfect: "+s.getPrecisionAt(j), 1.0, s.getPrecisionAt(j), 1E-2);
        }
        break;
      
      case 1:
        assertTrue("avg-p should be hurt", 1.0 > s.getAvp());
        assertEquals("recall should be perfect: "+s.getRecall(), 1.0, s.getRecall(), 1E-2);
        for (int j = 1; j <= QualityStats.MAX_POINTS; j++) {
          assertTrue("p_at_"+j+" should be hurt: "+s.getPrecisionAt(j), 1.0 > s.getPrecisionAt(j));
        }
        break;

      case 2:
        assertTrue("avg-p should be hurt: "+s.getAvp(), 1.0 > s.getAvp());
        assertTrue("recall should be hurt: "+s.getRecall(), 1.0 > s.getRecall());
        for (int j = 1; j <= QualityStats.MAX_POINTS; j++) {
          assertTrue("p_at_"+j+" should be hurt: "+s.getPrecisionAt(j), 1.0 > s.getPrecisionAt(j));
        }
        break;

      default: {
        assertEquals("avg-p should be perfect: "+s.getAvp(), 1.0, s.getAvp(), 1E-2);
        assertEquals("recall should be perfect: "+s.getRecall(), 1.0, s.getRecall(), 1E-2);
        for (int j = 1; j <= QualityStats.MAX_POINTS; j++) {
          assertEquals("p_at_"+j+" should be perfect: "+s.getPrecisionAt(j), 1.0, s.getPrecisionAt(j), 1E-2);
        }
      }
      
      }
    }
    
    QualityStats avg = QualityStats.average(stats);
    if (logger!=null) {
      avg.log("Average statistis:",1,logger,"  ");
    }
    
    assertTrue("mean avg-p should be hurt: "+avg.getAvp(), 1.0 > avg.getAvp());
    assertTrue("avg recall should be hurt: "+avg.getRecall(), 1.0 > avg.getRecall());
    for (int j = 1; j <= QualityStats.MAX_POINTS; j++) {
      assertTrue("avg p_at_"+j+" should be hurt: "+avg.getPrecisionAt(j), 1.0 > avg.getPrecisionAt(j));
    }
    
    reader.close();
    dir.close();
  }
  
  public void testTrecTopicsReader() throws Exception {    
    // prepare topics
    InputStream topicsFile = getClass().getResourceAsStream("trecTopics.txt");
    TrecTopicsReader qReader = new TrecTopicsReader();
    QualityQuery qqs[] = qReader.readQueries(
        new BufferedReader(new InputStreamReader(topicsFile, StandardCharsets.UTF_8)));
    
    assertEquals(20, qqs.length);
    
    QualityQuery qq = qqs[0];
    assertEquals("statement months  total 1987", qq.getValue("title"));
    assertEquals("Topic 0 Description Line 1 Topic 0 Description Line 2", 
        qq.getValue("description"));
    assertEquals("Topic 0 Narrative Line 1 Topic 0 Narrative Line 2", 
        qq.getValue("narrative"));
    
    qq = qqs[1];
    assertEquals("agreed 15  against five", qq.getValue("title"));
    assertEquals("Topic 1 Description Line 1 Topic 1 Description Line 2", 
        qq.getValue("description"));
    assertEquals("Topic 1 Narrative Line 1 Topic 1 Narrative Line 2", 
        qq.getValue("narrative"));
    
    qq = qqs[19];
    assertEquals("20 while  common week", qq.getValue("title"));
    assertEquals("Topic 19 Description Line 1 Topic 19 Description Line 2", 
        qq.getValue("description"));
    assertEquals("Topic 19 Narrative Line 1 Topic 19 Narrative Line 2", 
        qq.getValue("narrative"));
  }

  // use benchmark logic to create the mini Reuters index
  private void createReutersIndex() throws Exception {
    // 1. alg definition
    String algLines[] = {
        "# ----- properties ",
        "content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource",
        "analyzer=org.apache.lucene.analysis.standard.ClassicAnalyzer",
        "docs.file=" + getWorkDirResourcePath("reuters.578.lines.txt.bz2"),
        "content.source.log.step=2500",
        "doc.term.vector=false",
        "content.source.forever=false",
        "directory=FSDirectory",
        "doc.stored=true",
        "doc.tokenized=true",
        "# ----- alg ",
        "ResetSystemErase",
        "CreateIndex",
        "{ AddDoc } : *",
        "CloseIndex",
    };
    
    // 2. execute the algorithm  (required in every "logic" test)
    execBenchmark(algLines);
  }
}
