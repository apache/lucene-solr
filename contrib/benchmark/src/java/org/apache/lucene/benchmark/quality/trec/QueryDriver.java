package org.apache.lucene.benchmark.quality.trec;

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

import org.apache.lucene.benchmark.quality.trec.TrecJudge;
import org.apache.lucene.benchmark.quality.trec.TrecTopicsReader;
import org.apache.lucene.benchmark.quality.utils.SimpleQQParser;
import org.apache.lucene.benchmark.quality.utils.SubmissionReport;
import org.apache.lucene.benchmark.quality.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;


/**
 *
 *
 **/
public class QueryDriver {
  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: QueryDriver <topicsFile> <qrelsFile> <submissionFile> <indexDir>");
      System.err.println("topicsFile: input file containing queries");
      System.err.println("qrelsFile: input file containing relevance judgements");
      System.err.println("submissionFile: output submission file for trec_eval");
      System.err.println("indexDir: index directory");
      System.exit(1);
    }
    
    File topicsFile = new File(args[0]);
    File qrelsFile = new File(args[1]);
    SubmissionReport submitLog = new SubmissionReport(new PrintWriter(args[2]), "lucene");
    FSDirectory dir = FSDirectory.open(new File(args[3]));
    Searcher searcher = new IndexSearcher(dir, true);

    int maxResults = 1000;
    String docNameField = "docname";

    PrintWriter logger = new PrintWriter(System.out, true);

    // use trec utilities to read trec topics into quality queries
    TrecTopicsReader qReader = new TrecTopicsReader();
    QualityQuery qqs[] = qReader.readQueries(new BufferedReader(new FileReader(topicsFile)));

    // prepare judge, with trec utilities that read from a QRels file
    Judge judge = new TrecJudge(new BufferedReader(new FileReader(qrelsFile)));

    // validate topics & judgments match each other
    judge.validateData(qqs, logger);

    // set the parsing of quality queries into Lucene queries.
    QualityQueryParser qqParser = new SimpleQQParser("title", "body");

    // run the benchmark
    QualityBenchmark qrun = new QualityBenchmark(qqs, qqParser, searcher, docNameField);
    qrun.setMaxResults(maxResults);
    QualityStats stats[] = qrun.execute(judge, submitLog, logger);

    // print an avarage sum of the results
    QualityStats avg = QualityStats.average(stats);
    avg.log("SUMMARY", 2, logger, "  ");
  }
}
