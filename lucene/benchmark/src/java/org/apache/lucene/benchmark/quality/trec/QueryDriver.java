package org.apache.lucene.benchmark.quality.trec;

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

import org.apache.lucene.benchmark.quality.utils.SimpleQQParser;
import org.apache.lucene.benchmark.quality.utils.SubmissionReport;
import org.apache.lucene.benchmark.quality.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;


/**
 * Command-line tool for doing a TREC evaluation run.
 **/
public class QueryDriver {
  public static void main(String[] args) throws Exception {
    if (args.length < 4 || args.length > 5) {
      System.err.println("Usage: QueryDriver <topicsFile> <qrelsFile> <submissionFile> <indexDir> [querySpec]");
      System.err.println("topicsFile: input file containing queries");
      System.err.println("qrelsFile: input file containing relevance judgements");
      System.err.println("submissionFile: output submission file for trec_eval");
      System.err.println("indexDir: index directory");
      System.err.println("querySpec: string composed of fields to use in query consisting of T=title,D=description,N=narrative:");
      System.err.println("\texample: TD (query on Title + Description). The default is T (title only)");
      System.exit(1);
    }
    
    File topicsFile = new File(args[0]);
    File qrelsFile = new File(args[1]);
    SubmissionReport submitLog = new SubmissionReport(new PrintWriter(args[2], "UTF-8"), "lucene");
    FSDirectory dir = FSDirectory.open(new File(args[3]));
    String fieldSpec = args.length == 5 ? args[4] : "T"; // default to Title-only if not specified.
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    int maxResults = 1000;
    String docNameField = "docname";

    PrintWriter logger = new PrintWriter(new OutputStreamWriter(System.out, Charset.defaultCharset()), true);

    // use trec utilities to read trec topics into quality queries
    TrecTopicsReader qReader = new TrecTopicsReader();
    QualityQuery qqs[] = qReader.readQueries(new BufferedReader(IOUtils.getDecodingReader(topicsFile, IOUtils.CHARSET_UTF_8)));

    // prepare judge, with trec utilities that read from a QRels file
    Judge judge = new TrecJudge(new BufferedReader(IOUtils.getDecodingReader(qrelsFile, IOUtils.CHARSET_UTF_8)));

    // validate topics & judgments match each other
    judge.validateData(qqs, logger);

    Set<String> fieldSet = new HashSet<String>();
    if (fieldSpec.indexOf('T') >= 0) fieldSet.add("title");
    if (fieldSpec.indexOf('D') >= 0) fieldSet.add("description");
    if (fieldSpec.indexOf('N') >= 0) fieldSet.add("narrative");
    
    // set the parsing of quality queries into Lucene queries.
    QualityQueryParser qqParser = new SimpleQQParser(fieldSet.toArray(new String[0]), "body");

    // run the benchmark
    QualityBenchmark qrun = new QualityBenchmark(qqs, qqParser, searcher, docNameField);
    qrun.setMaxResults(maxResults);
    QualityStats stats[] = qrun.execute(judge, submitLog, logger);

    // print an avarage sum of the results
    QualityStats avg = QualityStats.average(stats);
    avg.log("SUMMARY", 2, logger, "  ");
    reader.close();
    dir.close();
  }
}
