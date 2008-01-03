package org.apache.lucene.benchmark.quality.trec;

import org.apache.lucene.benchmark.quality.trec.TrecJudge;
import org.apache.lucene.benchmark.quality.trec.TrecTopicsReader;
import org.apache.lucene.benchmark.quality.utils.SimpleQQParser;
import org.apache.lucene.benchmark.quality.utils.SubmissionReport;
import org.apache.lucene.benchmark.quality.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;

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

    
    File topicsFile = new File(args[0]);
    File qrelsFile = new File(args[1]);
    Searcher searcher = new IndexSearcher(args[3]);

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
    SubmissionReport submitLog = null;
    QualityStats stats[] = qrun.execute(judge, submitLog, logger);

    // print an avarage sum of the results
    QualityStats avg = QualityStats.average(stats);
    avg.log("SUMMARY", 2, logger, "  ");
  }
}
