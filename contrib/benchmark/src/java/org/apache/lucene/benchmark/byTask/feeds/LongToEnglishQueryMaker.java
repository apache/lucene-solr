package org.apache.lucene.benchmark.byTask.feeds;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.byTask.tasks.NewAnalyzerTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.English;
import org.apache.lucene.util.Version;


/**
 *
 *
 **/
public class LongToEnglishQueryMaker implements QueryMaker {
  long counter = Long.MIN_VALUE + 10;
  protected QueryParser parser;

  public Query makeQuery(int size) throws Exception {
    throw new UnsupportedOperationException();
  }

  public synchronized Query makeQuery() throws Exception {

    return parser.parse("" + English.longToEnglish(getNextCounter()) + "");
  }

  private synchronized long getNextCounter() {
    if (counter == Long.MAX_VALUE){
      counter = Long.MIN_VALUE + 10;
    }
    return counter++;
  }

  public void setConfig(Config config) throws Exception {
    Analyzer anlzr = NewAnalyzerTask.createAnalyzer(config.get("analyzer", StandardAnalyzer.class.getName()));
    parser = new QueryParser(Version.LUCENE_CURRENT, DocMaker.BODY_FIELD, anlzr);
  }

  public void resetInputs() {
    counter = Long.MIN_VALUE + 10;
  }

  public String printQueries() {
    return "LongToEnglish: [" + Long.MIN_VALUE + " TO " + counter + "]";
  }
}
