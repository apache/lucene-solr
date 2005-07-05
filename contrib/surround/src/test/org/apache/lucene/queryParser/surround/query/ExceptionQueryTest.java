package org.apache.lucene.queryParser.surround.query;

import org.apache.lucene.search.Query;

import org.apache.lucene.queryParser.surround.parser.QueryParser;
import org.apache.lucene.queryParser.surround.parser.ParseException;

import junit.framework.TestCase;


public class ExceptionQueryTest {
  private String queryText;
  private boolean verbose;
  private TestCase testCase;
  
  public ExceptionQueryTest(String queryText, boolean verbose) {
    this.queryText = queryText;
    this.verbose = verbose;
    this.testCase = testCase;
  }

  public void doTest(StringBuffer failQueries) {
    QueryParser parser = new QueryParser();
    boolean pass = false;
    SrndQuery lq = null;
    try {
      lq = parser.parse(queryText);
      if (verbose) {
        System.out.println("Query: " + queryText + "\nParsed as: " + lq.toString());
      }
    } catch (ParseException e) {
      if (verbose) {
        System.out.println("Parse exception for query:\n"
                            + queryText + "\n"
                            + e.getMessage());
      }
      pass = true;
    }
    if (! pass) {
      failQueries.append(queryText);
      failQueries.append("\nParsed as: ");
      failQueries.append(lq.toString());
      failQueries.append("\n");
    }
  }
  
  public static String getFailQueries(String[] exceptionQueries, boolean verbose) {
    StringBuffer failQueries = new StringBuffer();
    for (int i = 0; i < exceptionQueries.length; i++ ) {
      new ExceptionQueryTest( exceptionQueries[i], verbose).doTest(failQueries);
    }
    return failQueries.toString();
  }
}

