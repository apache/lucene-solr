package org.apache.lucene.queryParser.surround.query;

import org.apache.lucene.queryParser.surround.parser.ParseException;
import org.apache.lucene.queryParser.surround.parser.QueryParser;


public class ExceptionQueryTst {
  private String queryText;
  private boolean verbose;
  
  public ExceptionQueryTst(String queryText, boolean verbose) {
    this.queryText = queryText;
    this.verbose = verbose;
  }

  public void doTest(StringBuffer failQueries) {
    boolean pass = false;
    SrndQuery lq = null;
    try {
      lq = QueryParser.parse(queryText);
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
      new ExceptionQueryTst( exceptionQueries[i], verbose).doTest(failQueries);
    }
    return failQueries.toString();
  }
}

