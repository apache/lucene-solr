package org.apache.lucene.queryParser.surround.query;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class Test01Exceptions extends TestCase {
  /** Main for running test case by itself. */
  public static void main(String args[]) {
    TestRunner.run(new TestSuite(Test01Exceptions.class));
  }

  boolean verbose = false; /* to show actual parsing error messages */
  final String fieldName = "bi";

  String[] exceptionQueries = {
    "*",
    "a*",
    "ab*",
    "?",
    "a?",
    "ab?",
    "a???b",
    "a?",
    "a*b?",
    "word1 word2",
    "word2 AND",
    "word1 OR",
    "AND(word2)",
    "AND(word2,)",
    "AND(word2,word1,)",
    "OR(word2)",
    "OR(word2 ,",
    "OR(word2 , word1 ,)",
    "xx NOT",
    "xx (a AND b)",
    "(a AND b",
    "a OR b)",
    "or(word2+ not ord+, and xyz,def)",
    ""
  };

  public void test01Exceptions() throws Exception {
    String m = ExceptionQueryTst.getFailQueries(exceptionQueries, verbose);
    if (m.length() > 0) {
      fail("No ParseException for:\n" + m);
    }
  }
}


