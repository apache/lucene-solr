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
package org.apache.lucene.queryparser.surround.query;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.util.LuceneTestCase;

public class Test01Exceptions extends LuceneTestCase {
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



