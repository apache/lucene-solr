package org.apache.lucene.queryparser.surround.query;

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

import org.apache.lucene.queryparser.surround.parser.ParseException;
import org.apache.lucene.queryparser.surround.parser.QueryParser;


public class ExceptionQueryTst {
  private String queryText;
  private boolean verbose;
  
  public ExceptionQueryTst(String queryText, boolean verbose) {
    this.queryText = queryText;
    this.verbose = verbose;
  }

  public void doTest(StringBuilder failQueries) {
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
    StringBuilder failQueries = new StringBuilder();
    for (int i = 0; i < exceptionQueries.length; i++ ) {
      new ExceptionQueryTst( exceptionQueries[i], verbose).doTest(failQueries);
    }
    return failQueries.toString();
  }
}


