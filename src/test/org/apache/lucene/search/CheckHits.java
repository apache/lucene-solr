package org.apache.lucene.search;

/**
 * Copyright 2004-2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class CheckHits {
  /** Tests that a query has expected document number results.
   */
  public static void checkHits(
        Query query,
        String defaultFieldName,
        Searcher searcher,
        int[] results,
        TestCase testCase)
          throws IOException {
    Hits hits = searcher.search(query);

    Set correct = new TreeSet();
    for (int i = 0; i < results.length; i++) {
      correct.add(new Integer(results[i]));
    }

    Set actual = new TreeSet();
    for (int i = 0; i < hits.length(); i++) {
      actual.add(new Integer(hits.id(i)));
    }

    testCase.assertEquals(query.toString(defaultFieldName), correct, actual);
  }

  /** Tests that a Hits has an expected order of documents */
  public static void checkDocIds(String mes, int[] results, Hits hits, TestCase testCase)
  throws IOException {
    testCase.assertEquals(mes + " nr of hits", results.length, hits.length());
    for (int i = 0; i < results.length; i++) {
      testCase.assertEquals(mes + " doc nrs for hit " + i, results[i], hits.id(i));
    }
  }

  /** Tests that two queries have an expected order of documents,
   * and that the two queries have the same score values.
   */
  public static void checkHitsQuery(
        Query query,
        Hits hits1,
        Hits hits2,
        int[] results,
        TestCase testCase)
          throws IOException {

    checkDocIds("hits1", results, hits1, testCase);
    checkDocIds("hits2", results, hits2, testCase);
    
    final float scoreTolerance = 1.0e-7f;
    for (int i = 0; i < results.length; i++) {
      if (Math.abs(hits1.score(i) -  hits2.score(i)) > scoreTolerance) {
        testCase.fail("Hit " + i + ", doc nrs " + hits1.id(i) + " and " + hits2.id(i)
                      + "\nunequal scores: " + hits1.score(i)
                      + "\n           and: " + hits2.score(i)
                      + "\nfor query:" + query.toString());
      }
    }
  }

  public static void printDocNrs(Hits hits) throws IOException {
    System.out.print("new int[] {");
    for (int i = 0; i < hits.length(); i++) {
      System.out.print(hits.id(i));
      if (i != hits.length()-1)
        System.out.print(", ");
    }
    System.out.println("}");
  }
}

