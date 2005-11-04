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
        int[] results)
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

    TestCase.assertEquals(query.toString(defaultFieldName), correct, actual);
  }

  /** Tests that a Hits has an expected order of documents */
  public static void checkDocIds(String mes, int[] results, Hits hits)
  throws IOException {
    TestCase.assertEquals(mes + " nr of hits", results.length, hits.length());
    for (int i = 0; i < results.length; i++) {
      TestCase.assertEquals(mes + " doc nrs for hit " + i, results[i], hits.id(i));
    }
  }

  /** Tests that two queries have an expected order of documents,
   * and that the two queries have the same score values.
   */
  public static void checkHitsQuery(
        Query query,
        Hits hits1,
        Hits hits2,
        int[] results)
          throws IOException {

    checkDocIds("hits1", results, hits1);
    checkDocIds("hits2", results, hits2);
    checkEqual(query, hits1, hits2);
  }

  public static void checkEqual(Query query, Hits hits1, Hits hits2) throws IOException {
     final float scoreTolerance = 1.0e-6f;
     if (hits1.length() != hits2.length()) {
       TestCase.fail("Unequal lengths: hits1="+hits1.length()+",hits2="+hits2.length());
     }
    for (int i = 0; i < hits1.length(); i++) {
      if (hits1.id(i) != hits2.id(i)) {
        TestCase.fail("Hit " + i + " docnumbers don't match\n"
                + hits2str(hits1, hits2,0,0)
                + "for query:" + query.toString());
      }

      if ((hits1.id(i) != hits2.id(i))
          || Math.abs(hits1.score(i) -  hits2.score(i)) > scoreTolerance)
      {
        TestCase.fail("Hit " + i + ", doc nrs " + hits1.id(i) + " and " + hits2.id(i)
                      + "\nunequal       : " + hits1.score(i)
                      + "\n           and: " + hits2.score(i)
                      + "\nfor query:" + query.toString());
      }
    }
  }

  public static String hits2str(Hits hits1, Hits hits2, int start, int end) throws IOException {
    StringBuffer sb = new StringBuffer();
    int len1=hits1==null ? 0 : hits1.length();
    int len2=hits2==null ? 0 : hits2.length();
    if (end<=0) {
      end = Math.max(len1,len2);
    }

    sb.append("Hits length1=" + len1 + "\tlength2="+len2);

    sb.append("\n");
    for (int i=start; i<end; i++) {
      sb.append("hit=" + i + ":");
      if (i<len1) {
        sb.append(" doc"+hits1.id(i) + "=" + hits1.score(i));
      } else {
        sb.append("               ");
      }
      sb.append(",\t");
      if (i<len2) {
        sb.append(" doc"+hits2.id(i) + "=" + hits2.score(i));
      }
      sb.append("\n");
    }
    return sb.toString();
  }


  public static String topdocsString(TopDocs docs, int start, int end) {
    StringBuffer sb = new StringBuffer();
    sb.append("TopDocs totalHits="+docs.totalHits + " top="+docs.scoreDocs.length+"\n");
    if (end<=0) end=docs.scoreDocs.length;
    else end=Math.min(end,docs.scoreDocs.length);
    for (int i=start; i<end; i++) {
      sb.append("\t");
      sb.append(i);
      sb.append(") doc=");
      sb.append(docs.scoreDocs[i].doc);
      sb.append("\tscore=");
      sb.append(docs.scoreDocs[i].score);
      sb.append("\n");
    }
    return sb.toString();
  }


}

