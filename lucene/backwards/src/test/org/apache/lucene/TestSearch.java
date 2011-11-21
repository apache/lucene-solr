package org.apache.lucene;

/**
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

import java.util.GregorianCalendar;
import java.util.Random;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.lucene.util.LuceneTestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;

/** JUnit adaptation of an older test case SearchTest. */
public class TestSearch extends LuceneTestCase {

    /** Main for running test case by itself. */
    public static void main(String args[]) {
        TestRunner.run (new TestSuite(TestSearch.class));
    }

    /** This test performs a number of searches. It also compares output
     *  of searches using multi-file index segments with single-file
     *  index segments.
     *
     *  TODO: someone should check that the results of the searches are
     *        still correct by adding assert statements. Right now, the test
     *        passes if the results are the same between multi-file and
     *        single-file formats, even if the results are wrong.
     */
    public void testSearch() throws Exception {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      doTestSearch(random, pw, false);
      pw.close();
      sw.close();
      String multiFileOutput = sw.getBuffer().toString();
      //System.out.println(multiFileOutput);

      sw = new StringWriter();
      pw = new PrintWriter(sw, true);
      doTestSearch(random, pw, true);
      pw.close();
      sw.close();
      String singleFileOutput = sw.getBuffer().toString();

      assertEquals(multiFileOutput, singleFileOutput);
    }


    private void doTestSearch(Random random, PrintWriter out, boolean useCompoundFile)
    throws Exception {
      Directory directory = newDirectory();
      Analyzer analyzer = new MockAnalyzer(random);
      IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
      MergePolicy mp = conf.getMergePolicy();
      if (mp instanceof LogMergePolicy) {
        ((LogMergePolicy) mp).setUseCompoundFile(useCompoundFile);
      }
      
      IndexWriter writer = new IndexWriter(directory, conf);

      String[] docs = {
        "a b c d e",
        "a b c d e a b c d e",
        "a b c d e f g h i j",
        "a c e",
        "e c a",
        "a c e a c e",
        "a c e a b c"
      };
      for (int j = 0; j < docs.length; j++) {
        Document d = new Document();
        d.add(newField("contents", docs[j], Field.Store.YES, Field.Index.ANALYZED));
        d.add(newField("id", ""+j, Field.Index.NOT_ANALYZED_NO_NORMS));
        writer.addDocument(d);
      }
      writer.close();

      IndexReader reader = IndexReader.open(directory);
      IndexSearcher searcher = new IndexSearcher(reader);

      String[] queries = {
        "a b",
        "\"a b\"",
        "\"a b c\"",
        "a c",
        "\"a c\"",
        "\"a c e\"",
      };
      ScoreDoc[] hits = null;

      Sort sort = new Sort(new SortField[] {
          SortField.FIELD_SCORE,
          new SortField("id", SortField.INT)});

      QueryParser parser = new QueryParser(TEST_VERSION_CURRENT, "contents", analyzer);
      parser.setPhraseSlop(4);
      for (int j = 0; j < queries.length; j++) {
        Query query = parser.parse(queries[j]);
        out.println("Query: " + query.toString("contents"));

        hits = searcher.search(query, null, 1000, sort).scoreDocs;

        out.println(hits.length + " total results");
        for (int i = 0 ; i < hits.length && i < 10; i++) {
          Document d = searcher.doc(hits[i].doc);
          out.println(i + " " + hits[i].score
// 			   + " " + DateField.stringToDate(d.get("modified"))
                             + " " + d.get("contents"));
        }
      }
      searcher.close();
      reader.close();
      directory.close();
  }

  static long Time(int year, int month, int day) {
    GregorianCalendar calendar = new GregorianCalendar();
    calendar.clear();
    calendar.set(year, month, day);
    return calendar.getTime().getTime();
  }
}
