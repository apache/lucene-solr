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
package org.apache.lucene;


import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestSearchForDuplicates extends LuceneTestCase {

  static final String PRIORITY_FIELD ="priority";
  static final String ID_FIELD ="id";
  static final String HIGH_PRIORITY ="high";
  static final String MED_PRIORITY ="medium";
  static final String LOW_PRIORITY ="low";


  /** This test compares search results when using and not using compound
   *  files.
   *
   *  TODO: There is rudimentary search result validation as well, but it is
   *        simply based on asserting the output observed in the old test case,
   *        without really knowing if the output is correct. Someone needs to
   *        validate this output and make any changes to the checkHits method.
   */
  public void testRun() throws Exception {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      final int MAX_DOCS = atLeast(225);
      doTest(random(), pw, false, MAX_DOCS);
      pw.close();
      sw.close();
      String multiFileOutput = sw.toString();
      //System.out.println(multiFileOutput);

      sw = new StringWriter();
      pw = new PrintWriter(sw, true);
      doTest(random(), pw, true, MAX_DOCS);
      pw.close();
      sw.close();
      String singleFileOutput = sw.toString();

      assertEquals(multiFileOutput, singleFileOutput);
  }


  private void doTest(Random random, PrintWriter out, boolean useCompoundFiles, int MAX_DOCS) throws Exception {
      Directory directory = newDirectory();
      Analyzer analyzer = new MockAnalyzer(random);
      IndexWriterConfig conf = newIndexWriterConfig(analyzer);
      final MergePolicy mp = conf.getMergePolicy();
      mp.setNoCFSRatio(useCompoundFiles ? 1.0 : 0.0);
      IndexWriter writer = new IndexWriter(directory, conf);
      if (VERBOSE) {
        System.out.println("TEST: now build index MAX_DOCS=" + MAX_DOCS);
      }

      for (int j = 0; j < MAX_DOCS; j++) {
        Document d = new Document();
        d.add(newTextField(PRIORITY_FIELD, HIGH_PRIORITY, Field.Store.YES));
        d.add(new StoredField(ID_FIELD, j));
        d.add(new NumericDocValuesField(ID_FIELD, j));
        writer.addDocument(d);
      }
      writer.close();

      // try a search without OR
      IndexReader reader = DirectoryReader.open(directory);
      IndexSearcher searcher = newSearcher(reader);

      Query query = new TermQuery(new Term(PRIORITY_FIELD, HIGH_PRIORITY));
      out.println("Query: " + query.toString(PRIORITY_FIELD));
      if (VERBOSE) {
        System.out.println("TEST: search query=" + query);
      }

      final Sort sort = new Sort(SortField.FIELD_SCORE,
                                 new SortField(ID_FIELD, SortField.Type.INT));

      ScoreDoc[] hits = searcher.search(query, MAX_DOCS, sort).scoreDocs;
      printHits(out, hits, searcher);
      checkHits(hits, MAX_DOCS, searcher);

      // try a new search with OR
      searcher = newSearcher(reader);
      hits = null;

      BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
      booleanQuery.add(new TermQuery(new Term(PRIORITY_FIELD, HIGH_PRIORITY)), BooleanClause.Occur.SHOULD);
      booleanQuery.add(new TermQuery(new Term(PRIORITY_FIELD, MED_PRIORITY)), BooleanClause.Occur.SHOULD);
      out.println("Query: " + booleanQuery.build().toString(PRIORITY_FIELD));

      hits = searcher.search(booleanQuery.build(), MAX_DOCS, sort).scoreDocs;
      printHits(out, hits, searcher);
      checkHits(hits, MAX_DOCS, searcher);

      reader.close();
      directory.close();
  }


  private void printHits(PrintWriter out, ScoreDoc[] hits, IndexSearcher searcher) throws IOException {
    out.println(hits.length + " total results\n");
    for (int i = 0 ; i < hits.length; i++) {
      if ( i < 10 || (i > 94 && i < 105) ) {
        Document d = searcher.doc(hits[i].doc);
        out.println(i + " " + d.get(ID_FIELD));
      }
    }
  }

  private void checkHits(ScoreDoc[] hits, int expectedCount, IndexSearcher searcher) throws IOException {
    assertEquals("total results", expectedCount, hits.length);
    for (int i = 0 ; i < hits.length; i++) {
      if (i < 10 || (i > 94 && i < 105) ) {
        Document d = searcher.doc(hits[i].doc);
        assertEquals("check " + i, String.valueOf(i), d.get(ID_FIELD));
      }
    }
  }

}
