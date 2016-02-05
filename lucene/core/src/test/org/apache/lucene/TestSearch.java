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


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.LuceneTestCase;

/** JUnit adaptation of an older test case SearchTest. */
public class TestSearch extends LuceneTestCase {

  public void testNegativeQueryBoost() throws Exception {
    BoostQuery q = new BoostQuery(new TermQuery(new Term("foo", "bar")), -42f);
    assertEquals(-42f, q.getBoost(), 0f);

    Directory directory = newDirectory();
    try {
      Analyzer analyzer = new MockAnalyzer(random());
      IndexWriterConfig conf = newIndexWriterConfig(analyzer);
      
      IndexWriter writer = new IndexWriter(directory, conf);
      try {
        Document d = new Document();
        d.add(newTextField("foo", "bar", Field.Store.YES));
        writer.addDocument(d);
      } finally {
        writer.close();
      }
      
      IndexReader reader = DirectoryReader.open(directory);
      try {
        IndexSearcher searcher = newSearcher(reader);
        
        ScoreDoc[] hits = searcher.search(q, 1000).scoreDocs;
        assertEquals(1, hits.length);
        assertTrue("score is positive: " + hits[0].score,
                   hits[0].score <= 0);

        Explanation explain = searcher.explain(q, hits[0].doc);
        assertEquals("score doesn't match explanation",
                     hits[0].score, explain.getValue(), 0.001f);
        assertTrue("explain doesn't think doc is a match",
                   explain.isMatch());

      } finally {
        reader.close();
      }
    } finally {
      directory.close();
    }

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
      doTestSearch(random(), pw, false);
      pw.close();
      sw.close();
      String multiFileOutput = sw.toString();
      //System.out.println(multiFileOutput);

      sw = new StringWriter();
      pw = new PrintWriter(sw, true);
      doTestSearch(random(), pw, true);
      pw.close();
      sw.close();
      String singleFileOutput = sw.toString();

      assertEquals(multiFileOutput, singleFileOutput);
    }


    private void doTestSearch(Random random, PrintWriter out, boolean useCompoundFile)
    throws Exception {
      Directory directory = newDirectory();
      Analyzer analyzer = new MockAnalyzer(random);
      IndexWriterConfig conf = newIndexWriterConfig(analyzer);
      MergePolicy mp = conf.getMergePolicy();
      mp.setNoCFSRatio(useCompoundFile ? 1.0 : 0.0);
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
        d.add(newTextField("contents", docs[j], Field.Store.YES));
        d.add(new NumericDocValuesField("id", j));
        writer.addDocument(d);
      }
      writer.close();

      IndexReader reader = DirectoryReader.open(directory);
      IndexSearcher searcher = newSearcher(reader);

      ScoreDoc[] hits = null;

      Sort sort = new Sort(SortField.FIELD_SCORE,
                           new SortField("id", SortField.Type.INT));

      for (Query query : buildQueries()) {
        out.println("Query: " + query.toString("contents"));
        if (VERBOSE) {
          System.out.println("TEST: query=" + query);
        }

        hits = searcher.search(query, 1000, sort).scoreDocs;

        out.println(hits.length + " total results");
        for (int i = 0 ; i < hits.length && i < 10; i++) {
          Document d = searcher.doc(hits[i].doc);
          out.println(i + " " + hits[i].score + " " + d.get("contents"));
        }
      }
      reader.close();
      directory.close();
  }

  private List<Query> buildQueries() {
    List<Query> queries = new ArrayList<>();

    BooleanQuery.Builder booleanAB = new BooleanQuery.Builder();
    booleanAB.add(new TermQuery(new Term("contents", "a")), BooleanClause.Occur.SHOULD);
    booleanAB.add(new TermQuery(new Term("contents", "b")), BooleanClause.Occur.SHOULD);
    queries.add(booleanAB.build());

    PhraseQuery phraseAB = new PhraseQuery("contents", "a", "b");
    queries.add(phraseAB);

    PhraseQuery phraseABC = new PhraseQuery("contents", "a", "b", "c");
    queries.add(phraseABC);

    BooleanQuery.Builder booleanAC = new BooleanQuery.Builder();
    booleanAC.add(new TermQuery(new Term("contents", "a")), BooleanClause.Occur.SHOULD);
    booleanAC.add(new TermQuery(new Term("contents", "c")), BooleanClause.Occur.SHOULD);
    queries.add(booleanAC.build());

    PhraseQuery phraseAC = new PhraseQuery("contents", "a", "c");
    queries.add(phraseAC);

    PhraseQuery phraseACE = new PhraseQuery("contents", "a", "c", "e");
    queries.add(phraseACE);

    return queries;
  }
}
