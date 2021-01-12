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
package org.apache.lucene.search;

import java.util.Arrays;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.IndriDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndriAndQuery extends LuceneTestCase {

  /** threshold for comparing floats */
  public static final float SCORE_COMP_THRESH = 0.0000f;

  public Similarity sim = new IndriDirichletSimilarity();
  public Directory index;
  public IndexReader r;
  public IndexSearcher s;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    index = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            index,
            newIndexWriterConfig(
                    new MockAnalyzer(
                        random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET))
                .setSimilarity(sim)
                .setMergePolicy(newLogMergePolicy()));
    // Query is "President Washington"
    {
      Document d1 = new Document();
      d1.add(newField("id", "d1", TextField.TYPE_STORED));
      d1.add(
          newTextField(
              "body", "President Washington was the first leader of the US", Field.Store.YES));
      writer.addDocument(d1);
    }

    {
      Document d2 = new Document();
      d2.add(newField("id", "d2", TextField.TYPE_STORED));
      d2.add(
          newTextField(
              "body",
              "The president is head of the executive branch of government",
              Field.Store.YES));
      writer.addDocument(d2);
    }

    {
      Document d3 = new Document();
      d3.add(newField("id", "d3", TextField.TYPE_STORED));
      d3.add(
          newTextField(
              "body", "George Washington was a general in the Revolutionary War", Field.Store.YES));
      writer.addDocument(d3);
    }

    {
      Document d4 = new Document();
      d4.add(newField("id", "d4", TextField.TYPE_STORED));
      d4.add(newTextField("body", "A company or college can have a president", Field.Store.YES));
      writer.addDocument(d4);
    }

    writer.forceMerge(1);
    r = getOnlyLeafReader(writer.getReader());
    writer.close();
    s = new IndexSearcher(r);
    s.setSimilarity(sim);
  }

  @Override
  public void tearDown() throws Exception {
    r.close();
    index.close();
    super.tearDown();
  }

  public void testSimpleQuery1() throws Exception {

    BooleanClause clause1 = new BooleanClause(tq("body", "george"), Occur.SHOULD);
    BooleanClause clause2 = new BooleanClause(tq("body", "washington"), Occur.SHOULD);

    IndriAndQuery q = new IndriAndQuery(Arrays.asList(clause1, clause2));

    ScoreDoc[] h = s.search(q, 1000).scoreDocs;

    try {
      assertEquals("2 docs should match " + q.toString(), 2, h.length);
    } catch (Error e) {
      printHits("testSimpleEqualScores1", h, s);
      throw e;
    }
  }

  public void testSimpleQuery2() throws Exception {

    BooleanClause clause1 = new BooleanClause(tq("body", "president"), Occur.SHOULD);
    BooleanClause clause2 = new BooleanClause(tq("body", "washington"), Occur.SHOULD);

    IndriAndQuery q = new IndriAndQuery(Arrays.asList(clause1, clause2));

    ScoreDoc[] h = s.search(q, 1000).scoreDocs;

    try {
      assertEquals("all docs should match " + q.toString(), 4, h.length);
    } catch (Error e) {
      printHits("testSimpleEqualScores1", h, s);
      throw e;
    }
  }

  /** macro */
  protected Query tq(String f, String t) {
    return new TermQuery(new Term(f, t));
  }

  /** macro */
  protected Query tq(String f, String t, float b) {
    Query q = tq(f, t);
    return new BoostQuery(q, b);
  }

  protected void printHits(String test, ScoreDoc[] h, IndexSearcher searcher) throws Exception {

    System.err.println("------- " + test + " -------");

    for (int i = 0; i < h.length; i++) {
      Document d = searcher.doc(h[i].doc);
      float score = h[i].score;
      System.err.println("#" + i + ": " + score + " - " + d.get("body"));
    }
  }
}
