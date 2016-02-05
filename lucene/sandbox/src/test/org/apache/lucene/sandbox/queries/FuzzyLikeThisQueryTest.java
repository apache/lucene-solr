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
package org.apache.lucene.sandbox.queries;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.HashSet;

public class FuzzyLikeThisQueryTest extends LuceneTestCase {
  private Directory directory;
  private IndexSearcher searcher;
  private IndexReader reader;
  private Analyzer analyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    analyzer = new MockAnalyzer(random());
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(analyzer).setMergePolicy(newLogMergePolicy()));

    //Add series of docs with misspelt names
    addDoc(writer, "jonathon smythe", "1");
    addDoc(writer, "jonathan smith", "2");
    addDoc(writer, "johnathon smyth", "3");
    addDoc(writer, "johnny smith", "4");
    addDoc(writer, "jonny smith", "5");
    addDoc(writer, "johnathon smythe", "6");
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(reader, directory, analyzer);
    super.tearDown();
  }

  private void addDoc(RandomIndexWriter writer, String name, String id) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("name", name, Field.Store.YES));
    doc.add(newTextField("id", id, Field.Store.YES));
    writer.addDocument(doc);
  }


  //Tests that idf ranking is not favouring rare mis-spellings over a strong edit-distance match
  public void testClosestEditDistanceMatchComesFirst() throws Throwable {
    FuzzyLikeThisQuery flt = new FuzzyLikeThisQuery(10, analyzer);
    flt.addTerms("smith", "name", 0.3f, 1);
    Query q = flt.rewrite(searcher.getIndexReader());
    HashSet<Term> queryTerms = new HashSet<>();
    searcher.createWeight(q, true).extractTerms(queryTerms);
    assertTrue("Should have variant smythe", queryTerms.contains(new Term("name", "smythe")));
    assertTrue("Should have variant smith", queryTerms.contains(new Term("name", "smith")));
    assertTrue("Should have variant smyth", queryTerms.contains(new Term("name", "smyth")));
    TopDocs topDocs = searcher.search(flt, 1);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertTrue("score docs must match 1 doc", (sd != null) && (sd.length > 0));
    Document doc = searcher.doc(sd[0].doc);
    assertEquals("Should match most similar not most rare variant", "2", doc.get("id"));
  }

  //Test multiple input words are having variants produced
  public void testMultiWord() throws Throwable {
    FuzzyLikeThisQuery flt = new FuzzyLikeThisQuery(10, analyzer);
    flt.addTerms("jonathin smoth", "name", 0.3f, 1);
    Query q = flt.rewrite(searcher.getIndexReader());
    HashSet<Term> queryTerms = new HashSet<>();
    searcher.createWeight(q, true).extractTerms(queryTerms);
    assertTrue("Should have variant jonathan", queryTerms.contains(new Term("name", "jonathan")));
    assertTrue("Should have variant smith", queryTerms.contains(new Term("name", "smith")));
    TopDocs topDocs = searcher.search(flt, 1);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertTrue("score docs must match 1 doc", (sd != null) && (sd.length > 0));
    Document doc = searcher.doc(sd[0].doc);
    assertEquals("Should match most similar when using 2 words", "2", doc.get("id"));
  }
  
  // LUCENE-4809
  public void testNonExistingField() throws Throwable {
    FuzzyLikeThisQuery flt = new FuzzyLikeThisQuery(10, analyzer);
    flt.addTerms("jonathin smoth", "name", 0.3f, 1);
    flt.addTerms("jonathin smoth", "this field does not exist", 0.3f, 1);
    // don't fail here just because the field doesn't exits
    Query q = flt.rewrite(searcher.getIndexReader());
    HashSet<Term> queryTerms = new HashSet<>();
    searcher.createWeight(q, true).extractTerms(queryTerms);
    assertTrue("Should have variant jonathan", queryTerms.contains(new Term("name", "jonathan")));
    assertTrue("Should have variant smith", queryTerms.contains(new Term("name", "smith")));
    TopDocs topDocs = searcher.search(flt, 1);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertTrue("score docs must match 1 doc", (sd != null) && (sd.length > 0));
    Document doc = searcher.doc(sd[0].doc);
    assertEquals("Should match most similar when using 2 words", "2", doc.get("id"));
  }


  //Test bug found when first query word does not match anything
  public void testNoMatchFirstWordBug() throws Throwable {
    FuzzyLikeThisQuery flt = new FuzzyLikeThisQuery(10, analyzer);
    flt.addTerms("fernando smith", "name", 0.3f, 1);
    Query q = flt.rewrite(searcher.getIndexReader());
    HashSet<Term> queryTerms = new HashSet<>();
    searcher.createWeight(q, true).extractTerms(queryTerms);
    assertTrue("Should have variant smith", queryTerms.contains(new Term("name", "smith")));
    TopDocs topDocs = searcher.search(flt, 1);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertTrue("score docs must match 1 doc", (sd != null) && (sd.length > 0));
    Document doc = searcher.doc(sd[0].doc);
    assertEquals("Should match most similar when using 2 words", "2", doc.get("id"));
  }

  public void testFuzzyLikeThisQueryEquals() {
    Analyzer analyzer = new MockAnalyzer(random());
    FuzzyLikeThisQuery fltq1 = new FuzzyLikeThisQuery(10, analyzer);
    fltq1.addTerms("javi", "subject", 0.5f, 2);
    FuzzyLikeThisQuery fltq2 = new FuzzyLikeThisQuery(10, analyzer);
    fltq2.addTerms("javi", "subject", 0.5f, 2);
    assertEquals("FuzzyLikeThisQuery with same attributes is not equal", fltq1,
        fltq2);
  }
}
