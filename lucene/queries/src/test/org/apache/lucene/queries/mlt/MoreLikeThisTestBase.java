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

package org.apache.lucene.queries.mlt;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PriorityQueue;

import static org.hamcrest.CoreMatchers.is;

public class MoreLikeThisTestBase extends LuceneTestCase {

  protected static final String FIELD1 = "field1";
  protected static final String FIELD2 = "field2";
  public static final String SUFFIX_A = "a";
  public static final String SUFFIX_B = "b";

  protected int numDocs = 100;

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;
  protected Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    if (reader != null) {
      reader.close();
    }
    if (directory != null) {
      directory.close();
    }
    if (analyzer != null) {
      analyzer.close();
    }
    super.tearDown();
  }

  protected MoreLikeThisParameters getDefaultParams() {
    MoreLikeThisParameters params = new MoreLikeThisParameters();
    params.setAnalyzer(analyzer);
    params.setMinDocFreq(1);
    params.setMinTermFreq(1);
    params.setMinWordLen(1);
    params.setMaxQueryTerms(25);
    params.setFieldNames(new String[]{FIELD1});
    return params;
  }

  /**
   * This method will prepare an index on {@link #numDocs} total docs.
   * Each doc will have a single field with terms up to its sequential number :
   * <p>
   * Doc 4
   * 1a
   * 2a
   * 3a
   * ...
   * na
   * <p>
   * This means that '1a' will have the max docFrequency na
   * While 'na' will have min docFrequency ( = 1)
   *
   * @return
   * @throws IOException
   */
  protected int initIndexWithSingleFieldDocuments() throws IOException {
    // add series of docs with terms of decreasing df
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 1; i <= numDocs; i++) {
      addDocumentWithSingleField(writer, getArithmeticSeriesWithSuffix(1, i, SUFFIX_A));
    }
    reader = writer.getReader();
    int lastDocId = writer.numDocs() - 1;
    writer.close();
    searcher = newSearcher(reader);
    return lastDocId;
  }

  /**
   * This method will prepare an index on {@link #numDocs} total docs.
   * Each doc will have multiple fields with terms up to its sequential number :
   * <p>
   * Doc 4
   * 1a
   * 2a
   * 3a
   * ...
   * na
   * <p>
   * This means that '1a' will have the max docFrequency n
   * While na will have min docFrequency ( = 1)
   * <p>
   * Each field will have a difference suffix.
   *
   * @return
   * @throws IOException
   */
  protected int initIndex() throws IOException {
    // add series of docs with terms of decreasing document frequency
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 1; i <= numDocs; i++) {
      addDocument(writer, getArithmeticSeriesWithSuffix(1, i, SUFFIX_A), getArithmeticSeriesWithSuffix(1, i, SUFFIX_B));
    }
    reader = writer.getReader();
    int lastDocId = writer.numDocs() - 1;
    writer.close();
    searcher = newSearcher(reader);
    return lastDocId;
  }

  protected int addDocumentWithSingleField(RandomIndexWriter writer, String[] fieldValues) throws IOException {
    Document doc = new Document();
    for (String text : fieldValues) {
      doc.add(newTextField(FIELD1, text, Field.Store.YES));
    }
    writer.addDocument(doc);
    return writer.numDocs() - 1;
  }

  protected int addDocument(RandomIndexWriter writer, String[] field1Values, String[] field2Values) throws IOException {
    Document doc = new Document();
    for (String value1 : field1Values) {
      doc.add(newTextField(FIELD1, value1, Field.Store.YES));
    }
    for (String value2 : field2Values) {
      doc.add(newTextField(FIELD2, value2, Field.Store.YES));
    }
    writer.addDocument(doc);
    return writer.numDocs() - 1;
  }

  /**
   * Generates an arithmetic sequence of terms ( common difference = 1),
   * A suffix is added to each term.
   * Each term will appear with a frequency of 1 .
   * e.g.
   * 1a
   * 2a
   * ...
   * na
   *
   * @param from
   * @param size
   * @return
   */
  protected String[] getArithmeticSeriesWithSuffix(int from, int size, String suffix) {
    String[] generatedStrings = new String[size];
    for (int i = 0; i < generatedStrings.length; i++) {
      generatedStrings[i] = String.valueOf(from + i) + suffix;
    }
    return generatedStrings;
  }

  /**
   * Generates the multiple values for a field.
   * Each term N will appear with a frequency of N .
   * e.g.
   * 1a
   * 2a 2a
   * 3a 3a 3a
   * 4a 4a 4a 4a
   * ...
   *
   * @param from
   * @param size
   * @return
   */
  protected String[] getTriangularArithmeticSeriesWithSuffix(int from, int size, String suffix) {
    String[] generatedStrings = new String[size];
    for (int i = 0; i < generatedStrings.length; i++) {
      StringBuilder singleFieldValue = new StringBuilder();
      for (int j = 0; j < from + i; j++) {
        singleFieldValue.append(String.valueOf(from + i) + suffix + " ");
      }
      generatedStrings[i] = singleFieldValue.toString().trim();
    }
    return generatedStrings;
  }

  protected void assertScoredTermsPriorityOrder(PriorityQueue<ScoredTerm> scoredTerms, Term[] expectedTerms) {
    for (int i = 0; scoredTerms.top() != null; i++) {
      ScoredTerm singleTerm = scoredTerms.pop();
      Term term = new Term(FIELD1, singleTerm.term);
      assertThat(term, is(expectedTerms[i]));
    }
  }

  protected void assertScoredTermsPriorityOrder(PriorityQueue<ScoredTerm> scoredTerms, Term[] expectedField1Terms, Term[] expectedField2Terms) {
    int i1 = 0;
    int i2 = 0;
    while (i1 < expectedField1Terms.length || i2 < expectedField2Terms.length) {
      ScoredTerm singleTerm = scoredTerms.pop();
      Term term = new Term(singleTerm.field, singleTerm.term);
      if (term.field().equals(FIELD1)) {
        assertThat(term, is(expectedField1Terms[i1]));
        i1++;
      } else {
        assertThat(term, is(expectedField2Terms[i2]));
        i2++;
      }
    }
  }

}
