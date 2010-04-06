package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Test the automaton query for several unicode corner cases,
 * specifically enumerating strings/indexes containing supplementary characters,
 * and the differences between UTF-8/UTF-32 and UTF-16 binary sort order.
 */
public class TestAutomatonQueryUnicode extends LuceneTestCase {
  private IndexSearcher searcher;

  private final String FN = "field";

  public void setUp() throws Exception {
    super.setUp();
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new KeywordAnalyzer(), true,
        IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    Field titleField = new Field("title", "some title", Field.Store.NO,
        Field.Index.ANALYZED);
    Field field = new Field(FN, "", Field.Store.NO,
        Field.Index.ANALYZED);
    Field footerField = new Field("footer", "a footer", Field.Store.NO,
        Field.Index.ANALYZED);
    doc.add(titleField);
    doc.add(field);
    doc.add(footerField);
    field.setValue("\uD866\uDF05abcdef");
    writer.addDocument(doc);
    field.setValue("\uD866\uDF06ghijkl");
    writer.addDocument(doc);
    // this sorts before the previous two in UTF-8/UTF-32, but after in UTF-16!!!
    field.setValue("\uFB94mnopqr"); 
    writer.addDocument(doc);
    field.setValue("\uFB95stuvwx"); // this one too.
    writer.addDocument(doc);
    field.setValue("a\uFFFCbc");
    writer.addDocument(doc);
    field.setValue("a\uFFFDbc");
    writer.addDocument(doc);
    field.setValue("a\uFFFEbc");
    writer.addDocument(doc);
    field.setValue("a\uFB94bc");
    writer.addDocument(doc);
    field.setValue("bacadaba");
    writer.addDocument(doc);
    field.setValue("\uFFFD");
    writer.addDocument(doc);
    field.setValue("\uFFFD\uD866\uDF05");
    writer.addDocument(doc);
    field.setValue("\uFFFD\uFFFD");
    writer.addDocument(doc);
    writer.optimize();
    writer.close();
    searcher = new IndexSearcher(directory, true);
  }

  public void tearDown() throws Exception {
    searcher.close();
    super.tearDown();
  }

  private Term newTerm(String value) {
    return new Term(FN, value);
  }

  private int automatonQueryNrHits(AutomatonQuery query) throws IOException {
    return searcher.search(query, 5).totalHits;
  }

  private void assertAutomatonHits(int expected, Automaton automaton)
      throws IOException {
    AutomatonQuery query = new AutomatonQuery(newTerm("bogus"), automaton);

    query.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    assertEquals(expected, automatonQueryNrHits(query));

    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
    assertEquals(expected, automatonQueryNrHits(query));

    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    assertEquals(expected, automatonQueryNrHits(query));

    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
    assertEquals(expected, automatonQueryNrHits(query));
  }

  /**
   * Test that AutomatonQuery interacts with lucene's sort order correctly.
   * 
   * This expression matches something either starting with the arabic
   * presentation forms block, or a supplementary character.
   */
  public void testSortOrder() throws IOException {
    Automaton a = new RegExp("((\uD866\uDF05)|\uFB94).*").toAutomaton();
    assertAutomatonHits(2, a);
  }
  
  /**
   * Test that AutomatonQuery properly seeks to supplementary characters.
   * Transitions are modeled as UTF-16 code units, so without special handling
   * by default it will try to seek to a lead surrogate with some DFAs
   */
  public void testSeekSurrogate() throws IOException {
    Automaton a = new RegExp("\uD866[a\uDF05\uFB93][a-z]{0,5}[fl]").toAutomaton();
    assertAutomatonHits(1, a);
  }
  
  /**
   * Try seeking to an ending lead surrogate.
   */
  public void testSeekSurrogate2() throws IOException {
    Automaton a = new RegExp("\uD866(\uDF06ghijkl)?").toAutomaton();
    assertAutomatonHits(1, a);
  }
  
  /**
   * Try seeking to an starting trail surrogate.
   */
  public void testSeekSurrogate3() throws IOException {
    Automaton a = new RegExp("[\uDF06\uFB94]mnopqr").toAutomaton();
    assertAutomatonHits(1, a);
  }
  
  /**
   * Try seeking to an medial/final trail surrogate.
   */
  public void testSeekSurrogate4() throws IOException {
    Automaton a = new RegExp("a[\uDF06\uFB94]bc").toAutomaton();
    assertAutomatonHits(1, a);
  }
  
  /**
   * Ensure the 'constant suffix' does not contain a leading trail surrogate.
   */
  public void testSurrogateSuffix() throws IOException {
    Automaton a = new RegExp(".*[\uD865\uD866]\uDF06ghijkl").toAutomaton();
    assertAutomatonHits(1, a);
  }
  
  /**
   * Try when the constant suffix is only a leading trail surrogate.
   * instead this must use an empty suffix.
   */
  public void testSurrogateSuffix2() throws IOException {
    Automaton a = new RegExp(".*\uDF05").toAutomaton();
    assertAutomatonHits(1, a);
  }
}
