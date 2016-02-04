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


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/**
 * Some simple regex tests, mostly converted from contrib's TestRegexQuery.
 */
public class TestRegexpQuery extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;
  private final String FN = "field";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(newTextField(FN, "the quick brown fox jumps over the lazy ??? dog 493432 49344", Field.Store.NO));
    writer.addDocument(doc);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  private Term newTerm(String value) {
    return new Term(FN, value);
  }
  
  private int regexQueryNrHits(String regex) throws IOException {
    RegexpQuery query = new RegexpQuery(newTerm(regex));
    return searcher.search(query, 5).totalHits;
  }
  
  public void testRegex1() throws IOException {
    assertEquals(1, regexQueryNrHits("q.[aeiou]c.*"));
  }
  
  public void testRegex2() throws IOException {
    assertEquals(0, regexQueryNrHits(".[aeiou]c.*"));
  }
  
  public void testRegex3() throws IOException {
    assertEquals(0, regexQueryNrHits("q.[aeiou]c"));
  }
  
  public void testNumericRange() throws IOException {
    assertEquals(1, regexQueryNrHits("<420000-600000>"));
    assertEquals(0, regexQueryNrHits("<493433-600000>"));
  }
  
  public void testRegexComplement() throws IOException {
    assertEquals(1, regexQueryNrHits("4934~[3]"));
    // not the empty lang, i.e. match all docs
    assertEquals(1, regexQueryNrHits("~#"));
  }
  
  public void testCustomProvider() throws IOException {
    AutomatonProvider myProvider = new AutomatonProvider() {
      // automaton that matches quick or brown
      private Automaton quickBrownAutomaton = Operations.union(Arrays
          .asList(Automata.makeString("quick"),
          Automata.makeString("brown"),
          Automata.makeString("bob")));
      
      @Override
      public Automaton getAutomaton(String name) {
        if (name.equals("quickBrown")) return quickBrownAutomaton;
        else return null;
      }
    };
    RegexpQuery query = new RegexpQuery(newTerm("<quickBrown>"), RegExp.ALL,
      myProvider, DEFAULT_MAX_DETERMINIZED_STATES);
    assertEquals(1, searcher.search(query, 5).totalHits);
  }
  
  /**
   * Test a corner case for backtracking: In this case the term dictionary has
   * 493432 followed by 49344. When backtracking from 49343... to 4934, it's
   * necessary to test that 4934 itself is ok before trying to append more
   * characters.
   */
  public void testBacktracking() throws IOException {
    assertEquals(1, regexQueryNrHits("4934[314]"));
  }
}
