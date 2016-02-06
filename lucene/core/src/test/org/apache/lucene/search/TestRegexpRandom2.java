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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Create an index with random unicode terms
 * Generates random regexps, and validates against a simple impl.
 */
public class TestRegexpRandom2 extends LuceneTestCase {
  protected IndexSearcher searcher1;
  protected IndexSearcher searcher2;
  private IndexReader reader;
  private Directory dir;
  protected String fieldName;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    fieldName = random().nextBoolean() ? "field" : ""; // sometimes use an empty string as field name
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false))
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 50, 1000)));
    Document doc = new Document();
    Field field = newStringField(fieldName, "", Field.Store.NO);
    doc.add(field);
    Field dvField = new SortedDocValuesField(fieldName, new BytesRef());
    doc.add(dvField);
    List<String> terms = new ArrayList<>();
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      String s = TestUtil.randomUnicodeString(random());
      field.setStringValue(s);
      dvField.setBytesValue(new BytesRef(s));
      terms.add(s);
      writer.addDocument(doc);
    }

    if (VERBOSE) {
      // utf16 order
      Collections.sort(terms);
      System.out.println("UTF16 order:");
      for(String s : terms) {
        System.out.println("  " + UnicodeUtil.toHexString(s));
      }
    }
    
    reader = writer.getReader();
    searcher1 = newSearcher(reader);
    searcher2 = newSearcher(reader);
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  /** a stupid regexp query that just blasts thru the terms */
  private class DumbRegexpQuery extends MultiTermQuery {
    private final Automaton automaton;
    
    DumbRegexpQuery(Term term, int flags) {
      super(term.field());
      RegExp re = new RegExp(term.text(), flags);
      automaton = re.toAutomaton();
    }
    
    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
      return new SimpleAutomatonTermsEnum(terms.iterator());
    }

    private class SimpleAutomatonTermsEnum extends FilteredTermsEnum {
      CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
      CharsRefBuilder utf16 = new CharsRefBuilder();

      private SimpleAutomatonTermsEnum(TermsEnum tenum) {
        super(tenum);
        setInitialSeekTerm(new BytesRef(""));
      }
      
      @Override
      protected AcceptStatus accept(BytesRef term) throws IOException {
        utf16.copyUTF8Bytes(term.bytes, term.offset, term.length);
        return runAutomaton.run(utf16.chars(), 0, utf16.length()) ? 
            AcceptStatus.YES : AcceptStatus.NO;
      }
    }

    @Override
    public String toString(String field) {
      return field.toString() + automaton.toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      final DumbRegexpQuery that = (DumbRegexpQuery) obj;
      return automaton.equals(that.automaton);
    }
  }
  
  /** test a bunch of random regular expressions */
  public void testRegexps() throws Exception {
    int num = atLeast(1000);
    for (int i = 0; i < num; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random());
      if (VERBOSE) {
        System.out.println("TEST: regexp='" + reg + "'");
      }
      assertSame(reg);
    }
  }
  
  /** check that the # of hits is the same as from a very
   * simple regexpquery implementation.
   */
  protected void assertSame(String regexp) throws IOException {   
    RegexpQuery smart = new RegexpQuery(new Term(fieldName, regexp), RegExp.NONE);
    DumbRegexpQuery dumb = new DumbRegexpQuery(new Term(fieldName, regexp), RegExp.NONE);
   
    TopDocs smartDocs = searcher1.search(smart, 25);
    TopDocs dumbDocs = searcher2.search(dumb, 25);

    CheckHits.checkEqual(smart, smartDocs.scoreDocs, dumbDocs.scoreDocs);
  }
}
