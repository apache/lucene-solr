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
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
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
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.KEYWORD, false))
        .setMaxBufferedDocs(_TestUtil.nextInt(random, 50, 1000)));
    Document doc = new Document();
    Field field = newField("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(field);
    List<String> terms = new ArrayList<String>();
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      String s = _TestUtil.randomUnicodeString(random);
      field.setValue(s);
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
    searcher1.close();
    searcher2.close();
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
      CharsRef utf16 = new CharsRef(10);

      private SimpleAutomatonTermsEnum(TermsEnum tenum) throws IOException {
        super(tenum);
        setInitialSeekTerm(new BytesRef(""));
      }
      
      @Override
      protected AcceptStatus accept(BytesRef term) throws IOException {
        UnicodeUtil.UTF8toUTF16(term.bytes, term.offset, term.length, utf16);
        return runAutomaton.run(utf16.chars, 0, utf16.length) ? 
            AcceptStatus.YES : AcceptStatus.NO;
      }
    }

    @Override
    public String toString(String field) {
      return field.toString() + automaton.toString();
    }
  }
  
  /** test a bunch of random regular expressions */
  public void testRegexps() throws Exception {
    // we generate aweful regexps: good for testing.
    // but for preflex codec, the test can be very slow, so use less iterations.
    int num = CodecProvider.getDefault().getFieldCodec("field").equals("PreFlex") ? 100 * RANDOM_MULTIPLIER : atLeast(1000);
    for (int i = 0; i < num; i++) {
      String reg = AutomatonTestUtil.randomRegexp(random);
      assertSame(reg);
    }
  }
  
  /** check that the # of hits is the same as from a very
   * simple regexpquery implementation.
   */
  protected void assertSame(String regexp) throws IOException {   
    RegexpQuery smart = new RegexpQuery(new Term("field", regexp), RegExp.NONE);
    DumbRegexpQuery dumb = new DumbRegexpQuery(new Term("field", regexp), RegExp.NONE);
    
    // we can't compare the two if automaton rewrites to a simpler enum.
    // for example: "a\uda07\udcc7?.*?" gets rewritten to a simpler query:
    // a\uda07* prefixquery. Prefixquery then does the "wrong" thing, which
    // isn't really wrong as the query was undefined to begin with... but not
    // automatically comparable.
    
    // TODO: does this check even matter anymore?!
    Terms terms = MultiFields.getTerms(searcher1.getIndexReader(), "field");
    if (!(smart.getTermsEnum(terms) instanceof AutomatonTermsEnum))
      return;
    
    TopDocs smartDocs = searcher1.search(smart, 25);
    TopDocs dumbDocs = searcher2.search(dumb, 25);

    CheckHits.checkEqual(smart, smartDocs.scoreDocs, dumbDocs.scoreDocs);
  }
}
