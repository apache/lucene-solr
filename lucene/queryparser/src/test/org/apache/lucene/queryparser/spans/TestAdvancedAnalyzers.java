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

package org.apache.lucene.queryparser.spans;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.queryparser.spans.AnalyzingQueryParserBase.NORM_MULTI_TERMS;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAdvancedAnalyzers extends LuceneTestCase {

  private static IndexReader reader;
  private static IndexSearcher searcher;
  private static Directory directory;
  private static Analyzer synAnalyzer;
  private static Analyzer baseAnalyzer;
  private static Analyzer ucVowelAnalyzer;
  private static final String FIELD1 = "f1";
  private static final String FIELD2 = "f2";
  private static final String FIELD3 = "f3";
  private static final String FIELD4 = "f4";


  //   private static final CharacterRunAutomaton STOP_WORDS = new CharacterRunAutomaton(
  //     BasicOperations.union(Arrays.asList(makeString("a"), makeString("an"))));

  @BeforeClass
  public static void beforeClass() throws Exception {

    synAnalyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE,
            true);
        TokenFilter filter = new MockNonWhitespaceFilter(tokenizer);

        filter = new MockSynFilter(filter);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };

    baseAnalyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE,
            true);
        TokenFilter filter = new MockNonWhitespaceFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };

    ucVowelAnalyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE,
            true);
        TokenFilter filter = new MockUCVowelFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };
    Analyzer tmpUCVowelAnalyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE,
            true);
        TokenFilter filter = new MockUCVowelFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, baseAnalyzer)
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));
    String[] docs = new String[] {
        "abc_def",
        "lmnop",
        "abc",
        "qrs tuv",
        "qrs_tuv"
    };
    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(FIELD1, docs[i], Field.Store.YES));
      TextField tf = new TextField(FIELD2, docs[i], Field.Store.YES);
      tf.setTokenStream(ucVowelAnalyzer.tokenStream(FIELD2, docs[i]));
      doc.add(tf);
      doc.add(newTextField(FIELD3, docs[i], Field.Store.YES));

      TextField tf4 = new TextField(FIELD4, docs[i], Field.Store.YES);
      tf4.setTokenStream(tmpUCVowelAnalyzer.tokenStream(FIELD4, docs[i]));
      doc.add(tf4);
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    reader = null;
    directory = null;
    synAnalyzer = null;
    baseAnalyzer = null;
  }

  public void testSynBasic() throws Exception {
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, synAnalyzer);
    countSpansDocs(p, "tuv", 2, 2);

    countSpansDocs(p, "abc", 6, 4);
  }

  @Test
  public void testNonWhiteSpace() throws Exception {
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT,FIELD1, baseAnalyzer);
    String s = "[zqx_qrs^3.0]~3^2";
    Query q = p.parse(s);
    assertTrue(q instanceof SpanNearQuery);

    SpanNearQuery near = (SpanNearQuery)q;
    SpanQuery[] clauses = near.getClauses();
    assertEquals(2, clauses.length);

    assertEquals(3, near.getSlop());
    assertTrue(clauses[0] instanceof SpanTermQuery);
    assertTrue(clauses[1] instanceof SpanTermQuery);

    assertEquals("zqx", ((SpanTermQuery)clauses[0]).getTerm().text());
    assertEquals("qrs", ((SpanTermQuery)clauses[1]).getTerm().text());

    //take the boost from the phrase, ignore boost on term
    //not necessarily right choice, but this is how it works now
    assertEquals(2.0f, q.getBoost(), 0.00001f);

    s = "[zqx2_qrs3 lmnop]~3";
    p.setAutoGeneratePhraseQueries(true);
    q = p.parse(s);
    assertTrue(q instanceof SpanQuery);
    assertTrue(q instanceof SpanNearQuery);
    near = (SpanNearQuery)q;
    clauses = near.getClauses();
    assertEquals(2, clauses.length);

    assertEquals(3, near.getSlop());
    assertTrue(clauses[0] instanceof SpanNearQuery);
    assertTrue(clauses[1] instanceof SpanTermQuery);

    SpanNearQuery child = (SpanNearQuery)clauses[0];
    SpanQuery[] childClauses = child.getClauses();
    assertEquals(2, childClauses.length);

    assertEquals("zqx", ((SpanTermQuery)childClauses[0]).getTerm().text());
    assertEquals("qrs", ((SpanTermQuery)childClauses[1]).getTerm().text());

    assertTrue(child.isInOrder());
    assertEquals(child.getSlop(), 0);
  }

  //test different initializations/settings with multifield analyzers
  public void testAnalyzerCombos() throws Exception{

    //basic, correct set up
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, baseAnalyzer);
    assertEquals(1, countDocs((SpanQuery)p.parse("lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse("lm*op")));
    assertEquals(1, countDocs((SpanQuery)p.parse("LMNOP")));
    assertEquals(1, countDocs((SpanQuery)p.parse("LM*OP")));
    assertEquals(NORM_MULTI_TERMS.LOWERCASE, p.getNormMultiTerms());

    //basic, correct set up
    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD2, ucVowelAnalyzer);
    assertEquals(NORM_MULTI_TERMS.LOWERCASE, p.getNormMultiTerms());
    assertEquals(1, countDocs((SpanQuery)p.parse("lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse("LMNOP")));
    assertEquals(0, countDocs((SpanQuery)p.parse("LM*OP")));

    //set to lowercase only, won't analyze
    assertEquals(0, countDocs((SpanQuery)p.parse("lm*op")));
    p.setNormMultiTerms(NORM_MULTI_TERMS.ANALYZE);
    assertEquals(1, countDocs((SpanQuery)p.parse("lm*op")));
    assertEquals(1, countDocs((SpanQuery)p.parse("LM*OP")));

    //try sister field, to prove that default analyzer is ucVowelAnalyzer for 
    //unspecified fieldsd
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD4+":lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD4+":lm*op")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD4+":LMNOP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD4+":LM*OP")));

    //try mismatching sister field
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD3+":lmnop")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD3+":lm*op")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD3+":LMNOP")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD3+":LM*OP")));


    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, baseAnalyzer);
    assertEquals(p.getNormMultiTerms(), NORM_MULTI_TERMS.LOWERCASE);

    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, baseAnalyzer, ucVowelAnalyzer);
    assertEquals(NORM_MULTI_TERMS.ANALYZE, p.getNormMultiTerms());
    assertEquals(1, countDocs((SpanQuery)p.parse("lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse("LMNOP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lm*op")));

    //advanced, correct set up for both
    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD2, ucVowelAnalyzer, ucVowelAnalyzer);
    assertEquals(NORM_MULTI_TERMS.ANALYZE, p.getNormMultiTerms());
    assertEquals(1, countDocs((SpanQuery)p.parse("lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse("LMNOP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMNOP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lm*op")));

    p.setNormMultiTerms(NORM_MULTI_TERMS.NONE);
    assertEquals(NORM_MULTI_TERMS.NONE, p.getNormMultiTerms());
    assertEquals(1, countDocs((SpanQuery)p.parse("lmnop")));
    //analyzer still used on whole terms; don't forget!
    assertEquals(1, countDocs((SpanQuery)p.parse("LMNOP")));
    assertEquals(0, countDocs((SpanQuery)p.parse("LM*OP")));

    p.setNormMultiTerms(NORM_MULTI_TERMS.LOWERCASE);
    assertEquals(NORM_MULTI_TERMS.LOWERCASE, p.getNormMultiTerms());
    assertEquals(1, countDocs((SpanQuery)p.parse("lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse("LMNOP")));
    assertEquals(0, countDocs((SpanQuery)p.parse("LM*OP")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LM*OP")));

    //mismatch between default field and default analyzer; should return 0
    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, ucVowelAnalyzer);
    assertEquals(0, countDocs((SpanQuery)p.parse("lmnop")));
    assertEquals(0, countDocs((SpanQuery)p.parse("LMNOP")));
    assertEquals(0, countDocs((SpanQuery)p.parse("lmnOp")));

    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, baseAnalyzer, ucVowelAnalyzer);
    //cstr with two analyzers sets normMultiTerms = NORM_MULTI_TERM.ANALYZE
    //can't find any in field1 because these trigger multiTerm analysis
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD1+":lm*op")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD1+":lmno*")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD1+":lmmop~1")));

    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD1+":LM*OP")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD1+":LMNO*")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD1+":LMMOP~1")));

    //can find these in field2 because of multiterm analysis
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lm*op")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmno*")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmmop~1")));

    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LM*OP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMNO*")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMMOP~1")));

    //try basic use case
    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, baseAnalyzer);
    //can't find these in field2 because multiterm analysis is using baseAnalyzer
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lm*op")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lmno*")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lmmop~1")));

    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LM*OP")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LMNO*")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LMMOP~1")));


    p.setNormMultiTerms(NORM_MULTI_TERMS.ANALYZE);
    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD1, ucVowelAnalyzer, ucVowelAnalyzer);
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmnop")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lm*op")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmno*")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmmop~1")));

    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMNOP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LM*OP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMNO*")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMMOP~1")));


    //now try adding the wrong analyzer for the whole term, but the
    //right multiterm analyzer    
    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD2, baseAnalyzer, ucVowelAnalyzer);
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lmnop")));    
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lm*op")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmno*")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":lmmop~1")));

    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LMNOP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LM*OP")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMNO*")));
    assertEquals(1, countDocs((SpanQuery)p.parse(FIELD2+":LMMOP~1")));

    //now set them completely improperly
    p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD2, baseAnalyzer, baseAnalyzer);
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lmnop")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lm*op")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lmno*")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":lmmop~1")));

    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LMNOP")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LM*OP")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LMNO*")));
    assertEquals(0, countDocs((SpanQuery)p.parse(FIELD2+":LMMOP~1")));
  }

  private void countSpansDocs(SpanQueryParser p, String s, int spanCount,
      int docCount) throws Exception {
    SpanQuery q = (SpanQuery)p.parse(s);
    assertEquals("spanCount: " + s, spanCount, countSpans(q));
    assertEquals("docCount: " + s, docCount, countDocs(q));
  }

  private long countSpans(SpanQuery q) throws Exception {
    List<AtomicReaderContext> ctxs = reader.leaves();
    assert (ctxs.size() == 1);
    AtomicReaderContext ctx = ctxs.get(0);
    q = (SpanQuery) q.rewrite(ctx.reader());
    Spans spans = q.getSpans(ctx, null, new HashMap<Term, TermContext>());

    long i = 0;
    while (spans.next()) {
      i++;
    }
    return i;
  }

  private long countDocs(SpanQuery q) throws Exception {
    OpenBitSet docs = new OpenBitSet();
    List<AtomicReaderContext> ctxs = reader.leaves();
    assert (ctxs.size() == 1);
    AtomicReaderContext ctx = ctxs.get(0);
    IndexReaderContext parentCtx = reader.getContext();
    q = (SpanQuery) q.rewrite(ctx.reader());

    Set<Term> qTerms = new HashSet<Term>();
    q.extractTerms(qTerms);
    Map<Term, TermContext> termContexts = new HashMap<Term, TermContext>();

    for (Term t : qTerms) {
      TermContext c = TermContext.build(parentCtx, t);
      termContexts.put(t, c);
    }

    Spans spans = q.getSpans(ctx, null, termContexts);

    while (spans.next()) {
      docs.set(spans.doc());
    }
    long spanDocHits = docs.cardinality();
    // double check with a regular searcher
    TotalHitCountCollector coll = new TotalHitCountCollector();
    searcher.search(q, coll);
    assertEquals(coll.getTotalHits(), spanDocHits);
    return spanDocHits;
  }

  /**
   * Mocks a synonym filter. When it encounters "abc" it adds "qrs" and "tuv" 
   */
  private final static class MockSynFilter extends TokenFilter {
    private List<String> synBuffer = new LinkedList<String>();

    private final CharTermAttribute termAtt;
    private final PositionIncrementAttribute posIncrAtt;

    public MockSynFilter(TokenStream in) {
      super(in);
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws java.io.IOException {
      if (synBuffer.size() > 0) {
        termAtt.setEmpty().append(synBuffer.remove(0));
        posIncrAtt.setPositionIncrement(0);
        return true;
      } else {
        boolean next = input.incrementToken();
        if (!next) {
          return false;
        }
        String text = termAtt.toString();
        if (text.equals("abc")) {
          synBuffer.add("qrs");
          synBuffer.add("tuv");
        }
        return true;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
    }
  }


  /**
   * Mocks what happens in a non-whitespace language. Tokenizes on white space and "_". 
   */
  private final static class MockNonWhitespaceFilter extends TokenFilter {
    private List<String> buffer = new LinkedList<String>();

    private final CharTermAttribute termAtt;

    public MockNonWhitespaceFilter(TokenStream in) {
      super(in);
      termAtt = addAttribute(CharTermAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws java.io.IOException {
      if (buffer.size() > 0) {
        termAtt.setEmpty().append(buffer.remove(0));
        return true;
      } else {
        boolean next = input.incrementToken();
        if (!next) {
          return false;
        }
        String text = termAtt.toString();

        String[] bits = text.split("_");
        String ret = text;
        if (bits.length > 1) {
          ret = bits[0];
          for (int i = 1; i < bits.length; i++) {
            buffer.add(bits[i]);
          }
        }
        termAtt.setEmpty().append(ret);
        return true;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
    }
  }

  //mocks uppercasing vowels to test different analyzers for different fields
  private final static class MockUCVowelFilter extends TokenFilter {
    private final Pattern PATTERN = Pattern.compile("([aeiou])");
    private final CharTermAttribute termAtt;

    public MockUCVowelFilter(TokenStream in) {
      super(in);
      termAtt = addAttribute(CharTermAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws java.io.IOException {

      boolean next = input.incrementToken();
      if (!next) {
        return false;
      }
      String text = termAtt.toString().toLowerCase();
      Matcher m = PATTERN.matcher(text);
      StringBuffer sb = new StringBuffer();
      while (m.find()) {
        m.appendReplacement(sb, m.group(1).toUpperCase());
      }
      m.appendTail(sb);
      text = sb.toString();
      termAtt.setEmpty().append(text);
      return true;

    }

    @Override
    public void reset() throws IOException {
      super.reset();
    }
  }
}
