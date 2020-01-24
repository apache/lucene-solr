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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Random;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ValidatingTokenFilter;
import org.apache.lucene.analysis.core.TypeTokenFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.in.IndicNormalizationFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

public class TestConditionalTokenFilter extends BaseTokenStreamTestCase {

  boolean closed = false;
  boolean ended = false;
  boolean reset = false;

  private final class AssertingLowerCaseFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public AssertingLowerCaseFilter(TokenStream in) {
      super(in);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        CharacterUtils.toLowerCase(termAtt.buffer(), 0, termAtt.length());
        return true;
      } else
        return false;
    }

    @Override
    public void end() throws IOException {
      super.end();
      ended = true;
    }

    @Override
    public void close() throws IOException {
      super.close();
      closed = true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      reset = true;
    }
  }

  private class SkipMatchingFilter extends ConditionalTokenFilter {
    private final Pattern pattern;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    SkipMatchingFilter(TokenStream input, Function<TokenStream, TokenStream> inputFactory, String termRegex) {
      super(input, inputFactory);
      pattern = Pattern.compile(termRegex);
    }

    @Override
    protected boolean shouldFilter() throws IOException {
      return pattern.matcher(termAtt.toString()).matches() == false;
    }
  }

  public void testSimple() throws IOException {
    TokenStream stream = whitespaceMockTokenizer("Alice Bob Clara David");
    TokenStream t = new SkipMatchingFilter(stream, AssertingLowerCaseFilter::new, ".*o.*");
    assertTokenStreamContents(t, new String[]{ "alice", "Bob", "clara", "david" });
    assertTrue(closed);
    assertTrue(reset);
    assertTrue(ended);
  }

  private final class TokenSplitter extends TokenFilter {

    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    State state = null;
    String half;

    protected TokenSplitter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (half == null) {
        state = captureState();
        if (input.incrementToken() == false) {
          return false;
        }
        half = termAtt.toString().substring(4);
        termAtt.setLength(4);
        return true;
      }
      restoreState(state);
      termAtt.setEmpty().append(half);
      half = null;
      return true;
    }
  }

  public void testMultitokenWrapping() throws IOException {
    TokenStream stream = whitespaceMockTokenizer("tokenpos1 tokenpos2 tokenpos3 tokenpos4");
    TokenStream ts = new SkipMatchingFilter(stream, TokenSplitter::new, ".*2.*");
    assertTokenStreamContents(ts, new String[]{
        "toke", "npos1", "tokenpos2", "toke", "npos3", "toke", "npos4"
    });
  }

  private final class EndTrimmingFilter extends FilteringTokenFilter {

    final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    public EndTrimmingFilter(TokenStream in) {
      super(in);
    }

    @Override
    protected boolean accept() throws IOException {
      return true;
    }

    @Override
    public void end() throws IOException {
      super.end();
      offsetAtt.setOffset(0, offsetAtt.endOffset() - 2);
    }
  }

  public void testEndPropagation() throws IOException {

    CannedTokenStream cts2 = new CannedTokenStream(0, 20,
        new Token("alice", 0, 5), new Token("bob", 6, 8)
    );
    TokenStream ts2 = new ConditionalTokenFilter(cts2, EndTrimmingFilter::new) {
      @Override
      protected boolean shouldFilter() throws IOException {
        return true;
      }
    };
    assertTokenStreamContents(ts2, new String[]{ "alice", "bob" },
        null, null, null, null, null, 18);

    CannedTokenStream cts1 = new CannedTokenStream(0, 20,
        new Token("alice", 0, 5), new Token("bob", 6, 8)
    );
    TokenStream ts1 = new ConditionalTokenFilter(cts1, EndTrimmingFilter::new) {
      @Override
      protected boolean shouldFilter() throws IOException {
        return false;
      }
    };
    assertTokenStreamContents(ts1, new String[]{ "alice", "bob" },
        null, null, null, null, null, 20);

  }

  public void testWrapGraphs() throws Exception {

    TokenStream stream = whitespaceMockTokenizer("a b c d e");

    SynonymMap sm;
    try (Analyzer analyzer = new MockAnalyzer(random())) {
      SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
      parser.parse(new StringReader("a b, f\nc d, g"));
      sm = parser.build();
    }

    TokenStream ts = new SkipMatchingFilter(stream, in -> new SynonymGraphFilter(in, sm, true), "c");

    assertTokenStreamContents(ts, new String[]{
        "f", "a", "b", "c", "d", "e"
        },
        null, null, null,
        new int[]{
        1, 0, 1, 1, 1, 1
        },
        new int[]{
        2, 1, 1, 1, 1, 1
        });

  }

  public void testReadaheadWithNoFiltering() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new ClassicTokenizer();
        TokenStream sink = new ConditionalTokenFilter(source, in -> new ShingleFilter(in, 2)) {
          @Override
          protected boolean shouldFilter() throws IOException {
            return true;
          }
        };
        return new TokenStreamComponents(source, sink);
      }
    };

    String input = "one two three four";

    try (TokenStream ts = analyzer.tokenStream("", input)) {
      assertTokenStreamContents(ts, new String[]{
          "one", "one two",
          "two", "two three",
          "three", "three four",
          "four"
      });
    }
  }

  public void testReadaheadWithFiltering() throws IOException {

    CharArraySet protectedTerms = new CharArraySet(2, true);
    protectedTerms.add("three");

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new ClassicTokenizer();
        TokenStream sink = new ProtectedTermFilter(protectedTerms, source, in -> new ShingleFilter(in, 2));
        sink = new ValidatingTokenFilter(sink, "1");
        return new TokenStreamComponents(source, sink);
      }
    };

    String input = "one two three four";

    try (TokenStream ts = analyzer.tokenStream("", input)) {
      assertTokenStreamContents(ts, new String[]{
          "one", "one two", "two", "three", "four"
      }, new int[]{
           0,     0,         4,     8,       14
      }, new int[]{
           3,     7,         7,     13,      18
      }, new int[]{
           1,     0,         1,     1,       1
      }, new int[]{
           1,     2,         1,     1,       1
      }, 18);
    }
  }

  public void testFilteringWithReadahead() throws IOException {

    CharArraySet protectedTerms = new CharArraySet(2, true);
    protectedTerms.add("two");
    protectedTerms.add("two three");

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new StandardTokenizer();
        TokenStream sink = new ShingleFilter(source, 3);
        sink = new ProtectedTermFilter(protectedTerms, sink, in -> new TypeTokenFilter(in, Collections.singleton("ALL"), true));
        return new TokenStreamComponents(source, sink);
      }
    };

    String input = "one two three four";

    try (TokenStream ts = analyzer.tokenStream("", input)) {
      assertTokenStreamContents(ts, new String[]{
          "two", "two three"
      }, new int[]{
           4,     4
      }, new int[]{
           7,     13
      }, new int[]{
           2,     0
      }, new int[]{
           1,     2
      }, 18);
    }

  }

  public void testMultipleConditionalFilters() throws IOException {
    TokenStream stream = whitespaceMockTokenizer("Alice Bob Clara David");
    TokenStream t = new SkipMatchingFilter(stream, in -> {
      TruncateTokenFilter truncateFilter = new TruncateTokenFilter(in, 2);
      return new AssertingLowerCaseFilter(truncateFilter);
    }, ".*o.*");

    assertTokenStreamContents(t, new String[]{"al", "Bob", "cl", "da"});
    assertTrue(closed);
    assertTrue(reset);
    assertTrue(ended);
  }

  public void testFilteredTokenFilters() throws IOException {

    CharArraySet protectedTerms = new CharArraySet(2, true);
    protectedTerms.add("foobar");

    TokenStream ts = whitespaceMockTokenizer("wuthering foobar abc");
    ts = new ProtectedTermFilter(protectedTerms, ts, in -> new LengthFilter(in, 1, 4));
    assertTokenStreamContents(ts, new String[]{ "foobar", "abc" });

    ts = whitespaceMockTokenizer("foobar abc");
    ts = new ProtectedTermFilter(protectedTerms, ts, in -> new LengthFilter(in, 1, 4));
    assertTokenStreamContents(ts, new String[]{ "foobar", "abc" });

  }

  public void testConsistentOffsets() throws IOException {

    long seed = random().nextLong();
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new NGramTokenizer();
        TokenStream sink = new ValidatingTokenFilter(new KeywordRepeatFilter(source), "stage 0");
        sink = new ValidatingTokenFilter(sink, "stage 1");
        sink = new RandomSkippingFilter(sink, seed, in -> new TypeTokenFilter(in, Collections.singleton("word")));
        sink = new ValidatingTokenFilter(sink, "last stage");
        return new TokenStreamComponents(source, sink);
      }
    };

    checkRandomData(random(), analyzer, 1);

  }

  public void testEndWithShingles() throws IOException {
    TokenStream ts = whitespaceMockTokenizer("cyk jvboq \u092e\u0962\u093f");
    ts = new GermanStemFilter(ts);
    ts = new NonRandomSkippingFilter(ts, in -> new FixedShingleFilter(in, 2), true, false, true);
    ts = new NonRandomSkippingFilter(ts, IndicNormalizationFilter::new, true);

    assertTokenStreamContents(ts, new String[]{"jvboq"});
  }

  public void testInternalPositionAdjustment() throws IOException {
    // check that the partial TokenStream sent to the condition filter begins with a posInc of 1,
    // even if the input stream has a posInc of 0 at that position, and that the filtered stream
    // has the correct posInc afterwards
    TokenStream ts = whitespaceMockTokenizer("one two three");
    ts = new KeywordRepeatFilter(ts);
    ts = new NonRandomSkippingFilter(ts, PositionAssertingTokenFilter::new, false, true, true, true, true, false);

    assertTokenStreamContents(ts,
        new String[]{ "one", "one", "two", "two", "three", "three" },
        new int[]{    1,      0,    1,      0,    1,        0});
  }

  private static final class PositionAssertingTokenFilter extends TokenFilter {

    boolean reset = false;
    final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

    protected PositionAssertingTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.reset = true;
    }

    @Override
    public boolean incrementToken() throws IOException {
      boolean more = input.incrementToken();
      if (more && reset) {
        assertEquals(1, posIncAtt.getPositionIncrement());
      }
      reset = false;
      return more;
    }
  }

  private static class RandomSkippingFilter extends ConditionalTokenFilter {

    Random random;
    final long seed;

    protected RandomSkippingFilter(TokenStream input, long seed, Function<TokenStream, TokenStream> inputFactory) {
      super(input, inputFactory);
      this.seed = seed;
      this.random = new Random(seed);
    }

    @Override
    protected boolean shouldFilter() throws IOException {
      return random.nextBoolean();
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      random = new Random(seed);
    }
  }

  private static class NonRandomSkippingFilter extends ConditionalTokenFilter {

    final boolean[] shouldFilters;
    int pos;

    /**
     * Create a new BypassingTokenFilter
     *
     * @param input        the input TokenStream
     * @param inputFactory a factory function to create a new instance of the TokenFilter to wrap
     */
    protected NonRandomSkippingFilter(TokenStream input, Function<TokenStream, TokenStream> inputFactory, boolean... shouldFilters) {
      super(input, inputFactory);
      this.shouldFilters = shouldFilters;
    }

    @Override
    protected boolean shouldFilter() throws IOException {
      return shouldFilters[pos++ % shouldFilters.length];
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      pos = 0;
    }
  }

}
