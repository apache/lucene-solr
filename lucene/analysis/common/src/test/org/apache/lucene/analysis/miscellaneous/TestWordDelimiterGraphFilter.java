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
import java.util.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.*;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE;

/**
 * New WordDelimiterGraphFilter tests... most of the tests are in ConvertedLegacyTest
 * TODO: should explicitly test things like protWords and not rely on
 * the factory tests in Solr.
 */
public class TestWordDelimiterGraphFilter extends BaseTokenStreamTestCase {

  public void testOffsets() throws IOException {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    // test that subwords and catenated subwords have
    // the correct offsets.
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token("foo-bar", 5, 12)), DEFAULT_WORD_DELIM_TABLE, flags, null);

    assertTokenStreamContents(wdf, 
                              new String[] { "foobar", "foo", "bar" },
                              new int[] { 5, 5, 9 }, 
                              new int[] { 12, 8, 12 });

    // with illegal offsets:
    wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token("foo-bar", 5, 6)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    assertTokenStreamContents(wdf,
                              new String[] { "foobar", "foo", "bar" },
                              new int[] { 5, 5, 5 },
                              new int[] { 6, 6, 6 });
  }
  
  public void testOffsetChange() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token("übelkeit)", 7, 16)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf,
        new String[] { "übelkeit" },
        new int[] { 7 },
        new int[] { 15 });
  }
  
  public void testOffsetChange2() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token("(übelkeit", 7, 17)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    // illegal offsets:
    assertTokenStreamContents(wdf,
                              new String[] { "übelkeit" },
                              new int[] { 7 },
                              new int[] { 17 });
  }
  
  public void testOffsetChange3() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token("(übelkeit", 7, 16)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    assertTokenStreamContents(wdf,
                              new String[] { "übelkeit" },
                              new int[] { 8 },
                              new int[] { 16 });
  }
  
  public void testOffsetChange4() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token("(foo,bar)", 7, 16)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf,
        new String[] { "foobar", "foo", "bar"},
        new int[] { 8, 8, 12 },
        new int[] { 15, 11, 15 });
  }

  public void doSplit(final String input, String... output) throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(keywordMockTokenizer(input),
        WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf, output);
  }

  public void testSplits() throws Exception {
    doSplit("basic-split","basic","split");
    doSplit("camelCase","camel","Case");

    // non-space marking symbol shouldn't cause split
    // this is an example in Thai    
    doSplit("\u0e1a\u0e49\u0e32\u0e19","\u0e1a\u0e49\u0e32\u0e19");
    // possessive followed by delimiter
    doSplit("test's'", "test");

    // some russian upper and lowercase
    doSplit("Роберт", "Роберт");
    // now cause a split (russian camelCase)
    doSplit("РобЕрт", "Роб", "Ерт");

    // a composed titlecase character, don't split
    doSplit("aǅungla", "aǅungla");
    
    // a modifier letter, don't split
    doSplit("ســـــــــــــــــلام", "ســـــــــــــــــلام");
    
    // enclosing mark, don't split
    doSplit("test⃝", "test⃝");
    
    // combining spacing mark (the virama), don't split
    doSplit("हिन्दी", "हिन्दी");
    
    // don't split non-ascii digits
    doSplit("١٢٣٤", "١٢٣٤");
    
    // don't split supplementaries into unpaired surrogates
    doSplit("𠀀𠀀", "𠀀𠀀");
  }
  
  public void doSplitPossessive(int stemPossessive, final String input, final String... output) throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS;
    flags |= (stemPossessive == 1) ? STEM_ENGLISH_POSSESSIVE : 0;
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(keywordMockTokenizer(input), flags, null);

    assertTokenStreamContents(wdf, output);
  }
  
  /*
   * Test option that allows disabling the special "'s" stemming, instead treating the single quote like other delimiters. 
   */
  public void testPossessives() throws Exception {
    doSplitPossessive(1, "ra's", "ra");
    doSplitPossessive(0, "ra's", "ra", "s");
  }
  
  public void testTokenType() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    // test that subwords and catenated subwords have
    // the correct offsets.
    Token token = new Token("foo-bar", 5, 12);
    token.setType("mytype");
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(token), DEFAULT_WORD_DELIM_TABLE, flags, null);

    assertTokenStreamContents(wdf, 
                              new String[] {"foobar", "foo", "bar"},
                              new String[] {"mytype", "mytype", "mytype"});
  }
  
  /*
   * Set a large position increment gap of 10 if the token is "largegap" or "/"
   */
  private static final class LargePosIncTokenFilter extends TokenFilter {
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    
    protected LargePosIncTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (termAtt.toString().equals("largegap") || termAtt.toString().equals("/"))
          posIncAtt.setPositionIncrement(10);
        return true;
      } else {
        return false;
      }
    }  
  }

  public void testPositionIncrements() throws Exception {
    final int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    final CharArraySet protWords = new CharArraySet(new HashSet<>(Arrays.asList("NUTCH")), false);
    
    /* analyzer that uses whitespace + wdf */
    Analyzer a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(
            tokenizer,
            flags, protWords));
      }
    };

    /* in this case, works as expected. */
    assertAnalyzesTo(a, "LUCENE / SOLR", new String[] { "LUCENE", "SOLR" },
        new int[] { 0, 9 },
        new int[] { 6, 13 },
        null,
        new int[] { 1, 2 },
        null,
        false);

    /* only in this case, posInc of 2 ?! */
    assertAnalyzesTo(a, "LUCENE / solR", new String[] { "LUCENE", "solR", "sol", "R" },
        new int[] { 0, 9, 9, 12 },
        new int[] { 6, 13, 12, 13 },
        null,                     
        new int[] { 1, 2, 0, 1 },
        null,
        false);
    
    assertAnalyzesTo(a, "LUCENE / NUTCH SOLR", new String[] { "LUCENE", "NUTCH", "SOLR" },
        new int[] { 0, 9, 15 },
        new int[] { 6, 14, 19 },
        null,
        new int[] { 1, 2, 1 },
        null,
        false);
    
    /* analyzer that will consume tokens with large position increments */
    Analyzer a2 = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(
            new LargePosIncTokenFilter(tokenizer),
            flags, protWords));
      }
    };
    
    /* increment of "largegap" is preserved */
    assertAnalyzesTo(a2, "LUCENE largegap SOLR", new String[] { "LUCENE", "largegap", "SOLR" },
        new int[] { 0, 7, 16 },
        new int[] { 6, 15, 20 },
        null,
        new int[] { 1, 10, 1 },
        null,
        false);
    
    /* the "/" had a position increment of 10, where did it go?!?!! */
    assertAnalyzesTo(a2, "LUCENE / SOLR", new String[] { "LUCENE", "SOLR" },
        new int[] { 0, 9 },
        new int[] { 6, 13 },
        null,
        new int[] { 1, 11 },
        null,
        false);
    
    /* in this case, the increment of 10 from the "/" is carried over */
    assertAnalyzesTo(a2, "LUCENE / solR", new String[] { "LUCENE", "solR", "sol", "R" },
        new int[] { 0, 9, 9, 12 },
        new int[] { 6, 13, 12, 13 },
        null,
        new int[] { 1, 11, 0, 1 },
        null,
        false);
    
    assertAnalyzesTo(a2, "LUCENE / NUTCH SOLR", new String[] { "LUCENE", "NUTCH", "SOLR" },
        new int[] { 0, 9, 15 },
        new int[] { 6, 14, 19 },
        null,
        new int[] { 1, 11, 1 },
        null,
        false);

    Analyzer a3 = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        StopFilter filter = new StopFilter(tokenizer, StandardAnalyzer.STOP_WORDS_SET);
        return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(filter, flags, protWords));
      }
    };

    assertAnalyzesTo(a3, "lucene.solr", 
        new String[] { "lucenesolr", "lucene", "solr" },
        new int[] { 0, 0, 7 },
        new int[] { 11, 6, 11 },
        null,
        new int[] { 1, 0, 1 },
        null,
        false);

    /* the stopword should add a gap here */
    assertAnalyzesTo(a3, "the lucene.solr", 
        new String[] { "lucenesolr", "lucene", "solr" }, 
        new int[] { 4, 4, 11 }, 
        new int[] { 15, 10, 15 },
        null,
        new int[] { 2, 0, 1 },
        null,
        false);

    IOUtils.close(a, a2, a3);
  }
  
  /** concat numbers + words + all */
  public void testLotsOfConcatenating() throws Exception {
    final int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_WORDS | CATENATE_NUMBERS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;    

    /* analyzer that uses whitespace + wdf */
    Analyzer a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(tokenizer, flags, null));
      }
    };
    
    assertAnalyzesTo(a, "abc-def-123-456", 
        new String[] { "abcdef123456", "abcdef", "abc", "def", "123456", "123", "456" }, 
        new int[] { 0, 0, 0, 4, 8, 8, 12 }, 
        new int[] { 15, 7, 3, 7, 15, 11, 15 },
        null,
        new int[] { 1, 0, 0, 1, 1, 0, 1 },
        null,
        false);
    a.close();
  }
  
  /** concat numbers + words + all + preserve original */
  public void testLotsOfConcatenating2() throws Exception {
    final int flags = PRESERVE_ORIGINAL | GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_WORDS | CATENATE_NUMBERS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;    

    /* analyzer that uses whitespace + wdf */
    Analyzer a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(tokenizer, flags, null));
      }
    };
    
    assertAnalyzesTo(a, "abc-def-123-456", 
                     new String[] { "abcdef123456", "abc-def-123-456", "abcdef", "abc", "def", "123456", "123", "456" }, 
                     new int[] { 0, 0, 0, 0, 4, 8, 8, 12 }, 
                     new int[] { 15, 15, 7, 3, 7, 15, 11, 15 },
                     null,
                     new int[] { 1, 0, 0, 0, 1, 1, 0, 1 },
                     null,
                     false);
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    int numIterations = atLeast(5);
    for (int i = 0; i < numIterations; i++) {
      final int flags = random().nextInt(512);
      final CharArraySet protectedWords;
      if (random().nextBoolean()) {
        protectedWords = new CharArraySet(new HashSet<>(Arrays.asList("a", "b", "cd")), false);
      } else {
        protectedWords = null;
      }
      
      Analyzer a = new Analyzer() {
        
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(tokenizer, flags, protectedWords));
        }
      };
      // TODO: properly support positionLengthAttribute
      checkRandomData(random(), a, 200*RANDOM_MULTIPLIER, 20, false, false);
      a.close();
    }
  }
  
  /** blast some enormous random strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    int numIterations = atLeast(5);
    for (int i = 0; i < numIterations; i++) {
      final int flags = random().nextInt(512);
      final CharArraySet protectedWords;
      if (random().nextBoolean()) {
        protectedWords = new CharArraySet(new HashSet<>(Arrays.asList("a", "b", "cd")), false);
      } else {
        protectedWords = null;
      }
      
      Analyzer a = new Analyzer() {
        
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          TokenStream wdgf = new WordDelimiterGraphFilter(tokenizer, flags, protectedWords);
          return new TokenStreamComponents(tokenizer, wdgf);
        }
      };
      // TODO: properly support positionLengthAttribute
      checkRandomData(random(), a, 20*RANDOM_MULTIPLIER, 8192, false, false);
      a.close();
    }
  }
  
  public void testEmptyTerm() throws IOException {
    Random random = random();
    for (int i = 0; i < 512; i++) {
      final int flags = i;
      final CharArraySet protectedWords;
      if (random.nextBoolean()) {
        protectedWords = new CharArraySet(new HashSet<>(Arrays.asList("a", "b", "cd")), false);
      } else {
        protectedWords = null;
      }
    
      Analyzer a = new Analyzer() { 
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new KeywordTokenizer();
          return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(tokenizer, flags, protectedWords));
        }
      };
      // depending upon options, this thing may or may not preserve the empty term
      checkAnalysisConsistency(random, a, random.nextBoolean(), "");
      a.close();
    }
  }

  private Analyzer getAnalyzer(int flags) {
    return getAnalyzer(flags, null);
  }
  
  private Analyzer getAnalyzer(int flags, CharArraySet protectedWords) {
    return new Analyzer() { 
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new WordDelimiterGraphFilter(tokenizer, flags, protectedWords));
      }
    };
  }

  private static boolean has(int flags, int flag) {
    return (flags & flag) != 0;
  }

  private static boolean isEnglishPossessive(String text, int pos) {
    if (pos > 2) {
      if ((text.charAt(pos-1) == 's' || text.charAt(pos-1) == 'S') &&
          (pos == text.length() || text.charAt(pos) != '-')) {
        text = text.substring(0, text.length()-2);
      }
    }
    return true;
  }

  private static class WordPart {
    final String part;
    final int startOffset;
    final int endOffset;
    final int type;

    public WordPart(String text, int startOffset, int endOffset) {
      this.part = text.substring(startOffset, endOffset);
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.type = toType(part.charAt(0));
    }

    @Override
    public String toString() {
      return "WordPart(" + part + " " + startOffset + "-" + endOffset + ")";
    }
  }

  private static final int NUMBER = 0;
  private static final int LETTER = 1;
  private static final int DELIM = 2;

  private static int toType(char ch) {
    if (Character.isDigit(ch)) {
      // numbers
      return NUMBER;
    } else if (Character.isLetter(ch)) {
      // letters
      return LETTER;
    } else {
      // delimiter
      return DELIM;
    }
  }

  /** Does (hopefully) the same thing as WordDelimiterGraphFilter, according to the flags, but more slowly, returning all string paths combinations. */
  private Set<String> slowWDF(String text, int flags) {

    // first make word parts:
    List<WordPart> wordParts = new ArrayList<>();
    int lastCH = -1;
    int wordPartStart = 0;
    boolean inToken = false;

    for(int i=0;i<text.length();i++) {
      char ch = text.charAt(i);
      if (toType(ch) == DELIM) {
        // delimiter
        if (inToken) {
          // end current token
          wordParts.add(new WordPart(text, wordPartStart, i));
          inToken = false;
        }

        // strip english possessive at the end of this token?:
        if (has(flags, STEM_ENGLISH_POSSESSIVE) &&
            ch == '\'' && i > 0 &&
            i < text.length()-1 &&
            (text.charAt(i+1) == 's' || text.charAt(i+1) == 'S') &&
            toType(text.charAt(i-1)) == LETTER &&
            (i+2 == text.length() || toType(text.charAt(i+2)) == DELIM)) {
          i += 2;
        }
    
      } else if (inToken == false) {
        // start new token
        inToken = true;
        wordPartStart = i;
      } else {
        boolean newToken = false;
        if (Character.isLetter(lastCH)) {
          if (Character.isLetter(ch)) {
            if (has(flags, SPLIT_ON_CASE_CHANGE) && Character.isLowerCase(lastCH) && Character.isLowerCase(ch) == false) {
              // start new token on lower -> UPPER case change (but not vice versa!)
              newToken = true;
            }
          } else if (has(flags, SPLIT_ON_NUMERICS) && Character.isDigit(ch)) {
            // start new token on letter -> number change
            newToken = true;
          }
        } else {
          assert Character.isDigit(lastCH);
          if (Character.isLetter(ch) && has(flags, SPLIT_ON_NUMERICS) ) {
            // start new token on number -> letter change
            newToken = true;
          }
        }
        if (newToken) {
          wordParts.add(new WordPart(text, wordPartStart, i));
          wordPartStart = i;
        }
      }
      lastCH = ch;
    }

    if (inToken) {
      // add last token
      wordParts.add(new WordPart(text, wordPartStart, text.length()));
    }
    
    Set<String> paths = new HashSet<>();
    if (wordParts.isEmpty() == false) {
      enumerate(flags, 0, text, wordParts, paths, new StringBuilder());
    }

    if (has(flags, PRESERVE_ORIGINAL)) {
      paths.add(text);
    }

    if (has(flags, CATENATE_ALL) && wordParts.isEmpty() == false) {
      StringBuilder b = new StringBuilder();
      for(WordPart wordPart : wordParts) {
        b.append(wordPart.part);
      }
      paths.add(b.toString());
    }
    
    return paths;
  }

  private void add(StringBuilder path, String part) {
    if (path.length() != 0) {
      path.append(' ');
    }
    path.append(part);
  }

  private void add(StringBuilder path, List<WordPart> wordParts, int from, int to) {
    if (path.length() != 0) {
      path.append(' ');
    }
    // no spaces:
    for(int i=from;i<to;i++) {
      path.append(wordParts.get(i).part);
    }
  }

  private void addWithSpaces(StringBuilder path, List<WordPart> wordParts, int from, int to) {
    for(int i=from;i<to;i++) {
      add(path, wordParts.get(i).part);
    }
  }

  /** Finds the end (exclusive) of the series of part with the same type */
  private int endOfRun(List<WordPart> wordParts, int start) {
    int upto = start+1;
    while(upto < wordParts.size() && wordParts.get(upto).type == wordParts.get(start).type) {
      upto++;
    }
    return upto;
  }

  /** Recursively enumerates all paths through the word parts */
  private void enumerate(int flags, int upto, String text, List<WordPart> wordParts, Set<String> paths, StringBuilder path) {
    if (upto == wordParts.size()) {
      if (path.length() > 0) {
        paths.add(path.toString());
      }
    } else {
      int savLength = path.length();
      int end = endOfRun(wordParts, upto);

      if (wordParts.get(upto).type == NUMBER) {
        // always output single word, optionally surrounded by delims:
        if (has(flags, GENERATE_NUMBER_PARTS) || wordParts.size() == 1) {
          addWithSpaces(path, wordParts, upto, end);
          if (has(flags, CATENATE_NUMBERS)) {
            // recurse first with the parts
            enumerate(flags, end, text, wordParts, paths, path);
            path.setLength(savLength);
            // .. and second with the concat
            add(path, wordParts, upto, end);
          }
        } else if (has(flags, CATENATE_NUMBERS)) {
          add(path, wordParts, upto, end);
        }
        enumerate(flags, end, text, wordParts, paths, path);
        path.setLength(savLength);
      } else {
        assert wordParts.get(upto).type == LETTER;
        // always output single word, optionally surrounded by delims:
        if (has(flags, GENERATE_WORD_PARTS) || wordParts.size() == 1) {
          addWithSpaces(path, wordParts, upto, end);
          if (has(flags, CATENATE_WORDS)) {
            // recurse first with the parts
            enumerate(flags, end, text, wordParts, paths, path);
            path.setLength(savLength);
            // .. and second with the concat
            add(path, wordParts, upto, end);
          }
        } else if (has(flags, CATENATE_WORDS)) {
          add(path, wordParts, upto, end);
        }
        enumerate(flags, end, text, wordParts, paths, path);
        path.setLength(savLength);
      }
    }
  }

  public void testBasicGraphSplits() throws Exception {
    assertGraphStrings(getAnalyzer(0),
                       "PowerShotPlus",
                       "PowerShotPlus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS),
                       "PowerShotPlus",
                       "PowerShotPlus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE),
                       "PowerShotPlus",
                       "Power Shot Plus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE | PRESERVE_ORIGINAL),
                       "PowerShotPlus",
                       "PowerShotPlus",
                       "Power Shot Plus");

    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS),
                       "Power-Shot-Plus",
                       "Power Shot Plus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE),
                       "Power-Shot-Plus",
                       "Power Shot Plus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE | PRESERVE_ORIGINAL),
                       "Power-Shot-Plus",
                       "Power-Shot-Plus",
                       "Power Shot Plus");

    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE),
                       "PowerShotPlus",
                       "Power Shot Plus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE),
                       "PowerShot1000Plus",
                       "Power Shot1000Plus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE),
                       "Power-Shot-Plus",
                       "Power Shot Plus");

    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE | CATENATE_WORDS),
                       "PowerShotPlus",
                       "Power Shot Plus",
                       "PowerShotPlus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE | CATENATE_WORDS),
                       "PowerShot1000Plus",
                       "Power Shot1000Plus",
                       "PowerShot1000Plus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | SPLIT_ON_CASE_CHANGE | CATENATE_WORDS | CATENATE_NUMBERS),
                       "Power-Shot-1000-17-Plus",
                       "Power Shot 1000 17 Plus",
                       "Power Shot 100017 Plus",
                       "PowerShot 1000 17 Plus",
                       "PowerShot 100017 Plus");
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | SPLIT_ON_CASE_CHANGE | CATENATE_WORDS | CATENATE_NUMBERS | PRESERVE_ORIGINAL),
                       "Power-Shot-1000-17-Plus",
                       "Power-Shot-1000-17-Plus",
                       "Power Shot 1000 17 Plus",
                       "Power Shot 100017 Plus",
                       "PowerShot 1000 17 Plus",
                       "PowerShot 100017 Plus");
  }

  /*
  public void testToDot() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE | PRESERVE_ORIGINAL | CATENATE_WORDS | CATENATE_NUMBERS | STEM_ENGLISH_POSSESSIVE;
    String text = "PowerSystem2000-5-Shot's";
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token(text, 0, text.length())), DEFAULT_WORD_DELIM_TABLE, flags, null);
    //StringWriter sw = new StringWriter();
    // TokenStreamToDot toDot = new TokenStreamToDot(text, wdf, new PrintWriter(sw));
    PrintWriter pw = new PrintWriter("/tmp/foo2.dot");
    TokenStreamToDot toDot = new TokenStreamToDot(text, wdf, pw);
    toDot.toDot();
    pw.close();
    //System.out.println("DOT:\n" + sw.toString());
  }
  */

  private String randomWDFText() {
    StringBuilder b = new StringBuilder();
    int length = TestUtil.nextInt(random(), 1, 50);
    for(int i=0;i<length;i++) {
      int surpriseMe = random().nextInt(37);
      int lower = -1;
      int upper = -1;
      if (surpriseMe < 10) {
        // lowercase letter
        lower = 'a';
        upper = 'z';
      } else if (surpriseMe < 20) {
        // uppercase letter
        lower = 'A';
        upper = 'Z';
      } else if (surpriseMe < 30) {
        // digit
        lower = '0';
        upper = '9';
      } else if (surpriseMe < 35) {
        // punct
        lower = '-';
        upper = '-';
      } else {
        b.append("'s");
      }

      if (lower != -1) {
        b.append((char) TestUtil.nextInt(random(), lower, upper));
      }
    }

    return b.toString();
  }

  public void testInvalidFlag() throws Exception {
    expectThrows(IllegalArgumentException.class,
                 () -> {
                   new WordDelimiterGraphFilter(new CannedTokenStream(), 1 << 31, null);
                 });
  }

  public void testRandomPaths() throws Exception {
    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      String text = randomWDFText();
      if (VERBOSE) {
        System.out.println("\nTEST: text=" + text + " len=" + text.length());
      }

      int flags = 0;
      if (random().nextBoolean()) {
        flags |= GENERATE_WORD_PARTS;
      }
      if (random().nextBoolean()) {
        flags |= GENERATE_NUMBER_PARTS;
      }
      if (random().nextBoolean()) {
        flags |= CATENATE_WORDS;
      }
      if (random().nextBoolean()) {
        flags |= CATENATE_NUMBERS;
      }
      if (random().nextBoolean()) {
        flags |= CATENATE_ALL;
      }
      if (random().nextBoolean()) {
        flags |= PRESERVE_ORIGINAL;
      }
      if (random().nextBoolean()) {
        flags |= SPLIT_ON_CASE_CHANGE;
      }
      if (random().nextBoolean()) {
        flags |= SPLIT_ON_NUMERICS;
      }
      if (random().nextBoolean()) {
        flags |= STEM_ENGLISH_POSSESSIVE;
      }

      verify(text, flags);
    }
  }

  /** Runs normal and slow WDGF and compares results */
  private void verify(String text, int flags) throws IOException {

    Set<String> expected = slowWDF(text, flags);
    if (VERBOSE) {
      for(String path : expected) {
        System.out.println("  " + path);
      }
    }

    Set<String> actual = getGraphStrings(getAnalyzer(flags), text);
    if (actual.equals(expected) == false) {
      StringBuilder b = new StringBuilder();
      b.append("\n\nFAIL: text=");
      b.append(text);
      b.append(" flags=");
      b.append(WordDelimiterGraphFilter.flagsToString(flags));
      b.append('\n');
      b.append("  expected paths:\n");
      for (String s : expected) {
        b.append("    ");
        b.append(s);
        if (actual.contains(s) == false) {
          b.append(" [missing!]");
        }
        b.append('\n');
      }

      b.append("  actual paths:\n");
      for (String s : actual) {
        b.append("    ");
        b.append(s);
        if (expected.contains(s) == false) {
          b.append(" [unexpected!]");
        }
        b.append('\n');
      }

      fail(b.toString());
    }
  }

  public void testOnlyNumbers() throws Exception {
    // no token should be produced
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS), "7-586");
  }

  public void testNoCatenate() throws Exception {
    // no token should be produced
    assertGraphStrings(getAnalyzer(GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS), "a-b-c-9-d", "a b c 9 d");
  }

  public void testCuriousCase1() throws Exception {
    verify("u-0L-4836-ip4Gw--13--q7--L07E1", CATENATE_WORDS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE);
  }

  public void testCuriousCase2() throws Exception {
    verify("u-l-p", CATENATE_ALL);
  }

  public void testOriginalPosLength() throws Exception {
    verify("Foo-Bar-Baz", CATENATE_WORDS | SPLIT_ON_CASE_CHANGE | PRESERVE_ORIGINAL);
  }

  public void testCuriousCase3() throws Exception {
    verify("cQzk4-GL0izl0mKM-J8--4m-'s", GENERATE_NUMBER_PARTS | CATENATE_NUMBERS | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS);
  }

  public void testEmptyString() throws Exception {
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(new CannedTokenStream(new Token("", 0, 0)), DEFAULT_WORD_DELIM_TABLE, GENERATE_WORD_PARTS | CATENATE_ALL | PRESERVE_ORIGINAL, null);
    wdf.reset();
    assertTrue(wdf.incrementToken());
    assertFalse(wdf.incrementToken());
    wdf.end();
    wdf.close();
  }

  public void testProtectedWords() throws Exception {
    TokenStream tokens = new CannedTokenStream(new Token("foo17-bar", 0, 9),
                                               new Token("foo-bar", 0, 7));

    CharArraySet protectedWords = new CharArraySet(new HashSet<>(Arrays.asList("foo17-BAR")), true);
    WordDelimiterGraphFilter wdf = new WordDelimiterGraphFilter(tokens, DEFAULT_WORD_DELIM_TABLE, GENERATE_WORD_PARTS | PRESERVE_ORIGINAL | CATENATE_ALL, protectedWords);
    assertGraphStrings(wdf,
                       "foo17-bar foo bar",
                       "foo17-bar foo-bar",
                       "foo17-bar foobar");
  }
}
