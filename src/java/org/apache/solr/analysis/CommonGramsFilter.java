/*
 * Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License 
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License. 
 */

package org.apache.solr.analysis;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/*
 * TODO: Rewrite to use new TokenStream api from lucene 2.9 when BufferedTokenStream uses it.
 * TODO: Consider implementing https://issues.apache.org/jira/browse/LUCENE-1688 changes to stop list and
 * associated constructors 
 */

/**
 * Construct bigrams for frequently occurring terms while indexing. Single terms
 * are still indexed too, with bigrams overlaid. This is achieved through the
 * use of {@link Token#setPositionIncrement(int)}. Bigrams have a type
 * of "gram" Example
 * <ul>
 * <li>input:"the quick brown fox"</li>
 * <li>output:|"the","the-quick"|"brown"|"fox"|</li>
 * <li>"the-quick" has a position increment of 0 so it is in the same position
 * as "the" "the-quick" has a term.type() of "gram"</li>
 * 
 * </ul>
 */

/*
 * Constructors and makeCommonSet based on similar code in StopFilter
 */

public class CommonGramsFilter extends BufferedTokenStream {

  private static final char SEPARATOR = '_';

  private final CharArraySet commonWords;

  private StringBuilder buffer = new StringBuilder();

  /**
   * Construct a token stream filtering the given input using a Set of common
   * words to create bigrams. Outputs both unigrams with position increment and
   * bigrams with position increment 0 type=gram where one or both of the words
   * in a potential bigram are in the set of common words .
   * 
   * @param input TokenStream input in filter chain
   * @param commonWords The set of common words.
   * 
   */
  public CommonGramsFilter(TokenStream input, Set commonWords) {
    this(input, commonWords, false);
  }

  /**
   * Construct a token stream filtering the given input using a Set of common
   * words to create bigrams, case-sensitive if ignoreCase is false (unless Set
   * is CharArraySet). If <code>commonWords</code> is an instance of
   * {@link CharArraySet} (true if <code>makeCommonSet()</code> was used to
   * construct the set) it will be directly used and <code>ignoreCase</code>
   * will be ignored since <code>CharArraySet</code> directly controls case
   * sensitivity.
   * <p/>
   * If <code>commonWords</code> is not an instance of {@link CharArraySet}, a
   * new CharArraySet will be constructed and <code>ignoreCase</code> will be
   * used to specify the case sensitivity of that set.
   * 
   * @param input TokenStream input in filter chain.
   * @param commonWords The set of common words.
   * @param ignoreCase -Ignore case when constructing bigrams for common words.
   */
  public CommonGramsFilter(TokenStream input, Set commonWords,
      boolean ignoreCase) {
    super(input);
    if (commonWords instanceof CharArraySet) {
      this.commonWords = (CharArraySet) commonWords;
    } else {
      this.commonWords = new CharArraySet(commonWords.size(), ignoreCase);
      this.commonWords.addAll(commonWords);
    }
    init();
  }

  /**
   * Construct a token stream filtering the given input using an Array of common
   * words to create bigrams.
   * 
   * @param input Tokenstream in filter chain
   * @param commonWords words to be used in constructing bigrams
   */
  public CommonGramsFilter(TokenStream input, String[] commonWords) {
    this(input, commonWords, false);
    init();
  }

  /**
   * Construct a token stream filtering the given input using an Array of common
   * words to create bigrams and is case-sensitive if ignoreCase is false.
   * 
   * @param input Tokenstream in filter chain
   * @param commonWords words to be used in constructing bigrams
   * @param ignoreCase -Ignore case when constructing bigrams for common words.
   */
  public CommonGramsFilter(TokenStream input, String[] commonWords,
      boolean ignoreCase) {
    super(input);
    this.commonWords = (CharArraySet) makeCommonSet(commonWords, ignoreCase);
    init();
  }

  // Here for future moving to 2.9 api See StopFilter code

  public void init() {
    /**
     * termAtt = (TermAttribute) addAttribute(TermAttribute.class); posIncrAtt
     * =(PositionIncrementAttribute)
     * addAttribute(PositionIncrementAttribute.class); typeAdd =(TypeAttribute)
     * addAttribute(TypeAttribute.class);
     */
  }

  /**
   * Build a CharArraySet from an array of common words, appropriate for passing
   * into the CommonGramsFilter constructor. This permits this commonWords
   * construction to be cached once when an Analyzer is constructed.
   * 
   * @see #makeCommonSet(java.lang.String[], boolean) passing false to
   *      ignoreCase
   */
  public static final CharArraySet makeCommonSet(String[] commonWords) {
    return makeCommonSet(commonWords, false);
  }

  /**
   * Build a CharArraySet from an array of common words, appropriate for passing
   * into the CommonGramsFilter constructor,case-sensitive if ignoreCase is
   * false.
   * 
   * @param commonWords
   * @param ignoreCase If true, all words are lower cased first.
   * @return a Set containing the words
   */
  public static final CharArraySet makeCommonSet(String[] commonWords,
      boolean ignoreCase) {
    CharArraySet commonSet = new CharArraySet(commonWords.length, ignoreCase);
    commonSet.addAll(Arrays.asList(commonWords));
    return commonSet;
  }

  /**
   * Inserts bigrams for common words into a token stream. For each input token,
   * output the token. If the token and/or the following token are in the list
   * of common words also output a bigram with position increment 0 and
   * type="gram"
   */
  /*
   * TODO: implement new lucene 2.9 API incrementToken() instead of deprecated
   * Token.next() TODO:Consider adding an option to not emit unigram stopwords
   * as in CDL XTF BigramStopFilter, CommonGramsQueryFilter would need to be
   * changed to work with this. TODO: Consider optimizing for the case of three
   * commongrams i.e "man of the year" normally produces 3 bigrams: "man-of",
   * "of-the", "the-year" but with proper management of positions we could
   * eliminate the middle bigram "of-the"and save a disk seek and a whole set of
   * position lookups.
   */
  public Token process(Token token) throws IOException {
    Token next = peek(1);
    // if this is the last token just spit it out. Any commongram would have
    // been output in the previous call
    if (next == null) {
      return token;
    }

    /**
     * if this token or next are common then construct a bigram with type="gram"
     * position increment = 0, and put it in the output queue. It will be
     * returned when super.next() is called, before this method gets called with
     * a new token from the input stream See implementation of next() in
     * BufferedTokenStream
     */

    if (isCommon(token) || isCommon(next)) {
      Token gram = gramToken(token, next);
      write(gram);
    }
    // we always return the unigram token
    return token;
  }

  /** True if token is for a common term. */
  private boolean isCommon(Token token) {
    return commonWords != null
        && commonWords.contains(token.termBuffer(), 0, token.termLength());
  }

  /** Construct a compound token. */
  private Token gramToken(Token first, Token second) {
    buffer.setLength(0);
    buffer.append(first.termText());
    buffer.append(SEPARATOR);
    buffer.append(second.termText());
    Token result = new Token(buffer.toString(), first.startOffset(), second
        .endOffset(), "gram");
    result.setPositionIncrement(0);
    return result;
  }
  
  public void reset() throws IOException {
    super.reset();
    buffer.setLength(0);
  }
}
