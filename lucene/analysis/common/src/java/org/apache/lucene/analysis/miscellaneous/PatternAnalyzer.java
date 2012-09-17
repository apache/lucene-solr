package org.apache.lucene.analysis.miscellaneous;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Efficient Lucene analyzer/tokenizer that preferably operates on a String rather than a
 * {@link java.io.Reader}, that can flexibly separate text into terms via a regular expression {@link Pattern}
 * (with behaviour identical to {@link String#split(String)}),
 * and that combines the functionality of
 * {@link org.apache.lucene.analysis.core.LetterTokenizer},
 * {@link org.apache.lucene.analysis.core.LowerCaseTokenizer},
 * {@link org.apache.lucene.analysis.core.WhitespaceTokenizer},
 * {@link org.apache.lucene.analysis.core.StopFilter} into a single efficient
 * multi-purpose class.
 * <p>
 * If you are unsure how exactly a regular expression should look like, consider 
 * prototyping by simply trying various expressions on some test texts via
 * {@link String#split(String)}. Once you are satisfied, give that regex to 
 * PatternAnalyzer. Also see <a target="_blank" 
 * href="http://java.sun.com/docs/books/tutorial/extra/regex/">Java Regular Expression Tutorial</a>.
 * <p>
 * This class can be considerably faster than the "normal" Lucene tokenizers. 
 * It can also serve as a building block in a compound Lucene
 * {@link org.apache.lucene.analysis.TokenFilter} chain. For example as in this 
 * stemming example:
 * <pre>
 * PatternAnalyzer pat = ...
 * TokenStream tokenStream = new SnowballFilter(
 *     pat.tokenStream("content", "James is running round in the woods"), 
 *     "English"));
 * </pre>
 * @deprecated (4.0) use the pattern-based analysis in the analysis/pattern package instead.
 */
@Deprecated
public final class PatternAnalyzer extends Analyzer {
  
  /** <code>"\\W+"</code>; Divides text at non-letters (NOT Character.isLetter(c)) */
  public static final Pattern NON_WORD_PATTERN = Pattern.compile("\\W+");
  
  /** <code>"\\s+"</code>; Divides text at whitespaces (Character.isWhitespace(c)) */
  public static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
  
  private static final CharArraySet EXTENDED_ENGLISH_STOP_WORDS =
    CharArraySet.unmodifiableSet(new CharArraySet(Version.LUCENE_CURRENT, 
        Arrays.asList(
      "a", "about", "above", "across", "adj", "after", "afterwards",
      "again", "against", "albeit", "all", "almost", "alone", "along",
      "already", "also", "although", "always", "among", "amongst", "an",
      "and", "another", "any", "anyhow", "anyone", "anything",
      "anywhere", "are", "around", "as", "at", "be", "became", "because",
      "become", "becomes", "becoming", "been", "before", "beforehand",
      "behind", "being", "below", "beside", "besides", "between",
      "beyond", "both", "but", "by", "can", "cannot", "co", "could",
      "down", "during", "each", "eg", "either", "else", "elsewhere",
      "enough", "etc", "even", "ever", "every", "everyone", "everything",
      "everywhere", "except", "few", "first", "for", "former",
      "formerly", "from", "further", "had", "has", "have", "he", "hence",
      "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers",
      "herself", "him", "himself", "his", "how", "however", "i", "ie", "if",
      "in", "inc", "indeed", "into", "is", "it", "its", "itself", "last",
      "latter", "latterly", "least", "less", "ltd", "many", "may", "me",
      "meanwhile", "might", "more", "moreover", "most", "mostly", "much",
      "must", "my", "myself", "namely", "neither", "never",
      "nevertheless", "next", "no", "nobody", "none", "noone", "nor",
      "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
      "once one", "only", "onto", "or", "other", "others", "otherwise",
      "our", "ours", "ourselves", "out", "over", "own", "per", "perhaps",
      "rather", "s", "same", "seem", "seemed", "seeming", "seems",
      "several", "she", "should", "since", "so", "some", "somehow",
      "someone", "something", "sometime", "sometimes", "somewhere",
      "still", "such", "t", "than", "that", "the", "their", "them",
      "themselves", "then", "thence", "there", "thereafter", "thereby",
      "therefor", "therein", "thereupon", "these", "they", "this",
      "those", "though", "through", "throughout", "thru", "thus", "to",
      "together", "too", "toward", "towards", "under", "until", "up",
      "upon", "us", "very", "via", "was", "we", "well", "were", "what",
      "whatever", "whatsoever", "when", "whence", "whenever",
      "whensoever", "where", "whereafter", "whereas", "whereat",
      "whereby", "wherefrom", "wherein", "whereinto", "whereof",
      "whereon", "whereto", "whereunto", "whereupon", "wherever",
      "wherewith", "whether", "which", "whichever", "whichsoever",
      "while", "whilst", "whither", "who", "whoever", "whole", "whom",
      "whomever", "whomsoever", "whose", "whosoever", "why", "will",
      "with", "within", "without", "would", "xsubj", "xcal", "xauthor",
      "xother ", "xnote", "yet", "you", "your", "yours", "yourself",
      "yourselves"
    ), true));
    
  /**
   * A lower-casing word analyzer with English stop words (can be shared
   * freely across threads without harm); global per class loader.
   */
  public static final PatternAnalyzer DEFAULT_ANALYZER = new PatternAnalyzer(
    Version.LUCENE_CURRENT, NON_WORD_PATTERN, true, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    
  /**
   * A lower-casing word analyzer with <b>extended </b> English stop words
   * (can be shared freely across threads without harm); global per class
   * loader. The stop words are borrowed from
   * http://thomas.loc.gov/home/stopwords.html, see
   * http://thomas.loc.gov/home/all.about.inquery.html
   */
  public static final PatternAnalyzer EXTENDED_ANALYZER = new PatternAnalyzer(
    Version.LUCENE_CURRENT, NON_WORD_PATTERN, true, EXTENDED_ENGLISH_STOP_WORDS);
    
  private final Pattern pattern;
  private final boolean toLowerCase;
  private final CharArraySet stopWords;

  private final Version matchVersion;
  
  /**
   * Constructs a new instance with the given parameters.
   * 
   * @param matchVersion currently does nothing
   * @param pattern
   *            a regular expression delimiting tokens
   * @param toLowerCase
   *            if <code>true</code> returns tokens after applying
   *            String.toLowerCase()
   * @param stopWords
   *            if non-null, ignores all tokens that are contained in the
   *            given stop set (after previously having applied toLowerCase()
   *            if applicable). For example, created via
   *            {@link StopFilter#makeStopSet(Version, String[])}and/or
   *            {@link org.apache.lucene.analysis.util.WordlistLoader}as in
   *            <code>WordlistLoader.getWordSet(new File("samples/fulltext/stopwords.txt")</code>
   *            or <a href="http://www.unine.ch/info/clef/">other stop words
   *            lists </a>.
   */
  public PatternAnalyzer(Version matchVersion, Pattern pattern, boolean toLowerCase, CharArraySet stopWords) {
    if (pattern == null) 
      throw new IllegalArgumentException("pattern must not be null");
    
    if (eqPattern(NON_WORD_PATTERN, pattern)) pattern = NON_WORD_PATTERN;
    else if (eqPattern(WHITESPACE_PATTERN, pattern)) pattern = WHITESPACE_PATTERN;
    
    if (stopWords != null && stopWords.size() == 0) stopWords = null;
    
    this.pattern = pattern;
    this.toLowerCase = toLowerCase;
    this.stopWords = stopWords;
    this.matchVersion = matchVersion;
  }
  
  /**
   * Creates a token stream that tokenizes the given string into token terms
   * (aka words).
   * 
   * @param fieldName
   *            the name of the field to tokenize (currently ignored).
   * @param reader
   *            reader (e.g. charfilter) of the original text. can be null.
   * @param text
   *            the string to tokenize
   * @return a new token stream
   */
  public TokenStreamComponents createComponents(String fieldName, Reader reader, String text) {
    // Ideally the Analyzer superclass should have a method with the same signature, 
    // with a default impl that simply delegates to the StringReader flavour. 
    if (reader == null) 
      reader = new FastStringReader(text);
    
    if (pattern == NON_WORD_PATTERN) { // fast path
      return new TokenStreamComponents(new FastStringTokenizer(reader, true, toLowerCase, stopWords));
    } else if (pattern == WHITESPACE_PATTERN) { // fast path
      return new TokenStreamComponents(new FastStringTokenizer(reader, false, toLowerCase, stopWords));
    }

    Tokenizer tokenizer = new PatternTokenizer(reader, pattern, toLowerCase);
    TokenStream result = (stopWords != null) ? new StopFilter(matchVersion, tokenizer, stopWords) : tokenizer;
    return new TokenStreamComponents(tokenizer, result);
  }
  
  /**
   * Creates a token stream that tokenizes all the text in the given Reader;
   * This implementation forwards to <code>tokenStream(String, Reader, String)</code> and is
   * less efficient than <code>tokenStream(String, Reader, String)</code>.
   * 
   * @param fieldName
   *            the name of the field to tokenize (currently ignored).
   * @param reader
   *            the reader delivering the text
   * @return a new token stream
   */
  @Override
  public TokenStreamComponents createComponents(String fieldName, Reader reader) {
    return createComponents(fieldName, reader, null);
  }
  
  /**
   * Indicates whether some other object is "equal to" this one.
   * 
   * @param other
   *            the reference object with which to compare.
   * @return true if equal, false otherwise
   */
  @Override
  public boolean equals(Object other) {
    if (this  == other) return true;
    if (this  == DEFAULT_ANALYZER && other == EXTENDED_ANALYZER) return false;
    if (other == DEFAULT_ANALYZER && this  == EXTENDED_ANALYZER) return false;
    
    if (other instanceof PatternAnalyzer) {
      PatternAnalyzer p2 = (PatternAnalyzer) other;
      return 
        toLowerCase == p2.toLowerCase &&
        eqPattern(pattern, p2.pattern) &&
        eq(stopWords, p2.stopWords);
    }
    return false;
  }
  
  /**
   * Returns a hash code value for the object.
   * 
   * @return the hash code.
   */
  @Override
  public int hashCode() {
    if (this == DEFAULT_ANALYZER) return -1218418418; // fast path
    if (this == EXTENDED_ANALYZER) return 1303507063; // fast path
    
    int h = 1;
    h = 31*h + pattern.pattern().hashCode();
    h = 31*h + pattern.flags();
    h = 31*h + (toLowerCase ? 1231 : 1237);
    h = 31*h + (stopWords != null ? stopWords.hashCode() : 0);
    return h;
  }
  
  /** equality where o1 and/or o2 can be null */
  private static boolean eq(Object o1, Object o2) {
    return (o1 == o2) || (o1 != null ? o1.equals(o2) : false);
  }
  
  /** assumes p1 and p2 are not null */
  private static boolean eqPattern(Pattern p1, Pattern p2) {
    return p1 == p2 || (p1.flags() == p2.flags() && p1.pattern().equals(p2.pattern()));
  }
    
  /**
   * Reads until end-of-stream and returns all read chars, finally closes the stream.
   * 
   * @param input the input stream
   * @throws IOException if an I/O error occurs while reading the stream
   */
  private static String toString(Reader input) throws IOException {
    if (input instanceof FastStringReader) { // fast path
      return ((FastStringReader) input).getString();
    }

    try {
      int len = 256;
      char[] buffer = new char[len];
      char[] output = new char[len];
      
      len = 0;
      int n;
      while ((n = input.read(buffer)) >= 0) {
        if (len + n > output.length) { // grow capacity
          char[] tmp = new char[Math.max(output.length << 1, len + n)];
          System.arraycopy(output, 0, tmp, 0, len);
          System.arraycopy(buffer, 0, tmp, len, n);
          buffer = output; // use larger buffer for future larger bulk reads
          output = tmp;
        } else {
          System.arraycopy(buffer, 0, output, len, n);
        }
        len += n;
      }

      return new String(output, 0, len);
    } finally {
      input.close();
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * The work horse; performance isn't fantastic, but it's not nearly as bad
   * as one might think - kudos to the Sun regex developers.
   */
  private static final class PatternTokenizer extends Tokenizer {

    private final Pattern pattern;
    private String str;
    private final boolean toLowerCase;
    private Matcher matcher;
    private int pos = 0;
    private static final Locale locale = Locale.getDefault();
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
    public PatternTokenizer(Reader input, Pattern pattern, boolean toLowerCase) {
      super(input);
      this.pattern = pattern;
      this.matcher = pattern.matcher("");
      this.toLowerCase = toLowerCase;
    }

    @Override
    public final boolean incrementToken() {
      if (matcher == null) return false;
      clearAttributes();
      while (true) { // loop takes care of leading and trailing boundary cases
        int start = pos;
        int end;
        boolean isMatch = matcher.find();
        if (isMatch) {
          end = matcher.start();
          pos = matcher.end();
        } else { 
          end = str.length();
          matcher = null; // we're finished
        }
        
        if (start != end) { // non-empty match (header/trailer)
          String text = str.substring(start, end);
          if (toLowerCase) text = text.toLowerCase(locale);
          termAtt.setEmpty().append(text);
          offsetAtt.setOffset(correctOffset(start), correctOffset(end));
          return true;
        }
        if (!isMatch) return false;
      }
    }
    
    @Override
    public final void end() {
      // set final offset
      final int finalOffset = correctOffset(str.length());
      this.offsetAtt.setOffset(finalOffset, finalOffset);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.str = PatternAnalyzer.toString(input);
      this.matcher = pattern.matcher(this.str);
      this.pos = 0;
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * Special-case class for best performance in common cases; this class is
   * otherwise unnecessary.
   */
  private static final class FastStringTokenizer extends Tokenizer {
    
    private String str;
    private int pos;
    private final boolean isLetter;
    private final boolean toLowerCase;
    private final CharArraySet stopWords;
    private static final Locale locale = Locale.getDefault();
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
    public FastStringTokenizer(Reader input, boolean isLetter, boolean toLowerCase, CharArraySet stopWords) {
      super(input);
      this.isLetter = isLetter;
      this.toLowerCase = toLowerCase;
      this.stopWords = stopWords;
    }

    @Override
    public boolean incrementToken() {
      clearAttributes();
      // cache loop instance vars (performance)
      String s = str;
      int len = s.length();
      int i = pos;
      boolean letter = isLetter;
      
      int start = 0;
      String text;
      do {
        // find beginning of token
        text = null;
        while (i < len && !isTokenChar(s.charAt(i), letter)) {
          i++;
        }
        
        if (i < len) { // found beginning; now find end of token
          start = i;
          while (i < len && isTokenChar(s.charAt(i), letter)) {
            i++;
          }
          
          text = s.substring(start, i);
          if (toLowerCase) text = text.toLowerCase(locale);
//          if (toLowerCase) {            
////            use next line once JDK 1.5 String.toLowerCase() performance regression is fixed
////            see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6265809
//            text = s.substring(start, i).toLowerCase(); 
////            char[] chars = new char[i-start];
////            for (int j=start; j < i; j++) chars[j-start] = Character.toLowerCase(s.charAt(j));
////            text = new String(chars);
//          } else {
//            text = s.substring(start, i);
//          }
        }
      } while (text != null && isStopWord(text));
      
      pos = i;
      if (text == null)
      {
        return false;
      }
      termAtt.setEmpty().append(text);
      offsetAtt.setOffset(correctOffset(start), correctOffset(i));
      return true;
    }
    
    @Override
    public final void end() {
      // set final offset
      final int finalOffset = str.length();
      this.offsetAtt.setOffset(correctOffset(finalOffset), correctOffset(finalOffset));
    }    
    
    private boolean isTokenChar(char c, boolean isLetter) {
      return isLetter ? Character.isLetter(c) : !Character.isWhitespace(c);
    }
    
    private boolean isStopWord(String text) {
      return stopWords != null && stopWords.contains(text);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.str = PatternAnalyzer.toString(input);
      this.pos = 0;
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * A StringReader that exposes it's contained string for fast direct access.
   * Might make sense to generalize this to CharSequence and make it public?
   */
  static final class FastStringReader extends StringReader {

    private final String s;
    
    FastStringReader(String s) {
      super(s);
      this.s = s;
    }
    
    String getString() {
      return s;
    }
  }
  
}
