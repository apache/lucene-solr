package org.apache.lucene.index.memory;

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
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

/**
 * Various fulltext analysis utilities avoiding redundant code in several
 * classes.
 *
 */
public class AnalyzerUtil {
  
  private AnalyzerUtil() {};

  /**
   * Returns a simple analyzer wrapper that logs all tokens produced by the
   * underlying child analyzer to the given log stream (typically System.err);
   * Otherwise behaves exactly like the child analyzer, delivering the very
   * same tokens; useful for debugging purposes on custom indexing and/or
   * querying.
   * 
   * @param child
   *            the underlying child analyzer
   * @param log
   *            the print stream to log to (typically System.err)
   * @param logName
   *            a name for this logger (typically "log" or similar)
   * @return a logging analyzer
   */
  public static Analyzer getLoggingAnalyzer(final Analyzer child, 
      final PrintStream log, final String logName) {
    
    if (child == null) 
      throw new IllegalArgumentException("child analyzer must not be null");
    if (log == null) 
      throw new IllegalArgumentException("logStream must not be null");

    return new Analyzer() {
      public TokenStream tokenStream(final String fieldName, Reader reader) {
        return new TokenFilter(child.tokenStream(fieldName, reader)) {
          private int position = -1;
          
          public Token next(final Token reusableToken) throws IOException {
            assert reusableToken != null;
            Token nextToken = input.next(reusableToken); // from filter super class
            log.println(toString(nextToken));
            return nextToken;
          }
          
          private String toString(Token token) {
            if (token == null) return "[" + logName + ":EOS:" + fieldName + "]\n";
            
            position += token.getPositionIncrement();
            return "[" + logName + ":" + position + ":" + fieldName + ":"
                + token.term() + ":" + token.startOffset()
                + "-" + token.endOffset() + ":" + token.type()
                + "]";
          }         
        };
      }
    };
  }
  
  
  /**
   * Returns an analyzer wrapper that returns at most the first
   * <code>maxTokens</code> tokens from the underlying child analyzer,
   * ignoring all remaining tokens.
   * 
   * @param child
   *            the underlying child analyzer
   * @param maxTokens
   *            the maximum number of tokens to return from the underlying
   *            analyzer (a value of Integer.MAX_VALUE indicates unlimited)
   * @return an analyzer wrapper
   */
  public static Analyzer getMaxTokenAnalyzer(
      final Analyzer child, final int maxTokens) {
    
    if (child == null) 
      throw new IllegalArgumentException("child analyzer must not be null");
    if (maxTokens < 0) 
      throw new IllegalArgumentException("maxTokens must not be negative");
    if (maxTokens == Integer.MAX_VALUE) 
      return child; // no need to wrap
  
    return new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new TokenFilter(child.tokenStream(fieldName, reader)) {
          private int todo = maxTokens;
          
          public Token next(final Token reusableToken) throws IOException {
            assert reusableToken != null;
            return --todo >= 0 ? input.next(reusableToken) : null;
          }
        };
      }
    };
  }
  
  
  /**
   * Returns an English stemming analyzer that stems tokens from the
   * underlying child analyzer according to the Porter stemming algorithm. The
   * child analyzer must deliver tokens in lower case for the stemmer to work
   * properly.
   * <p>
   * Background: Stemming reduces token terms to their linguistic root form
   * e.g. reduces "fishing" and "fishes" to "fish", "family" and "families" to
   * "famili", as well as "complete" and "completion" to "complet". Note that
   * the root form is not necessarily a meaningful word in itself, and that
   * this is not a bug but rather a feature, if you lean back and think about
   * fuzzy word matching for a bit.
   * <p>
   * See the Lucene contrib packages for stemmers (and stop words) for German,
   * Russian and many more languages.
   * 
   * @param child
   *            the underlying child analyzer
   * @return an analyzer wrapper
   */
  public static Analyzer getPorterStemmerAnalyzer(final Analyzer child) {
    
    if (child == null) 
      throw new IllegalArgumentException("child analyzer must not be null");
  
    return new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new PorterStemFilter(
            child.tokenStream(fieldName, reader));
//        /* PorterStemFilter and SnowballFilter have the same behaviour, 
//        but PorterStemFilter is much faster. */
//        return new org.apache.lucene.analysis.snowball.SnowballFilter(
//            child.tokenStream(fieldName, reader), "English");
      }
    };
  }
  
  
  /**
   * Returns an analyzer wrapper that wraps the underlying child analyzer's
   * token stream into a {@link SynonymTokenFilter}.
   * 
   * @param child
   *            the underlying child analyzer
   * @param synonyms
   *            the map used to extract synonyms for terms
   * @param maxSynonyms
   *            the maximum number of synonym tokens to return per underlying
   *            token word (a value of Integer.MAX_VALUE indicates unlimited)
   * @return a new analyzer
   */
  public static Analyzer getSynonymAnalyzer(final Analyzer child, 
      final SynonymMap synonyms, final int maxSynonyms) {
    
    if (child == null) 
      throw new IllegalArgumentException("child analyzer must not be null");
    if (synonyms == null)
      throw new IllegalArgumentException("synonyms must not be null");
    if (maxSynonyms < 0) 
      throw new IllegalArgumentException("maxSynonyms must not be negative");
    if (maxSynonyms == 0)
      return child; // no need to wrap
  
    return new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new SynonymTokenFilter(
          child.tokenStream(fieldName, reader), synonyms, maxSynonyms);
      }
    };
  }

  
  /**
   * Returns an analyzer wrapper that caches all tokens generated by the underlying child analyzer's
   * token streams, and delivers those cached tokens on subsequent calls to 
   * <code>tokenStream(String fieldName, Reader reader)</code> 
   * if the fieldName has been seen before, altogether ignoring the Reader parameter on cache lookup.
   * <p>
   * If Analyzer / TokenFilter chains are expensive in terms of I/O or CPU, such caching can 
   * help improve performance if the same document is added to multiple Lucene indexes, 
   * because the text analysis phase need not be performed more than once.
   * <p>
   * Caveats: 
   * <ul>
   * <li>Caching the tokens of large Lucene documents can lead to out of memory exceptions.</li> 
   * <li>The Token instances delivered by the underlying child analyzer must be immutable.</li>
   * <li>The same caching analyzer instance must not be used for more than one document
   * because the cache is not keyed on the Reader parameter.</li>
   * </ul>
   * 
   * @param child
   *            the underlying child analyzer
   * @return a new analyzer
   */
  public static Analyzer getTokenCachingAnalyzer(final Analyzer child) {

    if (child == null)
      throw new IllegalArgumentException("child analyzer must not be null");

    return new Analyzer() {

      private final HashMap cache = new HashMap();

      public TokenStream tokenStream(String fieldName, Reader reader) {
        final ArrayList tokens = (ArrayList) cache.get(fieldName);
        if (tokens == null) { // not yet cached
          final ArrayList tokens2 = new ArrayList();
          TokenStream tokenStream = new TokenFilter(child.tokenStream(fieldName, reader)) {

            public Token next(final Token reusableToken) throws IOException {
              assert reusableToken != null;
              Token nextToken = input.next(reusableToken); // from filter super class
              if (nextToken != null) tokens2.add(nextToken.clone());
              return nextToken;
            }
          };
          
          cache.put(fieldName, tokens2);
          return tokenStream;
        } else { // already cached
          return new TokenStream() {

            private Iterator iter = tokens.iterator();

            public Token next(Token token) {
              assert token != null;
              if (!iter.hasNext()) return null;
              return (Token) iter.next();
            }
          };
        }
      }
    };
  }
      
  
  /**
   * Returns (frequency:term) pairs for the top N distinct terms (aka words),
   * sorted descending by frequency (and ascending by term, if tied).
   * <p>
   * Example XQuery:
   * <pre>
   * declare namespace util = "java:org.apache.lucene.index.memory.AnalyzerUtil";
   * declare namespace analyzer = "java:org.apache.lucene.index.memory.PatternAnalyzer";
   * 
   * for $pair in util:get-most-frequent-terms(
   *    analyzer:EXTENDED_ANALYZER(), doc("samples/shakespeare/othello.xml"), 10)
   * return &lt;word word="{substring-after($pair, ':')}" frequency="{substring-before($pair, ':')}"/>
   * </pre>
   * 
   * @param analyzer
   *            the analyzer to use for splitting text into terms (aka words)
   * @param text
   *            the text to analyze
   * @param limit
   *            the maximum number of pairs to return; zero indicates 
   *            "as many as possible".
   * @return an array of (frequency:term) pairs in the form of (freq0:term0,
   *         freq1:term1, ..., freqN:termN). Each pair is a single string
   *         separated by a ':' delimiter.
   */
  public static String[] getMostFrequentTerms(Analyzer analyzer, String text, int limit) {
    if (analyzer == null) 
      throw new IllegalArgumentException("analyzer must not be null");
    if (text == null) 
      throw new IllegalArgumentException("text must not be null");
    if (limit <= 0) limit = Integer.MAX_VALUE;
    
    // compute frequencies of distinct terms
    HashMap map = new HashMap();
    TokenStream stream = analyzer.tokenStream("", new StringReader(text));
    try {
      final Token reusableToken = new Token();
      for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
        MutableInteger freq = (MutableInteger) map.get(nextToken.term());
        if (freq == null) {
          freq = new MutableInteger(1);
          map.put(nextToken.term(), freq);
        } else {
          freq.setValue(freq.intValue() + 1);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        stream.close();
      } catch (IOException e2) {
        throw new RuntimeException(e2);
      }
    }
    
    // sort by frequency, text
    Map.Entry[] entries = new Map.Entry[map.size()];
    map.entrySet().toArray(entries);
    Arrays.sort(entries, new Comparator() {
      public int compare(Object o1, Object o2) {
        Map.Entry e1 = (Map.Entry) o1;
        Map.Entry e2 = (Map.Entry) o2;
        int f1 = ((MutableInteger) e1.getValue()).intValue();
        int f2 = ((MutableInteger) e2.getValue()).intValue();
        if (f2 - f1 != 0) return f2 - f1;
        String s1 = (String) e1.getKey();
        String s2 = (String) e2.getKey();
        return s1.compareTo(s2);
      }
    });
    
    // return top N entries
    int size = Math.min(limit, entries.length);
    String[] pairs = new String[size];
    for (int i=0; i < size; i++) {
      pairs[i] = entries[i].getValue() + ":" + entries[i].getKey();
    }
    return pairs;
  }
  
  private static final class MutableInteger {
    private int value;
    public MutableInteger(int value) { this.value = value; }
    public int intValue() { return value; }
    public void setValue(int value) { this.value = value; }
    public String toString() { return String.valueOf(value); }
  };
  
  
  
  // TODO: could use a more general i18n approach ala http://icu.sourceforge.net/docs/papers/text_boundary_analysis_in_java/
  /** (Line terminator followed by zero or more whitespace) two or more times */
  private static final Pattern PARAGRAPHS = Pattern.compile("([\\r\\n\\u0085\\u2028\\u2029][ \\t\\x0B\\f]*){2,}");
  
  /**
   * Returns at most the first N paragraphs of the given text. Delimiting
   * characters are excluded from the results. Each returned paragraph is
   * whitespace-trimmed via String.trim(), potentially an empty string.
   * 
   * @param text
   *            the text to tokenize into paragraphs
   * @param limit
   *            the maximum number of paragraphs to return; zero indicates "as
   *            many as possible".
   * @return the first N paragraphs
   */
  public static String[] getParagraphs(String text, int limit) {
    return tokenize(PARAGRAPHS, text, limit);
  }
    
  private static String[] tokenize(Pattern pattern, String text, int limit) {
    String[] tokens = pattern.split(text, limit);
    for (int i=tokens.length; --i >= 0; ) tokens[i] = tokens[i].trim();
    return tokens;
  }
  
  
  // TODO: don't split on floating point numbers, e.g. 3.1415 (digit before or after '.')
  /** Divides text into sentences; Includes inverted spanish exclamation and question mark */
  private static final Pattern SENTENCES  = Pattern.compile("[!\\.\\?\\xA1\\xBF]+");

  /**
   * Returns at most the first N sentences of the given text. Delimiting
   * characters are excluded from the results. Each returned sentence is
   * whitespace-trimmed via String.trim(), potentially an empty string.
   * 
   * @param text
   *            the text to tokenize into sentences
   * @param limit
   *            the maximum number of sentences to return; zero indicates "as
   *            many as possible".
   * @return the first N sentences
   */
  public static String[] getSentences(String text, int limit) {
//    return tokenize(SENTENCES, text, limit); // equivalent but slower
    int len = text.length();
    if (len == 0) return new String[] { text };
    if (limit <= 0) limit = Integer.MAX_VALUE;
    
    // average sentence length heuristic
    String[] tokens = new String[Math.min(limit, 1 + len/40)];
    int size = 0;
    int i = 0;
    
    while (i < len && size < limit) {
      
      // scan to end of current sentence
      int start = i;
      while (i < len && !isSentenceSeparator(text.charAt(i))) i++;
      
      if (size == tokens.length) { // grow array
        String[] tmp = new String[tokens.length << 1];
        System.arraycopy(tokens, 0, tmp, 0, size);
        tokens = tmp;
      }
      // add sentence (potentially empty)
      tokens[size++] = text.substring(start, i).trim();

      // scan to beginning of next sentence
      while (i < len && isSentenceSeparator(text.charAt(i))) i++;
    }
    
    if (size == tokens.length) return tokens;
    String[] results = new String[size];
    System.arraycopy(tokens, 0, results, 0, size);
    return results;
  }

  private static boolean isSentenceSeparator(char c) {
    // regex [!\\.\\?\\xA1\\xBF]
    switch (c) {
      case '!': return true;
      case '.': return true;
      case '?': return true;
      case 0xA1: return true; // spanish inverted exclamation mark
      case 0xBF: return true; // spanish inverted question mark
      default: return false;
    }   
  }
  
}
