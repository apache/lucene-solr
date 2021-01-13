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
package org.apache.lucene.analysis.compound;

import java.io.IOException;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.hyphenation.Hyphenation;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.xml.sax.InputSource;

/**
 * A {@link org.apache.lucene.analysis.TokenFilter} that decomposes compound words found in many
 * Germanic languages.
 *
 * <p>"Donaudampfschiff" becomes Donau, dampf, schiff so that you can find "Donaudampfschiff" even
 * when you only enter "schiff". It uses a hyphenation grammar and a word dictionary to achieve
 * this.
 */
public class HyphenationCompoundWordTokenFilter extends CompoundWordTokenFilterBase {
  private HyphenationTree hyphenator;
  private boolean noSubMatches;
  private boolean noOverlappingMatches;
  private boolean calcSubMatches;

  /**
   * Creates a new {@link HyphenationCompoundWordTokenFilter} instance.
   *
   * @param input the {@link org.apache.lucene.analysis.TokenStream} to process
   * @param hyphenator the hyphenation pattern tree to use for hyphenation
   * @param dictionary the word dictionary to match against.
   */
  public HyphenationCompoundWordTokenFilter(
      TokenStream input, HyphenationTree hyphenator, CharArraySet dictionary) {
    this(
        input,
        hyphenator,
        dictionary,
        DEFAULT_MIN_WORD_SIZE,
        DEFAULT_MIN_SUBWORD_SIZE,
        DEFAULT_MAX_SUBWORD_SIZE,
        false,
        false,
        false);
  }

  /**
   * Creates a new {@link HyphenationCompoundWordTokenFilter} instance.
   *
   * @param input the {@link org.apache.lucene.analysis.TokenStream} to process
   * @param hyphenator the hyphenation pattern tree to use for hyphenation
   * @param dictionary the word dictionary to match against.
   * @param minWordSize only words longer than this get processed
   * @param minSubwordSize only subwords longer than this get to the output stream
   * @param maxSubwordSize only subwords shorter than this get to the output stream
   * @param onlyLongestMatch Add only the longest matching subword to the stream
   */
  public HyphenationCompoundWordTokenFilter(
      TokenStream input,
      HyphenationTree hyphenator,
      CharArraySet dictionary,
      int minWordSize,
      int minSubwordSize,
      int maxSubwordSize,
      boolean onlyLongestMatch) {
    this(
        input,
        hyphenator,
        dictionary,
        minWordSize,
        minSubwordSize,
        maxSubwordSize,
        onlyLongestMatch,
        false,
        false);
  }

  /**
   * Creates a new {@link HyphenationCompoundWordTokenFilter} instance.
   *
   * @param input the {@link org.apache.lucene.analysis.TokenStream} to process
   * @param hyphenator the hyphenation pattern tree to use for hyphenation
   * @param dictionary the word dictionary to match against.
   * @param minWordSize only words longer than this get processed
   * @param minSubwordSize only subwords longer than this get to the output stream
   * @param maxSubwordSize only subwords shorter than this get to the output stream
   * @param onlyLongestMatch Add only the longest matching subword for each hyphenation to the
   *     stream
   * @param noSubMatches Excludes subwords that are enclosed by an other token
   * @param noOverlappingMatches Excludes subwords that overlap with an other subword
   */
  public HyphenationCompoundWordTokenFilter(
      TokenStream input,
      HyphenationTree hyphenator,
      CharArraySet dictionary,
      int minWordSize,
      int minSubwordSize,
      int maxSubwordSize,
      boolean onlyLongestMatch,
      boolean noSubMatches,
      boolean noOverlappingMatches) {
    super(input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);

    this.hyphenator = hyphenator;
    this.noSubMatches = noSubMatches;
    this.noOverlappingMatches = noOverlappingMatches;
    this.calcSubMatches = !onlyLongestMatch && !noSubMatches && !noOverlappingMatches;
  }

  /**
   * Create a HyphenationCompoundWordTokenFilter with no dictionary.
   *
   * <p>Calls {@link #HyphenationCompoundWordTokenFilter(org.apache.lucene.analysis.TokenStream,
   * org.apache.lucene.analysis.compound.hyphenation.HyphenationTree,
   * org.apache.lucene.analysis.CharArraySet, int, int, int, boolean)
   * HyphenationCompoundWordTokenFilter(matchVersion, input, hyphenator, null, minWordSize,
   * minSubwordSize, maxSubwordSize }
   */
  public HyphenationCompoundWordTokenFilter(
      TokenStream input,
      HyphenationTree hyphenator,
      int minWordSize,
      int minSubwordSize,
      int maxSubwordSize) {
    this(input, hyphenator, null, minWordSize, minSubwordSize, maxSubwordSize, false, false, false);
  }

  /**
   * Create a HyphenationCompoundWordTokenFilter with no dictionary.
   *
   * <p>Calls {@link #HyphenationCompoundWordTokenFilter(org.apache.lucene.analysis.TokenStream,
   * org.apache.lucene.analysis.compound.hyphenation.HyphenationTree, int, int, int)
   * HyphenationCompoundWordTokenFilter(matchVersion, input, hyphenator, DEFAULT_MIN_WORD_SIZE,
   * DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE }
   */
  public HyphenationCompoundWordTokenFilter(TokenStream input, HyphenationTree hyphenator) {
    this(
        input,
        hyphenator,
        DEFAULT_MIN_WORD_SIZE,
        DEFAULT_MIN_SUBWORD_SIZE,
        DEFAULT_MAX_SUBWORD_SIZE);
  }

  /**
   * Create a hyphenator tree
   *
   * @param hyphenationFilename the filename of the XML grammar to load
   * @return An object representing the hyphenation patterns
   * @throws java.io.IOException If there is a low-level I/O error.
   */
  public static HyphenationTree getHyphenationTree(String hyphenationFilename) throws IOException {
    return getHyphenationTree(new InputSource(hyphenationFilename));
  }

  /**
   * Create a hyphenator tree
   *
   * @param hyphenationSource the InputSource pointing to the XML grammar
   * @return An object representing the hyphenation patterns
   * @throws java.io.IOException If there is a low-level I/O error.
   */
  public static HyphenationTree getHyphenationTree(InputSource hyphenationSource)
      throws IOException {
    HyphenationTree tree = new HyphenationTree();
    tree.loadPatterns(hyphenationSource);
    return tree;
  }

  @Override
  protected void decompose() {
    // if the token is in the dictionary and we are not interested in subMatches
    // we can skip decomposing this token (see testNoSubAndTokenInDictionary unit test)
    // NOTE:
    // we check against token and the token that is one character
    // shorter to avoid problems with genitive 's characters and other binding characters
    if (dictionary != null
        && !this.calcSubMatches
        && (dictionary.contains(termAtt.buffer(), 0, termAtt.length())
            || termAtt.length() > 1
                && dictionary.contains(termAtt.buffer(), 0, termAtt.length() - 1))) {
      return; // the whole token is in the dictionary - do not decompose
    }

    // get the hyphenation points
    Hyphenation hyphens = hyphenator.hyphenate(termAtt.buffer(), 0, termAtt.length(), 1, 1);
    // No hyphen points found -> exit
    if (hyphens == null) {
      return;
    }
    int maxSubwordSize = Math.min(this.maxSubwordSize, termAtt.length() - 1);

    int consumed = -1; // hyp of the longest token added (for noSub)

    final int[] hyp = hyphens.getHyphenationPoints();

    for (int i = 0; i < hyp.length; ++i) {
      if (noOverlappingMatches) { // if we do not want overlapping subwords
        i = Math.max(i, consumed); // skip over consumed hyp
      }
      int start = hyp[i];
      int until = noSubMatches ? Math.max(consumed, i) : i;
      for (int j = hyp.length - 1; j > until; j--) {
        int partLength = hyp[j] - start;

        // if the part is longer than maxSubwordSize we
        // are done with this round
        if (partLength > maxSubwordSize) {
          continue;
        }

        // we only put subwords to the token stream
        // that are longer than minPartSize
        if (partLength < this.minSubwordSize) {
          // BOGUS/BROKEN/FUNKY/WACKO: somehow we have negative 'parts' according to the
          // calculation above, and we rely upon minSubwordSize being >=0 to filter them out...
          break;
        }

        // check the dictionary
        if (dictionary == null || dictionary.contains(termAtt.buffer(), start, partLength)) {
          tokens.add(new CompoundToken(start, partLength));
          consumed = j; // mark the current hyp as consumed
          if (!calcSubMatches) {
            break; // do not search for shorter matches
          }
        } else if (dictionary.contains(termAtt.buffer(), start, partLength - 1)) {
          // check the dictionary again with a word that is one character
          // shorter to avoid problems with genitive 's characters and
          // other binding characters
          tokens.add(new CompoundToken(start, partLength - 1));
          consumed = j; // mark the current hyp as consumed
          if (!calcSubMatches) {
            break; // do not search for shorter matches
          }
        } // else dictionary is present but does not contain the part
      }
    }
  }
}
