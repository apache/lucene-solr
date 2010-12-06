package org.apache.lucene.analysis.compound;

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

import java.io.File;
import java.util.Set;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.hyphenation.Hyphenation;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.apache.lucene.util.Version;
import org.xml.sax.InputSource;

/**
 * A {@link TokenFilter} that decomposes compound words found in many Germanic languages.
 * <p>
 * "Donaudampfschiff" becomes Donau, dampf, schiff so that you can find
 * "Donaudampfschiff" even when you only enter "schiff". It uses a hyphenation
 * grammar and a word dictionary to achieve this.
 * </p>
 */
public class HyphenationCompoundWordTokenFilter extends
    CompoundWordTokenFilterBase {
  private HyphenationTree hyphenator;
  
  /**
   * Creates a new {@link HyphenationCompoundWordTokenFilter} instance.
   *  
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * @param input
   *          the {@link TokenStream} to process
   * @param hyphenator
   *          the hyphenation pattern tree to use for hyphenation
   * @param dictionary
   *          the word dictionary to match against
   * @param minWordSize
   *          only words longer than this get processed
   * @param minSubwordSize
   *          only subwords longer than this get to the output stream
   * @param maxSubwordSize
   *          only subwords shorter than this get to the output stream
   * @param onlyLongestMatch
   *          Add only the longest matching subword to the stream
   */
  public HyphenationCompoundWordTokenFilter(Version matchVersion, TokenStream input,
      HyphenationTree hyphenator, String[] dictionary, int minWordSize,
      int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(matchVersion, input, dictionary, minWordSize, minSubwordSize, maxSubwordSize,
        onlyLongestMatch);

    this.hyphenator = hyphenator;
  }

  /**
   * Creates a new {@link HyphenationCompoundWordTokenFilter} instance.
   *  
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * @param input
   *          the {@link TokenStream} to process
   * @param hyphenator
   *          the hyphenation pattern tree to use for hyphenation
   * @param dictionary
   *          the word dictionary to match against
   */
  public HyphenationCompoundWordTokenFilter(Version matchVersion, TokenStream input,
      HyphenationTree hyphenator, String[] dictionary) {
    this(matchVersion, input, hyphenator, makeDictionary(dictionary), DEFAULT_MIN_WORD_SIZE,
        DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE, false);
  }

  /**
   * Creates a new {@link HyphenationCompoundWordTokenFilter} instance. 
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * @param input
   *          the {@link TokenStream} to process
   * @param hyphenator
   *          the hyphenation pattern tree to use for hyphenation
   * @param dictionary
   *          the word dictionary to match against. If this is a
   *          {@link org.apache.lucene.analysis.util.CharArraySet CharArraySet} it
   *          must have set ignoreCase=false and only contain lower case
   *          strings.
   */
  public HyphenationCompoundWordTokenFilter(Version matchVersion, TokenStream input,
      HyphenationTree hyphenator, Set<?> dictionary) {
    this(matchVersion, input, hyphenator, dictionary, DEFAULT_MIN_WORD_SIZE,
        DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE, false);
  }

  /**
   * Creates a new {@link HyphenationCompoundWordTokenFilter} instance.
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * @param input
   *          the {@link TokenStream} to process
   * @param hyphenator
   *          the hyphenation pattern tree to use for hyphenation
   * @param dictionary
   *          the word dictionary to match against. If this is a
   *          {@link org.apache.lucene.analysis.util.CharArraySet CharArraySet} it
   *          must have set ignoreCase=false and only contain lower case
   *          strings.
   * @param minWordSize
   *          only words longer than this get processed
   * @param minSubwordSize
   *          only subwords longer than this get to the output stream
   * @param maxSubwordSize
   *          only subwords shorter than this get to the output stream
   * @param onlyLongestMatch
   *          Add only the longest matching subword to the stream
   */
  public HyphenationCompoundWordTokenFilter(Version matchVersion, TokenStream input,
      HyphenationTree hyphenator, Set<?> dictionary, int minWordSize,
      int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(matchVersion, input, dictionary, minWordSize, minSubwordSize, maxSubwordSize,
        onlyLongestMatch);

    this.hyphenator = hyphenator;
  }

  /**
   * Create a HyphenationCompoundWordTokenFilter with no dictionary.
   * <p>
   * Calls {@link #HyphenationCompoundWordTokenFilter(Version, TokenStream, HyphenationTree, Set, int, int, int, boolean)
   * HyphenationCompoundWordTokenFilter(matchVersion, input, hyphenator,
   * null, minWordSize, minSubwordSize, maxSubwordSize }
   */
  public HyphenationCompoundWordTokenFilter(Version matchVersion, TokenStream input,
      HyphenationTree hyphenator, int minWordSize, int minSubwordSize,
      int maxSubwordSize) {
    this(matchVersion, input, hyphenator, (Set<?>) null, minWordSize, minSubwordSize,
        maxSubwordSize, false);
  }
  
  /**
   * Create a HyphenationCompoundWordTokenFilter with no dictionary.
   * <p>
   * Calls {@link #HyphenationCompoundWordTokenFilter(Version, TokenStream, HyphenationTree, int, int, int) 
   * HyphenationCompoundWordTokenFilter(matchVersion, input, hyphenator, 
   * DEFAULT_MIN_WORD_SIZE, DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE }
   */
  public HyphenationCompoundWordTokenFilter(Version matchVersion, TokenStream input,
      HyphenationTree hyphenator) {
    this(matchVersion, input, hyphenator, DEFAULT_MIN_WORD_SIZE, DEFAULT_MIN_SUBWORD_SIZE, 
        DEFAULT_MAX_SUBWORD_SIZE);
  }

  /**
   * Create a hyphenator tree
   * 
   * @param hyphenationFilename the filename of the XML grammar to load
   * @return An object representing the hyphenation patterns
   * @throws Exception
   */
  public static HyphenationTree getHyphenationTree(String hyphenationFilename)
      throws Exception {
    return getHyphenationTree(new InputSource(hyphenationFilename));
  }

  /**
   * Create a hyphenator tree
   * 
   * @param hyphenationFile the file of the XML grammar to load
   * @return An object representing the hyphenation patterns
   * @throws Exception
   */
  public static HyphenationTree getHyphenationTree(File hyphenationFile)
      throws Exception {
    return getHyphenationTree(new InputSource(hyphenationFile.toURL().toExternalForm()));
  }

  /**
   * Create a hyphenator tree
   * 
   * @param hyphenationSource the InputSource pointing to the XML grammar
   * @return An object representing the hyphenation patterns
   * @throws Exception
   */
  public static HyphenationTree getHyphenationTree(InputSource hyphenationSource)
      throws Exception {
    HyphenationTree tree = new HyphenationTree();
    tree.loadPatterns(hyphenationSource);
    return tree;
  }

  @Override
  protected void decomposeInternal(final Token token) {
    // get the hyphenation points
    Hyphenation hyphens = hyphenator.hyphenate(token.buffer(), 0, token
        .length(), 1, 1);
    // No hyphen points found -> exit
    if (hyphens == null) {
      return;
    }

    final int[] hyp = hyphens.getHyphenationPoints();
    char[] lowerCaseTermBuffer=makeLowerCaseCopy(token.buffer());

    for (int i = 0; i < hyp.length; ++i) {
      int remaining = hyp.length - i;
      int start = hyp[i];
      Token longestMatchToken = null;
      for (int j = 1; j < remaining; j++) {
        int partLength = hyp[i + j] - start;

        // if the part is longer than maxSubwordSize we
        // are done with this round
        if (partLength > this.maxSubwordSize) {
          break;
        }

        // we only put subwords to the token stream
        // that are longer than minPartSize
        if (partLength < this.minSubwordSize) {
          continue;
        }

        // check the dictionary
        if (dictionary == null || dictionary.contains(lowerCaseTermBuffer, start, partLength)) {
          if (this.onlyLongestMatch) {
            if (longestMatchToken != null) {
              if (longestMatchToken.length() < partLength) {
                longestMatchToken = createToken(start, partLength, token);
              }
            } else {
              longestMatchToken = createToken(start, partLength, token);
            }
          } else {
            tokens.add(createToken(start, partLength, token));
          }
        } else if (dictionary.contains(lowerCaseTermBuffer, start,
            partLength - 1)) {
          // check the dictionary again with a word that is one character
          // shorter
          // to avoid problems with genitive 's characters and other binding
          // characters
          if (this.onlyLongestMatch) {
            if (longestMatchToken != null) {
              if (longestMatchToken.length() < partLength - 1) {
                longestMatchToken = createToken(start, partLength - 1, token);
              }
            } else {
              longestMatchToken = createToken(start, partLength - 1, token);
            }
          } else {
            tokens.add(createToken(start, partLength - 1, token));
          }
        }
      }
      if (this.onlyLongestMatch && longestMatchToken!=null) {
        tokens.add(longestMatchToken);
      }
    }
  }
}
