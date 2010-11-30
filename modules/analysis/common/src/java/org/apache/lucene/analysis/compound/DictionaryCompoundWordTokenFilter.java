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


import java.util.Set;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

/**
 * A {@link TokenFilter} that decomposes compound words found in many Germanic languages.
 * <p>
 * "Donaudampfschiff" becomes Donau, dampf, schiff so that you can find
 * "Donaudampfschiff" even when you only enter "schiff". 
 *  It uses a brute-force algorithm to achieve this.
 * </p>
 */
public class DictionaryCompoundWordTokenFilter extends CompoundWordTokenFilterBase {
  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * @param input
   *          the {@link TokenStream} to process
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
  public DictionaryCompoundWordTokenFilter(Version matchVersion, TokenStream input, String[] dictionary,
      int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(matchVersion, input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
  }

  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * 
   * @param input
   *          the {@link TokenStream} to process
   * @param dictionary
   *          the word dictionary to match against
   */
  public DictionaryCompoundWordTokenFilter(Version matchVersion, TokenStream input, String[] dictionary) {
    super(matchVersion, input, dictionary);
  }
  
  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * @param input
   *          the {@link TokenStream} to process
   * @param dictionary
   *          the word dictionary to match against. If this is a
   *          {@link org.apache.lucene.analysis.util.CharArraySet CharArraySet} it
   *          must have set ignoreCase=false and only contain lower case
   *          strings.
   */
  public DictionaryCompoundWordTokenFilter(Version matchVersion, TokenStream input, Set dictionary) {
    super(matchVersion, input, dictionary);
  }
  
  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the
   *          dictionaries if Version > 3.0. See <a
   *          href="CompoundWordTokenFilterBase#version"
   *          >CompoundWordTokenFilterBase</a> for details.
   * @param input
   *          the {@link TokenStream} to process
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
  public DictionaryCompoundWordTokenFilter(Version matchVersion, TokenStream input, Set dictionary,
      int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(matchVersion, input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
  }

  @Override
  protected void decomposeInternal(final Token token) {
    // Only words longer than minWordSize get processed
    if (token.length() < this.minWordSize) {
      return;
    }
    
    char[] lowerCaseTermBuffer=makeLowerCaseCopy(token.buffer());
    
    for (int i=0;i<token.length()-this.minSubwordSize;++i) {
        Token longestMatchToken=null;
        for (int j=this.minSubwordSize-1;j<this.maxSubwordSize;++j) {
            if(i+j>token.length()) {
                break;
            }
            if(dictionary.contains(lowerCaseTermBuffer, i, j)) {
                if (this.onlyLongestMatch) {
                   if (longestMatchToken!=null) {
                     if (longestMatchToken.length()<j) {
                       longestMatchToken=createToken(i,j,token);
                     }
                   } else {
                     longestMatchToken=createToken(i,j,token);
                   }
                } else {
                   tokens.add(createToken(i,j,token));
                }
            } 
        }
        if (this.onlyLongestMatch && longestMatchToken!=null) {
          tokens.add(longestMatchToken);
        }
    }
  }
}
