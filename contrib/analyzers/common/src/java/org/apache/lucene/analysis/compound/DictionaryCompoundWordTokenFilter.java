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
import org.apache.lucene.analysis.TokenStream;

/**
 * A TokenFilter that decomposes compound words found in many germanic languages
 * "Donaudampfschiff" becomes Donau, dampf, schiff so that you can find
 * "Donaudampfschiff" even when you only enter "schiff". 
 *  It uses a brute-force algorithm to achieve this.
 */
public class DictionaryCompoundWordTokenFilter extends CompoundWordTokenFilterBase {
  /**
   * 
   * @param input the token stream to process
   * @param dictionary the word dictionary to match against
   * @param minWordSize only words longer than this get processed
   * @param minSubwordSize only subwords longer than this get to the output stream
   * @param maxSubwordSize only subwords shorter than this get to the output stream
   * @param onlyLongestMatch Add only the longest matching subword to the stream
   */
  public DictionaryCompoundWordTokenFilter(TokenStream input, String[] dictionary,
      int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
  }

  /**
   * 
   * @param input the token stream to process
   * @param dictionary the word dictionary to match against
   */
  public DictionaryCompoundWordTokenFilter(TokenStream input, String[] dictionary) {
    super(input, dictionary);
  }

  /**
   * 
   * @param input the token stream to process
   * @param dictionary the word dictionary to match against. If this is a {@link org.apache.lucene.analysis.CharArraySet CharArraySet} it must have set ignoreCase=false and only contain
   *        lower case strings. 
   */
  public DictionaryCompoundWordTokenFilter(TokenStream input, Set dictionary) {
    super(input, dictionary);
  }

  /**
   * 
   * @param input the token stream to process
   * @param dictionary the word dictionary to match against. If this is a {@link org.apache.lucene.analysis.CharArraySet CharArraySet} it must have set ignoreCase=false and only contain
   *        lower case strings. 
   * @param minWordSize only words longer than this get processed
   * @param minSubwordSize only subwords longer than this get to the output stream
   * @param maxSubwordSize only subwords shorter than this get to the output stream
   * @param onlyLongestMatch Add only the longest matching subword to the stream
   */
  public DictionaryCompoundWordTokenFilter(TokenStream input, Set dictionary,
      int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
  }

  protected void decomposeInternal(final Token token) {
    // Only words longer than minWordSize get processed
    if (token.termLength() < this.minWordSize) {
      return;
    }
    
    char[] lowerCaseTermBuffer=makeLowerCaseCopy(token.termBuffer());
    
    for (int i=0;i<token.termLength()-this.minSubwordSize;++i) {
        Token longestMatchToken=null;
        for (int j=this.minSubwordSize-1;j<this.maxSubwordSize;++j) {
            if(i+j>token.termLength()) {
                break;
            }
            if(dictionary.contains(lowerCaseTermBuffer, i, j)) {
                if (this.onlyLongestMatch) {
                   if (longestMatchToken!=null) {
                     if (longestMatchToken.termLength()<j) {
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
