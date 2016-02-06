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



import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * A {@link org.apache.lucene.analysis.TokenFilter} that decomposes compound words found in many Germanic languages.
 * <p>
 * "Donaudampfschiff" becomes Donau, dampf, schiff so that you can find
 * "Donaudampfschiff" even when you only enter "schiff".
 *  It uses a brute-force algorithm to achieve this.
 */
public class DictionaryCompoundWordTokenFilter extends CompoundWordTokenFilterBase {

  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   *
   * @param input
   *          the {@link org.apache.lucene.analysis.TokenStream} to process
   * @param dictionary
   *          the word dictionary to match against.
   */
  public DictionaryCompoundWordTokenFilter(TokenStream input, CharArraySet dictionary) {
    super(input, dictionary);
    if (dictionary == null) {
      throw new IllegalArgumentException("dictionary cannot be null");
    }
  }

  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   *
   * @param input
   *          the {@link org.apache.lucene.analysis.TokenStream} to process
   * @param dictionary
   *          the word dictionary to match against.
   * @param minWordSize
   *          only words longer than this get processed
   * @param minSubwordSize
   *          only subwords longer than this get to the output stream
   * @param maxSubwordSize
   *          only subwords shorter than this get to the output stream
   * @param onlyLongestMatch
   *          Add only the longest matching subword to the stream
   */
  public DictionaryCompoundWordTokenFilter(TokenStream input, CharArraySet dictionary,
                                           int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
    if (dictionary == null) {
      throw new IllegalArgumentException("dictionary cannot be null");
    }
  }

  @Override
  protected void decompose() {
    final int len = termAtt.length();
    for (int i=0;i<=len-this.minSubwordSize;++i) {
        CompoundToken longestMatchToken=null;
        for (int j=this.minSubwordSize;j<=this.maxSubwordSize;++j) {
            if(i+j>len) {
                break;
            }
            if(dictionary.contains(termAtt.buffer(), i, j)) {
                if (this.onlyLongestMatch) {
                   if (longestMatchToken!=null) {
                     if (longestMatchToken.txt.length()<j) {
                       longestMatchToken=new CompoundToken(i,j);
                     }
                   } else {
                     longestMatchToken=new CompoundToken(i,j);
                   }
                } else {
                   tokens.add(new CompoundToken(i,j));
                }
            } 
        }
        if (this.onlyLongestMatch && longestMatchToken!=null) {
          tokens.add(longestMatchToken);
        }
    }
  }
}
