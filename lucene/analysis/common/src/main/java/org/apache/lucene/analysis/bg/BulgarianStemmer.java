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
package org.apache.lucene.analysis.bg;


import static org.apache.lucene.analysis.util.StemmerUtil.*;

/**
 * Light Stemmer for Bulgarian.
 * <p>
 * Implements the algorithm described in:  
 * <i>
 * Searching Strategies for the Bulgarian Language
 * </i>
 * http://members.unine.ch/jacques.savoy/Papers/BUIR.pdf
 */
public class BulgarianStemmer {
  
  /**
   * Stem an input buffer of Bulgarian text.
   * 
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int stem(final char s[], int len) {
    if (len < 4) // do not stem
      return len;
    
    if (len > 5 && endsWith(s, len, "ища"))
      return len - 3;
    
    len = removeArticle(s, len);
    len = removePlural(s, len);
    
    if (len > 3) {
      if (endsWith(s, len, "я"))
        len--;
      if (endsWith(s, len, "а") ||
          endsWith(s, len, "о") ||
          endsWith(s, len, "е"))
        len--;
    }
    
    // the rule to rewrite ен -> н is duplicated in the paper.
    // in the perl implementation referenced by the paper, this is fixed.
    // (it is fixed here as well)
    if (len > 4 && endsWith(s, len, "ен")) {
      s[len - 2] = 'н'; // replace with н
      len--;
    }
    
    if (len > 5 && s[len - 2] == 'ъ') {
      s[len - 2] = s[len - 1]; // replace ъN with N
      len--;
    }

    return len;
  }
  
  /**
   * Mainly remove the definite article
   * @param s input buffer
   * @param len length of input buffer
   * @return new stemmed length
   */
  private int removeArticle(final char s[], final int len) {
    if (len > 6 && endsWith(s, len, "ият"))
      return len - 3;
    
    if (len > 5) {
      if (endsWith(s, len, "ът") ||
          endsWith(s, len, "то") ||
          endsWith(s, len, "те") ||
          endsWith(s, len, "та") ||
          endsWith(s, len, "ия"))
        return len - 2;
    }
    
    if (len > 4 && endsWith(s, len, "ят"))
      return len - 2;

    return len;
  }
  
  private int removePlural(final char s[], final int len) {
    if (len > 6) {
      if (endsWith(s, len, "овци"))
        return len - 3; // replace with о
      if (endsWith(s, len, "ове"))
        return len - 3;
      if (endsWith(s, len, "еве")) {
        s[len - 3] = 'й'; // replace with й
        return len - 2;
      }
    }
    
    if (len > 5) {
      if (endsWith(s, len, "ища"))
        return len - 3;
      if (endsWith(s, len, "та"))
        return len - 2;
      if (endsWith(s, len, "ци")) {
        s[len - 2] = 'к'; // replace with к
        return len - 1;
      }
      if (endsWith(s, len, "зи")) {
        s[len - 2] = 'г'; // replace with г
        return len - 1;
      }
      
      if (s[len - 3] == 'е' && s[len - 1] == 'и') {
        s[len - 3] = 'я'; // replace е with я, remove и
        return len - 1;
      }
    }
    
    if (len > 4) {
      if (endsWith(s, len, "си")) {
        s[len - 2] = 'х'; // replace with х
        return len - 1;
      }
      if (endsWith(s, len, "и"))
        return len - 1;
    }
    
    return len;
  }
}
