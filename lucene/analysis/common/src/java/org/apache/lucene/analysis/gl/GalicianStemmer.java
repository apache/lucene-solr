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
package org.apache.lucene.analysis.gl;


import java.util.Map;

import org.apache.lucene.analysis.pt.RSLPStemmerBase;

/**
 * Galician stemmer implementing "Regras do lematizador para o galego".
 * 
 * @see RSLPStemmerBase
 * @see <a href="http://bvg.udc.es/recursos_lingua/stemming.jsp">Description of rules</a>
 */
public class GalicianStemmer extends RSLPStemmerBase {
  private static final Step plural, unification, adverb, augmentative, noun, verb, vowel;
  
  static {
    Map<String,Step> steps = parse(GalicianStemmer.class, "galician.rslp");
    plural = steps.get("Plural");
    unification = steps.get("Unification");
    adverb = steps.get("Adverb");
    augmentative = steps.get("Augmentative");
    noun = steps.get("Noun");
    verb = steps.get("Verb");
    vowel = steps.get("Vowel");
  }
  
  /**
   * @param s buffer, oversized to at least <code>len+1</code>
   * @param len initial valid length of buffer
   * @return new valid length, stemmed
   */
  public int stem(char s[], int len) {
    assert s.length >= len + 1 : "this stemmer requires an oversized array of at least 1";
    
    len = plural.apply(s, len);
    len = unification.apply(s, len);
    len = adverb.apply(s, len);
    
    int oldlen;
    do {
      oldlen = len;
      len = augmentative.apply(s, len);
    } while (len != oldlen);
    
    oldlen = len;
    len = noun.apply(s, len);
    if (len == oldlen) { /* suffix not removed */
      len = verb.apply(s, len);
    }
      
    len = vowel.apply(s, len);
    
    // RSLG accent removal
    for (int i = 0; i < len; i++)
      switch(s[i]) {
        case 'á': s[i] = 'a'; break;
        case 'é':
        case 'ê': s[i] = 'e'; break;
        case 'í': s[i] = 'i'; break;
        case 'ó': s[i] = 'o'; break;
        case 'ú': s[i] = 'u'; break;
      }
    
    return len;
  }
}
