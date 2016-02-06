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
package org.apache.lucene.analysis.no;


/* 
 * This algorithm is updated based on code located at:
 * http://members.unine.ch/jacques.savoy/clef/
 * 
 * Full copyright for that code follows:
 */

/*
 * Copyright (c) 2005, Jacques Savoy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer. Redistributions in binary 
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials 
 * provided with the distribution. Neither the name of the author nor the names 
 * of its contributors may be used to endorse or promote products derived from 
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

import static org.apache.lucene.analysis.util.StemmerUtil.*;

/**
 * Light Stemmer for Norwegian.
 * <p>
 * Parts of this stemmer is adapted from SwedishLightStemFilter, except
 * that while the Swedish one has a pre-defined rule set and a corresponding
 * corpus to validate against whereas the Norwegian one is hand crafted.
 */
public class NorwegianLightStemmer {
  /** Constant to remove Bokmål-specific endings */
  public static final int BOKMAAL = 1;
  /** Constant to remove Nynorsk-specific endings */
  public static final int NYNORSK = 2;
  
  final boolean useBokmaal;
  final boolean useNynorsk;
  
  /** 
   * Creates a new NorwegianLightStemmer
   * @param flags set to {@link #BOKMAAL}, {@link #NYNORSK}, or both.
   */
  public NorwegianLightStemmer(int flags) {
    if (flags <= 0 || flags > BOKMAAL + NYNORSK) {
      throw new IllegalArgumentException("invalid flags");
    }
    useBokmaal = (flags & BOKMAAL) != 0;
    useNynorsk = (flags & NYNORSK) != 0;
  }
      
  public int stem(char s[], int len) {   
    // Remove posessive -s (bilens -> bilen) and continue checking 
    if (len > 4 && s[len-1] == 's')
      len--;

    // Remove common endings, single-pass
    if (len > 7 &&
        ((endsWith(s, len, "heter") &&
          useBokmaal) ||  // general ending (hemmelig-heter -> hemmelig)
         (endsWith(s, len, "heten") &&
          useBokmaal) ||  // general ending (hemmelig-heten -> hemmelig)
         (endsWith(s, len, "heita") &&
          useNynorsk)))   // general ending (hemmeleg-heita -> hemmeleg)
      return len - 5;
    
    // Remove Nynorsk common endings, single-pass
    if (len > 8 && useNynorsk &&
        (endsWith(s, len, "heiter") ||  // general ending (hemmeleg-heiter -> hemmeleg)
         endsWith(s, len, "leiken") ||  // general ending (trygg-leiken -> trygg)
         endsWith(s, len, "leikar")))   // general ending (trygg-leikar -> trygg)
      return len - 6;

    if (len > 5 &&
        (endsWith(s, len, "dom") ||  // general ending (kristen-dom -> kristen)
         (endsWith(s, len, "het") &&
          useBokmaal)))              // general ending (hemmelig-het -> hemmelig)
      return len - 3;
    
    if (len > 6 && useNynorsk &&
        (endsWith(s, len, "heit") ||  // general ending (hemmeleg-heit -> hemmeleg)
         endsWith(s, len, "semd") ||  // general ending (verk-semd -> verk)
         endsWith(s, len, "leik")))   // general ending (trygg-leik -> trygg)
      return len - 4;
    
    if (len > 7 && 
        (endsWith(s, len, "elser") ||   // general ending (føl-elser -> føl)
         endsWith(s, len, "elsen")))    // general ending (føl-elsen -> føl)
      return len - 5;
    
    if (len > 6 &&
        ((endsWith(s, len, "ende") &&
          useBokmaal) ||      // (sov-ende -> sov)
         (endsWith(s, len, "ande") &&
          useNynorsk) ||      // (sov-ande -> sov)
         endsWith(s, len, "else") ||  // general ending (føl-else -> føl)
         (endsWith(s, len, "este") &&
          useBokmaal) ||      // adj (fin-este -> fin)
         (endsWith(s, len, "aste") &&
          useNynorsk) ||      // adj (fin-aste -> fin)
         (endsWith(s, len, "eren") &&
          useBokmaal) ||      // masc
         (endsWith(s, len, "aren") &&
          useNynorsk)))       // masc 
      return len - 4;
    
    if (len > 5 &&
        ((endsWith(s, len, "ere") &&
         useBokmaal) ||     // adj (fin-ere -> fin)
         (endsWith(s, len, "are") &&
          useNynorsk) ||    // adj (fin-are -> fin)
         (endsWith(s, len, "est") &&
          useBokmaal) ||    // adj (fin-est -> fin)
         (endsWith(s, len, "ast") &&
          useNynorsk) ||    // adj (fin-ast -> fin)
         endsWith(s, len, "ene") || // masc/fem/neutr pl definite (hus-ene)
         (endsWith(s, len, "ane") &&
          useNynorsk)))     // masc pl definite (gut-ane)
      return len - 3;
    
    if (len > 4 &&
        (endsWith(s, len, "er") ||  // masc/fem indefinite
         endsWith(s, len, "en") ||  // masc/fem definite
         endsWith(s, len, "et") ||  // neutr definite
         (endsWith(s, len, "ar") &&
          useNynorsk) ||    // masc pl indefinite
         (endsWith(s, len, "st") &&
          useBokmaal) ||    // adj (billig-st -> billig)
         endsWith(s, len, "te")))
      return len - 2;

    if (len > 3)
      switch(s[len-1]) {
        case 'a':     // fem definite
        case 'e':     // to get correct stem for nouns ending in -e (kake -> kak, kaker -> kak)
        case 'n': 
          return len - 1;
      }
    
    return len;
  }
}
