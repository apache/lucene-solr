package org.apache.lucene.analysis.no;

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
  
  public int stem(char s[], int len) {   
    // Remove posessive -s (bilens -> bilen) and continue checking 
    if (len > 4 && s[len-1] == 's')
      len--;

    // Remove common endings, single-pass
    if (len > 7 && 
        (endsWith(s, len, "heter") ||  // general ending (hemmelig-heter -> hemmelig)
         endsWith(s, len, "heten")))   // general ending (hemmelig-heten -> hemmelig)
      return len - 5;

    if (len > 5 &&
        (endsWith(s, len, "dom") || // general ending (kristen-dom -> kristen)
         endsWith(s, len, "het")))  // general ending (hemmelig-het -> hemmelig)
      return len - 3;
    
    if (len > 7 && 
        (endsWith(s, len, "elser") ||   // general ending (føl-elser -> føl)
         endsWith(s, len, "elsen")))    // general ending (føl-elsen -> føl)
      return len - 5;
    
    if (len > 6 &&
        (endsWith(s, len, "ende") ||  // (sov-ende -> sov)
         endsWith(s, len, "else") ||  // general ending (føl-else -> føl)
         endsWith(s, len, "este") ||  // adj (fin-este -> fin)
         endsWith(s, len, "eren")))   // masc
      return len - 4;
    
    if (len > 5 &&
        (endsWith(s, len, "ere") || // adj (fin-ere -> fin)
         endsWith(s, len, "est") || // adj (fin-est -> fin)
         endsWith(s, len, "ene")    // masc/fem/neutr pl definite (hus-ene)
         )) 
      return len - 3;
    
    if (len > 4 &&
        (endsWith(s, len, "er") ||  // masc/fem indefinite
         endsWith(s, len, "en") ||  // masc/fem definite
         endsWith(s, len, "et") ||  // neutr definite
         endsWith(s, len, "st") ||  // adj (billig-st -> billig)
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
