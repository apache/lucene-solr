package org.apache.lucene.analysis.ru;

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
 * Light Stemmer for Russian.
 * <p>
 * This stemmer implements the following algorithm:
 * <i>Indexing and Searching Strategies for the Russian Language.</i>
 * Ljiljana Dolamic and Jacques Savoy.
 */
public class RussianLightStemmer {

  public int stem(char s[], int len) {
    len = removeCase(s, len);
    return normalize(s, len);
  }
  
  private int normalize(char s[], int len) {
    if (len > 3)
      switch(s[len-1]) { 
        case 'ь':
        case 'и': return len - 1;
        case 'н': if (s[len-2] == 'н') return len - 1;
      }
    return len;
  }

  private int removeCase(char s[], int len) {
    if (len > 6 && 
        (endsWith(s, len, "иями") ||
         endsWith(s, len, "оями")))
      return len - 4;
    
    if (len > 5 && 
        (endsWith(s, len, "иям") ||
         endsWith(s, len, "иях") ||
         endsWith(s, len, "оях") ||
         endsWith(s, len, "ями") ||
         endsWith(s, len, "оям") ||
         endsWith(s, len, "оьв") ||
         endsWith(s, len, "ами") ||
         endsWith(s, len, "его") ||
         endsWith(s, len, "ему") ||
         endsWith(s, len, "ери") ||
         endsWith(s, len, "ими") ||
         endsWith(s, len, "ого") ||
         endsWith(s, len, "ому") ||
         endsWith(s, len, "ыми") ||
         endsWith(s, len, "оев")))
      return len - 3;
    
    if (len > 4 &&
        (endsWith(s, len, "ая") ||
         endsWith(s, len, "яя") ||
         endsWith(s, len, "ях") ||
         endsWith(s, len, "юю") ||
         endsWith(s, len, "ах") ||
         endsWith(s, len, "ею") ||
         endsWith(s, len, "их") ||
         endsWith(s, len, "ия") ||
         endsWith(s, len, "ию") ||
         endsWith(s, len, "ьв") ||
         endsWith(s, len, "ою") ||
         endsWith(s, len, "ую") ||
         endsWith(s, len, "ям") ||
         endsWith(s, len, "ых") ||
         endsWith(s, len, "ея") ||
         endsWith(s, len, "ам") ||
         endsWith(s, len, "ем") ||
         endsWith(s, len, "ей") ||
         endsWith(s, len, "ём") ||
         endsWith(s, len, "ев") ||
         endsWith(s, len, "ий") ||
         endsWith(s, len, "им") ||
         endsWith(s, len, "ое") ||
         endsWith(s, len, "ой") ||
         endsWith(s, len, "ом") ||
         endsWith(s, len, "ов") ||
         endsWith(s, len, "ые") ||
         endsWith(s, len, "ый") ||
         endsWith(s, len, "ым") ||
         endsWith(s, len, "ми")))
      return len - 2;
    
    if (len > 3)
      switch(s[len-1]) {
        case 'а':
        case 'е':
        case 'и':
        case 'о':
        case 'у':
        case 'й':
        case 'ы':
        case 'я':
        case 'ь': return len - 1;
      }
    
    return len;
  }
}
