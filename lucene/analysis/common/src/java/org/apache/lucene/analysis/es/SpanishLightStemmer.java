package org.apache.lucene.analysis.es;

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

/**
 * Light Stemmer for Spanish
 * <p>
 * This stemmer implements the algorithm described in:
 * <i>Report on CLEF-2001 Experiments</i>
 * Jacques Savoy
 */
public class SpanishLightStemmer {
  
  public int stem(char s[], int len) {
    if (len < 5)
      return len;
    
    for (int i = 0; i < len; i++)
      switch(s[i]) {
        case 'à': 
        case 'á':
        case 'â':
        case 'ä': s[i] = 'a'; break;
        case 'ò':
        case 'ó':
        case 'ô':
        case 'ö': s[i] = 'o'; break;
        case 'è':
        case 'é':
        case 'ê':
        case 'ë': s[i] = 'e'; break;
        case 'ù':
        case 'ú':
        case 'û':
        case 'ü': s[i] = 'u'; break;
        case 'ì':
        case 'í':
        case 'î':
        case 'ï': s[i] = 'i'; break;
      }
    
    switch(s[len-1]) {
      case 'o':
      case 'a':
      case 'e': return len - 1;
      case 's':
        if (s[len-2] == 'e' && s[len-3] == 's' && s[len-4] == 'e')
          return len-2;
        if (s[len-2] == 'e' && s[len-3] == 'c') {
          s[len-3] = 'z';
          return len - 2;
        }
        if (s[len-2] == 'o' || s[len-2] == 'a' || s[len-2] == 'e')
          return len - 2;
    }
    
    return len;
  }
}
