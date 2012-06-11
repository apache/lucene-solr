package org.apache.lucene.analysis.de;

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
 * Light Stemmer for German.
 * <p>
 * This stemmer implements the "UniNE" algorithm in:
 * <i>Light Stemming Approaches for the French, Portuguese, German and Hungarian Languages</i>
 * Jacques Savoy
 */
public class GermanLightStemmer {
  
  public int stem(char s[], int len) {   
    for (int i = 0; i < len; i++)
      switch(s[i]) {
        case 'ä':
        case 'à':
        case 'á':
        case 'â': s[i] = 'a'; break;
        case 'ö':
        case 'ò':
        case 'ó':
        case 'ô': s[i] = 'o'; break;
        case 'ï':
        case 'ì':
        case 'í':
        case 'î': s[i] = 'i'; break;
        case 'ü': 
        case 'ù': 
        case 'ú':
        case 'û': s[i] = 'u'; break;
      }
    
    len = step1(s, len);
    return step2(s, len);
  }
  
  private boolean stEnding(char ch) {
    switch(ch) {
      case 'b':
      case 'd':
      case 'f':
      case 'g':
      case 'h':
      case 'k':
      case 'l':
      case 'm':
      case 'n':
      case 't': return true;
      default: return false;
    }
  }
  
  private int step1(char s[], int len) {
    if (len > 5 && s[len-3] == 'e' && s[len-2] == 'r' && s[len-1] == 'n')
      return len - 3;
    
    if (len > 4 && s[len-2] == 'e')
      switch(s[len-1]) {
        case 'm':
        case 'n':
        case 'r':
        case 's': return len - 2;
      }
    
    if (len > 3 && s[len-1] == 'e')
      return len - 1;
    
    if (len > 3 && s[len-1] == 's' && stEnding(s[len-2]))
      return len - 1;
    
    return len;
  }
  
  private int step2(char s[], int len) {
    if (len > 5 && s[len-3] == 'e' && s[len-2] == 's' && s[len-1] == 't')
      return len - 3;
    
    if (len > 4 && s[len-2] == 'e' && (s[len-1] == 'r' || s[len-1] == 'n'))
      return len - 2;
    
    if (len > 4 && s[len-2] == 's' && s[len-1] == 't' && stEnding(s[len-3]))
      return len - 2;
    
    return len;
  }
}
