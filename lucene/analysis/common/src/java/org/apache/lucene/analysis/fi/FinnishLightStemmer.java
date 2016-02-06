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
package org.apache.lucene.analysis.fi;


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
 * Light Stemmer for Finnish.
 * <p>
 * This stemmer implements the algorithm described in:
 * <i>Report on CLEF-2003 Monolingual Tracks</i>
 * Jacques Savoy
 */
public class FinnishLightStemmer {
  
  public int stem(char s[], int len) {
    if (len < 4)
      return len;
    
    for (int i = 0; i < len; i++)
      switch(s[i]) {
        case 'ä':
        case 'å': s[i] = 'a'; break;
        case 'ö': s[i] = 'o'; break;
      }
    
    len = step1(s, len);
    len = step2(s, len);
    len = step3(s, len);
    len = norm1(s, len);
    len = norm2(s, len);
    return len;
  }
  
  private int step1(char s[], int len) {
    if (len > 8) {
      if (endsWith(s, len, "kin"))
        return step1(s, len-3);
      if (endsWith(s, len, "ko"))
        return step1(s, len-2);
    }
    
    if (len > 11) {
      if (endsWith(s, len, "dellinen"))
        return len-8;
      if (endsWith(s, len, "dellisuus"))
        return len-9;
    }
    return len;
  }
  
  private int step2(char s[], int len) {
    if (len > 5) {
      if (endsWith(s, len, "lla")
          || endsWith(s, len, "tse")
          || endsWith(s, len, "sti"))
        return len-3;
      
      if (endsWith(s, len, "ni"))
        return len-2;
      
      if (endsWith(s, len, "aa"))
        return len-1; // aa -> a
    }
    
    return len;
  }
  
  private int step3(char s[], int len) {
    if (len > 8) {
      if (endsWith(s, len, "nnen")) {
        s[len-4] = 's';
        return len-3;
      }
      
      if (endsWith(s, len, "ntena")) {
        s[len-5] = 's';
        return len-4;
      }
      
      if (endsWith(s, len, "tten"))
        return len-4;
      
      if (endsWith(s, len, "eiden"))
        return len-5;
    }
    
    if (len > 6) {
      if (endsWith(s, len, "neen")
          || endsWith(s, len, "niin")
          || endsWith(s, len, "seen")
          || endsWith(s, len, "teen")
          || endsWith(s, len, "inen"))
          return len-4;
      
      if (s[len-3] == 'h' && isVowel(s[len-2]) && s[len-1] == 'n')
        return len-3;
      
      if (endsWith(s, len, "den")) {
        s[len-3] = 's';
        return len-2;
      }
      
      if (endsWith(s, len, "ksen")) {
        s[len-4] = 's';
        return len-3;
      }
      
      if (endsWith(s, len, "ssa")
          || endsWith(s, len, "sta")
          || endsWith(s, len, "lla")
          || endsWith(s, len, "lta")
          || endsWith(s, len, "tta")
          || endsWith(s, len, "ksi")
          || endsWith(s, len, "lle"))
        return len-3; 
    }
    
    if (len > 5) {
      if (endsWith(s, len, "na")
          || endsWith(s, len, "ne"))
        return len-2;
      
      if (endsWith(s, len, "nei"))
        return len-3;
    }
    
    if (len > 4) {
      if (endsWith(s, len, "ja")
          || endsWith(s, len, "ta"))
        return len-2;
      
      if (s[len-1] == 'a')
        return len-1;
      
      if (s[len-1] == 'n' && isVowel(s[len-2]))
        return len-2;
      
      if (s[len-1] == 'n')
        return len-1;
    }
    
    return len;
  }
  
  private int norm1(char s[], int len) {
    if (len > 5 && endsWith(s, len, "hde")) {
        s[len-3] = 'k';
        s[len-2] = 's';
        s[len-1] = 'i';
    }
    
    if (len > 4) {
      if (endsWith(s, len, "ei") || endsWith(s, len, "at"))
        return len-2;
    }
    
    if (len > 3)
      switch(s[len-1]) {
        case 't':
        case 's':
        case 'j':
        case 'e':
        case 'a':
        case 'i': return len-1;
      }
    
    return len;
  }
  
  private int norm2(char s[], int len) {
    if (len > 8) {
      if (s[len-1] == 'e' 
          || s[len-1] == 'o' 
          || s[len-1] == 'u')
        len--;
    }
    
    if (len > 4) {
      if (s[len-1] == 'i')
        len--;
      
      if (len > 4) {
        char ch = s[0];
        for (int i = 1; i < len; i++) {
          if (s[i] == ch &&
              (ch == 'k' || ch == 'p' || ch == 't'))
            len = delete(s, i--, len);
          else
            ch = s[i];
        }
      }
    }
    
    return len;
  }
  
  private boolean isVowel(char ch) {
    switch(ch) {
      case 'a':
      case 'e':
      case 'i':
      case 'o':
      case 'u':
      case 'y': return true;
      default: return false;
    }
  }
}
