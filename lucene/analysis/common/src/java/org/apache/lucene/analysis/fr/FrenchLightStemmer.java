package org.apache.lucene.analysis.fr;

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
 * Light Stemmer for French.
 * <p>
 * This stemmer implements the "UniNE" algorithm in:
 * <i>Light Stemming Approaches for the French, Portuguese, German and Hungarian Languages</i>
 * Jacques Savoy
 */
public class FrenchLightStemmer {
 
  public int stem(char s[], int len) {
    if (len > 5 && s[len-1] == 'x') {
      if (s[len-3] == 'a' && s[len-2] == 'u' && s[len-4] != 'e')
        s[len-2] = 'l';
      len--;
    }
    
    if (len > 3 && s[len-1] == 'x')
      len--;
    
    if (len > 3 && s[len-1] == 's')
      len--;
    
    if (len > 9 && endsWith(s, len, "issement")) {
      len -= 6;
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 8 && endsWith(s, len, "issant")) {
      len -= 4;
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 6 && endsWith(s, len, "ement")) {
      len -= 4;
      if (len > 3 && endsWith(s, len, "ive")) {
        len--;
        s[len-1] = 'f';
      }
      return norm(s, len);
    }
    
    if (len > 11 && endsWith(s, len, "ficatrice")) {
      len -= 5;
      s[len-2] = 'e';
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 10 && endsWith(s, len, "ficateur")) {
      len -= 4;
      s[len-2] = 'e';
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 9 && endsWith(s, len, "catrice")) {
      len -= 3;
      s[len-4] = 'q';
      s[len-3] = 'u';
      s[len-2] = 'e';
      //s[len-1] = 'r' <-- unnecessary, already 'r'.
      return norm(s, len);
    }
    
    if (len > 8 && endsWith(s, len, "cateur")) {
      len -= 2;
      s[len-4] = 'q';
      s[len-3] = 'u';
      s[len-2] = 'e';
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 8 && endsWith(s, len, "atrice")) {
      len -= 4;
      s[len-2] = 'e';
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 7 && endsWith(s, len, "ateur")) {
      len -= 3;
      s[len-2] = 'e';
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 6 && endsWith(s, len, "trice")) {
      len--;
      s[len-3] = 'e';
      s[len-2] = 'u';
      s[len-1] = 'r';
    }
    
    if (len > 5 && endsWith(s, len, "ième"))
      return norm(s, len-4);
    
    if (len > 7 && endsWith(s, len, "teuse")) {
      len -= 2;
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 6 && endsWith(s, len, "teur")) {
      len--;
      s[len-1] = 'r';
      return norm(s, len);
    }
    
    if (len > 5 && endsWith(s, len, "euse"))
      return norm(s, len-2);
    
    if (len > 8 && endsWith(s, len, "ère")) {
      len--;
      s[len-2] = 'e';
      return norm(s, len);
    }
    
    if (len > 7 && endsWith(s, len, "ive")) {
      len--;
      s[len-1] = 'f';
      return norm(s, len);
    }
    
    if (len > 4 && 
        (endsWith(s, len, "folle") ||
         endsWith(s, len, "molle"))) {
      len -= 2;
      s[len-1] = 'u';
      return norm(s, len);
    }
    
    if (len > 9 && endsWith(s, len, "nnelle"))
      return norm(s, len-5);
    
    if (len > 9 && endsWith(s, len, "nnel"))
      return norm(s, len-3);
    
    if (len > 4 && endsWith(s, len, "ète")) {
      len--;
      s[len-2] = 'e';
    }
    
    if (len > 8 && endsWith(s, len, "ique"))
      len -= 4;
    
    if (len > 8 && endsWith(s, len, "esse"))
      return norm(s, len-3);
    
    if (len > 7 && endsWith(s, len, "inage"))
      return norm(s, len-3);
    
    if (len > 9 && endsWith(s, len, "isation")) {
      len -= 7;
      if (len > 5 && endsWith(s, len, "ual"))
        s[len-2] = 'e';
      return norm(s, len);
    }
    
    if (len > 9 && endsWith(s, len, "isateur"))
      return norm(s, len-7);
    
    if (len > 8 && endsWith(s, len, "ation"))
      return norm(s, len-5);

    if (len > 8 && endsWith(s, len, "ition"))
      return norm(s, len-5);
    
    return norm(s, len);
  }

  private int norm(char s[], int len) {
    if (len > 4) {
      for (int i = 0; i < len; i++)
        switch(s[i]) {
          case 'à': 
          case 'á':
          case 'â': s[i] = 'a'; break;
          case 'ô': s[i] = 'o'; break;
          case 'è':
          case 'é':
          case 'ê': s[i] = 'e'; break;
          case 'ù':
          case 'û': s[i] = 'u'; break;
          case 'î': s[i] = 'i'; break;
          case 'ç': s[i] = 'c'; break;
        }
      
      char ch = s[0];
      for (int i = 1; i < len; i++) {
        if (s[i] == ch && Character.isLetter(ch))
          len = delete(s, i--, len);
        else
          ch = s[i];
      }
    }
    
    if (len > 4 && endsWith(s, len, "ie"))
      len -= 2;
    
    if (len > 4) {
        if (s[len-1] == 'r') len--;
        if (s[len-1] == 'e') len--;
        if (s[len-1] == 'e') len--;
        if (s[len-1] == s[len-2] && Character.isLetter(s[len-1])) len--;
    }
    return len;
  }
}
