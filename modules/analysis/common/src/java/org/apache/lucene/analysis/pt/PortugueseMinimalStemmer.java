package org.apache.lucene.analysis.pt;

import java.util.Arrays;

import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
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

/**
 * Minimal Stemmer for Portuguese
 * <p>
 * This follows the "RSLP-S" algorithm presented in:
 * <i>A study on the Use of Stemming for Monolingual Ad-Hoc Portuguese
 * Information Retrieval</i> (Orengo, et al)
 * which is just the plural reduction step of the RSLP
 * algorithm from <i>A Stemming Algorithmm for the Portuguese Language</i>,
 * Orengo et al.
 */
public class PortugueseMinimalStemmer {
  
  private static final CharArraySet excIS = new CharArraySet(Version.LUCENE_31,
      Arrays.asList("lápis", "cais", "mais", "crúcis", "biquínis", "pois", 
          "depois","dois","leis"),
      false);
  
  private static final CharArraySet excS = new CharArraySet(Version.LUCENE_31,
      Arrays.asList("aliás", "pires", "lápis", "cais", "mais", "mas", "menos",
          "férias", "fezes", "pêsames", "crúcis", "gás", "atrás", "moisés",
          "através", "convés", "ês", "país", "após", "ambas", "ambos",
          "messias", "depois"), 
      false);
  
  public int stem(char s[], int len) {
    if (len < 3 || s[len-1] != 's')
      return len;
    
    if (s[len-2] == 'n') {
      len--;
      s[len-1] = 'm';
      return len;
    }
    
    if (len >= 6 && s[len-3] == 'õ' && s[len-2] == 'e') {
      len--;
      s[len-2] = 'ã';
      s[len-1] = 'o';
      return len;
    }
      
    if (len >= 4 && s[len-3] == 'ã' && s[len-2] == 'e')
      if (!(len == 4 && s[0] == 'm')) {
        len--;
        s[len-1] = 'o';
        return len;
      }
    
    if (len >= 4 && s[len-2] == 'i') {
      if (s[len-3] == 'a')
        if (!(len == 4 && (s[0] == 'c' || s[0] == 'm'))) {
          len--;
          s[len-1] = 'l';
          return len;
        }
   
      if (len >= 5 && s[len-3] == 'é') {
        len--;
        s[len-2] = 'e';
        s[len-1] = 'l';
        return len;
      }
    
      if (len >= 5 && s[len-3] == 'e') {
        len--;
        s[len-1] = 'l';
        return len;
      }
    
      if (len >= 5 && s[len-3] == 'ó') {
        len--;
        s[len-2] = 'o';
        s[len-1] = 'l';
        return len;
      }
  
      if (!excIS.contains(s, 0, len)) {
        s[len-1] = 'l';
        return len;
      }
    }
    
    if (len >= 6 && s[len-3] == 'l' && s[len-2] == 'e')
      return len - 2;
    
    if (len >= 6 && s[len-3] == 'r' && s[len-2] == 'e')
      if (!(len == 7 && s[0] == 'á' && s[1] == 'r' && s[2] == 'v' && s[3] == 'o'))
        return len - 2;
      
    if (excS.contains(s, 0, len))
      return len;
    else
      return len-1;
  }
}
