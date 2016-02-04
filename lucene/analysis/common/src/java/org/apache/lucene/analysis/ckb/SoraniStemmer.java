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
package org.apache.lucene.analysis.ckb;


import static org.apache.lucene.analysis.util.StemmerUtil.endsWith;

/**
 * Light stemmer for Sorani
 */
public class SoraniStemmer {
  
  /**
   * Stem an input buffer of Sorani text.
   * 
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int stem(char s[], int len) {
    // postposition
    if (len > 5 && endsWith(s, len, "دا")) {
      len -= 2;
    } else if (len > 4 && endsWith(s, len, "نا")) {
      len--;
    } else if (len > 6 && endsWith(s, len, "ەوە")) {
      len -= 3;
    }
    
    // possessive pronoun
    if (len > 6 && (endsWith(s, len, "مان") || endsWith(s, len, "یان") || endsWith(s, len, "تان"))) {
      len -= 3;
    }
    
    // indefinite singular ezafe
    if (len > 6 && endsWith(s, len, "ێکی")) {
      return len-3;
    } else if (len > 7 && endsWith(s, len, "یەکی")) {
      return len-4;
    }
    // indefinite singular
    if (len > 5 && endsWith(s, len, "ێک")) {
      return len-2;
    } else if (len > 6 && endsWith(s, len, "یەک")) {
      return len-3;
    }
    // definite singular
    else if (len > 6 && endsWith(s, len, "ەکە")) {
      return len-3;
    } else if (len > 5 && endsWith(s, len, "کە")) {
      return len-2;
    }
    // definite plural
    else if (len > 7 && endsWith(s, len, "ەکان")) {
      return len-4;
    } else if (len > 6 && endsWith(s, len, "کان")) {
      return len-3;
    }
    // indefinite plural ezafe
    else if (len > 7 && endsWith(s, len, "یانی")) {
      return len-4;
    } else if (len > 6 && endsWith(s, len, "انی")) {
      return len-3;
    }
    // indefinite plural
    else if (len > 6 && endsWith(s, len, "یان")) {
      return len-3;
    } else if (len > 5 && endsWith(s, len, "ان")) {
      return len-2;
    } 
    // demonstrative plural
    else if (len > 7 && endsWith(s, len, "یانە")) {
      return len-4;
    } else if (len > 6 && endsWith(s, len, "انە")) {
      return len-3;
    }
    // demonstrative singular
    else if (len > 5 && (endsWith(s, len, "ایە") || endsWith(s, len, "ەیە"))) {
      return len-2;
    } else if (len > 4 && endsWith(s, len, "ە")) {
      return len-1;
    }
    // absolute singular ezafe
    else if (len > 4 && endsWith(s, len, "ی")) {
      return len-1;
    }
    return len;
  }
}
