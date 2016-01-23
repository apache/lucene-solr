package org.apache.lucene.analysis.en;

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

/**
 * Minimal plural stemmer for English.
 * <p>
 * This stemmer implements the "S-Stemmer" from
 * <i>How Effective Is Suffixing?</i>
 * Donna Harman.
 */
public class EnglishMinimalStemmer {
  @SuppressWarnings("fallthrough")
  public int stem(char s[], int len) {
    if (len < 3 || s[len-1] != 's')
      return len;
    
    switch(s[len-2]) {
      case 'u':
      case 's': return len;
      case 'e':
        if (len > 3 && s[len-3] == 'i' && s[len-4] != 'a' && s[len-4] != 'e') {
          s[len - 3] = 'y';
          return len - 2;
        }
        if (s[len-3] == 'i' || s[len-3] == 'a' || s[len-3] == 'o' || s[len-3] == 'e')
          return len; /* intentional fallthrough */
      default: return len - 1;
    }
  }
}
