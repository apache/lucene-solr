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

package org.apache.lucene.analysis.es;

/**
 * Minimal plural stemmer for Spanish.
 * <p>
 * This stemmer implements the "plurals" stemmer for
 * spanish lanugage.
 *
 */
public class SpanishMinimalStemmer {

  public int stem(char s[], int len) {
    if (len < 4 || s[len-1] != 's')
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
        case 'ñ': s[i] = 'n'; break;
      }

    switch(s[len-1]) {
      case 's':
        if (s[len-2] == 'a' || s[len-2] == 'o') {
          return len-1;
        }
        if (s[len-2] == 'e') {
          if (s[len-3] == 's' && s[len-4] == 'e') {
            return len-2;
          }
          if (s[len-3] == 'c') {
            s[len-3] = 'z';
            return len-2;
          } else {
            return len-2;
          }
        } else {
          return len-1;
        }
    }

    return len;
  }
}
