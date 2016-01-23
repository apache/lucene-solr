package org.apache.lucene.analysis.sr;

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

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Normalizes Serbian Cyrillic and Latin characters to "bald" Latin.
 *
 * Cyrillic characters are first converted to Latin; then, Latin characters
 * have their diacritics removed, with the exception of đ which is converted to
 * dj.
 *
 * Note that it expects lowercased input.
 */
public final class SerbianNormalizationFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
  public SerbianNormalizationFilter(TokenStream input) {
    super(input);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      char buffer[] = termAtt.buffer();
      int length = termAtt.length();
      for (int i = 0; i < length; i++) {
        final char c = buffer[i];
        switch(c) {
        case 'а':
          buffer[i] = 'a';
          break;
        case 'б':
          buffer[i] = 'b';
          break;
        case 'в':
          buffer[i] = 'v';
          break;
        case 'г':
          buffer[i] = 'g';
          break;
        case 'д':
          buffer[i] = 'd';
          break;
        case 'ђ':
        case 'đ':
          buffer = termAtt.resizeBuffer(1+length);
          if (i < length) {
            System.arraycopy(buffer, i, buffer, i+1, (length-i));
          }
          buffer[i] = 'd';
          buffer[++i] = 'j';
          length++;
          break;
        case 'е':
          buffer[i] = 'e';
          break;
        case 'ж':
        case 'з':
        case 'ž':
          buffer[i] = 'z';
          break;
        case 'и':
          buffer[i] = 'i';
          break;
        case 'ј':
          buffer[i] = 'j';
          break;
        case 'к':
          buffer[i] = 'k';
          break;
        case 'л':
          buffer[i] = 'l';
          break;
        case 'љ':
          buffer = termAtt.resizeBuffer(1+length);
          if (i < length) {
            System.arraycopy(buffer, i, buffer, i+1, (length-i));
          }
          buffer[i] = 'l';
          buffer[++i] = 'j';
          length++;
          break;
        case 'м':
          buffer[i] = 'm';
          break;
        case 'н':
          buffer[i] = 'n';
          break;
        case 'њ':
          buffer = termAtt.resizeBuffer(1+length);
          if (i < length) {
            System.arraycopy(buffer, i, buffer, i+1, (length-i));
          }
          buffer[i] = 'n';
          buffer[++i] = 'j';
          length++;
          break;
        case 'о':
          buffer[i] = 'o';
          break;
        case 'п':
          buffer[i] = 'p';
          break;
        case 'р':
          buffer[i] = 'r';
          break;
        case 'с':
          buffer[i] = 's';
          break;
        case 'т':
          buffer[i] = 't';
          break;
        case 'ћ':
        case 'ц':
        case 'ч':
        case 'č':
        case 'ć':
          buffer[i] = 'c';
          break;
        case 'у':
          buffer[i] = 'u';
          break;
        case 'ф':
          buffer[i] = 'f';
          break;
        case 'х':
          buffer[i] = 'h';
          break;
        case 'џ':
          buffer = termAtt.resizeBuffer(1+length);
          if (i < length) {
            System.arraycopy(buffer, i, buffer, i+1, (length-i));
          }
          buffer[i] = 'd';
          buffer[++i] = 'z';
          length++;
          break;
        case 'ш':
        case 'š':
          buffer[i] = 's';
          break;
        default:
          break;
        }
      }
      termAtt.setLength(length);
      return true;
    } else {
      return false;
    }
  }
}
