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
package org.apache.lucene.analysis.cn.smart.hhmm;

import org.apache.lucene.analysis.cn.smart.Utility;
import org.apache.lucene.analysis.cn.smart.WordType;

/**
 * Filters a {@link SegToken} by converting full-width latin to half-width, then lowercasing latin.
 * Additionally, all punctuation is converted into {@link Utility#COMMON_DELIMITER}
 *
 * @lucene.experimental
 */
public class SegTokenFilter {

  /**
   * Filter an input {@link SegToken}
   *
   * <p>Full-width latin will be converted to half-width, then all latin will be lowercased. All
   * punctuation is converted into {@link Utility#COMMON_DELIMITER}
   *
   * @param token input {@link SegToken}
   * @return normalized {@link SegToken}
   */
  public SegToken filter(SegToken token) {
    switch (token.wordType) {
      case WordType.FULLWIDTH_NUMBER:
      case WordType.FULLWIDTH_STRING: /* first convert full-width -> half-width */
        for (int i = 0; i < token.charArray.length; i++) {
          if (token.charArray[i] >= 0xFF10) token.charArray[i] -= 0xFEE0;

          if (token.charArray[i] >= 0x0041 && token.charArray[i] <= 0x005A) /* lowercase latin */
            token.charArray[i] += 0x0020;
        }
        break;
      case WordType.STRING:
        for (int i = 0; i < token.charArray.length; i++) {
          if (token.charArray[i] >= 0x0041 && token.charArray[i] <= 0x005A) /* lowercase latin */
            token.charArray[i] += 0x0020;
        }
        break;
      case WordType.DELIMITER: /* convert all punctuation to Utility.COMMON_DELIMITER */
        token.charArray = Utility.COMMON_DELIMITER;
        break;
      default:
        break;
    }
    return token;
  }
}
