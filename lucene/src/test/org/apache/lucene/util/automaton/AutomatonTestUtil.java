package org.apache.lucene.util.automaton;

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

import java.util.Random;

import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;

public class AutomatonTestUtil {
  /** Returns random string, including full unicode range. */
  public static RegExp randomRegexp(Random r) {
    while (true) {
      String regexp = randomRegexpString(r);
      // we will also generate some undefined unicode queries
      if (!UnicodeUtil.validUTF16String(regexp))
        continue;
      try {
        return new RegExp(regexp, RegExp.NONE);
      } catch (Exception e) {}
    }
  }

  private static String randomRegexpString(Random r) {
    final int end = r.nextInt(20);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      int t = r.nextInt(11);
      if (0 == t && i < end - 1) {
        // Make a surrogate pair
        // High surrogate
        buffer[i++] = (char) _TestUtil.nextInt(r, 0xd800, 0xdbff);
        // Low surrogate
        buffer[i] = (char) _TestUtil.nextInt(r, 0xdc00, 0xdfff);
      }
      else if (t <= 1) buffer[i] = (char) r.nextInt(0x80);
      else if (2 == t) buffer[i] = (char) _TestUtil.nextInt(r, 0x80, 0x800);
      else if (3 == t) buffer[i] = (char) _TestUtil.nextInt(r, 0x800, 0xd7ff);
      else if (4 == t) buffer[i] = (char) _TestUtil.nextInt(r, 0xe000, 0xffff);
      else if (5 == t) buffer[i] = '.';
      else if (6 == t) buffer[i] = '?';
      else if (7 == t) buffer[i] = '*';
      else if (8 == t) buffer[i] = '+';
      else if (9 == t) buffer[i] = '(';
      else if (10 == t) buffer[i] = ')';
    }
    return new String(buffer, 0, end);
  }
}
