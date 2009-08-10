package org.apache.lucene.analysis.fa;

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
 * Normalizer for Persian.
 * <p>
 * Normalization is done in-place for efficiency, operating on a termbuffer.
 * <p>
 * Normalization is defined as:
 * <ul>
 * <li>Normalization of various heh + hamza forms and heh goal to heh.
 * <li>Normalization of farsi yeh and yeh barree to arabic yeh
 * <li>Normalization of persian keheh to arabic kaf
 * </ul>
 * 
 */
public class PersianNormalizer {
  public static final char YEH = '\u064A';

  public static final char FARSI_YEH = '\u06CC';

  public static final char YEH_BARREE = '\u06D2';

  public static final char KEHEH = '\u06A9';

  public static final char KAF = '\u0643';

  public static final char HAMZA_ABOVE = '\u0654';

  public static final char HEH_YEH = '\u06C0';

  public static final char HEH_GOAL = '\u06C1';

  public static final char HEH = '\u0647';

  /**
   * Normalize an input buffer of Persian text
   * 
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char s[], int len) {

    for (int i = 0; i < len; i++) {
      if (s[i] == FARSI_YEH || s[i] == YEH_BARREE)
        s[i] = YEH;

      if (s[i] == KEHEH)
        s[i] = KAF;

      if (s[i] == HEH_YEH || s[i] == HEH_GOAL)
        s[i] = HEH;

      if (s[i] == HAMZA_ABOVE) { // necessary for HEH + HAMZA
        len = delete(s, i, len);
        i--;
      }
    }

    return len;
  }

  /**
   * Delete a character in-place
   * 
   * @param s Input Buffer
   * @param pos Position of character to delete
   * @param len length of input buffer
   * @return length of input buffer after deletion
   */
  protected int delete(char s[], int pos, int len) {
    if (pos < len)
      System.arraycopy(s, pos + 1, s, pos, len - pos - 1);

    return len - 1;
  }

}
