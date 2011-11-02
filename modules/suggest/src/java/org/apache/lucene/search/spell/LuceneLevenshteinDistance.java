package org.apache.lucene.search.spell;

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

import org.apache.lucene.util.IntsRef;

/**
 *  Levenshtein implemented in a consistent way as Lucene's FuzzyTermsEnum.
 *  
 *  Note also that this metric differs in subtle ways from {@link LevensteinDistance}:
 *  <ul>
 *    <li> This metric treats full unicode codepoints as characters, but
 *         LevenshteinDistance calculates based on UTF-16 code units.
 *    <li> This metric scales raw edit distances into a floating point score
 *         differently than LevenshteinDistance: the scaling is based upon the
 *         shortest of the two terms instead of the longest.
 *  </ul>
 */
public final class LuceneLevenshteinDistance implements StringDistance {

  @Override
  public float getDistance(String target, String other) {
    IntsRef targetPoints;
    IntsRef otherPoints;
    int n;
    int p[]; //'previous' cost array, horizontally
    int d[]; // cost array, horizontally
    int _d[]; //placeholder to assist in swapping p and d
    
    // cheaper to do this up front once
    targetPoints = toIntsRef(target);
    otherPoints = toIntsRef(other);
    n = targetPoints.length;
    p = new int[n+1]; 
    d = new int[n+1]; 
    
    final int m = otherPoints.length;
    if (n == 0 || m == 0) {
      if (n == m) {
        return 1;
      }
      else {
        return 0;
      }
    } 


    // indexes into strings s and t
    int i; // iterates through s
    int j; // iterates through t

    int t_j; // jth character of t

    int cost; // cost

    for (i = 0; i <= n; i++) {
      p[i] = i;
    }

    for (j = 1; j <= m; j++) {
      t_j = otherPoints.ints[j - 1];
      d[0] = j;

      for (i=1; i <= n; i++) {
        cost = targetPoints.ints[i - 1] == t_j ? 0 : 1;
        // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
        d[i] = Math.min(Math.min(d[i - 1] + 1, p[i] + 1),  p[i - 1] + cost);
      }

      // copy current distance counts to 'previous row' distance counts
      _d = p;
       p = d;
       d = _d;
    }

    // our last action in the above loop was to switch d and p, so p now
    // actually has the most recent cost counts
    return 1.0f - ((float) p[n] / Math.min(m, n));
  }
  
  private static IntsRef toIntsRef(String s) {
    IntsRef ref = new IntsRef(s.length()); // worst case
    int utf16Len = s.length();
    for (int i = 0, cp = 0; i < utf16Len; i += Character.charCount(cp)) {
      cp = ref.ints[ref.length++] = Character.codePointAt(s, i);
    }
    return ref;
  }
}
