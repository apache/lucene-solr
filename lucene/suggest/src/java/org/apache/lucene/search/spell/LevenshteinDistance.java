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
package org.apache.lucene.search.spell;

/**
 * Levenshtein edit distance class.
 */
public class LevenshteinDistance implements StringDistance {

    /**
     * Optimized to run a bit faster than the static getDistance().
     * In one benchmark times were 5.3sec using ctr vs 8.5sec w/ static method, thus 37% faster.
     */
    public LevenshteinDistance () {
    }


    //*****************************
    // Compute Levenshtein distance: see org.apache.commons.lang.StringUtils#getLevenshteinDistance(String, String)
    //*****************************
    @Override
    public final float getDistance (String target, String other) {
      char[] sa;
      int n;
      int p[]; //'previous' cost array, horizontally
      int d[]; // cost array, horizontally
      int _d[]; //placeholder to assist in swapping p and d
      
        /*
           The difference between this impl. and the previous is that, rather
           than creating and retaining a matrix of size s.length()+1 by t.length()+1,
           we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
           is the 'current working' distance array that maintains the newest distance cost
           counts as we iterate through the characters of String s.  Each time we increment
           the index of String t we are comparing, d is copied to p, the second int[].  Doing so
           allows us to retain the previous cost counts as required by the algorithm (taking
           the minimum of the cost count to the left, up one, and diagonally up and to the left
           of the current cost count being calculated).  (Note that the arrays aren't really
           copied anymore, just switched...this is clearly much better than cloning an array
           or doing a System.arraycopy() each time  through the outer loop.)

           Effectively, the difference between the two implementations is this one does not
           cause an out of memory condition when calculating the LD over two very large strings.
         */

        sa = target.toCharArray();
        n = sa.length;
        p = new int[n+1]; 
        d = new int[n+1]; 
      
        final int m = other.length();
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

        char t_j; // jth character of t

        int cost; // cost

        for (i = 0; i<=n; i++) {
            p[i] = i;
        }

        for (j = 1; j<=m; j++) {
            t_j = other.charAt(j-1);
            d[0] = j;

            for (i=1; i<=n; i++) {
                cost = sa[i-1]==t_j ? 0 : 1;
                // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                d[i] = Math.min(Math.min(d[i-1]+1, p[i]+1),  p[i-1]+cost);
            }

            // copy current distance counts to 'previous row' distance counts
            _d = p;
            p = d;
            d = _d;
        }

        // our last action in the above loop was to switch d and p, so p now
        // actually has the most recent cost counts
        return 1.0f - ((float) p[n] / Math.max(other.length(), sa.length));
    }

  @Override
  public final int hashCode() {
    return 163 * getClass().hashCode();
  }

  @Override
  public final boolean equals(Object obj) {
    if (this == obj) return true;
    if (null == obj) return false;
    return (getClass() == obj.getClass());
  }

  @Override
  public final String toString() {
    return "levensthein";
  }
}
