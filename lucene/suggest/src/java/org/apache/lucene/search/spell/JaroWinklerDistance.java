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

import java.util.Arrays;

/**
 * Similarity measure for short strings such as person names.
 * @see <a
 *     href="http://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance">http://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance</a>
 */
public class JaroWinklerDistance implements StringDistance {

  private float threshold = 0.7f;
  
  /**
   * Creates a new distance metric with the default threshold
   * for the Jaro Winkler bonus (0.7)
   * @see #setThreshold(float)
   */
  public JaroWinklerDistance() {}

  private int[] matches(String s1, String s2) {
    String max, min;
    if (s1.length() > s2.length()) {
      max = s1;
      min = s2;
    } else {
      max = s2;
      min = s1;
    }
    int range = Math.max(max.length() / 2 - 1, 0);
    int[] matchIndexes = new int[min.length()];
    Arrays.fill(matchIndexes, -1);
    boolean[] matchFlags = new boolean[max.length()];
    int matches = 0;
    for (int mi = 0; mi < min.length(); mi++) {
      char c1 = min.charAt(mi);
      for (int xi = Math.max(mi - range, 0), xn = Math.min(mi + range + 1, max
          .length()); xi < xn; xi++) {
        if (!matchFlags[xi] && c1 == max.charAt(xi)) {
          matchIndexes[mi] = xi;
          matchFlags[xi] = true;
          matches++;
          break;
        }
      }
    }
    char[] ms1 = new char[matches];
    char[] ms2 = new char[matches];
    for (int i = 0, si = 0; i < min.length(); i++) {
      if (matchIndexes[i] != -1) {
        ms1[si] = min.charAt(i);
        si++;
      }
    }
    for (int i = 0, si = 0; i < max.length(); i++) {
      if (matchFlags[i]) {
        ms2[si] = max.charAt(i);
        si++;
      }
    }
    int transpositions = 0;
    for (int mi = 0; mi < ms1.length; mi++) {
      if (ms1[mi] != ms2[mi]) {
        transpositions++;
      }
    }
    int prefix = 0;
    for (int mi = 0; mi < min.length(); mi++) {
      if (s1.charAt(mi) == s2.charAt(mi)) {
        prefix++;
      } else {
        break;
      }
    }
    return new int[] { matches, transpositions / 2, prefix, max.length() };
  }

  @Override
  public float getDistance(String s1, String s2) {
    int[] mtp = matches(s1, s2);
    float m = mtp[0];
    if (m == 0) {
      return 0f;
    }
    float j = ((m / s1.length() + m / s2.length() + (m - mtp[1]) / m)) / 3;
    float jw = j < getThreshold() ? j : j + Math.min(0.1f, 1f / mtp[3]) * mtp[2]
        * (1 - j);
    return jw;
  }

  /**
   * Sets the threshold used to determine when Winkler bonus should be used.
   * Set to a negative value to get the Jaro distance.
   * @param threshold the new value of the threshold
   */
  public void setThreshold(float threshold) {
    this.threshold = threshold;
  }

  /**
   * Returns the current value of the threshold used for adding the Winkler bonus.
   * The default value is 0.7.
   * @return the current value of the threshold
   */
  public float getThreshold() {
    return threshold;
  }

  @Override
  public int hashCode() {
    return 113 * Float.floatToIntBits(threshold) * getClass().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (null == obj || getClass() != obj.getClass()) return false;
    
    JaroWinklerDistance o = (JaroWinklerDistance)obj;
    return (Float.floatToIntBits(o.threshold) 
            == Float.floatToIntBits(this.threshold));
  }

  @Override
  public String toString() {
    return "jarowinkler(" + threshold + ")";
  }

}
