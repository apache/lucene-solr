package org.apache.lucene.benchmark.stats;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
 * This class holds a set of memory usage values.
 *
 */
public class MemUsage {
  public long maxFree, minFree, avgFree;

  public long maxTotal, minTotal, avgTotal;

  public String toString() {
    return toScaledString(1, "B");
  }

  /** Scale down the values by divisor, append the unit string. */
  public String toScaledString(int div, String unit) {
    StringBuffer sb = new StringBuffer();
      sb.append("free=").append(minFree / div);
      sb.append("/").append(avgFree / div);
      sb.append("/").append(maxFree / div).append(" ").append(unit);
      sb.append(", total=").append(minTotal / div);
      sb.append("/").append(avgTotal / div);
      sb.append("/").append(maxTotal / div).append(" ").append(unit);
    return sb.toString();
  }
}
