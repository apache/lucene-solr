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

package org.apache.lucene.analysis;

import java.util.ArrayList;
import java.util.List;

/**
 * Base utility class for implementing a {@link CharFilter}.
 * You subclass this, and then record mappings by calling
 * {@link #addOffCorrectMap}, and then invoke the correct
 * method to correct an offset.
 *
 * <p><b>NOTE</b>: This class is not particularly efficient.
 * For example, a new class instance is created for every
 * call to {@link #addOffCorrectMap}, which is then appended
 * to a private list.
 */
public abstract class BaseCharFilter extends CharFilter {

  //private List<OffCorrectMap> pcmList;
  private List pcmList;
  
  public BaseCharFilter(CharStream in) {
    super(in);
  }

  /** Retrieve the corrected offset.  Note that this method
   *  is slow, if you correct positions far before the most
   *  recently added position, as it's a simple linear
   *  search backwards through all offset corrections added
   *  by {@link #addOffCorrectMap}. */
  protected int correct(int currentOff) {
    if (pcmList == null || pcmList.isEmpty()) {
      return currentOff;
    }
    for (int i = pcmList.size() - 1; i >= 0; i--) {
      if (currentOff >= ((OffCorrectMap) pcmList.get(i)).off) {
        return currentOff + ((OffCorrectMap) pcmList.get(i)).cumulativeDiff;
      }
    }
    return currentOff;
  }
  
  protected int getLastCumulativeDiff() {
    return pcmList == null || pcmList.isEmpty() ?
      0 : ((OffCorrectMap)pcmList.get(pcmList.size() - 1)).cumulativeDiff;
  }

  protected void addOffCorrectMap(int off, int cumulativeDiff) {
    if (pcmList == null) {
      pcmList = new ArrayList();
    }
    pcmList.add(new OffCorrectMap(off, cumulativeDiff));
  }

  static class OffCorrectMap {

    int off;
    int cumulativeDiff;

    OffCorrectMap(int off, int cumulativeDiff) {
      this.off = off;
      this.cumulativeDiff = cumulativeDiff;
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append('(');
      sb.append(off);
      sb.append(',');
      sb.append(cumulativeDiff);
      sb.append(')');
      return sb.toString();
    }
  }
}
