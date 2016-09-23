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

package org.apache.lucene.concordance.charoffsets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * In some versions of Lucene, getSpans returned overlapping spans. This class
 * can remove the overlapping spans and will sort them if startComparator is not
 * null.
 */

public class OffsetUtil {

  /**
   * @param offsets         offsets to process
   * @param comparator      initial OffsetLengthStartComparator to use to rule out overlaps
   * @param startComparator comparator for final sort
   * @return sorted list of offsets
   */
  public static List<OffsetAttribute> removeOverlapsAndSort(
      List<OffsetAttribute> offsets, OffsetLengthStartComparator comparator,
      OffsetStartComparator startComparator) {
    if (offsets == null || offsets.size() < 2)
      return offsets;

    Collections.sort(offsets, comparator);
    Set<Integer> seen = new HashSet<>();
    List<OffsetAttribute> filtered = new ArrayList<>();
    for (OffsetAttribute offset : offsets) {
      if (!alreadySeen(offset, seen)) {
        filtered.add(offset);
        for (int i = offset.startOffset(); i < offset.endOffset(); i++) {
          seen.add(i);
        }
      }
    }
    if (startComparator != null) {
      Collections.sort(filtered, startComparator);
    }
    return filtered;
  }

  private static boolean alreadySeen(OffsetAttribute offset, Set<Integer> seen) {
    for (int i = offset.startOffset(); i <= offset.endOffset(); i++) {
      if (seen.contains(i))
        return true;
    }
    return false;
  }

}
