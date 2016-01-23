package org.apache.lucene.index;
/**
 * Copyright 2007 The Apache Software Foundation
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


import java.util.Comparator;

/**
 * Compares {@link org.apache.lucene.index.TermVectorEntry}s first by frequency and then by
 * the term (case-sensitive)
 *
 **/
public class TermVectorEntryFreqSortedComparator implements Comparator<TermVectorEntry> {
  public int compare(TermVectorEntry entry, TermVectorEntry entry1) {
    int result = 0;
    result = entry1.getFrequency() - entry.getFrequency();
    if (result == 0)
    {
      result = entry.getTerm().compareTo(entry1.getTerm());
      if (result == 0)
      {
        result = entry.getField().compareTo(entry1.getField());
      }
    }
    return result;
  }
}
