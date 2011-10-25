package org.apache.lucene.search;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 *  The results of a SpanQueryFilter.  Wraps the BitSet and the position information from the SpanQuery
 *
 * @lucene.experimental 
 *
 **/
public class SpanFilterResult {
  private DocIdSet docIdSet;
  private List<PositionInfo> positions;//Spans spans;
  
  public static final SpanFilterResult EMPTY_SPAN_FILTER_RESULT =
    new SpanFilterResult(DocIdSet.EMPTY_DOCIDSET, Collections.<PositionInfo>emptyList());
  
  /**
  *
  * @param docIdSet The DocIdSet for the Filter
  * @param positions A List of {@link org.apache.lucene.search.SpanFilterResult.PositionInfo} objects
  */
  public SpanFilterResult(DocIdSet docIdSet, List<PositionInfo> positions) {
    this.docIdSet = docIdSet;
    this.positions = positions;
  }
  
  /**
   * The first entry in the array corresponds to the first "on" bit.
   * Entries are increasing by document order
   * @return A List of PositionInfo objects
   */
  public List<PositionInfo> getPositions() {
    return positions;
  }

  /** Returns the docIdSet */
  public DocIdSet getDocIdSet() {
    return docIdSet;
  }

  public static class PositionInfo {
    private int doc;
    private List<StartEnd> positions;


    public PositionInfo(int doc) {
      this.doc = doc;
      positions = new ArrayList<StartEnd>();
    }

    public void addPosition(int start, int end)
    {
      positions.add(new StartEnd(start, end));
    }

    public int getDoc() {
      return doc;
    }

    /**
     *
     * @return Positions
     */
    public List<StartEnd> getPositions() {
      return positions;
    }
  }

  public static class StartEnd
  {
    private int start;
    private int end;


    public StartEnd(int start, int end) {
      this.start = start;
      this.end = end;
    }

    /**
     *
     * @return The end position of this match
     */
    public int getEnd() {
      return end;
    }

    /**
     * The Start position
     * @return The start position of this match
     */
    public int getStart() {
      return start;
    }

  }
}



