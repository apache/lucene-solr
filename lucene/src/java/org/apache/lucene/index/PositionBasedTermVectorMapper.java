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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For each Field, store position by position information.  It ignores frequency information
 * <p/>
 * This is not thread-safe.
 */
public class PositionBasedTermVectorMapper extends TermVectorMapper{
  private Map<String, Map<Integer,TVPositionInfo>> fieldToTerms;

  private String currentField;
  /**
   * A Map of Integer and TVPositionInfo
   */
  private Map<Integer,TVPositionInfo> currentPositions;
  private boolean storeOffsets;

  


  /**
   *
   *
   */
  public PositionBasedTermVectorMapper() {
    super(false, false);
  }

  public PositionBasedTermVectorMapper(boolean ignoringOffsets)
  {
    super(false, ignoringOffsets);
  }

  /**
   * Never ignores positions.  This mapper doesn't make much sense unless there are positions
   * @return false
   */
  @Override
  public boolean isIgnoringPositions() {
    return false;
  }

  /**
   * Callback for the TermVectorReader. 
   * @param term
   * @param frequency
   * @param offsets
   * @param positions
   */
  @Override
  public void map(String term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
    for (int i = 0; i < positions.length; i++) {
      Integer posVal = Integer.valueOf(positions[i]);
      TVPositionInfo pos = currentPositions.get(posVal);
      if (pos == null) {
        pos = new TVPositionInfo(positions[i], storeOffsets);
        currentPositions.put(posVal, pos);
      }
      pos.addTerm(term, offsets != null ? offsets[i] : null);
    }
  }

  /**
   * Callback mechanism used by the TermVectorReader
   * @param field  The field being read
   * @param numTerms The number of terms in the vector
   * @param storeOffsets Whether offsets are available
   * @param storePositions Whether positions are available
   */
  @Override
  public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
    if (storePositions == false)
    {
      throw new RuntimeException("You must store positions in order to use this Mapper");
    }
    if (storeOffsets == true)
    {
      //ignoring offsets
    }
    fieldToTerms = new HashMap<String,Map<Integer,TVPositionInfo>>(numTerms);
    this.storeOffsets = storeOffsets;
    currentField = field;
    currentPositions = new HashMap<Integer,TVPositionInfo>();
    fieldToTerms.put(currentField, currentPositions);
  }

  /**
   * Get the mapping between fields and terms, sorted by the comparator
   *
   * @return A map between field names and a Map.  The sub-Map key is the position as the integer, the value is {@link org.apache.lucene.index.PositionBasedTermVectorMapper.TVPositionInfo}.
   */
  public Map<String,Map<Integer,TVPositionInfo>>  getFieldToTerms() {
    return fieldToTerms;
  }

  /**
   * Container for a term at a position
   */
  public static class TVPositionInfo{
    private int position;

    private List<String> terms;

    private List<TermVectorOffsetInfo> offsets;


    public TVPositionInfo(int position, boolean storeOffsets) {
      this.position = position;
      terms = new ArrayList<String>();
      if (storeOffsets) {
        offsets = new ArrayList<TermVectorOffsetInfo>();
      }
    }

    void addTerm(String term, TermVectorOffsetInfo info)
    {
      terms.add(term);
      if (offsets != null) {
        offsets.add(info);
      }
    }

    /**
     *
     * @return The position of the term
     */
    public int getPosition() {
      return position;
    }

    /**
     * Note, there may be multiple terms at the same position
     * @return A List of Strings
     */
    public List<String> getTerms() {
      return terms;
    }

    /**
     * Parallel list (to {@link #getTerms()}) of TermVectorOffsetInfo objects.  There may be multiple entries since there may be multiple terms at a position
     * @return A List of TermVectorOffsetInfo objects, if offsets are stored.
     */
    public List<TermVectorOffsetInfo> getOffsets() {
      return offsets;
    }
  }


}
