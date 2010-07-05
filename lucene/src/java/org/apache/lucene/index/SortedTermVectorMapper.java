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

import java.util.*;

import org.apache.lucene.util.BytesRef;

/**
 * Store a sorted collection of {@link org.apache.lucene.index.TermVectorEntry}s.  Collects all term information
 * into a single, SortedSet.
 * <br/>
 * NOTE: This Mapper ignores all Field information for the Document.  This means that if you are using offset/positions you will not
 * know what Fields they correlate with.
 *  <br/>
 * This is not thread-safe  
 */
public class SortedTermVectorMapper extends TermVectorMapper{


  private SortedSet<TermVectorEntry> currentSet;
  private Map<BytesRef,TermVectorEntry> termToTVE = new HashMap<BytesRef,TermVectorEntry>();
  private boolean storeOffsets;
  private boolean storePositions;
  /**
   * Stand-in name for the field in {@link TermVectorEntry}.
   */
  public static final String ALL = "_ALL_";

  /**
   *
   * @param comparator A Comparator for sorting {@link TermVectorEntry}s
   */
  public SortedTermVectorMapper(Comparator<TermVectorEntry> comparator) {
    this(false, false, comparator);
  }


  public SortedTermVectorMapper(boolean ignoringPositions, boolean ignoringOffsets, Comparator<TermVectorEntry> comparator) {
    super(ignoringPositions, ignoringOffsets);
    currentSet = new TreeSet<TermVectorEntry>(comparator);
  }

  /**
   *
   * @param term The term to map
   * @param frequency The frequency of the term
   * @param offsets Offset information, may be null
   * @param positions Position information, may be null
   */
  //We need to combine any previous mentions of the term
  @Override
  public void map(BytesRef term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
    TermVectorEntry entry =  termToTVE.get(term);
    if (entry == null) {
      entry = new TermVectorEntry(ALL, term, frequency, 
              storeOffsets == true ? offsets : null,
              storePositions == true ? positions : null);
      termToTVE.put(term, entry);
      currentSet.add(entry);
    } else {
      entry.setFrequency(entry.getFrequency() + frequency);
      if (storeOffsets)
      {
        TermVectorOffsetInfo [] existingOffsets = entry.getOffsets();
        //A few diff. cases here:  offsets is null, existing offsets is null, both are null, same for positions
        if (existingOffsets != null && offsets != null && offsets.length > 0)
        {
          //copy over the existing offsets
          TermVectorOffsetInfo [] newOffsets = new TermVectorOffsetInfo[existingOffsets.length + offsets.length];
          System.arraycopy(existingOffsets, 0, newOffsets, 0, existingOffsets.length);
          System.arraycopy(offsets, 0, newOffsets, existingOffsets.length, offsets.length);
          entry.setOffsets(newOffsets);
        }
        else if (existingOffsets == null && offsets != null && offsets.length > 0)
        {
          entry.setOffsets(offsets);
        }
        //else leave it alone
      }
      if (storePositions)
      {
        int [] existingPositions = entry.getPositions();
        if (existingPositions != null && positions != null && positions.length > 0)
        {
          int [] newPositions = new int[existingPositions.length + positions.length];
          System.arraycopy(existingPositions, 0, newPositions, 0, existingPositions.length);
          System.arraycopy(positions, 0, newPositions, existingPositions.length, positions.length);
          entry.setPositions(newPositions);
        }
        else if (existingPositions == null && positions != null && positions.length > 0)
        {
          entry.setPositions(positions);
        }
      }
    }


  }

  @Override
  public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {

    this.storeOffsets = storeOffsets;
    this.storePositions = storePositions;
  }

  /**
   * The TermVectorEntrySet.  A SortedSet of {@link TermVectorEntry} objects.  Sort is by the comparator passed into the constructor.
   *<br/>
   * This set will be empty until after the mapping process takes place.
   *
   * @return The SortedSet of {@link TermVectorEntry}.
   */
  public SortedSet<TermVectorEntry> getTermVectorEntrySet()
  {
    return currentSet;
  }

}
