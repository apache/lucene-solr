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

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to record results for looking up normalized terms (String) and
 * character offsets for specified tokens. Will return NULL_TERM/NULL_OFFSET if
 * a token offset was not found.
 * <p>
 * Has utility methods for safely getting the closest found token. This is
 * useful for when a concordance window ends in a stop word (no term/offset
 * info).
 */

public class RandomAccessCharOffsetContainer {

  public final static String NULL_TERM = "";
  public final static int NULL_OFFSET = -1;

  private BitSet set = new BitSet();
  private int last = -1;
  private Map<Integer, String> terms = new HashMap<Integer, String>();
  private Map<Integer, Integer> starts = new HashMap<>();
  private Map<Integer, Integer> ends = new HashMap<>();

  /**
   * @param tokenOffset     token of interest
   * @param startCharOffset start character offset within the string stored in StoredField[fieldIndex]
   * @param endCharOffset   end character offset within the string stored in StoredField[fieldIndex]
   * @param term            string term at that position
   */
  public void add(int tokenOffset, int startCharOffset,
                  int endCharOffset, String term) {
    addStart(tokenOffset, startCharOffset);
    addEnd(tokenOffset, endCharOffset);
    addTerm(tokenOffset, term);
    set.set(tokenOffset);
  }

  private void addTerm(int tokenOffset, String term) {
    if (term != null) {
      terms.put(tokenOffset, term);
    }
    last = (tokenOffset > last) ? tokenOffset : last;
  }

  private void addStart(int tokenOffset, int charOffset) {
    starts.put(tokenOffset, charOffset);
    last = (tokenOffset > last) ? tokenOffset : last;
  }

  private void addEnd(int tokenOffset, int charOffset) {
    ends.put(tokenOffset, charOffset);
    last = (tokenOffset > last) ? tokenOffset : last;
  }

  /**
   * @param tokenOffset target token
   * @return the character offset for the first character of the tokenOffset.
   * returns {@link #NULL_OFFSET} if tokenOffset wasn't found
   */
  public int getCharacterOffsetStart(int tokenOffset) {
    Integer start = starts.get(tokenOffset);
    if (start == null) {
      return NULL_OFFSET;
    }
    return start.intValue();
  }

  /**
   * @param tokenOffset target token
   * @return the character offset for the final character of the tokenOffset.
   */
  public int getCharacterOffsetEnd(int tokenOffset) {
    Integer end = ends.get(tokenOffset);
    if (end == null) {
      return NULL_OFFSET;
    }
    return end.intValue();
  }

  /**
   * @param tokenOffset tokenOffset
   * @return term stored at this tokenOffset; can return {@link #NULL_TERM}
   */
  public String getTerm(int tokenOffset) {
    String s = terms.get(tokenOffset);
    if (s == null) {
      return NULL_TERM;
    }
    return s;
  }

  /**
   * @return last/largest token offset
   */
  public int getLast() {
    return last;
  }

  /**
   * reset state
   */
  public void clear() {
    terms.clear();
    starts.clear();
    ends.clear();
    last = -1;
    set = new BitSet();
  }

  protected boolean isEmpty() {
    return set.isEmpty();
  }

  /**
   * Find the closest non-null token starting from startToken
   * and ending with stopToken (inclusive).
   *
   * @param startToken start token
   * @param stopToken end token
   * @param map map to use
   * @return closest non-null token offset to the startToken; can return
   * {@link #NULL_OFFSET} if no non-null offset was found
   */
  private int getClosestToken(int startToken, int stopToken,
                              Map<Integer, Integer> map) {

    if (startToken < 0 || stopToken < 0) {
      return NULL_OFFSET;
    }
    if (startToken == stopToken) {
      return startToken;
    }
    if (startToken < stopToken) {
      for (int i = startToken; i <= stopToken; i++) {
        Integer charOffset = map.get(i);
        if (charOffset != null && charOffset != NULL_OFFSET) {
          return i;
        }
      }
    } else if (startToken > stopToken) {
      for (int i = startToken; i >= stopToken; i--) {
        Integer charOffset = map.get(i);
        if (charOffset != null && charOffset != NULL_OFFSET) {
          return i;
        }
      }
    }
    return NULL_OFFSET;
  }

  public int getClosestCharStart(int startToken, int stopToken) {

    int i = getClosestToken(startToken, stopToken, starts);
    Integer charStart = getCharacterOffsetStart(i);
    if (charStart == null) {
      return NULL_OFFSET;
    }
    return charStart.intValue();
  }

  public int getClosestCharEnd(int startToken, int stopToken) {
    int i = getClosestToken(startToken, stopToken, ends);
    Integer charEnd = getCharacterOffsetEnd(i);
    if (charEnd == null) {
      return NULL_OFFSET;
    }
    return charEnd.intValue();
  }

  protected String getClosestTerm(int startToken, int stopToken) {
    int i = getClosestToken(startToken, stopToken, starts);
    return getTerm(i);
  }

  /*
   * return: -1 if
   
  public int getFieldIndex(int tokenOffset) {
    CharCoordinate p = starts.get(tokenOffset);
    if (p == null) {
      return NULL_OFFSET;
    }
    return p.getFieldIndex();
  }
*/
  protected String debugToString() {
    StringBuilder sb = new StringBuilder();
    for (Integer i : terms.keySet()) {
      sb.append(i + " : " + terms.get(i) + " : " + starts.get(i) + " : "
          + ends.get(i) + "\n");
    }
    return sb.toString();
  }

  protected BitSet getSet() {
    return set;
  }

  public void remove(int token) {
    if (token == last) {
      last = getClosestToken(last - 1, 0, starts);
    }
    set.clear(token);
    terms.remove(token);
    starts.remove(token);
    ends.remove(token);
  }
}
