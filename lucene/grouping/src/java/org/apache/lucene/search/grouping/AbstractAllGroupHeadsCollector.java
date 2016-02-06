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
package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;

/**
 * This collector specializes in collecting the most relevant document (group head) for each group that match the query.
 *
 * @lucene.experimental
 */
@SuppressWarnings({"unchecked","rawtypes"})
public abstract class AbstractAllGroupHeadsCollector<GH extends AbstractAllGroupHeadsCollector.GroupHead> extends SimpleCollector {

  protected final int[] reversed;
  protected final int compIDXEnd;
  protected final TemporalResult temporalResult;

  protected AbstractAllGroupHeadsCollector(int numberOfSorts) {
    this.reversed = new int[numberOfSorts];
    this.compIDXEnd = numberOfSorts - 1;
    temporalResult = new TemporalResult();
  }

  /**
   * @param maxDoc The maxDoc of the top level {@link IndexReader}.
   * @return a {@link FixedBitSet} containing all group heads.
   */
  public FixedBitSet retrieveGroupHeads(int maxDoc) {
    FixedBitSet bitSet = new FixedBitSet(maxDoc);

    Collection<GH> groupHeads = getCollectedGroupHeads();
    for (GroupHead groupHead : groupHeads) {
      bitSet.set(groupHead.doc);
    }

    return bitSet;
  }

  /**
   * @return an int array containing all group heads. The size of the array is equal to number of collected unique groups.
   */
  public int[] retrieveGroupHeads() {
    Collection<GH> groupHeads = getCollectedGroupHeads();
    int[] docHeads = new int[groupHeads.size()];

    int i = 0;
    for (GroupHead groupHead : groupHeads) {
      docHeads[i++] = groupHead.doc;
    }

    return docHeads;
  }

  /**
   * @return the number of group heads found for a query.
   */
  public int groupHeadsSize() {
    return getCollectedGroupHeads().size();
  }

  /**
   * Returns the group head and puts it into {@link #temporalResult}.
   * If the group head wasn't encountered before then it will be added to the collected group heads.
   * <p>
   * The {@link TemporalResult#stop} property will be <code>true</code> if the group head wasn't encountered before
   * otherwise <code>false</code>.
   *
   * @param doc The document to retrieve the group head for.
   * @throws IOException If I/O related errors occur
   */
  protected abstract void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException;

  /**
   * Returns the collected group heads.
   * Subsequent calls should return the same group heads.
   *
   * @return the collected group heads
   */
  protected abstract Collection<GH> getCollectedGroupHeads();

  @Override
  public void collect(int doc) throws IOException {
    retrieveGroupHeadAndAddIfNotExist(doc);
    if (temporalResult.stop) {
      return;
    }
    GH groupHead = temporalResult.groupHead;

    // Ok now we need to check if the current doc is more relevant then current doc for this group
    for (int compIDX = 0; ; compIDX++) {
      final int c = reversed[compIDX] * groupHead.compare(compIDX, doc);
      if (c < 0) {
        // Definitely not competitive. So don't even bother to continue
        return;
      } else if (c > 0) {
        // Definitely competitive.
        break;
      } else if (compIDX == compIDXEnd) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }
    groupHead.updateDocHead(doc);
  }

  /**
   * Contains the result of group head retrieval.
   * To prevent new object creations of this class for every collect.
   */
  protected class TemporalResult {

    public GH groupHead;
    public boolean stop;

  }

  /**
   * Represents a group head. A group head is the most relevant document for a particular group.
   * The relevancy is based is usually based on the sort.
   *
   * The group head contains a group value with its associated most relevant document id.
   */
  public static abstract class GroupHead<GROUP_VALUE_TYPE> {

    public final GROUP_VALUE_TYPE groupValue;
    public int doc;

    protected GroupHead(GROUP_VALUE_TYPE groupValue, int doc) {
      this.groupValue = groupValue;
      this.doc = doc;
    }

    /**
     * Compares the specified document for a specified comparator against the current most relevant document.
     *
     * @param compIDX The comparator index of the specified comparator.
     * @param doc The specified document.
     * @return -1 if the specified document wasn't competitive against the current most relevant document, 1 if the
     *         specified document was competitive against the current most relevant document. Otherwise 0.
     * @throws IOException If I/O related errors occur
     */
    protected abstract int compare(int compIDX, int doc) throws IOException;

    /**
     * Updates the current most relevant document with the specified document.
     *
     * @param doc The specified document
     * @throws IOException If I/O related errors occur
     */
    protected abstract void updateDocHead(int doc) throws IOException;

  }

}
