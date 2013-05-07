package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

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

class StackedDocsEnum extends DocsAndPositionsEnum {
  
  /**
   * A list containing the active enumerations, ordered from.
   */
  final private LinkedList<DocsEnumWithIndex> active;
  
  /**
   * A queue containing non-active enums, ordered by doc ID.
   */
  final private DocsEnumDocIdPriorityQueue queueByDocId;
  
  /**
   * A queue for ordering active enums by decreasing enum index.
   */
  final private DocsEnumIndexPriorityQueue queueByIndex;
  
  /**
   * Field generation replacements for the enclosing field.
   */
  final private FieldGenerationReplacements replacements;
  
  /**
   * Current doc ID.
   */
  private int docId;
  
  /**
   * An iterator over active enums (for positions).
   */
  private Iterator<DocsEnumWithIndex> activeIterator;
  
  /**
   * Current positions enum.
   */
  private DocsEnumWithIndex positionsEnum;
  
  /**
   * Number of positions left in positionsEnum.
   */
  private int positionsLeft;
  
  private static final FieldGenerationReplacements NO_REPLACEMENTS = new FieldGenerationReplacements();
  private static final int STACKED_SEGMENT_POSITION_INCREMENT = 50000;
  
  public StackedDocsEnum(Map<DocsEnum,Integer> activeMap,
      FieldGenerationReplacements replacements) {
    active = new LinkedList<StackedDocsEnum.DocsEnumWithIndex>();
    for (DocsEnum docsEnum : activeMap.keySet()) {
      active.add(new DocsEnumWithIndex(docsEnum, activeMap.get(docsEnum)));
    }
    
    queueByDocId = new DocsEnumDocIdPriorityQueue(activeMap.size());
    queueByIndex = new DocsEnumIndexPriorityQueue(activeMap.size());
    if (replacements == null) {
      this.replacements = NO_REPLACEMENTS;
    } else {
      this.replacements = replacements;
    }
  }
  
  @Override
  public int nextDoc() throws IOException {
    // advance all enums that were active in last docId, and put in queue
    for (DocsEnumWithIndex docsEnum : active) {
      if (docsEnum.docsEnum.nextDoc() != NO_MORE_DOCS) {
        queueByDocId.add(docsEnum);
      }
    }
    active.clear();
    
    actualNextDoc();
    return docId;
  }
  
  @Override
  public int advance(int target) throws IOException {
    // advance all enums, and put in queue
    for (DocsEnumWithIndex docsEnum : active) {
      if (docsEnum.docsEnum.advance(target) != NO_MORE_DOCS) {
        queueByDocId.add(docsEnum);
      }
    }
    active.clear();
    
    actualNextDoc();
    return docId;
  }
  
  private void actualNextDoc() throws IOException {
    positionsEnum = null;
    while (queueByDocId.size() > 0) {
      // put all enums with minimal docId in active list
      docId = queueByDocId.top().docsEnum.docID();
      while (queueByDocId.size() > 0
          && docId == queueByDocId.top().docsEnum.docID()) {
        queueByIndex.add(queueByDocId.pop());
      }
      
      // make sure non-replaced fields exist
      while (queueByIndex.size() > 0
          && queueByIndex.top().index >= replacements.get(docId)) {
        active.addFirst(queueByIndex.pop());
      }
      // put replaced fields back in the queue
      while (queueByIndex.size() > 0) {
        DocsEnumWithIndex docsEnum = queueByIndex.pop();
        if (docsEnum.docsEnum.nextDoc() != NO_MORE_DOCS) {
          queueByDocId.add(docsEnum);
        }
      }
      if (!active.isEmpty()) {
        return;
      }
    }
    
    docId = NO_MORE_DOCS;
  }
  
  @Override
  public int docID() {
    return docId;
  }
  
  @Override
  public int freq() throws IOException {
    int freq = 0;
    for (DocsEnumWithIndex docsEnum : active) {
      freq += docsEnum.docsEnum.freq();
    }
    return freq;
  }
  
  @Override
  public int nextPosition() throws IOException {
    if (positionsEnum == null) {
      if (active.size() == 1) {
        activeIterator = active.iterator();
      } else {
        ArrayList<DocsEnumWithIndex> tempList = new ArrayList<>(active);
        Collections.sort(tempList);
        activeIterator = tempList.iterator();
      }
      positionsLeft = 0;
    }
    
    if (positionsLeft == 0) {
      positionsEnum = activeIterator.next();
      positionsLeft = positionsEnum.docsEnum.freq();
    }
    
    positionsLeft--;
    int pos = positionsEnum.index * STACKED_SEGMENT_POSITION_INCREMENT
        + ((DocsAndPositionsEnum) positionsEnum.docsEnum).nextPosition();
    return pos;
  }
  
  @Override
  public int startOffset() throws IOException {
    return ((DocsAndPositionsEnum) positionsEnum.docsEnum).startOffset();
  }
  
  @Override
  public int endOffset() throws IOException {
    return ((DocsAndPositionsEnum) positionsEnum.docsEnum).endOffset();
  }
  
  @Override
  public BytesRef getPayload() throws IOException {
    return ((DocsAndPositionsEnum) positionsEnum.docsEnum).getPayload();
  }
  
  @Override
  public long cost() {
    long cost = 0;
    for (DocsEnumWithIndex docsEnum : active) {
      cost += docsEnum.docsEnum.cost();
    }
    return cost;
  }

  protected class DocsEnumWithIndex implements Comparable<DocsEnumWithIndex> {
    
    DocsEnum docsEnum;
    int index;
    
    public DocsEnumWithIndex(DocsEnum docsEnum, int index) {
      this.docsEnum = docsEnum;
      this.index = index;
    }

    @Override
    public int compareTo(DocsEnumWithIndex other) {
      return this.index - other.index;
    }
    
  }
  
  private class DocsEnumDocIdPriorityQueue extends
      PriorityQueue<DocsEnumWithIndex> {
    
    public DocsEnumDocIdPriorityQueue(int maxSize) {
      super(maxSize);
    }
    
    @Override
    protected boolean lessThan(DocsEnumWithIndex a, DocsEnumWithIndex b) {
      return a.docsEnum.docID() < b.docsEnum.docID();
    }
    
  }
  
  private class DocsEnumIndexPriorityQueue extends
      PriorityQueue<DocsEnumWithIndex> {
    
    public DocsEnumIndexPriorityQueue(int maxSize) {
      super(maxSize);
    }
    
    @Override
    protected boolean lessThan(DocsEnumWithIndex a, DocsEnumWithIndex b) {
      // bigger index should be first
      return a.index > b.index;
    }
    
  }
  
}
