package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldValueHitQueue.Entry;

/**
 * A {@link HitCollector} that sorts by {@link SortField} using 
 * {@link FieldComparator}s.
 *
 * <b>NOTE:</b> This API is experimental and might change in
 * incompatible ways in the next release.
 */
public final class TopFieldCollector extends MultiReaderHitCollector {

  private final FieldValueHitQueue queue;

  private final FieldComparator[] comparators;
  private FieldComparator comparator1;
  private final int numComparators;
  private int[] reverseMul;
  private int reverseMul1 = 0;

  private final int numHits;
  private int totalHits;
  private FieldValueHitQueue.Entry bottom = null;

  /** Stores the maximum score value encountered, needed for normalizing. */
  private float maxScore = Float.NEGATIVE_INFINITY;

  private boolean queueFull;

  private boolean fillFields;

  public TopFieldCollector(Sort sort, int numHits,  IndexReader[] subReaders, boolean fillFields)
      throws IOException {

    if (sort.fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    queue = new FieldValueHitQueue(sort.fields, numHits, subReaders);
    comparators = queue.getComparators(); 
    reverseMul = queue.getReverseMul(); 
    numComparators = comparators.length;

    if (numComparators == 1) {
      comparator1 = comparators[0];
      reverseMul1 = reverseMul[0];
    } else {
      comparator1 = null;
      reverseMul1 = 0;
    }
    this.numHits = numHits;
    this.fillFields = fillFields;
  }

  int currentDocBase;

  // javadoc inherited
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    final int numSlotsFull;
    if (queueFull)
      numSlotsFull = numHits;
    else
      numSlotsFull = totalHits;

    currentDocBase = docBase;

    for (int i = 0; i < numComparators; i++) {
      comparators[i].setNextReader(reader, docBase, numSlotsFull);
    }
  }

  private final void updateBottom(int doc, float score) {
    bottom.docID = currentDocBase + doc;
    bottom.score = score;
    queue.adjustTop();
    bottom = (FieldValueHitQueue.Entry) queue.top();
  }

  private final void add(int slot, int doc, float score) {
    queue.put(new FieldValueHitQueue.Entry(slot, currentDocBase+doc, score));
    bottom = (FieldValueHitQueue.Entry) queue.top();
    queueFull = totalHits == numHits;
  }     

  // javadoc inherited
  public void collect(int doc, float score) {
    if (score > 0.0f) {

      maxScore = Math.max(maxScore, score);
      totalHits++;

      // TODO: one optimization we could do is to pre-fill
      // the queue with sentinel value that guaranteed to
      // always compare lower than a real hit; this would
      // save having to check queueFull on each insert

      if (queueFull) {

        if (numComparators == 1) {
          // Common case

          // Fastmatch: return if this hit is not competitive
          final int cmp = reverseMul1 * comparator1.compareBottom(doc, score);
          if (cmp < 0) {
            // Definitely not competitive
            return;
          } else if (cmp == 0 && doc + currentDocBase > bottom.docID) {
            // Definitely not competitive
            return;
          }

          // This hit is competitive -- replace bottom
          // element in queue & adjustTop
          comparator1.copy(bottom.slot, doc, score);

          updateBottom(doc, score);

          comparator1.setBottom(bottom.slot);

        } else {

          // Fastmatch: return if this hit is not competitive
          for(int i=0;;i++) {
            final int c = reverseMul[i] * comparators[i].compareBottom(doc, score);
            if (c < 0) {
              // Definitely not competitive
              return;
            } else if (c > 0) {
              // Definitely competitive
              break;
            } else if (i == numComparators-1) {
              // This is the equals case.
              if (doc + currentDocBase > bottom.docID) {
                // Definitely not competitive
                return;
              } else {
                break;
              }
            }
          }

          // This hit is competitive -- replace bottom
          // element in queue & adjustTop
          for (int i = 0; i < numComparators; i++) {
            comparators[i].copy(bottom.slot, doc, score);
          }

          updateBottom(doc, score);

          for(int i=0;i<numComparators;i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      } else {
        // Startup transient: queue hasn't gathered numHits
        // yet

        final int slot = totalHits-1;
        // Copy hit into queue
        if (numComparators == 1) {
          // Common case
          comparator1.copy(slot, doc, score);
          add(slot, doc, score);
          if (queueFull) {
            comparator1.setBottom(bottom.slot);
          }

        } else {
          for (int i = 0; i < numComparators; i++) {
            comparators[i].copy(slot, doc, score);
          }
          add(slot, doc, score);
          if (queueFull) {
            for(int i=0;i<numComparators;i++) {
              comparators[i].setBottom(bottom.slot);
            }
          }
        }
      }
    }
  }

  // javadoc inherited
  public TopDocs topDocs() {
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    if (fillFields) {
      for (int i = queue.size() - 1; i >= 0; i--) {
        scoreDocs[i] = queue.fillFields((FieldValueHitQueue.Entry) queue.pop());
      }
    } else {
      Entry entry = (FieldValueHitQueue.Entry) queue.pop();
      for (int i = queue.size() - 1; i >= 0; i--) {
        scoreDocs[i] = new FieldDoc(entry.docID,
                                    entry.score);
      }
    }

    return new TopFieldDocs(totalHits, scoreDocs, queue.getFields(), maxScore);
  }
}
